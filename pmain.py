import asyncio
import concurrent.futures
import multiprocessing
import threading 

from modbus.main import run_modbus_isolated_loop
from controller_layer import ControllerManager
from auto_configs import PlantArchitect
from model import create_database, create_application_schema, create_tables_from_json, create_tables_from_json_center
from scheduler import scheduler_manager_loop

from sse import SSEPublisher

from utils.db_utils import init_db
from utils.health_monitor import start_health_monitor
from utils.retry_worker import retry_worker
from utils.loop_manager import start_loop
# from utils.process_worker_startup import process_workers_startup
from utils.device_based_distribution import modbus_processor_node_helper
from utils.mapping_queue import ASYNC_MAPPING_QUEUE, MP_MAPPING_QUEUE
from utils.worker_startup import queue_workers_startup

from license import LicenseManager, LicenseError

import logging
import os
import sys
from pathlib import Path

from runtime_paths import DataPaths

from utils.redundant_db import is_redundant_enabled

print(" REDUNDANT_DB =", os.getenv("REDUNDANT_DB"))
print(" CHECK =", is_redundant_enabled())

from utils.redundant_db import is_redundant_enabled

DEVICES_COUNT = 800
MODBUS_PROCESS_NODE = 2
MODBUS_NODE_DEVICE_SUPPORT_COUNT = 500



if getattr(sys, "frozen", False):
    exe_dir = Path(sys.executable).parent
else:
    exe_dir = Path(__file__).parent

from hypercorn.config import Config
# from hypercorn.asyncio import serve

from dotenv import load_dotenv
load_dotenv(exe_dir / ".env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from processor.src.mappings.garbage_file_writer import GarbageFileWriter
# garbage_writer = GarbageFileWriter()

def _env_bool(name: str, default: bool = True) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")

if _env_bool("ENABLE_FILE_LOGS", True):
    from log_setup import augment_logging
    augment_logging(os.getenv("LOG_LEVEL", "INFO"))
else:
    logger.info("File logging disabled via .env (ENABLE_FILE_LOGS is false)")

async def run_scheduler():
    await asyncio.to_thread(scheduler_manager_loop)  # non-async scheduler

async def run_all():

    # Load Publick Key
    with open(DataPaths.resolve("license/public_key.pem"), "rb") as f:
        PUBLIC_KEY = f.read()

    # Validate License
    lm = LicenseManager(
            public_key_pem=PUBLIC_KEY, 
            license_path=DataPaths.resolve("license/license.lic")
        )
    
    # lm.validate(required_features=["EMS"])
    print("License valid. Application starting...")

    #  Start license watchdop in background
    # asyncio.create_task(license_watchdog(lm))
    
    # create_database()

    # if not os.path.exists("plant_architecture"):
        # print("'plant_architecture' folder not found. Creating architecture...")
    architect = PlantArchitect(
        config_path="config/device_config.json",
        base_dir="plant_architecture",
    )
    # architect.build_architecture()
    # else:
    #     print("'plant_architecture' folder found. Skipping creation.")

    # create_application_schema()
    # create_tables_from_json()

    #  STANDBY SCHEMA CREATE
    if is_redundant_enabled():
        print("Creating standby DB schema...")

        os.environ["DB_ROLE"] = "standby"
        # create_application_schema()

        os.environ["DB_ROLE"] = "primary"

    #  STANDBY TABLE CREATE
    if is_redundant_enabled():
        print("Creating tables in standby DB...")

        os.environ["DB_ROLE"] = "standby"
        # create_tables_from_json()

        os.environ["DB_ROLE"] = "primary"

    start_loop()

    if os.getenv("SERVER_ROLE", "").upper() == "SECONDARY":
        print("🩺 Starting Health Monitor...")
        asyncio.create_task(start_health_monitor())

    #  START RETRY WORKER
    asyncio.create_task(retry_worker())

    # for center DB
    # create_tables_from_json_center()

    garbage_writer = GarbageFileWriter()
    asyncio.create_task(
        garbage_writer.start()
    )
    
    # initialize DB
    await init_db()

    publisher = SSEPublisher("http://127.0.0.1:5179", "y74y32bmcd")
    await publisher.start()

    # 🚨 START PROCESSES HERE (NOT inside gather)
    # processes = process_workers_startup()
    # for p in processes:
    #     print(
    #         p.pid,
    #         p.is_alive(),
    #         p.exitcode
    #     )

    # This thread will safely pull from mp_queue and push to asyncio_queue
    loop = asyncio.get_running_loop()
    threading.Thread(
        target=bridge_mp_to_asyncio,
        args=(MP_MAPPING_QUEUE, ASYNC_MAPPING_QUEUE, loop),
        daemon=True
    ).start()
    logger.info("Bridge thread started.")

    processes = []

    ranges = modbus_processor_node_helper(
        DEVICES_COUNT,
        MODBUS_PROCESS_NODE,
        MODBUS_NODE_DEVICE_SUPPORT_COUNT,
    )

    for node_id, (start_device, end_device) in enumerate(ranges):
        process = multiprocessing.Process(
            target=run_modbus_isolated_loop,
            args=(
                MP_MAPPING_QUEUE,
                start_device,
                end_device,
            ),
        )

        process.start()
        processes.append(process)

        logger.info(
            f"Started Modbus Process-{node_id} "
            f"(PID={process.pid}) "
            f"Range={start_device} -> {end_device}"
        )

    # modbus_process = multiprocessing.Process(
    #     target=run_modbus_isolated_loop, 
    #     args=(MP_MAPPING_QUEUE, 0, 800,), # <--- Pass the multiprocessing queue
    #     # daemon=True
    #     # Note: We don't use daemon=True for processes, we manage shutdown manually if needed
    # )
    # modbus_process.start()
    # logger.info(f"Modbus layer started in isolated OS Process (PID: {modbus_process.pid})...")


    # Initialize Controller Manager
    controller_manager = ControllerManager()

    await asyncio.gather(
        run_scheduler(),             # Scheduler
        controller_manager.start("plant_1_pcs_controller_1"),   # Controller Manager Start
        queue_workers_startup(device_publisher=publisher, async_mapping_queue=ASYNC_MAPPING_QUEUE),          # Queue Workers Startup
    )

def bridge_mp_to_asyncio(mp_queue: multiprocessing.Queue, asyncio_queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
    """
    Runs in a dedicated background thread.
    Safely bridges the blocking multiprocessing.Queue to the non-blocking asyncio.Queue.
    """
    while True:
        try:
            task = mp_queue.get()

            def safe_put():
                try:
                    asyncio_queue.put_nowait(task)
                except asyncio.QueueFull:
                    logger.warning(
                        f"Async queue full. size={asyncio_queue.qsize()}"
                    )

            loop.call_soon_threadsafe(safe_put)
        except Exception as e:
            logger.error(f"Bridge thread error: {e}")
            break

async def license_watchdog(lm):
    while True:
        try:
            lm.validate(required_features=["EMS"])
        except LicenseError:
            print("License became invalid. Shutting down...")
            sys.exit(1)

        await asyncio.sleep(3600)  # Check every hour

if __name__ == "__main__":
    try:
        # ==========================================
        # HIGH PERFORMANCE EVENT LOOP SETUP FOR MAIN APP
        # ==========================================
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=64)
        loop.set_default_executor(executor)

        # ==========================================

        # Run main application loop using the custom loop
        loop.run_until_complete(run_all())
    
    except LicenseError as e:
        print(f"License validation failed: {e}")
        sys.exit(1)

    except KeyboardInterrupt:
        print("Shutting down backend...")
        sys.exit(1)
        # if 'modbus_process' in locals() and modbus_process.is_alive():
        #     modbus_process.terminate()
        
    finally:
        # Graceful shutdown of tasks, loop, and executor
        try:
            tasks = asyncio.all_tasks(loop)
            for task in tasks:
                task.cancel()
            if tasks:
                loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        finally:
            loop.close()
            executor.shutdown(wait=False)
            sys.exit(1)
