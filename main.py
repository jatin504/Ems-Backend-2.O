import asyncio
import concurrent.futures

from controller_layer import ControllerManager
from auto_configs import PlantArchitect
from model import create_database, create_application_schema, create_tables_from_json, create_tables_from_json_center
from runtime.run_scheduler import run_scheduler
from runtime.start_modbus import start_modbus_layer
from scheduler import scheduler_manager_loop

from sse import SSEPublisher

from utils.db_utils import init_db
from utils.health_monitor import self_health_monitor, start_health_monitor
from utils.retry_worker import retry_worker
from utils.loop_manager import start_loop
# from utils.process_worker_startup import process_workers_startup
from utils.mapping_queue import ASYNC_MAPPING_QUEUE, MP_MAPPING_QUEUE
from utils.secondary_failover_worker import secondary_failover_worker
from utils.worker_startup import queue_workers_startup

from license import LicenseManager, LicenseError

import logging
import os
import sys
from pathlib import Path

from runtime_paths import DataPaths
from runtime.environment_const import CREATE_DB_SCHEMA_TABLE, TIMESERIES_DB

from utils.redundant_db import is_redundant_enabled

print(" REDUNDANT_DB =", os.getenv("REDUNDANT_DB"))
print(" CHECK =", is_redundant_enabled())

from utils.redundant_db import is_redundant_enabled


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


async def run_all():

    # Load Publick Key
    with open(DataPaths.resolve("license/public_key.pem"), "rb") as f:
        PUBLIC_KEY = f.read()

    # Validate License
    lm = LicenseManager(
            public_key_pem=PUBLIC_KEY, 
            license_path=DataPaths.resolve("license/license.lic")
        )
    
    #lm.validate(required_features=["EMS"])
    print("License valid. Application starting...")

    #  Start license watchdop in background
    #asyncio.create_task(license_watchdog(lm))

    if CREATE_DB_SCHEMA_TABLE == "True":
        create_database()

        # if not os.path.exists("plant_architecture"):
            # print("'plant_architecture' folder not found. Creating architecture...")
        architect = PlantArchitect(
            config_path="config/device_config.json",
            base_dir="plant_architecture",
        )
        # try:
        #     architect.build_architecture()
        # except Exception as e:
        #     print(f"Error : {e}")
        # else:
        #     print("'plant_architecture' folder found. Skipping creation.")

        create_application_schema()

    if TIMESERIES_DB == "POSTGRES":
        create_tables_from_json()

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

    #  START RETRY WORKER
    # asyncio.create_task(retry_worker())

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


    server_role = os.getenv("SERVER_ROLE", "").upper()

    primary_down_event = asyncio.Event()

    if server_role == "PRIMARY":

        logger.info(
            "========================================"
        )
        logger.info(
            "🟢 SERVER ROLE: PRIMARY"
        )
        logger.info(
            "========================================"
        )

        # ==========================================
        # PRIMARY STARTS MODBUS
        # ==========================================

        processes = await start_modbus_layer(
            MP_MAPPING_QUEUE,
            ASYNC_MAPPING_QUEUE,
        )

        logger.info(
            f"PRIMARY Modbus started. "
            f"Processes={len(processes)}"
        )

        # ==========================================
        # PRIMARY APPLICATION SERVICES
        # ==========================================

        # controller_manager = ControllerManager()

        try:

            await asyncio.gather(

                self_health_monitor(),

                # run_scheduler(),

                # controller_manager.start(
                #     "plant_1_pcs_controller_1"
                # ),

                queue_workers_startup(
                    device_publisher=publisher,
                    async_mapping_queue=ASYNC_MAPPING_QUEUE,
                ),
            )

        finally:

            logger.warning(
                "Stopping PRIMARY Modbus processes..."
            )

            for process in processes:

                if process.is_alive():
                    process.terminate()

            for process in processes:

                process.join(timeout=5)

                if process.is_alive():
                    process.kill()


    elif server_role == "SECONDARY":

        logger.info(
            "========================================"
        )
        logger.info(
            "🟡 SERVER ROLE: SECONDARY"
        )
        logger.info(
            "========================================"
        )

        # ==========================================
        # SECONDARY DOES NOT START MODBUS HERE
        # ==========================================
        
        # Only health monitor + failover worker

        await asyncio.gather(

            start_health_monitor(
                primary_down_event
            ),

            secondary_failover_worker(
                MP_MAPPING_QUEUE,
                ASYNC_MAPPING_QUEUE,
                primary_down_event,
                publisher,
            ),
        )


    else:

        raise RuntimeError(
            f"Invalid SERVER_ROLE={server_role}"
        )



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
