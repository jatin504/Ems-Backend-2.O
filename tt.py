import asyncio
from app import create_app
from modbus.main import main as modbus_main
from controller_layer import ControllerManager
from auto_configs import PlantArchitect
from model import create_database, create_application_schema, create_tables_from_json, create_tables_from_json_center
from scheduler import scheduler_manager_loop

from sse import SSEPublisher

from utils.loop_manager import start_loop
from utils.db_worker_startup import DBWorkerStartup

from license import LicenseManager, LicenseError

import logging
import os
import sys
from pathlib import Path

from runtime_paths import DataPaths

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
garbage_writer = GarbageFileWriter()

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
    
    lm.validate(required_features=["EMS"])
    print("License valid. Application starting...")

    #  Start license watchdop in background
    asyncio.create_task(license_watchdog(lm))
    
    create_database()

    # if not os.path.exists("plant_architecture"):
        # print("'plant_architecture' folder not found. Creating architecture...")
    architect = PlantArchitect(
        config_path="config/device_config.json",
        base_dir="plant_architecture",
    )
    architect.build_architecture()
    # else:
    #     print("'plant_architecture' folder found. Skipping creation.")

    create_application_schema()
    create_tables_from_json()

    # for center DB
    # create_tables_from_json_center()
    
    asyncio.create_task(
        garbage_writer.start()
    )
    
    start_loop()

    publisher = SSEPublisher("http://127.0.0.1:5179", "y74y32bmcd")
    await publisher.start()

    # Store broadcaster on app so stream route can use it
    # app.broadcaster = broadcaster

    # Initialize Controller Manager
    controller_manager = ControllerManager()

    await asyncio.gather(
        modbus_main(publisher),    # Modbus polling
        run_scheduler(),             # Scheduler
        # controller_manager.start("plant_1_pcs_controller_1"),   # Controller Manager Start
        DBWorkerStartup(),          # DB Worker Startup
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
        
        
        # Run main application loop
        asyncio.run(run_all())
    
    except LicenseError as e:
        print(f"License validation failed: {e}")
        sys.exit(1)

    except KeyboardInterrupt:
        print("Shutting down backend...")
