import asyncio
import logging
from multiprocessing import Process

from auto_configs.latest_telemetry_manager import LatestTelemetryManager
from utils.latest_telemetry_queue import LATEST_TELEMETRY_QUEUE

logger = logging.getLogger(__name__)

def telemetry_runner(queue):
    manager = LatestTelemetryManager()
    asyncio.run(manager.writer_loop(queue))

def process_workers_startup():
    print("Telemetry process started")
    try:
        
        p1 = Process(target=telemetry_runner, args=(LATEST_TELEMETRY_QUEUE, ))

        p1.start()

        return [p1]
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        # logger.exception(f"Error while creating process: {e}")
        # return [None]
