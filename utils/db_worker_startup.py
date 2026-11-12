import asyncio
import os

from processor.src.mappings.telemetry_garbage import TelemetryGarbageDAO
from processor.src.samplings.src.db_worker import DBWriterWorkerResolutionSampling
from processor.src.samplings.src.victoria_writer import VictoriaWriter
from processor.src.samplings.src.raw_recording import RawVictoriaWriter

# async def DBWorkerStartup(worker_count: int = 4):
#     res_worker = DBWriterWorkerResolutionSampling()
#     vm_worker = VictoriaWriter()
#     for _ in range(worker_count):
#         asyncio.create_task(res_worker.run())
#         asyncio.create_task(vm_worker.start())

    # asyncio.create_task(TelemetryGarbageDAO.garbage_worker())

async def DBWorkerStartup(worker_count):

    # res_worker = DBWriterWorkerResolutionSampling()
    vm_worker = VictoriaWriter()

    tasks = [
        # asyncio.create_task(res_worker.run())
        asyncio.create_task(vm_worker.start())
        for _ in range(worker_count)
    ]

    # One independent consumer handles raw packets; existing minute writer stays intact.
    if os.getenv("EMS_RAW_VM_ENABLED", "false").lower() == "true":
        tasks.append(asyncio.create_task(RawVictoriaWriter().start()))
    await asyncio.gather(*tasks)
