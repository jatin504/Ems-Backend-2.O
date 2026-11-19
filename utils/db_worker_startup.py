import asyncio
import os

from utils.db_write_queue import get_timeseries_backend

# async def DBWorkerStartup(worker_count: int = 4):
#     res_worker = DBWriterWorkerResolutionSampling()
#     vm_worker = VictoriaWriter()
#     for _ in range(worker_count):
#         asyncio.create_task(res_worker.run())
#         asyncio.create_task(vm_worker.start())

    # asyncio.create_task(TelemetryGarbageDAO.garbage_worker())

async def DBWorkerStartup(worker_count):

    backend = get_timeseries_backend()
    if backend == "POSTGRES":
        from processor.src.samplings.src.db_worker import DBWriterWorkerResolutionSampling
        tasks = [asyncio.create_task(DBWriterWorkerResolutionSampling().run())
                 for _ in range(worker_count)]
    else:
        from processor.src.samplings.src.victoria_writer import VictoriaWriter
        tasks = [asyncio.create_task(VictoriaWriter().start())
                 for _ in range(worker_count)]

    # One independent consumer handles raw packets; existing minute writer stays intact.
    if os.getenv("EMS_RAW_VM_ENABLED", "false").lower() == "true":
        from processor.src.samplings.src.raw_recording import RawVictoriaWriter
        tasks.append(asyncio.create_task(RawVictoriaWriter().start()))
    await asyncio.gather(*tasks)
