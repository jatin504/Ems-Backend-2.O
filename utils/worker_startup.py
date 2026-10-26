import asyncio

# from auto_configs.latest_telemetry_manager import LatestTelemetryManager
from processor.src.mappings.mapper import PostMapper
from utils.db_worker_startup import DBWorkerStartup
from utils.sampling_worker_startup import sampling_worker_startup

# Queues
from utils.sampling_queue import SAMPLING_QUEUE

async def queue_workers_startup(device_publisher, async_mapping_queue):
    # latest_telemetry_manager = LatestTelemetryManager()
    mapper = PostMapper(sse_broadcaster=device_publisher, sampling_queue=SAMPLING_QUEUE)

    # asyncio.create_task(
    # )
    await asyncio.gather(
        # latest_telemetry_manager.writer_loop(),
        DBWorkerStartup(worker_count=1),
        sampling_worker_startup(sampling_queue=SAMPLING_QUEUE, worker_count=4),
        mapper.start(batch_queue=async_mapping_queue)
    )
    # def runner():
    #     asyncio.run(sampling_worker_startup(worker_count=4))

    # t = threading.Thread(target=runner, daemon=True)
    # t.start()
