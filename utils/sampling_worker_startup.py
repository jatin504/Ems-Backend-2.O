import asyncio

from processor.src.samplings.src.sampling_worker import SamplingWorker

async def sampling_worker_startup(sampling_queue : asyncio.Queue, worker_count=4):

    sampling_worker = SamplingWorker(sampling_queue=sampling_queue)

    tasks = [
        asyncio.create_task(
            sampling_worker.worker()
        )
        for _ in range(worker_count)
    ]

    await asyncio.gather(*tasks)