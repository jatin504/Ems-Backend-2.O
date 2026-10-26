import asyncio

from processor.src.samplings.src.sampling_manager import SamplingManager

import logging

logger = logging.getLogger(__name__)

class SamplingWorker:
    def __init__(self, sampling_queue: asyncio.Queue):
        self.sampling_manager = SamplingManager()
        self.sampling_queue = sampling_queue


    async def worker(self):
        while True:
            mapped_values = await self.sampling_queue.get()
            # logger.info(f"Got task from Sampling queue...")

            try:
                await self.sampling_manager.process_mapped_data(
                    mapped_values
                )
            except Exception as e:
                logger.exception(e)
            except asyncio.CancelledError:
                logger.info("Worker stopping...")
                raise

            finally:
                self.sampling_queue.task_done()

            # await asyncio.sleep(1)