
from scheduler.scheduler_manager import scheduler_manager_loop, stop_scheduler

import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_scheduler():

    await asyncio.to_thread(
        scheduler_manager_loop
    )

    try:

        # Keep this task alive while scheduler
        # is supposed to be active.

        while True:

            await asyncio.sleep(1)

    except asyncio.CancelledError:

        logger.info(
            "[Scheduler] Cancellation received"
        )

        await asyncio.to_thread(
            stop_scheduler
        )

        raise
