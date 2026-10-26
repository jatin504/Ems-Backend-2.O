

import asyncio
import logging

from runtime.run_scheduler import run_scheduler
from runtime.start_modbus import start_modbus_layer

from controller_layer import ControllerManager

from utils.worker_startup import queue_workers_startup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def secondary_failover_worker(
    MP_MAPPING_QUEUE,
    ASYNC_MAPPING_QUEUE,
    primary_down_event,
    publisher,
):

    logger.info(
        "Secondary failover worker started."
    )

    while True:

        # ==================================================
        # WAIT FOR PRIMARY TO GO DOWN
        # ==================================================

        await primary_down_event.wait()

        logger.warning(
            "PRIMARY DOWN. "
            "SECONDARY BECOMING ACTIVE."
        )

        # ==================================================
        # START SECONDARY SERVICES
        # ==================================================

        processes = await start_modbus_layer(
            MP_MAPPING_QUEUE,
            ASYNC_MAPPING_QUEUE,
        )

        logger.info(
            f"Secondary Modbus layer started. "
            f"Processes={len(processes)}"
        )

        controller_manager = ControllerManager()

        active_tasks = [
            run_scheduler(),
            controller_manager.start(
                "plant_1_pcs_controller_1"
            ),
            queue_workers_startup(
                device_publisher=publisher,
                async_mapping_queue=ASYNC_MAPPING_QUEUE,
            ),
        ]

        active_task_group = asyncio.gather(
            *active_tasks,
            return_exceptions=True,
        )

        # ==================================================
        # WAIT FOR PRIMARY TO RECOVER
        # ==================================================

        while primary_down_event.is_set():

            # Don't block event loop
            await asyncio.sleep(1)

        # ==================================================
        # PRIMARY RECOVERED
        # ==================================================

        logger.warning(
            "PRIMARY RECOVERED. "
            "SECONDARY BECOMING PASSIVE."
        )

        # ==================================================
        # STOP CONTROLLER / QUEUE / SCHEDULER
        # ==================================================

        active_task_group.cancel()

        try:
            await active_task_group
        except asyncio.CancelledError:
            pass

        logger.info(
            "Secondary async services stopped."
        )

        # ==================================================
        # STOP MODBUS PROCESSES
        # ==================================================

        for process in processes:

            if process.is_alive():

                logger.info(
                    f"Stopping Modbus process "
                    f"PID={process.pid}"
                )

                process.terminate()

        for process in processes:

            process.join(timeout=5)

            if process.is_alive():

                logger.warning(
                    f"Force killing Modbus process "
                    f"PID={process.pid}"
                )

                process.kill()

                process.join()

        logger.info(
            "Secondary Modbus layer stopped."
        )

        # ==================================================
        # LOOP BACK TO WAITING FOR PRIMARY FAILURE
        # ==================================================

        logger.info(
            "Secondary is now PASSIVE. "
            "Waiting for next PRIMARY failure."
        )
