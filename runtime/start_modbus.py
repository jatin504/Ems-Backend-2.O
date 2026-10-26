

import asyncio
import logging
import multiprocessing
import threading

from modbus.main import run_modbus_isolated_loop

from utils.device_based_distribution import modbus_processor_node_helper

DEVICES_COUNT = 6
MODBUS_PROCESS_NODE = 1
MODBUS_NODE_DEVICE_SUPPORT_COUNT = 6

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def start_modbus_layer(
        MP_MAPPING_QUEUE,
        ASYNC_MAPPING_QUEUE,
):
    loop = asyncio.get_running_loop()

    threading.Thread(
        target=bridge_mp_to_asyncio,
        args=(
            MP_MAPPING_QUEUE,
            ASYNC_MAPPING_QUEUE,
            loop,
        ),
        daemon=True,
    ).start()

    logger.info("Bridge thread started.")

    processes = []

    ranges = modbus_processor_node_helper(
        DEVICES_COUNT,
        MODBUS_PROCESS_NODE,
        MODBUS_NODE_DEVICE_SUPPORT_COUNT,
    )

    for node_id, (start_device, end_device) in enumerate(ranges):
        process = multiprocessing.Process(
            target=run_modbus_isolated_loop,
            args=(
                MP_MAPPING_QUEUE,
                start_device,
                end_device,
            ),
        )

        process.start()
        processes.append(process)

        logger.info(
            f"Started Modbus Process-{node_id} "
            f"(PID={process.pid}) "
            f"Range={start_device} -> {end_device}"
        )

    # modbus_process = multiprocessing.Process(
    #     target=run_modbus_isolated_loop, 
    #     args=(MP_MAPPING_QUEUE, 0, 800,), # <--- Pass the multiprocessing queue
    #     # daemon=True
    #     # Note: We don't use daemon=True for processes, we manage shutdown manually if needed
    # )
    # modbus_process.start()
    # logger.info(f"Modbus layer started in isolated OS Process (PID: {modbus_process.pid})...")

    return processes


def bridge_mp_to_asyncio(mp_queue: multiprocessing.Queue, asyncio_queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
    """
    Runs in a dedicated background thread.
    Safely bridges the blocking multiprocessing.Queue to the non-blocking asyncio.Queue.
    """
    while True:
        try:
            task = mp_queue.get()

            def safe_put():
                try:
                    asyncio_queue.put_nowait(task)
                except asyncio.QueueFull:
                    logger.warning(
                        f"Async queue full. size={asyncio_queue.qsize()}"
                    )

            loop.call_soon_threadsafe(safe_put)
        except Exception as e:
            logger.error(f"Bridge thread error: {e}")
            break