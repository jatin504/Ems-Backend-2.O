import asyncio
import logging

import concurrent.futures

from modbus import ChannelProcessor
from controller_layer.writers.ems_modbus_server import EMSModbus
from config_loader import ConfigLoader

# from utils.loop_manager import get_main_loop
from modbus import ModbusClientManager
from processor.src.mappings.mapper import Mapper

# main_loop = get_main_loop()
# ModbusClientManager.initialize(main_loop)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(device_id)s] %(message)s",
)
logger = logging.getLogger(__name__)

def run_modbus_isolated_loop(mapping_queue, start_device_range, end_device_range):
    """
    This function runs inside the background thread.
    It creates a completely isolated event loop just for Modbus.
    """

    # logging.basicConfig(level=logging.INFO, format="[MODBUS PROCESS] %(asctime)s - %(levelname)s - %(message)s")
    
    # 1. Create a brand new event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # 2. Give this isolated loop its own massive thread pool
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2000)
    loop.set_default_executor(executor)
    
    # 3. Initialize ModbusClientManager on THIS specific loop
    ModbusClientManager.initialize(loop)
    
    try:
        # 4. Run the modbus polling, passing the bridge_queue
        loop.run_until_complete(main(mapping_queue, start_device_range, end_device_range))
    except Exception as e:
        print(f"Modbus isolated loop crashed: {e}")

async def main(mapping_queue, start_device_range, end_device_range):
    
    # This ensures it attaches to the high-performance loop we created in main.py!
    # ModbusClientManager.initialize(asyncio.get_running_loop())

    config_loader = ConfigLoader()
    mapper = Mapper()
    device_config = config_loader.load_config("config/device_config.json")
    ems_modbus_server = EMSModbus.get_instance()
    devices = device_config["devices"]
    chunk_size = device_config["chunk_size"]

    # Start EMS Modbus server in background
    asyncio.create_task(ems_modbus_server.start())
    
    logger.info("EMS Modbus Server started in background...")
    # Split devices into channels
    channels = [devices[i:i + chunk_size] for i in range(start_device_range, 800 if end_device_range == -1 else end_device_range, chunk_size)]

    # logger.info(f"Total devices: {len(devices)}, Channels: {len(channels)}, channels: {channels}, Chunk size: {chunk_size}")

    # Create a task for each channel to run its own polling loop
    channel_tasks = []
    for idx, channel_devices in enumerate(channels):
        processor = ChannelProcessor(
            channel_id=idx+1,
            devices=channel_devices,
            modbus_client_manager=ModbusClientManager,
            mapper=mapper,
            mapping_queue=mapping_queue
        )
        task = asyncio.create_task(run_channel_loop(processor, device_config.get("polling_interval", 1)))
        channel_tasks.append(task)

    # Keep the event loop running
    await asyncio.gather(*channel_tasks)


async def run_channel_loop(processor, polling_interval):
    while True:
        try:
            start_time = asyncio.get_event_loop().time()
            await processor.process_channel()
            elapsed_time = asyncio.get_event_loop().time() - start_time

            # logger.info(
            #     f"Channel {processor.channel_id} cycle took "
            #     f"{elapsed_time:.3f}s "
            #     f"sleep time: {polling_interval - elapsed_time}s"
            # )
            # Sleep for the remaining time to maintain a 1-second interval
            sleep_time = max(0, polling_interval - elapsed_time)
            await asyncio.sleep(sleep_time)
        except Exception as e:
            logger.error(f"Error in channel {processor.channel_id}: {e}")
            await asyncio.sleep(polling_interval)  # Ensure loop continues even on error

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("modbus Shutting down...")