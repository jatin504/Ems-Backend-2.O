import asyncio
import logging
import queue
import time

from processor.src.mappings.mapper import MappedDeviceOutput, MappedBatch
from .device_reader import deviceReader
from device_library.resolver import DeviceLibraryResolver

logger = logging.getLogger(__name__)


class ChannelProcessor:
    def __init__(self, channel_id, devices, modbus_client_manager, mapper, mapping_queue):
        self.channel_id = channel_id
        self.devices = devices
        self.modbus_client_manager = modbus_client_manager
        self.resolver = DeviceLibraryResolver()
        self.mapper = mapper
        self.mapping_queue = mapping_queue

        self._prepare_devices()

    def _prepare_devices(self):
        for device_config in self.devices:
            mapping = self.resolver.get_mapping(
                device_config["deviceType"],
                device_config["make"],
                device_config["model"]
            )

            read_plan = DeviceLibraryResolver.build(
                register_mapping=mapping,
                max_regs={1: 2000, 2: 2000, 3: 125, 4: 125}
            )

            device_config["_read_plan"] = read_plan
            device_config["_reader"] = deviceReader(
                config=device_config,
                modbus_client_manager=self.modbus_client_manager,
                mapping_queue=self.mapping_queue
            )
            device_config["_reader"].channel_id = self.channel_id

    async def process_channel(self):
        batch_devices = []

        for device in self.devices:
            device_results = await self._poll_device(device)
            if device_results:
                batch_devices.extend(device_results)

        if batch_devices:
            batch = MappedBatch(devices=batch_devices)
            try:
                self.mapping_queue.put(batch, block=False)
            except queue.Full:
                try:
                    self.mapping_queue.get_nowait()
                    logger.warning(
                        f"CH-{self.channel_id}: queue full, dropped oldest batch"
                    )
                except queue.Empty:
                    pass

                try:
                    self.mapping_queue.put_nowait(batch)
                except queue.Full:
                    logger.warning(
                        f"CH-{self.channel_id}: queue still full after drop"
                    )

    async def _poll_device(self, device):
        device_start = time.perf_counter()

        reader = device["_reader"]
        read_plan = device["_read_plan"]

        max_concurrent_reads = 1
        semaphore = asyncio.Semaphore(max_concurrent_reads)

        async def read_block(fc, r):
            block_start = time.perf_counter()

            async with semaphore:
                try:
                    result = await reader.read_registers_with_retries(
                        start_address=r["start"],
                        count=r["count"],
                        functionCode=fc,
                        register_mapping=r["mapping"]
                    )

                    block_elapsed = time.perf_counter() - block_start

                    # logger.info(
                    #     f"CH-{self.channel_id} | "
                    #     f"Device {device['device_id']} | "
                    #     f"FC={fc} | "
                    #     f"Start={r['start']} | "
                    #     f"Count={r['count']} | "
                    #     f"Block read time={block_elapsed:.3f}s"
                    # )

                    return result

                except Exception as e:
                    block_elapsed = time.perf_counter() - block_start

                    # logger.error(
                    #     f"CH-{self.channel_id} | "
                    #     f"Device {device['device_id']} | "
                    #     f"FC={fc} | "
                    #     f"Start={r['start']} | "
                    #     f"Count={r['count']} | "
                    #     f"Block read FAILED after {block_elapsed:.3f}s | "
                    #     f"Error: {e}"
                    # )
                    return None

        tasks = []

        for fc, ranges in read_plan.items():
            for r in ranges:
                tasks.append(asyncio.create_task(read_block(fc, r)))

        raw_blocks = await asyncio.gather(*tasks)
        raw_blocks = [res for res in raw_blocks if res is not None]

        if not raw_blocks:
            device_elapsed = time.perf_counter() - device_start

            logger.warning(
                f"CH-{self.channel_id} | "
                f"Device {device['device_id']} | "
                f"No successful blocks | "
                f"Total device time={device_elapsed:.3f}s"
            )

            return None

        mapped = []

        for block in raw_blocks:
            mapped_values, garbage_records = self.mapper.map_device(
                registers=block["registers"],
                block=device["block"],
                device_id=device["device_id"],
                device_type=device["deviceType"],
                register_mapper=block["register_mapper"]
            )

            mapped.append(
                MappedDeviceOutput(
                    device_id=device["device_id"],
                    mapped_values=mapped_values,
                    garbage=garbage_records
                )
            )

        device_elapsed = time.perf_counter() - device_start

        # logger.info(
        #     f"CH-{self.channel_id} | "
        #     f"Device {device['device_id']} | "
        #     f"Successful blocks={len(raw_blocks)} | "
        #     f"Total device read+map time={device_elapsed:.3f}s"
        # )

        return mapped