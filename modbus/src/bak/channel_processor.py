# channel proccessor

import asyncio
import logging
import queue

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

        # Mapper
        self.mapper = mapper

        # Queue
        self.mapping_queue = mapping_queue

        # Pre-build device runtime objects
        self._prepare_devices()

    # -----------------------------
    # Device preparation (ONCE)
    # -----------------------------
    def _prepare_devices(self):
        for device_config in self.devices:
            
            mapping = self.resolver.get_mapping(
                device_config["deviceType"],
                device_config["make"],
                device_config["model"]
            )

            # Build optimized read plan
            read_plan = DeviceLibraryResolver.build(
                register_mapping=mapping,
                max_regs={1: 2000, 2: 2000, 3: 125, 4: 125}
            )

            # Attach runtime objects
            # device_config["_mapping"] = mapping
            device_config["_read_plan"] = read_plan

            device_config["_reader"] = deviceReader(
                config=device_config,
                modbus_client_manager=self.modbus_client_manager,
                mapping_queue=self.mapping_queue
            )

            device_config["_reader"].channel_id = self.channel_id

            # logger.info(
            #     f"Prepared device {device_config['device_id']} "
            #     # f"with read plan: {read_plan}"
            #     f" on channel {self.channel_id}"
            #     f" length of read plan: {sum(len(ranges) for ranges in read_plan.values())}"
            # )

    # -----------------------------
    # Channel polling loop
    # -----------------------------
    
    async def process_channel(self):

        batch_devices = []

        for device in self.devices:

            device_results = await self._poll_device(device)

            if device_results:
                batch_devices.extend(device_results)

        if batch_devices:

            batch = MappedBatch(
                devices=batch_devices
            )

            try:
                self.mapping_queue.put(
                    batch,
                    block=False
                )

                # logger.info(
                #     f"CH-{self.channel_id}: queued batch "
                #     f"with {len(batch_devices)} mapping jobs"
                # )

            except queue.Full:
                try:
                    # Remove oldest item
                    self.mapping_queue.get_nowait()

                    logger.warning(
                        f"CH-{self.channel_id}: queue full, dropped oldest batch"
                    )

                except queue.Empty:
                    pass

                # Retry enqueue
                try:
                    self.mapping_queue.put_nowait(batch)
                except queue.Full:
                    logger.warning(
                        f"CH-{self.channel_id}: queue still full after drop"
                    )
            
    # -----------------------------
    # Poll a single device
    # -----------------------------

    async def _poll_device(self, device):
        reader = device["_reader"]
        read_plan = device["_read_plan"]

        raw_blocks = []

        for fc, ranges in read_plan.items():
            for r in ranges:
                try:
                    result = await reader.read_registers_with_retries(
                        start_address=r["start"],
                        count=r["count"],
                        functionCode=fc,
                        register_mapping=r["mapping"]
                    )

                    if result:
                        raw_blocks.append(result)

                except Exception as e:
                    logger.error(
                        f"Channel {self.channel_id} | "
                        f"Device {device['device_id']} failed: {e}"
                    )

        if not raw_blocks:
            return None
        
        # merge + map once

        mapped = []

        for block in raw_blocks:

            mapped_values, garbage_records = self.mapper.map_device(
                registers=block["registers"],
                block=device["block"],
                device_id=device["device_id"],
                device_type=device["deviceType"],
                register_mapper=block["register_mapper"]
            )


            mapped.append(MappedDeviceOutput(
                device_id=device["device_id"],
                mapped_values=mapped_values,
                garbage=garbage_records
            ))

        return mapped

    async def p_process_channel(self):
        logger.info(
            f"Channel {self.channel_id} started "
            f"with {len(self.devices)} devices"
        )

        # await asyncio.gather(
        #     *(self._poll_device(device) for device in self.devices)
        # )

        for device in self.devices:
            await self._poll_device(device)

    # async def _poll_device(self, device):
    #     reader = device["_reader"]
    #     read_plan = device["_read_plan"]

    #     for fc, ranges in read_plan.items():
    #         for r in ranges:
    #             try:
    #                 await reader.read_registers_with_retries(
    #                     start_address=r["start"],
    #                     count=r["count"],
    #                     functionCode=fc,
    #                     register_mapping=r["mapping"]
    #                 )
    #             except Exception as e:
    #                 logger.error(
    #                     f"Channel {self.channel_id} | "
    #                     f"Device {device['device_id']} | "
    #                     # f"FC {fc} | Range {r} failed: {e}"
    #                 )