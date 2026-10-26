import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import datetime
import queue

from auto_configs.latest_telemetry_manager import LatestTelemetryManager
from processor.src.mappings.telemetry_garbage import TelemetryGarbageDAO

from utils.db_write_queue import VICTORIA_DB_WRITE_QUEUE

from dataclasses import dataclass
from typing import List, Any

@dataclass
class MappingTask:
    channel: int
    server_ip: str
    server_port: int
    slave_id: int
    registers: List[Any]
    block: str
    device_id: str
    device_type: str
    register_mapper: Any

logger = logging.getLogger(__name__)

class Mapper:
    def __init__(self, sse_broadcaster, sampling_queue: asyncio.Queue):
        """
        Initialize the Mapper with references to SamplingManager and SSEBroadcaster.
        """
        # Publishes new mapped device
        self.see_publisher = sse_broadcaster

        # Initialize latest telemetry manager
        self.latest_telemetry_manager = LatestTelemetryManager()
        
        # The shared queue
        self.sampling_queue = sampling_queue

        # Offload synchronous work to a thread
        self.mapping_executor = ThreadPoolExecutor(
            max_workers=64
        )

    async def start_mapper_worker(self, mapping_queue, worker_count):
        """
        Background loop that continuously pulls tasks from the queue and processes them.
        """
        self.mapper_tasks = [
            asyncio.create_task(
                self.start_mapper(mapping_queue)
            )
            for _ in range(worker_count)
        ]

        await asyncio.gather(*self.mapper_tasks)

    async def start_mapper(self, mapping_queue):
        """
        Background loop that continuously pulls tasks from the queue and processes them.
        """
        logger.info("Mapping worker started.")
        while True:
            try:
                # logger.info("Mapping worker loop started.")
                # This is now a pure, non-blocking async call.
                # logger.info(f"queue type = {type(mapping_queue)}")
                task = await mapping_queue.get()
                # logger.info(f"✅ Successfully pulled task from queue!")
                
                # Process the task
                loop = asyncio.get_event_loop()
                start_time = loop.time()
                await self._process_task(task)
                elapsed = loop.time() - start_time
                logger.info(
                        f"Mapped  in {elapsed:.3f}s"
                    )

                # asyncio.create_task(
                #     self._process_task(task)
                # )
                
                # Mark as done
                mapping_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error in mapping worker loop: {e}")

    async def _process_task(self, task: MappingTask):
        """
        Map raw register values to their tags, apply transformations, and broadcast data via SSE.
        """
            
        
        loop = asyncio.get_event_loop()
        # start_time = loop.time()
        mapped_values, garbage_records = (await loop.run_in_executor(
            self.mapping_executor,  # Use default thread pool
            self._map_registers_sync,  # Synchronous function
            task.registers,
            task.block,
            task.device_id,
            task.device_type,
            task.register_mapper
        ))
        # mapped_values, garbage_records = self._map_registers_sync(
        #     task.registers,
        #     task.block,
        #     task.device_id,
        #     task.device_type,
        #     task.register_mapper
        # )
        # elapsed = loop.time() - start_time
        # logger.info(
        #     f"Mapped {len(task.register_mapper)} tags in {elapsed:.3f}s"
        # )

        # Determine the topic
        # topic = f"{device_id}"

        # Push to PostgreSQL Queue
        await self.sampling_queue.put(mapped_values)
        # logger.info(f"put the task in SAMPLING Queue !!")

        # Push to VictoriaMetrics queue
        # await VICTORIA_DB_WRITE_QUEUE.put((device_id, mapped_values))
        # if mapped_values:
            
        #     try:
        #         await self.see_publisher.publish(topic, mapped_values)
        #     except Exception as e:
        #         logger.info("ERROR WHILE PUBLISHING TOPIC", e)
            
        # logger.info("New Topic Added")

        # Save mapped data and raw registers
        # self.latest_telemetry_manager.save_device_data(device_id, mapped_values)
        # self.latest_telemetry_manager.enqueue_device_data(
        #     device_id,
        #     mapped_values
        # )
        self.latest_telemetry_manager.update_device_data(
            device_id=task.device_id,
            data=mapped_values
        )
        # Insert garbage records if any
        # logger.info(f"Garbage records to insert: {len(garbage_records)}, {garbage_records}")
        if garbage_records:
            for rec in garbage_records:
                # await GARBAGE_QUEUE.put(rec)
                await TelemetryGarbageDAO.insert(rec)

        # return mapped_values

    def _map_registers_sync(self, registers, block, device_id, device_type, register_mapper):
        try:
            mapped_values = {}
            garbage_records = []
            # logger.info(device_id, registers)
            for key, mapping in register_mapper.items():
                try:
                    register = mapping["register"]
                    start_addr = mapping.get("srno", int(register))  # Use 'register' if present
                    data_type = mapping["dataType"]
                    scaling = mapping.get("scaling", 1)
                    min_val = mapping.get("min", float("-inf"))
                    max_val = mapping.get("max", float("inf"))
                    name = mapping["name"]
                    alias = mapping["alias"]
                    type = mapping["type"]
                    unit = mapping.get("unit", "")
                    endian = mapping.get("endian", "big")
                    byte_order = mapping.get("byte_order", "big_endian")
                    word_order = mapping.get("word_order", "big_endian")
                    enable = mapping.get("enable", "true")

                    if not enable:
                        continue

                    # Determine how many registers to read
                    if data_type in ["int32", "float", "float32"]:
                        reg_len = 2
                    else:
                        reg_len = 1

                    reg_slice = registers[start_addr:start_addr + reg_len]
                    # print(reg_slice, alias)
                    # Decode and scale
                    decoded = decode_value(reg_slice, data_type, byte_order, word_order)
                    scaled_value = None

                    if decoded is not None:
                        scaled_value = decoded / scaling
                        if isinstance(scaled_value, float):
                            scaled_value = round(scaled_value, 2)
                        if not (min_val <= scaled_value <= max_val):
                            garbage_records.append({
                                "device_id": device_id,
                                "device_type": device_type,
                                "block": block,
                                "telemetry_alias": alias,
                                "telemetry_name": name,
                                "raw_value": decoded,
                                "scaled_value": scaled_value,
                                "unit": unit,
                                "min_value": min_val,
                                "max_value": max_val,
                                "reason": "OUT_OF_RANGE",
                                "remarks": f"value {scaled_value} not in range {min_val}–{max_val}",
                                "register": register,
                                "data_type": data_type,
                            })
                            logger.warning(f"Garbage Data Detected for device {device_id}, tag {alias}: {scaled_value} not in range {min_val}–{max_val}")
                            scaled_value = None  # Out-of-range

                    mapped_values[alias] = {
                        "block": block,
                        "device_type": device_type,
                        "device_id": device_id,
                        "alias": alias,
                        "name" : name,
                        "value": scaled_value,
                        "type": type,
                        "unit": unit,
                        "dataType" : data_type,
                        "register" : register
                    }
                    
                    # Handle alarm subtypes
                    if type == "alarm":
                        alarmType = mapping["alarmType"]
                        mapped_values[alias]["alarmType"] = alarmType
                        isEvent = mapping.get("isEvent", False)
                        severity = mapping.get("severity", "critical")
                        mapped_values[alias]["isEvent"] = isEvent
                        mapped_values[alias]["severity"] = severity
                        isNormalReverse = mapping.get("isNormalReverse", False)
                        mapped_values[alias]["isNormalReverse"] = isNormalReverse


                        if alarmType == "bitwise":
                            bit_map = mapping.get("bitMap", {})

                            bitwise_values = {}
                            for bit_pos, defn in bit_map.items():
                                bit_state = (decoded >> int(bit_pos)) & 1
                                tag = defn["alias"]

                                bitwise_values[tag] = {
                                    "device_id": device_id,
                                    "alias": tag,
                                    "value": bit_state,
                                    "type": "alarm",
                                    "severity": defn.get("severity", "info"),
                                    "parent_alias": alias
                                }

                                # Attach to parent entry
                                mapped_values[alias]["bitMapAlarm"] = bitwise_values

                except Exception as e_inner:
                    logger.warning(f"Failed to map {key}: {e_inner}---map register : {registers}")

            now = datetime.datetime.now(datetime.timezone.utc)
            # logger.info(f'mapped values at time {now} :  {mapped_values}')
            return mapped_values, garbage_records

        except Exception as e:
            logger.error(f"Error mapping registers: {e}")
            return {}



import struct
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian

logging.getLogger("pymodbus").setLevel(logging.ERROR)
logging.getLogger("pymodbus.logging").setLevel(logging.ERROR)

logging.basicConfig(level=logging.ERROR)

def decode_value(registers, data_type,
                 byte_order="big_endian",
                 word_order="big_endian"):
    try:
        # Guard against empty/None registers
        if isinstance(registers, list) and all(r is None for r in registers):
            logger.error(
                "Empty registers for data_type=%s",
                data_type
            )
            return None

        if data_type == "boolean" or data_type == "bool":
            return bool(registers[0])

        elif data_type == "uint16":
            return registers[0]

        elif data_type in ("int16", "int"):
            val = registers[0]
            return val - 65536 if val > 32767 else val

        elif data_type == "int32":
            if len(registers) < 2:
                logger.error(
                    "Insufficient registers for int32: %s",
                    registers
                )
                return None

            decoder = BinaryPayloadDecoder.fromRegisters(
                registers,
                byteorder=Endian.BIG if byte_order == "big_endian" else Endian.LITTLE,
                wordorder=Endian.BIG if word_order == "big_endian" else Endian.LITTLE,
            )

            return decoder.decode_32bit_int()

        elif data_type in ("float", "float32"):
            if len(registers) < 2:
                logger.error(
                    "Insufficient registers for float32: %s",
                    registers
                )
                return None

            decoder = BinaryPayloadDecoder.fromRegisters(
                registers,
                byteorder=Endian.BIG if byte_order == "big_endian" else Endian.LITTLE,
                wordorder=Endian.BIG if word_order == "big_endian" else Endian.LITTLE,
            )

            return decoder.decode_32bit_float()

    except Exception as e:
        logger.exception(
            "Decoding error for data_type=%s registers=%s",
            data_type,
            registers
        )

    return None

# Format the mapped values to Update method block input
def extract_updates(raw_data: dict) -> dict:
    return {details['alias']: {"value": details["value"]} for tag, details in raw_data.items()}