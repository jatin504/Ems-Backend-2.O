from dataclasses import dataclass
from typing import List, Any, Dict

from auto_configs.latest_telemetry_manager import LatestTelemetryManager
from processor.src.samplings.src.raw_recording import RawRecorder
from processor.src.mappings.telemetry_garbage import TelemetryGarbageDAO
from runtime.environment_const import PUBLISH_SSE

@dataclass
class DeviceMappingInput:
    registers: List[int]
    device_id: str
    device_type: str
    block: str
    register_mapper: Dict[str, Any]

@dataclass
class MappedDeviceOutput:
    device_id: str
    mapped_values: dict
    garbage: List

@dataclass
class MappingBatch:
    devices: List[DeviceMappingInput]

@dataclass
class MappedBatch:
    devices: List[MappedDeviceOutput]

import asyncio
import logging

logger = logging.getLogger(__name__)

class PostMapper:
    def __init__(self, sampling_queue: asyncio.Queue, sse_broadcaster):
        self.sampling_queue = sampling_queue
        self.see_publisher = sse_broadcaster
        # Own the first snapshot store: mapped device tags, before aggregation.
        # This manager chooses JSON or Valkey using EMS_CACHE_BACKEND at startup.
        self.latest_telemetry_manager = LatestTelemetryManager()
        # Raw history branches before averaging; JSON/Valkey remains the latest-value store.
        self.raw_recorder = RawRecorder()

        # tune this based on CPU cores
        self.worker_count = 4

    async def start(self, batch_queue: asyncio.Queue):
        """
        Start fixed worker pool (NO per-device tasks)
        """
        workers = [
            asyncio.create_task(self.worker(batch_queue))
            for _ in range(self.worker_count)
        ]

        await asyncio.gather(*workers)

    async def worker(self, batch_queue: asyncio.Queue):
        logger.info("Fast mapper worker started")

        while True:
            try:
                batch: MappedBatch = await batch_queue.get()

                try:
                    # ---------------------------------------------------------
                    # 1. Merge mapped values for the same device_id
                    # ---------------------------------------------------------

                    merged_devices = {}

                    for device in batch.devices:
                        device_id = device.device_id

                        if device_id not in merged_devices:
                            merged_devices[device_id] = {}

                        # New values overwrite old values for the
                        # same field.
                        merged_devices[device_id].update(
                            device.mapped_values
                        )

                    # logger.info(
                    #     "Batch contains %d devices, %d unique device IDs",
                    #     len(batch.devices),
                    #     len(merged_devices),
                    # )

                    # ---------------------------------------------------------
                    # 2. Process each device only once
                    # ---------------------------------------------------------

                    for device_id, mapped_values in merged_devices.items():

                        # Only configured critical aliases enter Victoria raw history.
                        # This synchronous enqueue does not change any mapped values.
                        self.raw_recorder.record(device_id, mapped_values)

                        # Send the same mapped tags to sampling independently of caching.
                        # The sampling worker later builds avg/last results (second layer).
                        # This queue insertion itself does not write JSON or Valkey.
                        # Sampling queue
                        await self.sampling_queue.put(
                            mapped_values
                        )
                        # -----------------------------------------------------
                        # 3. Publish one SSE packet per device
                        # -----------------------------------------------------

                        # if PUBLISH_SSE == "True":
                        #     try:
                        #         await self.see_publisher.publish(
                        #             device_id,
                        #             mapped_values,
                        #         )

                        #     except Exception:
                        #         logger.exception(
                        #             "Error while publishing SSE "
                        #             "for device_id=%s",
                        #             device_id,
                        #         )

                        # self.latest_telemetry_manager.update_device_data(
                        #     device_id=device.device_id,
                        #     data=device.mapped_values
                        # )

                        # FIRST CACHE ENTRY POINT (after mapping): save latest named tags.
                        # save_device_data -> TelemetryStore.amerge -> merge:

                        await self.latest_telemetry_manager.save_device_data(
                            device_id=device_id,
                            data=mapped_values,
                        )


                    # ---------------------------------------------------------
                    # 4. Process garbage from original devices
                    # ---------------------------------------------------------

                    for device in batch.devices:

                        if device.garbage:
                            for rec in device.garbage:
                                try:
                                    await TelemetryGarbageDAO.insert(
                                        rec
                                    )
                                except Exception:
                                    logger.exception(
                                        "Error inserting garbage record "
                                        "for device_id=%s",
                                        device.device_id,
                                    )

                finally:
                    # Always tell asyncio.Queue that this batch is done.
                    batch_queue.task_done()

            except asyncio.CancelledError:
                logger.info("Fast mapper worker stopping...")
                raise

            except Exception:
                logger.exception(
                    "Error in Mapper Worker"
                )


    def _process_batch(self, batch: MappingBatch):
        results = []

        for device in batch.devices:
            mapped_values, garbage = self._map_device(
                device.registers,
                device.block,
                device.device_id,
                device.device_type,
                device.register_mapper
            )

            if garbage:
                # optional: async insert can be done later in another worker
                for rec in garbage:
                    asyncio.create_task(TelemetryGarbageDAO.insert(rec))

            results.append((mapped_values, device.device_id))

        return results

class CompiledMapper:

    def __init__(self, register_mapper):

        self.entries = []

        for mapping in register_mapper.values():

            if not mapping.get("enable", True):
                continue

            data_type = mapping["dataType"]

            self.entries.append({

                "start_addr": mapping.get("srno", int(mapping["register"])),

                "length": 2 if data_type in ("float", "float32", "int32") else 1,

                "data_type": data_type,

                "scale": mapping.get("scaling", 1),

                "min": mapping.get("min", float("-inf")),

                "max": mapping.get("max", float("inf")),

                "register": mapping["register"],

                "alias": mapping["alias"],

                "name": mapping["name"],

                "type": mapping["type"],

                "unit": mapping.get("unit", ""),

                "byte_order": mapping.get("byte_order", "big_endian"),

                "word_order": mapping.get("word_order", "big_endian"),

                "alarmType": mapping.get("alarmType"),

                "bitMap": mapping.get("bitMap"),

                "severity": mapping.get("severity", "critical"),

                "isEvent": mapping.get("isEvent", False),

                "isNormalReverse": mapping.get("isNormalReverse", False)

            })
@dataclass(slots=True)
class MappingEntry:
    start_addr: int
    length: int
    alias: str
    name: str
    data_type: str
    scale: float
    min: float
    max: float
    register: int
    byte_order: str
    word_order: str
    tag_type: str
    unit: str
    alarm_type: str | None
    bit_map: dict | None
    severity: str
    is_event: bool
    is_normal_reverse: bool

class CacheMapper:

    def map_device(
        self,
        registers,
        block,
        device_id,
        device_type,
        compiled_mapper
    ):

        mapped_values = {}
        garbage_records = []

        reg = registers

        for entry in compiled_mapper.entries:

            decoded = decode_value(
                reg[
                    entry["start_addr"]:
                    entry["start_addr"] + entry["length"]
                ],
                entry["data_type"],
                entry["byte_order"],
                entry["word_order"]
            )

            value = None

            if decoded is not None:

                value = decoded / entry["scale"]

                if isinstance(value, float):
                    value = round(value, 2)

                if not (entry["min"] <= value <= entry["max"]):

                    garbage_records.append({

                        "device_id": device_id,
                        "device_type": device_type,
                        "block": block,
                        "telemetry_alias": entry["alias"],
                        "telemetry_name": entry["name"],
                        "raw_value": decoded,
                        "scaled_value": value,
                        "unit": entry["unit"],
                        "min_value": entry["min"],
                        "max_value": entry["max"],
                        "reason": "OUT_OF_RANGE",
                        "remarks": f"value {value} outside range",
                        "register": entry["register"],
                        "data_type": entry["data_type"]

                    })

                    value = None

            obj = {

                "block": block,
                "device_type": device_type,
                "device_id": device_id,
                "alias": entry["alias"],
                "name": entry["name"],
                "value": value,
                "type": entry["type"],
                "unit": entry["unit"],
                "dataType": entry["data_type"],
                "register": entry["register"]

            }

            if entry["type"] == "alarm":

                obj["alarmType"] = entry["alarmType"]
                obj["severity"] = entry["severity"]
                obj["isEvent"] = entry["isEvent"]
                obj["isNormalReverse"] = entry["isNormalReverse"]

                if entry["alarmType"] == "bitwise":

                    bitmap = {}

                    for bit_pos, bit_def in entry["bitMap"].items():

                        bitmap[bit_def["alias"]] = {

                            "device_id": device_id,
                            "alias": bit_def["alias"],
                            "value": (decoded >> int(bit_pos)) & 1 if decoded is not None else None,
                            "type": "alarm",
                            "severity": bit_def.get("severity", "info"),
                            "parent_alias": entry["alias"]

                        }

                    obj["bitMapAlarm"] = bitmap

            mapped_values[entry["alias"]] = obj

        return mapped_values, garbage_records

class Mapper:
    """
        This class is for only map the data.
    """
    def __init__(self):
        pass
    
    def map_device(self, registers, block, device_id, device_type, register_mapper):
        mapped_values = {}
        garbage_records = []

        reg = registers  # local ref = faster

        for key, mapping in register_mapper.items():
            try:
                if not mapping.get("enable", True):
                    continue

                register = mapping["register"]
                start_addr = mapping.get("srno", int(register))
                data_type = mapping["dataType"]

                scaling = mapping.get("scaling", 1)
                min_val = mapping.get("min", float("-inf"))
                max_val = mapping.get("max", float("inf"))

                alias = mapping["alias"]
                name = mapping["name"]
                typ = mapping["type"]
                unit = mapping.get("unit", "")

                byte_order = mapping.get("byte_order", "big_endian")
                word_order = mapping.get("word_order", "big_endian")

                reg_len = 2 if data_type in ("int32", "float", "float32") else 1
                reg_slice = reg[start_addr:start_addr + reg_len]

                decoded = decode_value(reg_slice, data_type, byte_order, word_order)

                if decoded is None:
                    value = None
                else:
                    value = decoded / scaling

                    if isinstance(value, float):
                        value = round(value, 2)

                    if not (min_val <= value <= max_val):
                        garbage_records.append({
                            "device_id": device_id,
                            "device_type": device_type,
                            "block": block,
                            "telemetry_alias": alias,
                            "telemetry_name": name,
                            "raw_value": decoded,
                            "scaled_value": value,
                            "unit": unit,
                            "min_value": min_val,
                            "max_value": max_val,
                            "reason": "OUT_OF_RANGE",
                            "remarks": f"value {value} not in range {min_val}–{max_val}",
                            "register": register,
                            "data_type": data_type,
                        })
                        # logger.warning(f"Garbage Data Detected for device {device_id}, tag {alias}: {value} not in range {min_val}–{max_val}")
                        value = None

                mapped_values[alias] = {
                    "block": block,
                    "device_type": device_type,
                    "device_id": device_id,
                    "alias": alias,
                    "name": name,
                    "value": value,
                    "type": typ,
                    "unit": unit,
                    "dataType": data_type,
                    "register": register
                }

                # Handle alarm subtypes
                if typ == "alarm":
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

            except Exception as e:
                logger.warning(f"Mapping failed {key}: {e}")

        return mapped_values, garbage_records
    

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
            # logger.error(
            #     "Empty registers=%s for data_type=%s",
            #     registers,
            #     data_type
            # )
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
