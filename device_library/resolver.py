import os
import json
from pathlib import Path
import logging

from runtime_paths import DataPaths

logger = logging.getLogger(__name__)

class DeviceLibraryResolver:

    size_map = {
        "bool": 1,
        "uint16": 1,
        "int16": 1,
        "float": 2,
        "float32": 2,
        "uint32": 2,
        "int32": 2,
        "double": 4,
        "int64": 4,
        "string": 1,  # this would depend on actual implementation
    }
    
    def __init__(self, library_path="device_library"):
        self.library_path = DataPaths.resolve(library_path)

    def get_mapping(self, device_type, make, model):
        """
        Fetch the register mapping for a specific device type, make, and model.
        """
        try:
            # Construct the path to the JSON file
            make_folder = self.library_path / device_type.lower() / make.lower()
            model_file = make_folder / f"{model.lower()}.json"

            if not model_file.exists():
                raise FileNotFoundError(f"No mapping found for {device_type}, {make}, {model}")

            # Load and return the mapping
            with open(model_file, "r") as f:
                mapping = json.load(f)
            logger.info(f"Loaded mapping for {device_type}, {make}, {model}")
            return mapping
        except Exception as e:
            logger.error(f"Error loading mapping for {device_type}, {make}, {model}: {e}")
            raise ValueError(f"Failed to load mapping for {device_type}, {make}, {model}")
    

    def get_mapping(self, device_type, make, model):
        # Check for computed devices first
        computed_path = DataPaths.resolve("device_library/computed/computed_device")
        computed_file = computed_path / f"{model.lower()}.json"
        if computed_file.exists():
            with open(computed_file, 'r') as f:
                return json.load(f)
                
        # Fall back to regular device library
        make_folder = self.library_path / device_type.lower() / make.lower()
        model_file = make_folder / f"{model.lower()}.json"
        if model_file.exists():
            with open(model_file, "r") as f:
                return json.load(f)
                
        raise FileNotFoundError(f"No mapping found for {device_type}, {make}, {model}")
    
    @staticmethod
    def get_data_type_size(data_type) -> int:
        """
        Return the number of 16-bit registers used by a given data type.
        """
        return DeviceLibraryResolver.size_map.get(data_type.lower(), 1)  # default to 1 if unknown

    @staticmethod
    def build(register_mapping, max_regs):
        """
            read_plan = {
                3: [
                    {"start": 83, "count": 10, "mapping": {"volt": {...}}}  # 83–92
                ],
                1: [
                    {"start": 1, "count": 1, "mapping": {"volt": {...}}}
                ],
                4: [
                    {"start": 10, "count": 2, "mapping": {"volt": {...}}}
                ]
            }

        """
        from collections import defaultdict

        fc_buckets = defaultdict(list)

        # Bucket registers by FC
        for tag, meta in register_mapping.items():
            reg = meta["register"]
            fc = meta["fc"]
            size = DeviceLibraryResolver.size_map[meta["dataType"]]

            fc_buckets[fc].append({
                "tag": tag,
                "register": reg,
                "size": size,
                "meta": meta
            })

        read_plan = {}

        for fc, regs in fc_buckets.items():
            regs.sort(key=lambda x: x["register"])
            read_plan[fc] = DeviceLibraryResolver._build_ranges(
                regs, max_regs[fc], max_gap=max_regs[fc]
            )

        return read_plan

    @staticmethod
    def _build_ranges(regs, max_count, max_gap=8):
        """
        Docstring for _build_ranges
        
        :param regs: Description
        :param max_count: Description
        :param max_gap: max_gap: how many missing registers we tolerate inside a range
        """
        ranges = []

        # initialize first range
        first = regs[0]
        start = first["register"]
        end = start + first["size"] - 1
        mapping = {
            first["tag"]: first["meta"]
        }

        for item in regs[1:]:
            reg = item["register"]
            sz = item["size"]
            next_end = reg + sz - 1
            gap = reg - end - 1

            if gap <= max_gap and (next_end - start + 1) <= max_count:
                # extend current range
                end = next_end
                mapping[item["tag"]] = item["meta"]
            else:
                # close current range
                ranges.append({
                    "start": start,
                    "count": end - start + 1,
                    "mapping": mapping
                })

                # start new range
                start = reg
                end = next_end
                mapping = {
                    item["tag"]: item["meta"]
                }

        # final range
        ranges.append({
            "start": start,
            "count": end - start + 1,
            "mapping": mapping
        })

        return ranges

