import json
from pathlib import Path

from runtime_paths import DataPaths


class DeviceConfigHelper:
    def __init__(self, config_path="config/device_config.json"):
        self.config_path = DataPaths.resolve(config_path)
        self.device_list = self._load_devices()

    def _load_devices(self):
        """Load device list from config file"""
        try:
            with open(self.config_path, "r") as f:
                config = json.load(f)
                return config.get("devices", [])
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found at {self.config_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON in config file at {self.config_path}")

    def get_device_ids_by_type_pattern(self, pattern):
        """
        Return list of device_ids where device_id contains the given pattern
        Example: pattern = 'ess_ess_' → returns all ESS tables
        """
        return [d["device_id"] for d in self.device_list if pattern in d["device_id"]]

    def get_device_ids_by_device_type(self, device_type):
        """
        Return list of device_ids where deviceType == device_type
        Example: device_type = 'ess' → returns all ESS devices
        """
        return [d["device_id"] for d in self.device_list if d["deviceType"] == device_type]

    def get_all_device_ids(self):
        """Return all device IDs"""
        return [d["device_id"] for d in self.device_list]