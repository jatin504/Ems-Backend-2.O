import json
import os
from typing import Optional, Dict, Any, List

from runtime_paths import DataPaths

CONFIG_PATH = "config/device_config.json"

class DeviceConfigResolver:
    def __init__(self, config_path: str = CONFIG_PATH):
        self.config_path = DataPaths.resolve(config_path)
        self.devices = []
        self.chunk_size = 1  # default
        self._load_config()

    def _load_config(self):
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Device config not found: {self.config_path}")
        with open(self.config_path, "r") as f:
            config = json.load(f)
            self.devices: List[Dict[str, Any]] = config.get("devices", [])
            self.chunk_size: int = config.get("chunk_size", 1)

    def get_device_info_by_id(self, device_id: str) -> Optional[Dict[str, Any]]:
        for index, device in enumerate(self.devices):
            if device.get("device_id") == device_id:
                channel = index // self.chunk_size
                return {
                    "channel": f"channel_{channel}",
                    "server_ip": device.get("server_ip"),
                    "server_port": device.get("server_port"),
                    "slave_id": device.get("slave_id"),
                    "deviceType": device.get("deviceType"),
                    "make": device.get("make"),
                    "model": device.get("model"),
                    "device_id": device.get("device_id"),
                    "function": device.get("function"),
                    "raw_index": index,
                    "channel_index": channel,
                    "dc_capacity": device.get("dc_capacity", 0)
                }
        return None
