import json
import os
from typing import Optional, Dict, Any, List

from runtime_paths import DataPaths

CONFIG_PATH = "config/plant_config.json"

class PlantConfigResolver:
    def __init__(self, config_path: str = CONFIG_PATH):
        self.config_path = DataPaths.resolve(config_path)

    def load_config(self):
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Plant config not found: {self.config_path}")
        with open(self.config_path, "r") as f:
            config = json.load(f)
            return config