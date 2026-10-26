import json
import os

from runtime_paths import DataPaths

def load_cycle_config():
    file_path = DataPaths.resolve("controller_layer/strategies/cycle_management/cycle_config.json")
    with open(file_path, "r") as f:
        return json.load(f)
