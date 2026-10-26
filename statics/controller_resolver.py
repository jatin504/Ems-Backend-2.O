import json
import os
from typing import Any, Optional

from runtime_paths import DataPaths

CONTROLLER_STATIC_FILE = "statics/controller.json"

class ControllerStaticResolver:
    def __init__(self, file_path: str = CONTROLLER_STATIC_FILE):
        self.file_path = DataPaths.resolve(file_path)
        self.data = {}
        self._load_file()

    def _load_file(self):
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Static controller config not found: {self.file_path}")
        with open(self.file_path, "r") as f:
            self.data = json.load(f)

    def get(self, key: str) -> Optional[Any]:
        """
        Get the value of a key from the controller.json file.

        :param key: Key to look up (e.g. "max_soc")
        :return: Value if exists, else None
        """
        return self.data.get(key)
