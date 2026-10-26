import json
from pathlib import Path

from runtime_paths import DataPaths

STATUS_FILE = DataPaths.resolve("status/ems_status.json")

class FileService:

    @staticmethod
    async def write_status(data: dict):
        STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)

        if not STATUS_FILE.exists():
            STATUS_FILE.write_text(json.dumps({}, indent=2))

        STATUS_FILE.write_text(json.dumps(data, indent=2))

    @staticmethod
    async def update_flag(flag_name, flag_value):
        try:
            current = json.loads(STATUS_FILE.read_text())
        except:
            current = {}

        current["flags"] = current.get("flags", {})
        current["flags"][flag_name] = flag_value

        STATUS_FILE.write_text(json.dumps(current, indent=2))
