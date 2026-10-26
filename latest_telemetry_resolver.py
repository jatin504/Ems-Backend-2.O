import json
from pathlib import Path

from runtime_paths import DataPaths

class LatestTelemetryResolver:
    def __init__(self, base_dir="latest_telemetry"):
        self.base_dir = DataPaths.resolve(base_dir)

    def get_latest_device_data(self, device_id: str) -> dict:
        """
        Read and return latest telemetry data (including original registers) 
        from latest_telemetry/<device_id>.json
        """
        file_path = self.base_dir / f"{device_id}.json"

        if not file_path.exists():
            return {"error": f"{device_id}.json not found"}

        try:
            with open(file_path, "r") as f:
                return json.load(f)["data"]
        except Exception as e:
            return {"error": f"Failed to read {device_id}.json: {e}"}
