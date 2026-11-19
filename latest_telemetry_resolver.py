from runtime_paths import DataPaths
from utils.telemetry_store import TelemetryStore


class LatestTelemetryResolver:
    def __init__(self, base_dir="latest_telemetry"):
        # The selected store preserves JSON as the default architecture.
        self.base_dir = DataPaths.resolve(base_dir)
        self.store = TelemetryStore("mapped", self.base_dir)

    def get_latest_device_data(self, device_id: str) -> dict:
        """Read mapped tags from JSON or Valkey while retaining the error envelope."""
        try:
            # Mapped-stage reader: JSON/Valkey selection belongs to TelemetryStore.
            # Missing data retains this resolver's existing error-response shape.
            data = self.store.read(device_id)
            return data if data is not None else {"error": f"No mapped telemetry for {device_id}"}
        except Exception:
            return {"error": f"Failed to read telemetry for {device_id}"}
