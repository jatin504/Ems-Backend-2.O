import json
import os
from datetime import datetime
from typing import List, Dict, Optional, Union

from runtime_paths import DataPaths

METADATA_PATH = DataPaths.resolve("metadata/metadata.json")

class MetadataResolver:
    def __init__(self, metadata_path: str = METADATA_PATH):
        self.metadata_path = metadata_path
        self.data = self._load_metadata()

    def _load_metadata(self) -> dict:
        if not os.path.exists(self.metadata_path):
            raise FileNotFoundError(f"Metadata file not found: {self.metadata_path}")
        with open(self.metadata_path, "r") as f:
            return json.load(f)

    def get_latest_tariff(self, at_time: Optional[str] = None) -> Optional[float]:
        """
        Returns the latest tariff value for the current or given time block.
        """
        target_time = datetime.strptime(at_time, "%Y-%m-%d %H:%M:%S") if at_time else datetime.now()
        latest_tariff = None

        for entry in self.data.get("tariff", []):
            ts = datetime.strptime(entry["ts"], "%Y-%m-%d %H:%M:%S")
            if ts <= target_time:
                latest_tariff = entry["value"]
            else:
                break

        return latest_tariff

    def get_tariffs_between(self, start_ts: str, end_ts: str) -> List[Dict[str, Union[str, float]]]:
        """
        Returns tariff values between a given time range.
        """
        start = datetime.strptime(start_ts, "%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(end_ts, "%Y-%m-%d %H:%M:%S")

        return [
            entry for entry in self.data.get("tariff", [])
            if start <= datetime.strptime(entry["ts"], "%Y-%m-%d %H:%M:%S") <= end
        ]

    def get_all_tariffs(self) -> List[Dict[str, Union[str, float]]]:
        return self.data.get("tariff", [])

    from datetime import datetime

    def get_discharge_schedules(self) -> List[Dict[str, Union[str, float, List]]]:
        """
        Returns only active (non-expired) discharge-type schedules.
        Discards schedules where endTs is in the past.
        """
        current_time = datetime.now()
        schedules = self.data.get("schedule", {}).get("discharge", [])

        active_schedules = []
        for sched in schedules:
            end_ts_str = sched.get("endTs")
            if not end_ts_str:
                continue  # skip if no endTs
            try:
                end_time = datetime.strptime(end_ts_str, "%Y-%m-%d %H:%M:%S")
                if end_time > current_time:
                    active_schedules.append(sched)
            except ValueError as e:
                # Optionally log this malformed entry
                continue

        return active_schedules
    
    def get_active_schedules(
        self
    ) -> List[Dict[str, Union[str, float, List]]]:
        """
        Returns all active (non-expired) schedules, regardless of type.

        - If endTs is present:
            Include the schedule only if endTs is in the future.
        - If endTs is missing:
            Include the schedule because it has no expiry time.
        - Discards schedules with malformed endTs.
        """
        current_time = datetime.now()
        schedules_by_type = self.data.get("schedule", {})

        active_schedules = []

        for sched_type, schedules in schedules_by_type.items():
            for sched in schedules:
                end_ts_str = sched.get("endTs")

                # No end time means the schedule does not expire
                if not end_ts_str:
                    sched["type"] = sched_type
                    active_schedules.append(sched)
                    continue

                try:
                    end_time = datetime.strptime(
                        end_ts_str,
                        "%Y-%m-%d %H:%M:%S"
                    )

                    # Only include schedules that have not expired
                    if end_time > current_time:
                        sched["type"] = sched_type
                        active_schedules.append(sched)

                except ValueError:
                    # Skip malformed end date
                    continue

        return active_schedules


    def get_discharge_tariffs(self) -> List[float]:
        return self.data.get("discharge_tariff", [])

    def get_discharge_tariff_stats(self) -> Dict[str, float]:
        """
        Returns min, max, and average discharge tariffs.
        """
        tariffs = self.get_discharge_tariffs()
        if not tariffs:
            return {"min": 0, "max": 0, "avg": 0}

        return {
            "min": min(tariffs),
            "max": max(tariffs),
            "avg": sum(tariffs) / len(tariffs)
        }
