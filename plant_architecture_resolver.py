import asyncio
import json
from pathlib import Path
import logging

from runtime_paths import DataPaths

from utils.telemetry_store import TelemetryStore

logger = logging.getLogger(__name__)

class PlantArchitectureResolver:
    def __init__(self, base_dir="plant_architecture"):
        """
        Initialize the resolver with the base directory of the plant architecture.
        """
        self.base_dir = DataPaths.define(base_dir)
        if not self.base_dir.exists():
            raise FileNotFoundError(f"Base directory '{base_dir}' does not exist.")

        # device_id -> Path
        self._device_cache = {}
        # Latest aggregate values use the sampled-stage store; plant JSON remains
        # the source of tag names/units/configuration. The .env backend is shared
        # with mapped caching, but the keys/files are separate.
        self.telemetry_store = TelemetryStore("sampled")

    def _find_device_file(self, device_id):
        """
        Search for a device file named {device_id}.json in the base directory and its subdirectories.
        :param device_id: The device ID (e.g., 'block_1_inverter_inverter_1').
        :return: The Path object of the found file.
        :raises FileNotFoundError: If no file matching the device ID is found.
        """
        # Return cached path if available
        cached = self._device_cache.get(device_id)

        if cached and cached.exists():
            return cached
        
        # Recursively search for the file
        matches = list(self.base_dir.rglob(f"{device_id}.json"))
        if not matches:
            raise FileNotFoundError(f"No device file found for device ID: {device_id}")
        elif len(matches) > 1:
            raise ValueError(f"Multiple files found for device ID: {device_id}")
        return matches[0]
    
    def find_devices_by_type(self, device_type):
        """
        Search for all device files containing {device_type}.json in base directory and subdirectories.
        :param device_type: The type of device (e.g., 'inverter').
        :return: List of Path objects matching the device type.
        """
        matches = list(self.base_dir.rglob(f"*{device_type}*.json"))
        if not matches:
            raise FileNotFoundError(f"No files found for device type: {device_type}")
        return matches
    
    def get_device_id_meter_file_name(self, device_id):
        """
        Search for a device file named {device_id}.json in the base directory and its subdirectories.
        :param device_id: The device ID (e.g., 'block_1_inverter_inverter_1').
        :return: The Path object of the found file.
        :raises FileNotFoundError: If no file matching the device ID is found.
        """
        # Recursively search for the file
        matches = list(self.base_dir.rglob(f"{device_id}.json"))
        if not matches:
            raise FileNotFoundError(f"No device file found for device ID: {device_id}")
        elif len(matches) > 1:
            raise ValueError(f"Multiple files found for device ID: {device_id}")
        return str(matches[0]).split('\\')[-4]

    async def resolve_block_device(self, device_id):
        """
        Resolve a specific device file by searching for {device_id}.json.
        :param device_id: The device ID (e.g., 'block_1_inverter_inverter_1').
        :return: A dictionary representing the device's data.
        """
        return await asyncio.to_thread(self.resolve_block_device_sync, device_id)

    def resolve_block_device_sync(self, device_id):
        """
        Resolve a specific device file by searching for {device_id}.json.
        :param device_id: The device ID (e.g., 'block_1_inverter_inverter_1').
        :return: A dictionary representing the device's data.
        """
        device_file = self._find_device_file(device_id)

        if not device_file.exists():
            raise FileNotFoundError(f"Device file not found: {device_file}")

        # Load the existing plant structure/metadata first, then overlay sampled
        # values in memory. Reading through this method never rewrites this file.
        with open(device_file, 'r') as f:
            data = json.load(f)
        return self._overlay_sampled(device_id, data)

    async def previous_update_block_device(self, device_id, updates):
        """
        Update a specific device file by searching for {device_id}.json.
        :param device_id: The device ID (e.g., 'block_1_inverter_inverter_1').
        :param updates: A dictionary of updates to apply to the device file.
        """

        await asyncio.to_thread(self.previous_update_device_file, device_id, updates)
    
    def _overlay_sampled(self, device_id, data):
        """Preserve plant JSON metadata and overlay current sampled tag values."""
        # read() selects the sampled JSON file or Valkey hash from .env.
        sampled = self.telemetry_store.read(device_id)
        if sampled is None:
            # Preserve legacy bootstrap behavior. These plant values may be old;
            # a successful API response alone is therefore not proof of a cache hit.
            return data
        # Match lowercase sampled aliases to original plant key casing.
        keys = {key.lower(): key for key in data}
        for tag, value in sampled.items():
            key = keys.get(tag.lower(), tag)
            if isinstance(data.get(key), dict):
                # Keep alias, unit and datatype; replace only the current value.
                data[key]["value"] = value
            else:
                data[key] = {"value": value}
        return data

    async def update_block_devices(self, batch):
        """Accept the sampled batch produced by ResolutionSampling.final_flush().

        Offload synchronous snapshot writes to a thread. Do not change the
        independent Victoria queue or overwrite plant architecture metadata.
        """
        await asyncio.to_thread(self.update_device_cache, batch)

    def update_device_cache(self, batch):
        """Save each device's sampled result through the common backend store.

        batch contains (device_id, payload) pairs. Normalize tags to lowercase,
        merge the latest values, and log per-device failures so other devices
        can still be processed. This does not insert historical records.
        """
        for device_id, payload in batch:
            try:
                self.telemetry_store.merge(
                    device_id, {k.lower(): v for k, v in payload.items()}
                )
            except Exception:
                logger.exception("Sampled telemetry cache write failed for %s", device_id)

    def update_device_files(self, batch):
        for device_id, payload in batch:
            device_file = self._find_device_file(device_id)

            if not device_file.exists():
                continue

            with open(device_file, "r") as f:
                data = json.load(f)

            for key, value in data.items():
                json_key = key.lower()

                if json_key not in payload:
                    continue

                if isinstance(data[key], dict):
                    data[key]["value"] = payload[json_key]
                else:
                    data[key] = {"value": payload[json_key]}

            data["App_Status"] = {"value": payload.get("app_status", 500)}

            with open(device_file, "w") as f:
                json.dump(data, f, indent=2)


    def snake_to_title_case(self, key: str) -> str:
        return "_".join(word.capitalize() for word in key.split("_"))

    def previous_update_device_file(self, device_id, updates):
        """Runs inside worker process."""

        device_file = self._find_device_file(device_id)

        if not device_file.exists():
            raise FileNotFoundError(f"Device file not found: {device_file}")

        with open(device_file, "r") as f:
            data = json.load(f)

        for tag, tag_updates in updates.items():
            if tag in data:
                data[tag].update(tag_updates)
            else:
                data[tag] = tag_updates

        with open(device_file, "w") as f:
            json.dump(data, f, indent=2)

        return True

    async def resolve_alarm(self, alarm_key=None):
        """
        Resolve the centralized alarms file.
        :param alarm_key: Optional. If provided, resolve a specific alarm entry.
        :return: A dictionary representing the alarms' data.
        """
        alarm_file = self.base_dir / "alarms" / "alarms.json"

        if not alarm_file.exists():
            raise FileNotFoundError(f"Alarm file not found: {alarm_file}")

        with open(alarm_file, 'r') as f:
            alarms = json.load(f)

        if alarm_key:
            return alarms.get(alarm_key, None)
        return alarms

    async def update_alarm(self, alarm_key, updates):
        """
        Update a specific alarm entry in the centralized alarms file.
        :param alarm_key: The key of the alarm to update (e.g., 'block_1_inverter_inverter_2_Temperature_C').
        :param updates: A dictionary of updates to apply to the alarm entry.
        """
        alarm_file = self.base_dir / "alarms" / "alarms.json"

        if not alarm_file.exists():
            raise FileNotFoundError(f"Alarm file not found: {alarm_file}")

        # Read existing data
        with open(alarm_file, 'r') as f:
            alarms = json.load(f)

        # Check if the alarm key exists
        if alarm_key not in alarms:
            raise KeyError(f"Alarm key '{alarm_key}' not found in alarms file.")

        # Update the specific alarm entry while preserving existing metadata
        alarms[alarm_key]['info'].update(updates)

        # Write updated data back to the file
        with open(alarm_file, 'w') as f:
            json.dump(alarms, f, indent=2)

        # logger.info(f"Updated alarm entry: {alarm_key}")

    async def resolve_computed_device(self, device_id):
        """
        Resolve a computed device file (e.g., plant_device.json or plant_alarm.json).
        :param device_id: The computed device ID (e.g., 'plant_device', 'plant_alarm').
        :return: A dictionary representing the computed device's data.
        """
        computed_file = self.base_dir / f"{device_id}.json"

        if not computed_file.exists():
            raise FileNotFoundError(f"Computed device file not found: {computed_file}")

        with open(computed_file, 'r') as f:
            return json.load(f)

    async def update_computed_device(self, device_id, updates):
        """
        Update a computed device file (e.g., plant_device.json or plant_alarm.json).
        :param device_id: The computed device ID (e.g., 'plant_device', 'plant_alarm').
        :param updates: A dictionary of updates to apply to the computed device file.
        """
        computed_file = self.base_dir / f"{device_id}.json"

        if not computed_file.exists():
            raise FileNotFoundError(f"Device file not found: {computed_file}")

        # Read existing data
        with open(computed_file, 'r') as f:
            data = json.load(f)

        # Apply updates while preserving existing metadata
        for tag, tag_updates in updates.items():
            if tag in data:
                # Preserve existing metadata and only update specified keys
                data[tag].update(tag_updates)
            else:
                # If the tag doesn't exist, add it as a new entry
                data[tag] = tag_updates

        # Write updated data back to the file
        with open(computed_file, 'w') as f:
            json.dump(data, f, indent=2)

        logger.info(f"Updated computed device: {computed_file}")

    def update_computed_device_sync(self, device_id, updates):
        """
        Update a computed device file (e.g., plant_device.json or plant_alarm.json).
        :param device_id: The computed device ID (e.g., 'plant_device', 'plant_alarm').
        :param updates: A dictionary of updates to apply to the computed device file.
        """
        computed_file = self.base_dir / f"{device_id}.json"

        if not computed_file.exists():
            raise FileNotFoundError(f"Device file not found: {computed_file}")

        # Read existing data
        with open(computed_file, 'r') as f:
            data = json.load(f)

        # Apply updates while preserving existing metadata
        for tag, tag_updates in updates.items():
            if tag in data:
                # Preserve existing metadata and only update specified keys
                data[tag].update(tag_updates)
            else:
                # If the tag doesn't exist, add it as a new entry
                data[tag] = tag_updates

        # Write updated data back to the file
        with open(computed_file, 'w') as f:
            json.dump(data, f, indent=2)

        logger.info(f"Updated computed device: {computed_file}")