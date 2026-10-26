import os
import json
from pathlib import Path
from device_library.resolver import DeviceLibraryResolver
import logging
import shutil
import re

from runtime_paths import DataPaths

logger = logging.getLogger(__name__)

class PlantArchitect:
    def __init__(self, config_path="config/device_config.json", base_dir="plant_architecture"):
        self.config = self._load_config(config_path)
        self.base_dir = DataPaths.resolve(base_dir)
        self.resolver = DeviceLibraryResolver()
        self.computed_device_dir = DataPaths.resolve("device_library/computed/computed_device")
        self.computed_tags_dir = DataPaths.resolve("device_library/computed/computed_tags")

        # convenience
        self._all_meter_ids = [m["device_id"] for m in self.config.get("meters", [])]

    def _load_config(self, config_path):
        config_path = DataPaths.resolve(config_path)
        with open(config_path, 'r') as f:
            return json.load(f)

    def build_architecture(self):
        """Main method to build the plant architecture."""
        self._recreate_plant_architecture()
        self._create_base_structure()
        self._process_meters()
        self._process_devices()
        self._process_computed_devices()
        self._process_computed_tags()
        self._centralize_alarms()
        self._handle_virtual_meters()

    def _recreate_plant_architecture(self):
        if self.base_dir.exists():
            logger.info(f"Deleting existing plant architecture: {self.base_dir}")
            shutil.rmtree(self.base_dir)
        logger.info(f"Creating new plant architecture: {self.base_dir}")
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _create_base_structure(self):
        """Create base directories."""
        self.base_dir.mkdir(exist_ok=True)
        (self.base_dir / "alarms").mkdir(exist_ok=True)
        (self.base_dir / "events").mkdir(exist_ok=True)
        # top-level 'plant' folder (as per your layout)
        (self.base_dir / "plant").mkdir(exist_ok=True)

    # -------------------------
    # Helpers for path building
    # -------------------------
    def _extract_group_from_device_id(self, device_id: str, group: str) -> int | None:
        """Extract an index like pcs_1, ess_2 etc. from device_id."""
        m = re.search(rf"{re.escape(group)}_(\d+)", device_id)
        return int(m.group(1)) if m else None

    def _find_meter_for_block(self, block_num: int) -> str | None:
        """Return meter_id that owns this block, if any."""
        for meter in self.config.get('meters', []):
            if block_num in meter.get("blocks", []):
                return meter['device_id']
        return None

    def _preferred_meter_for_or_default(self) -> str | None:
        """Return the single meter id if exactly one is configured, else None."""
        return self._all_meter_ids[0] if len(self._all_meter_ids) == 1 else None

    def _resolve_paths_for_device(self, device: dict) -> tuple[Path, Path, Path]:
        """
        Return (block_dir, device_type_dir, device_file) according to the rules:

        - block != None:
              block_dir = (meter/<block_X>) if block belongs to a meter else <block_X> at root
        - block is None and pcs != None:
              if exactly one meter in config:
                  parent = <that meter id>
              else:
                  parent = "plant"     # fallback
              block_dir = parent / f"pcs_{pcsIndex}"
        - block is None and pcs is None:
              default parent = "plant"
              SPECIAL: if deviceType == 'pcs_controller' and exactly one meter:
                       parent = <that meter id> / "pcs_controller"
        """
        device_id = device['device_id']
        device_type = device['deviceType'].lower()

        # 1) Blocked devices → block_X path, nested under owning meter if any
        if device.get('block') is not None:
            blk = device['block']
            block_folder = f"block_{blk}"
            meter_id = self._find_meter_for_block(blk)
            if meter_id:
                block_dir = self.base_dir / meter_id / block_folder
            else:
                block_dir = self.base_dir / block_folder

        # 2) PCS devices without block → pcs_{i} under meter (if single meter) else under plant
        elif device.get('pcs') is not None:
            pcs_index = device.get('pcs') or self._extract_group_from_device_id(device_id, 'pcs')
            if pcs_index is None:
                raise ValueError(f"PCS index not found for device_id={device_id}")

            single_meter = self._preferred_meter_for_or_default()
            parent = single_meter if single_meter else "plant"
            block_dir = self.base_dir / parent / f"pcs_{pcs_index}"

        # 3) No block, no pcs
        else:
            # default parent = 'plant'
            block_dir = self.base_dir / "plant"

            # SPECIAL: pcs_controller goes inside meter if we have exactly one meter
            if device_type == "pcs_controller":
                single_meter = self._preferred_meter_for_or_default()
                if single_meter:
                    block_dir = self.base_dir / single_meter / "pcs_controller"

        device_type_dir = block_dir / device_type
        device_file = device_type_dir / f"{device_id}.json"
        return block_dir, device_type_dir, device_file

    # -------------------------
    # Meters
    # -------------------------
    def _process_meters(self):
        """
        Create each meter's own folder at root: <meter_id>/
        (We NO LONGER write duplicate meter JSON files under 'plant/'.)
        """
        for meter in self.config['meters']:
            meter_id = meter['device_id']

            # meter folder at root
            meter_root_dir = self.base_dir / meter_id
            meter_root_dir.mkdir(exist_ok=True)

            # move any precreated block dirs under the meter folder
            if meter.get("blocks"):
                for block_num in meter["blocks"]:
                    block_dir = self.base_dir / f"block_{block_num}"
                    if block_dir.exists():
                        shutil.move(str(block_dir), str(meter_root_dir))
                        logger.info(f"Moved block_{block_num} to meter {meter_id}")

            # handle nested/constituent meters if any
            if meter.get("meters"):
                for constituent_meter in meter["meters"]:
                    self._add_constituent_meter(meter_root_dir, constituent_meter)

    def _add_constituent_meter(self, parent_dir, meter_id):
        """Recursively add a constituent meter and its structure."""
        for meter in self.config['meters']:
            if meter['device_id'] == meter_id:
                meter_dir = parent_dir / meter_id
                meter_dir.mkdir(exist_ok=True)
                logger.info(f"Created constituent meter directory: {meter_dir}")

                if meter.get("blocks"):
                    for block_num in meter["blocks"]:
                        block_dir = self.base_dir / f"block_{block_num}"
                        if block_dir.exists():
                            shutil.move(str(block_dir), str(meter_dir))
                            logger.info(f"Moved block_{block_num} to constituent meter {meter_id}")

                if meter.get("meters"):
                    for constituent_meter in meter["meters"]:
                        self._add_constituent_meter(meter_dir, constituent_meter)

    # -------------------------
    # Physical devices
    # -------------------------
    def _process_devices(self):
        """Process physical devices and store their tags."""
        for device in self.config['devices']:
            block_dir, device_type_dir, device_file = self._resolve_paths_for_device(device)

            device_type_dir.mkdir(parents=True, exist_ok=True)
            if device_file.exists():
                continue

            # Load physical tags from device library
            mapping = self.resolver.get_mapping(
                device['deviceType'], device['make'], device['model']
            )

            # Create a new dictionary with Time_Stamp at the top
            formatted_tags = {
                "Time_Stamp": {
                    'dataType': 'timestamp',
                    'value': None,
                    'type': 'analog'
                }
            }

            # Add other tags under their alias keys
            formatted_tags.update({
                tag_info['alias']: {**tag_info, 'value': None}
                for tag_info in mapping.values()
            })

            # Save device file
            with open(device_file, 'w') as f:
                json.dump(formatted_tags, f, indent=2)
            logger.info(f"Created device file: {device_file}")

    # -------------------------
    # Computed devices (top-level)
    # -------------------------
    def _process_computed_devices(self):
        """Process computed devices (e.g., plant_device, plant_alarm)."""
        for comp_device in self.config['computed_devices']:
            device_file = self.base_dir / f"{comp_device['device_id']}.json"
            if device_file.exists():
                continue

            # Load computed tags for the device
            computed_tags = self._load_computed_device_tags(comp_device)

            # Create a new dictionary with Time_Stamp at the top
            formatted_tags = {
                "Time_Stamp": {
                    'dataType': 'timestamp',
                    'value': None,
                    'type': 'analog'
                }
            }

            # Format tags under their alias keys
            formatted_tags.update({
                tag_info['alias']: {**tag_info, 'value': None}
                for tag_info in computed_tags.values()
            })

            # Save computed device file
            with open(device_file, 'w') as f:
                json.dump(formatted_tags, f, indent=2)
            logger.info(f"Created computed device: {device_file}")

    def _load_computed_device_tags(self, comp_device):
        """Load computed tags for a specific computed device."""
        model_file = self.computed_device_dir / f"{comp_device['model'].lower()}.json"
        if not model_file.exists():
            return {}
        with open(model_file, 'r') as f:
            return json.load(f)

    # -------------------------
    # Merge computed tags into physical device files
    # -------------------------
    def _process_computed_tags(self):
        """Merge computed tags into their respective device files."""
        for comp_tag in self.config['computed_tags']:
            computed_tags = self._load_computed_tags(comp_tag)

            for device in self.config['devices']:
                if (device['deviceType'] == comp_tag['deviceType']):
                    block_dir, device_type_dir, device_file = self._resolve_paths_for_device(device)

                    # Ensure the block directory exists
                    if not block_dir.exists():
                        logger.warning(f"Block directory not found: {block_dir}")
                        continue

                    with open(device_file, 'r+') as f:
                        data = json.load(f)
                        f.seek(0)

                        # Add computed tags under their alias keys
                        for tag_name, tag_info in computed_tags.items():
                            alias = tag_info['alias']
                            if alias not in data:
                                data[alias] = {**tag_info, 'value': None}

                        json.dump(data, f, indent=2)
                        f.truncate()

            logger.info(f"Added computed tags for: {comp_tag['deviceType']}")

    def _load_computed_tags(self, comp_tag_config):
        """Load computed tags for a specific device type."""
        tags_file = self.computed_tags_dir / f"{comp_tag_config['deviceType'].lower()}.json"
        if not tags_file.exists():
            return {}
        with open(tags_file, 'r') as f:
            return json.load(f)

    # -------------------------
    # Alarms (centralized)
    # -------------------------
    def _centralize_alarms(self):
        """Centralize alarms into alarms/alarms.json and plant_alarm.json."""
        alarm_file = self.base_dir / "alarms/alarms.json"
        event_file = self.base_dir / "events/events.json"
        plant_alarm_file = self.base_dir / "plant_alarm.json"

        centralized_alarms = {}
        # centralized_events = {}
        plant_alarms = {}

        for device in self.config['devices']:
            block_dir, device_type_dir, device_file = self._resolve_paths_for_device(device)

            if not device_file.exists():
                logger.warning(f"Device file missing: {device_file}")
                continue

            with open(device_file, 'r+') as f:
                data = json.load(f)
                alarms = {
                    tag: info for tag, info in data.items()
                    if isinstance(info, dict) and info.get('type') == 'alarm' and not info.get("isEvent", False)
                }

                # events = {
                #     tag: info for tag, info in data.items()
                #     if isinstance(info, dict) and info.get('type') == 'alarm' and info.get("isEvent", False)
                # }

                if not alarms:
                    continue

                for tag, info in alarms.items():
                    alarm_key = f"{device['device_id']}_{tag}"
                    centralized_alarms[alarm_key] = {
                        'device_id': device['device_id'],
                        'tag': tag,
                        'info': info
                    }

                    # Add to plant_alarm.json under alias key
                    alias = info['alias']
                    if alias not in plant_alarms:
                        plant_alarms[alias] = {**info, 'value': None, 'Time_Stamp': None}

                # Remove alarms from device files
                for tag in alarms:
                    data.pop(tag)

                f.seek(0)
                json.dump(data, f, indent=2)
                f.truncate()

        # Save centralized alarms
        with open(alarm_file, 'w') as f:
            json.dump(centralized_alarms, f, indent=2)
        logger.info(f"Centralized alarms to: {alarm_file}")

        # Save events file
        with open(event_file, 'w') as f:
            json.dump({}, f, indent=2)
        logger.info(f"Centralized alarms to: {alarm_file}")

        # Save plant_alarm.json
        with open(plant_alarm_file, 'w') as f:
            json.dump(plant_alarms, f, indent=2)
        logger.info(f"Wrote plant alarms to: {plant_alarm_file}")

    # -------------------------
    # Virtual meters
    # -------------------------
    def _handle_virtual_meters(self):
        """
        Handle virtual meters by checking if they contain other meters.
        If yes, move the constituent meters' folders into the virtual meter's folder.
        Replace the virtual meter directory if it already exists.
        """
        for virtual_meter in self.config.get("virtual", []):
            virtual_meter_id = virtual_meter["device_id"]
            constituent_meters = virtual_meter.get("meters", [])

            # Define the virtual meter directory
            virtual_meter_dir = self.base_dir / virtual_meter_id

            # Remove the virtual meter directory if it already exists
            if virtual_meter_dir.exists():
                shutil.rmtree(virtual_meter_dir)
                logger.info(f"Deleted existing virtual meter directory: {virtual_meter_dir}")

            # Create the virtual meter directory
            virtual_meter_dir.mkdir(exist_ok=True)
            logger.info(f"Created virtual meter directory: {virtual_meter_dir}")

            # If the virtual meter contains other meters, move their folders into this directory
            if constituent_meters:
                for meter_id in constituent_meters:
                    meter_folder = self.base_dir / meter_id
                    if meter_folder.exists():
                        shutil.move(str(meter_folder), str(virtual_meter_dir))
                        logger.info(f"Moved meter folder '{meter_id}' into virtual meter '{virtual_meter_id}'")
                    else:
                        logger.warning(f"Meter folder '{meter_id}' not found for virtual meter '{virtual_meter_id}'")
