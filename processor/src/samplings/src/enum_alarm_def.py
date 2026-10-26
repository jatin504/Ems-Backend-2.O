import logging
from dataclasses import dataclass
from typing import Dict, Tuple
import json
from pathlib import Path

from runtime_paths import DataPaths

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


@dataclass
class EnumAlarmDef:
    mapping: Dict[int, Tuple[str, str]]  # value -> (state_name, severity)


def load_enum_alarms(config_path: Path) -> Dict[str, EnumAlarmDef]:
    """
    Loads enum alarm definitions from JSON file.
    """
    with config_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    enum_alarms: Dict[str, EnumAlarmDef] = {}

    for tag, cfg in raw.items():
        mapping = {}

        for value_str, meta in cfg.get("mapping", {}).items():
            value = int(value_str)  # JSON keys are strings
            mapping[value] = (meta["state"], meta["severity"])

        enum_alarms[tag] = EnumAlarmDef(mapping=mapping)

    logger.info("Loaded %d enum alarm definitions", len(enum_alarms))
    return enum_alarms

ENUM_ALARM_CONFIG = Path(DataPaths.resolve(
    "processor/src/samplings/alarm_configs/enum_alarms.json"
))


ENUM_ALARMS: Dict[str, EnumAlarmDef] = load_enum_alarms(ENUM_ALARM_CONFIG)