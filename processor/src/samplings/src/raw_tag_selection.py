"""Read raw flags from the same runtime tag definitions used by sampling."""
import json
from pathlib import Path
import re

from runtime_paths import DataPaths

_METRIC_NAME = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*\Z")


def load_raw_tags(definitions_path=None):
    """Return device_type -> exact alias -> VM metric name, cached by the caller.

    Only the JSON boolean raw_insertaion=true opts in. Runtime config changes
    take effect on process restart, never through per-reading filesystem reads.
    """
    folder = (Path(definitions_path) if definitions_path is not None else
              DataPaths.root() / "processor/src/samplings/tag_definitions")
    if not folder.is_dir():
        raise ValueError("Raw tag definitions folder not found: " + str(folder))

    selected = {}
    for path in folder.glob("*.json"):
        definitions = json.loads(path.read_text(encoding="utf-8-sig"))
        if not isinstance(definitions, dict):
            raise ValueError("Tag definitions must be an object: " + str(path))
        aliases = {}
        for alias, rule in definitions.items():
            if not isinstance(rule, dict) or rule.get("raw_insertaion") is not True:
                continue
            metric = alias.lower()
            if not _METRIC_NAME.fullmatch(metric):
                raise ValueError(f"Invalid raw metric alias in {path}: {alias}")
            aliases[alias] = metric
        selected[path.stem.lower()] = aliases
    return selected
