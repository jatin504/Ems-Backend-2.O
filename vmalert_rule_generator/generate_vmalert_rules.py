"""
VMAlert Recording Rule Generator & Reloader (Self-Contained Module)
===================================================================
Automates the generation of VictoriaMetrics vmalert recording rules from
device JSON configs, ensuring all raw metrics are rolled up into 15-minute
aggregations with zero manual Prometheus rule authoring.

Classes:
1. MetricNormalizer        - Sanitizes and standardizes device parameter names to Prometheus metric names.
2. DeviceConfigLoader      - Discovers and parses device JSON definitions (e.g. pcs.json, bank.json).
4. VMAlertRuleBuilder      - Constructs valid YAML recording rules using PromQL window aggregations.
5. VMAlertReloader         - Triggers hot-reload HTTP endpoint on VMAlert service.
6. VMAlertRuleGenerator    - Central coordinator driving generation, output formatting, and reload.
"""

import argparse
import json
import logging
import os
import re
import sys
import time
import tempfile
import hashlib
from pathlib import Path
from typing import Dict, List, Optional
import requests
from dotenv import load_dotenv
import yaml

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
# Preserve the approved generator, loading EMS settings from its repository root.
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv()


def env_path(name):
    """Resolve relative paths from the project root."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing {name} in .env")

    path = Path(value).expanduser()
    return (path if path.is_absolute() else PROJECT_ROOT / path).resolve()

def env_bool(name, default=False):
    """Reject invalid boolean settings rather than silently changing behavior."""
    value = os.getenv(name, str(default)).strip().lower()
    if value not in {"true", "false", "1", "0"}:
        raise ValueError(f"{name} must be true or false")
    return value in {"true", "1"}

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s:%(lineno)d | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("VMAlertGenerator")


class MetricNormalizer:
    """Normalizes and standardizes device parameter names into valid Prometheus metric names."""

    @staticmethod
    def normalize(param_name: str, param_cfg: dict, prefix: str = "") -> str:
        if "metric" in param_cfg:
            return param_cfg["metric"]
        cleaned = re.sub(r'[^a-zA-Z0-9_]+', '_', param_name).strip('_').lower()
        if cleaned.startswith(prefix):
            return cleaned
        return f"{prefix}{cleaned}"


class DeviceConfigLoader:
    """Loads and aggregates active metric definitions across all device JSON files."""

    def __init__(self, devices_dir: Path, fallback_config: Path):
        self.devices_dir = devices_dir
        self.fallback_config = fallback_config

    def load_device_metrics(self) -> List[dict]:
        """Scans devices_dir for *.json files and collects metrics with rules.enabled=true."""
        # The configured folder is the single source of device tag definitions.
        if not self.devices_dir.exists():
            raise FileNotFoundError(f"Devices directory not found: {self.devices_dir}")

        collected = {}  # metric_name -> dict

        for json_file in sorted(self.devices_dir.glob("*.json")):
            device_name = json_file.stem
            try:
                with open(json_file, "r", encoding="utf-8-sig") as f:
                    data = json.load(f)
            except Exception as err:
                # Do not replace active rules with an incomplete result after a bad edit.
                raise ValueError(f"Failed to read device config {json_file}: {err}") from err

            if not isinstance(data, dict):
                raise ValueError(f"Device JSON must contain a tag object: {json_file}")
            for param_name, param_cfg in data.items():
                if not isinstance(param_cfg, dict):
                    continue

                rules = param_cfg.get("rules", {})
                if not isinstance(rules, dict) or not isinstance(rules.get("enabled", False), bool):
                    raise ValueError(f"{json_file.name}/{param_name}: rules.enabled must be true or false")
                if rules.get("enabled", False) is not True:
                    continue

                metric = MetricNormalizer.normalize(param_name, param_cfg)
                raw_agg = param_cfg.get("aggregation", "avg").lower()
                if not re.fullmatch(r"[a-zA-Z_:][a-zA-Z0-9_:]*", metric):
                    raise ValueError(f"Invalid metric name for {json_file.name}/{param_name}: {metric}")
                if raw_agg not in VMAlertRuleBuilder.AGGREGATION_MAP:
                    raise ValueError(f"Unsupported aggregation for {json_file.name}/{param_name}: {raw_agg}")

                if metric not in collected:
                    collected[metric] = {
                        "metric": metric,
                        "param_name": param_name,
                        "aggregation": raw_agg,
                        "devices": [device_name],
                        "sampling_resolution": param_cfg.get("resolution", 59),
                    }
                else:
                    if VMAlertRuleBuilder.AGGREGATION_MAP[collected[metric]["aggregation"]] != VMAlertRuleBuilder.AGGREGATION_MAP[raw_agg]:
                        raise ValueError(f"Conflicting aggregations for shared metric {metric}")
                    if device_name not in collected[metric]["devices"]:
                        collected[metric]["devices"].append(device_name)

        # An intentionally disabled directory means zero rules, never fallback ON.

        logger.info(
            f"Discovered {len(collected)} active parameter(s) across device configs in {self.devices_dir.name}/"
        )
        return list(collected.values())

class VMAlertRuleBuilder:
    """Constructs valid YAML recording rules using PromQL window aggregations."""

    AGGREGATION_MAP = {
        "avg": {"fn": "avg_over_time", "suffix": "avg"},
        "average": {"fn": "avg_over_time", "suffix": "avg"},
        "last": {"fn": "last_over_time", "suffix": "last"},
        "state": {"fn": "last_over_time", "suffix": "last"},
        "max": {"fn": "max_over_time", "suffix": "max"},
        "maximum": {"fn": "max_over_time", "suffix": "max"},
        "min": {"fn": "min_over_time", "suffix": "min"},
        "minimum": {"fn": "min_over_time", "suffix": "min"},
        "sum": {"fn": "sum_over_time", "suffix": "sum"},
    }

    @classmethod
    def build_rule(cls, res_name: str, res_window: str, param: dict) -> str:
        metric = param.get("metric")
        if not metric:
            return ""

        raw_agg = param.get("aggregation", "avg").lower()
        if raw_agg not in cls.AGGREGATION_MAP:
            raise ValueError(f"Unsupported aggregation {raw_agg}; counter increase cannot preserve continuity across monthly partitions")
        agg_cfg = cls.AGGREGATION_MAP[raw_agg]

        param_label = param.get("param_name", metric)
        devices_str = ", ".join(param.get("devices", []))
        record_name = f"{metric}:{agg_cfg['suffix']}_{res_name}"
        # EMS also stores raw_1_sec under the same metric name: select only
        # the existing minute data, preserving the approved rollup semantics.
        selector = f'{metric}{{resolution="1_min"'
        database = os.getenv("PRIMARY_DB_NAME")
        if database:
            selector += ",database=" + json.dumps(database)
        selector += "}"
        # Calendar buckets are [start, end): exclude an exact midnight sample
        # from the previous month. Explicit alignment keeps query windows calendar-based.
        expr = f"{agg_cfg['fn']}({selector}[{res_window}] offset 1ms)"

        return (
            f"      # Parameter: {param_label} (Devices: {devices_str} | Aggregation: {raw_agg})\n"
            f'      - record: "{record_name}"\n'
            f"        expr: {json.dumps(expr)}\n"
            f"        labels:\n"
            f'          aggregation: "{raw_agg}"\n'
            f'          resolution: "{res_name}"\n'
        )

    @classmethod
    def build_yaml_document(cls, resolutions: List[dict], enabled_params: List[dict]) -> str:
        header = [
            "# ================================================================",
            "# VMAlert Recording Rules — AUTO-GENERATED FROM DEVICE JSON CONFIGS",
            f"# Generated At: {time.strftime('%Y-%m-%d %H:%M:%S %Z')}",
            "# DO NOT EDIT DIRECTLY — Run generate_vmalert_rules.py to update",
            "# ================================================================",
            "groups:",
        ]

        groups = []
        for res in resolutions:
            res_name = res["name"]
            res_interval = res["interval"]
            res_window = res["window"]

            group_lines = [
                f'  - name: bess_rollups_{res_name}',
                f'    interval: {res_interval}',
                # Match VM's normal 30-second visibility delay before alignment;
                # this may evaluate the preceding completed bucket, never a partial one.
                '    eval_delay: 30s',
                '    eval_alignment: true',
                '    rules:',
            ]

            rule_count = 0
            for param in sorted(enabled_params, key=lambda x: x["metric"]):
                rule_snippet = cls.build_rule(res_name, res_window, param)
                if rule_snippet:
                    group_lines.append(rule_snippet.rstrip())
                    rule_count += 1

            if rule_count > 0:
                groups.append("\n".join(group_lines))

        return "\n".join(header) + "\n\n" + "\n\n".join(groups) + "\n"


class VMAlertReloader:
    """Triggers hot reload on VMAlert service via HTTP POST."""

    def __init__(self, reload_url: str):
        self.reload_url = reload_url

    def trigger_reload(self) -> bool:
        if not self.reload_url:
            logger.info("No reload URL specified; skipping hot reload.")
            return False

        logger.info(f"Triggering VMAlert configuration reload at {self.reload_url}...")
        try:
            resp = requests.post(self.reload_url, timeout=5)
            if resp.status_code in (200, 204):
                logger.info("VMAlert hot reload successful! Recording rules are now active.")
                return True
            logger.warning(f"VMAlert reload returned HTTP {resp.status_code}: {resp.text}")
        except requests.RequestException as err:
            logger.warning(f"Could not reach VMAlert reload endpoint: {err}")
        return False


class VMAlertRuleGenerator:
    """Central orchestrator driving discovery, YAML construction, and deployment."""

    DEFAULT_RESOLUTIONS = [
        {"name": "15m", "interval": "15m", "window": "15m"},
    ]
    # A comment in the existing YAML survives a crash between writing and reload.
    # No separate state file is needed; YAML readers ignore this marker.
    RELOAD_PENDING = "# VMAlert reload pending\n"

    def __init__(
        self,
        devices_dir: Path,
        fallback_config: Path,
        output_path: Path,
        vm_url: str = "http://localhost:8428",
        reload_url: str = "http://localhost:8880/-/reload",
        resolutions: Optional[List[dict]] = None
    ):
        self.loader = DeviceConfigLoader(devices_dir, fallback_config)
        self.vm_url = vm_url.rstrip("/")
        self.reloader = VMAlertReloader(reload_url)
        self.output_path = output_path.resolve()
        # Preserve an explicit programmatic override; otherwise use --config JSON.
        # None and [] differ: an empty schedule means every group is disabled.
        self.resolutions = resolutions
        self._last_input_hash = None
        self._last_file_hash = None
        self._parameter_state = {}

    @staticmethod
    def _hash(value):
        """Stable comparison independent of dictionary ordering or JSON whitespace."""
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    @staticmethod
    def _document(text):
        """Read actual rule content, ignoring comments/timestamps and list ordering."""
        try:
            document = yaml.safe_load(text)
            if not isinstance(document, dict) or not isinstance(document.get("groups"), list):
                return None
            for group in document["groups"]:
                if not isinstance(group.get("name"), str) or not isinstance(group.get("rules"), list):
                    return None
                if any(not isinstance(rule, dict) or not isinstance(rule.get("record"), str)
                       for rule in group["rules"]):
                    return None
                group["rules"] = sorted(group["rules"], key=lambda rule: json.dumps(rule, sort_keys=True))
            document["groups"].sort(key=lambda group: group["name"])
            json.dumps(document, sort_keys=True)  # Treat unsupported YAML value types as invalid output.
            return document
        except (yaml.YAMLError, KeyError, TypeError, AttributeError):
            return None  # Repair missing/invalid output from the validated source configs.

    @classmethod
    def _parameters_from_document(cls, document):
        """Reconstruct per-metric rule signatures from YAML, including after restart."""
        parameters = {}
        for group in (document or {}).get("groups", []):
            for rule in group["rules"]:
                metric = rule["record"].rsplit(":", 1)[0]
                parameters.setdefault(metric, []).append({
                    "group": {key: value for key, value in group.items() if key != "rules"},
                    "rule": rule,
                })
        return {metric: cls._hash(rules) for metric, rules in parameters.items()}

    def _write_output(self, text):
        """Publish complete YAML atomically, preserving the last good file on failure."""
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(dir=self.output_path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(text)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.output_path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def _reload_if_pending(self, text, no_reload):
        """Retry deployment of a changed file without regenerating unchanged rules."""
        if not text.startswith(self.RELOAD_PENDING) or no_reload:
            return True
        if not self.reloader.trigger_reload():
            return False  # Keep the marker so the next poll/restart retries this change.
        if self.output_path.read_text(encoding="utf-8-sig") != text:
            logger.warning("Rules file changed during reload; reconciling on the next check")
            return False
        self._write_output(text[len(self.RELOAD_PENDING):])
        self._last_file_hash = self._hash(self.output_path.read_text(encoding="utf-8-sig"))
        return True

    def _enabled_resolutions(self) -> List[dict]:
        """Read the master schedule on each generation and select enabled groups."""
        if self.resolutions is None:
            config = json.loads(self.loader.fallback_config.read_text(encoding="utf-8-sig"))
            configured = config.get("resolutions", self.DEFAULT_RESOLUTIONS)
        else:
            configured = self.resolutions
        if not isinstance(configured, list):
            raise ValueError("resolutions must be a JSON list")
        enabled, names = [], set()
        for resolution in configured:
            if not isinstance(resolution, dict) or not isinstance(resolution.get("enabled", True), bool):
                raise ValueError("Each resolution needs a boolean enabled setting")
            if not resolution.get("enabled", True):
                continue
            name = resolution.get("name", "")
            if not isinstance(name, str) or not re.fullmatch(r"[a-zA-Z0-9_]+", name) or name in names:
                raise ValueError("Enabled resolution names must be valid and unique")
            for key in ("interval", "window"):
                value = resolution.get(key, "")
                if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]*(?:ms|s|m|h|d|w|y)", value):
                    raise ValueError(f"Invalid {key} for resolution {name}; use a duration such as 15m")
            # Monthly partitions support non-overlapping UTC calendar buckets.
            # Sliding/unaligned windows would combine different month series.
            duration = re.fullmatch(r"([1-9][0-9]*)(s|m|h|d)", resolution["interval"])
            seconds = int(duration[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[duration[2]] if duration else 0
            if resolution["window"] != resolution["interval"] or seconds < 60 or seconds > 86400 or 86400 % seconds:
                raise ValueError(f"{name}: window must equal interval and divide one UTC day (e.g. 5m, 15m, 30m, 1h, 1d)")
            names.add(name)
            enabled.append(resolution)
        return enabled

    def generate(self, no_reload: bool = False) -> bool:
        # Resolve and validate before replacing the existing rule file.
        resolutions = self._enabled_resolutions()
        enabled_params = self.loader.load_device_metrics()

        # Only fields affecting recording rules participate. Legacy sampling
        # resolution and device/comment names do not change the rule expressions.
        input_hash = self._hash({
            "parameters": sorted(({"metric": p["metric"], "aggregation": p["aggregation"]}
                                  for p in enabled_params), key=lambda p: p["metric"]),
            "resolutions": sorted(({key: r[key] for key in ("name", "interval", "window")}
                                   for r in resolutions), key=lambda r: r["name"]),
            "database": os.getenv("PRIMARY_DB_NAME"),
        })
        try:
            previous_text = self.output_path.read_text(encoding="utf-8-sig")
        except FileNotFoundError:
            previous_text = None
        file_hash = self._hash(previous_text)
        if input_hash == self._last_input_hash and file_hash == self._last_file_hash:
            action = "retrying pending reload" if previous_text.startswith(self.RELOAD_PENDING) and not no_reload else "reload skipped"
            logger.info("Parameters: added=0 changed=0 removed=0 skipped=%d; rules unchanged, generation/write skipped; %s",
                        len(self._parameter_state), action)
            return self._reload_if_pending(previous_text, no_reload)

        yaml_content = VMAlertRuleBuilder.build_yaml_document(resolutions, enabled_params)
        if not resolutions or not enabled_params:
            # Replace stale rules even when every schedule is OFF; reload clears them.
            yaml_content = "# All configured resolution groups are disabled.\ngroups: []\n"

        previous_document = self._document(previous_text) if previous_text is not None else None
        desired_document = self._document(yaml_content)
        if desired_document is None:
            raise ValueError("Generated rules are invalid; keeping the existing rules file")
        previous_parameters = self._parameters_from_document(previous_document)
        desired_parameters = self._parameters_from_document(desired_document)
        added = len(desired_parameters.keys() - previous_parameters.keys())
        removed = len(previous_parameters.keys() - desired_parameters.keys())
        shared = desired_parameters.keys() & previous_parameters.keys()
        changed = sum(desired_parameters[key] != previous_parameters[key] for key in shared)
        logger.info("Parameters: added=%d changed=%d removed=%d skipped=%d",
                    added, changed, removed, len(shared) - changed)

        if previous_document == desired_document:
            # Reconcile against disk on startup; a fresh process must not rewrite
            # identical rules merely because it has no in-memory hash yet.
            self._last_input_hash, self._last_file_hash = input_hash, file_hash
            self._parameter_state = desired_parameters
            action = "retrying pending reload" if previous_text.startswith(self.RELOAD_PENDING) and not no_reload else "reload skipped"
            logger.info("Rules unchanged; file write skipped; %s", action)
            return self._reload_if_pending(previous_text, no_reload)

        text = yaml_content if no_reload else self.RELOAD_PENDING + yaml_content
        self._write_output(text)
        self._last_input_hash, self._last_file_hash = input_hash, self._hash(text)
        self._parameter_state = desired_parameters

        total_rules = len(enabled_params) * len(resolutions)
        logger.info(
            f"Successfully generated {total_rules} recording rules ({len(enabled_params)} params x "
            f"{len(resolutions)} resolution) -> {self.output_path.name}"
        )

        if no_reload:
            logger.info("Automatic reload skipped (--no-reload specified).")
        return self._reload_if_pending(text, no_reload)


def main(service_default=False):
    """Generate once for the legacy CLI, or periodically for the root launcher."""
    default_config = env_path("VMALERT_CONFIG_PATH")
    default_devices = env_path("VMALERT_DEVICES_DIR")
    default_output = env_path("VMALERT_RULES_FILE")

    default_vm = os.getenv("VMALERT_DATASOURCE_URL")
    default_reload = os.getenv("VMALERT_RELOAD_URL")

    parser = argparse.ArgumentParser(description="VMAlert Recording Rule Generator")
    parser.add_argument("--config", default=str(default_config), help="Master resolution schedule JSON")
    parser.add_argument("--devices-dir", default=str(default_devices), help="Directory with per-device JSONs")
    parser.add_argument("--output", default=str(default_output), help="Output path for alerts.yml")
    parser.add_argument("--vm-url", default=default_vm, help="VictoriaMetrics endpoint URL")
    parser.add_argument("--reload-url", default=default_reload, help="VMAlert reload URL")
    parser.add_argument("--no-reload", action="store_true", default=not env_bool("VMALERT_AUTO_RELOAD", True), help="Skip hot-reloading VMAlert")
    parser.add_argument("--once", action="store_true", help="Generate once instead of running the root service loop")
    parser.add_argument("--watch", action="store_true", help="Keep checking JSON configuration")
    parser.add_argument("--interval-seconds", type=int, default=int(os.getenv("VMALERT_GENERATE_INTERVAL_SECONDS", "60")), help="Configuration check interval")

    args = parser.parse_args()
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be positive")

    generator = VMAlertRuleGenerator(
        devices_dir=Path(args.devices_dir),
        fallback_config=Path(args.config),
        output_path=Path(args.output),
        vm_url=args.vm_url,
        reload_url=args.reload_url
    )

    print("\n" + "=" * 66)
    print("      VMALERT RECORDING RULE GENERATOR")
    print("=" * 66)
    print(f"Devices Directory  : {generator.loader.devices_dir}")
    print(f"Output Rules File  : {generator.output_path}")
    print(f"VictoriaMetrics    : {generator.vm_url}")
    print(f"VMAlert Reload URL : {generator.reloader.reload_url}")
    print("=" * 66 + "\n")

    # Reuse the same approved generator; no EMS queues, readers or API are started.
    watching = not args.once and (args.watch or service_default)
    while True:
        try:
            success = generator.generate(no_reload=args.no_reload)
        except Exception:
            logger.exception("Rule generation failed; correct the configuration and retry")
            success = False
        if not watching:
            break
        logger.info("Next configuration check in %s seconds", args.interval_seconds)
        try:
            time.sleep(args.interval_seconds)
        except KeyboardInterrupt:
            return

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
