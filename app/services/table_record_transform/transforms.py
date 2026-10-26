from datetime import datetime, timezone, timedelta
from utils.db_config import db_config

DB_NAME = db_config("primary")["database"]

def timeseries_records_to_nested_dict(records):
    """
    Transform list of asyncpg.Record to:
    {
      device_id: {
        key: [ {ts, value}, ... ]
      }
    }
    """
    result = {}

    for r in records:
        device_id = r["device_id"]
        key = r["key"]
        # Ensure timestamp is timezone-aware
        dt = r["time_stamp"]
        dt = dt + timedelta(hours=5, minutes=30)
        # if dt.tzinfo is None:
            # Assume it's UTC if you know that's the case
            # dt = dt.replace(tzinfo=timezone.utc)
        # Or, if it's in a specific TZ:
        #   import pytz
        #   dt = pytz.timezone("Asia/Kolkata").localize(dt)

        ts = int(dt.timestamp() * 1000)  # now safe
        value = r["value"]

        # Initialize nested dicts
        if device_id not in result:
            result[device_id] = {}
        if key not in result[device_id]:
            result[device_id][key] = []

        result[device_id][key].append({"ts": ts, "value": value})

    return result

# Format Records from VictoriaMetrics to nested dict

def victoria_to_nested(vm_result, key_name):
    output = {}

    for series in vm_result["data"]["result"]:
        device = series["metric"]["device"]

        output.setdefault(device, {})
        output[device].setdefault(key_name, [])

        for ts, value in series["values"]:
            output[device][key_name].append({
                "ts": int(float(ts) * 1000) + (1000*60*330),   # seconds -> milliseconds
                "value": float(value),
            })

    return output

# Merge multiple nested dicts (of keys) into one
def merge_results(results):
    merged = {}

    for result in results:
        for device, metrics in result.items():
            merged.setdefault(device, {})

            for metric, values in metrics.items():
                merged[device][metric] = values

    return merged

FUNC_MAP = {
    "AVG": "avg_over_time",
    "MIN": "min_over_time",
    "MAX": "max_over_time",
    "SUM": "sum_over_time",
    "LAST": "last_over_time",
}
# Nuilding MetricsQL query based on aggregation and devices
def build_query(metric, devices, aggregation):

    ids = devices.get("ids")
    if len(ids) > 0:
        device_values = "|".join(ids)

    patterns = devices.get("patterns")
    if len(patterns) > 0:
            matched_ids = build_device_regex(patterns)#device_helper.get_device_ids_by_type_pattern(pattern)
            if matched_ids:
                device_values = matched_ids

    if not device_values:
        raise ValueError("No devices specified")

    selector = f'{metric}{{device=~"{device_values}", database="{DB_NAME}", resolution="1_min"}}'

    func = aggregation["func"].upper()

    if func == "NONE":
        return selector

    window = interval_to_metricsql(
        aggregation["interval"],
        aggregation["intervalType"],
    )

    return f'{FUNC_MAP[func]}({selector}[{window}])'

import re
def build_device_regex(patterns):
    escaped = [re.escape(p) for p in patterns]
    return f".*({'|'.join(escaped)}).*"

INTERVAL_SUFFIX_MAP = {
    "minute": "m",
    "hour": "h",
    "day": "d",
    "week": "w",
}

# Convert interval to MetricsQL duration
def interval_to_metricsql(interval: int, interval_type: str) -> str:
    """
    Convert UI interval to MetricsQL duration.

    Examples:
        (5, "minute") -> "5m"
        (2, "hour")   -> "2h"
        (1, "day")    -> "1d"
    """

    try:
        suffix = INTERVAL_SUFFIX_MAP[interval_type.lower()]
    except KeyError:
        raise ValueError(f"Unsupported interval type: {interval_type}")

    return f"{interval}{suffix}"


# Resolve time range based on the provided timeRange object for VM

INTERVAL_MAP = {
    "minute": "minutes",
    "hour": "hours",
    "day": "days",
    "week": "weeks",
}

def resolve_time_range(time_range):
    now = datetime.now(timezone.utc)

    if time_range["type"] == "absolute":
        return (
            time_range["startTs"] // 1000,
            time_range["endTs"] // 1000,
        )

    if time_range["type"] == "last":
        kwargs = {
            INTERVAL_MAP[time_range["intervalType"]]: time_range["interval"]
        }

        start = now - timedelta(**kwargs)
        return int(start.timestamp()), int(now.timestamp())

    if time_range["type"] == "relative":
        relative = time_range["relativeType"]

        if relative == "currentDaySoFar":
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            return int(start.timestamp()), int(now.timestamp())

        elif relative == "currentWeekSoFar":
            start = (now - timedelta(days=now.weekday())).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            return int(start.timestamp()), int(now.timestamp())

        elif relative == "currentMonthSoFar":
            start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            return int(start.timestamp()), int(now.timestamp())

        elif relative == "currentQuarterSoFar":
            quarter_start_month = ((now.month - 1) // 3) * 3 + 1
            start = now.replace(
                month=quarter_start_month,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
            return int(start.timestamp()), int(now.timestamp())

        elif relative == "yesterday":
            start = (now - timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            end = start + timedelta(days=1)
            return int(start.timestamp()), int(end.timestamp())

        raise ValueError(f"Unsupported relativeType: {relative}")

    raise ValueError(f"Unsupported timeRange type: {time_range['type']}")