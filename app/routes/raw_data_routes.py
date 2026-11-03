"""Fetch actual stored raw samples, without minute aggregation or graph resampling."""

import asyncio
import json
import os
import re
import time
from urllib.parse import urlsplit, urlunsplit

import aiohttp
from quart import Blueprint, jsonify, request

raw_data_bp = Blueprint("raw_data", __name__)
MAX_SAMPLES = 20_000
MAX_BYTES = 8 * 1024 * 1024


def export_url():
    """Use the same single-node host as the raw writer, with its export API path."""
    host = os.getenv("PRIMARY_BACKEND_HOST", "127.0.0.1")
    source = os.getenv("EMS_RAW_VM_URL", f"http://{host}:8428/api/v1/import/prometheus")
    parts = urlsplit(source)
    return urlunsplit((parts.scheme, parts.netloc, "/api/v1/export", "", ""))


@raw_data_bp.route("/api/raw-data", methods=["GET"])
async def get_raw_data():
    """Return bounded raw history and counts for the requested device/tag/time range."""
    started = time.perf_counter()
    try:
        # Explicit selectors avoid accidentally downloading the entire historian.
        devices = list(dict.fromkeys(x.strip() for x in request.args.get("device_ids", "").split(",") if x.strip()))
        tags = list(dict.fromkeys(x.strip().lower() for x in request.args.get("tags", "").split(",") if x.strip()))
        if not devices or not tags or len(devices) > 10 or len(tags) > 20:
            raise ValueError("Provide device_ids (1-10) and tags (1-20) as comma-separated names")
        if any(not re.fullmatch(r"[a-zA-Z_:][a-zA-Z0-9_:]*", tag) for tag in tags):
            raise ValueError("Invalid tag/metric name")
        if any(len(device) > 200 for device in devices):
            raise ValueError("Device ID is too long")
        # start/end are Unix seconds; returned sample timestamps remain milliseconds.
        end = float(request.args.get("end", time.time()))
        start = float(request.args.get("start", end - 300))
        if not (0 <= start < end <= time.time() + 60) or end - start > 3600:
            raise ValueError("Use a valid start/end range of at most 3600 seconds")
    except (ValueError, OverflowError) as exc:
        return jsonify(success=False, error=str(exc)), 400

    database = os.getenv("PRIMARY_DB_NAME", "ems")
    params = [("start", str(start)), ("end", str(end))]
    for device in devices:
        for tag in tags:
            # Escape label values, and isolate raw data from the existing 1_min series.
            selector = (f'{tag}{{device={json.dumps(device)},database={json.dumps(database)},'
                        'resolution="raw_1_sec"}')
            params.append(("match[]", selector))
    try:
        # Read incrementally with hard bounds; never return a silently truncated count.
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.get(export_url(), params=params) as response:
                if response.status != 200:
                    return jsonify(success=False, error="VictoriaMetrics raw export failed"), 502
                body = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    body.extend(chunk)
                    if len(body) > MAX_BYTES:
                        return jsonify(success=False, error="Result too large; reduce time range, tags or devices"), 413
        grouped, count = {}, 0
        for line in body.decode("utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            labels, values, timestamps = row["metric"], row["values"], row["timestamps"]
            if len(values) != len(timestamps):
                raise ValueError("Mismatched Victoria sample arrays")
            identity = tuple(sorted(labels.items()))
            series = grouped.setdefault(identity, {"metric": labels, "samples": []})
            for timestamp, value in zip(timestamps, values):
                count += 1
                if count > MAX_SAMPLES:
                    return jsonify(success=False, error="More than 20000 samples; reduce requested range"), 413
                series["samples"].append({"timestamp_ms": timestamp, "value": value})
        for series in grouped.values():
            series["samples"].sort(key=lambda sample: sample["timestamp_ms"])
        return jsonify(success=True, resolution="raw_1_sec", start=start, end=end,
                       series_count=len(grouped), sample_count=count,
                       fetch_time_ms=round((time.perf_counter() - started) * 1000, 3),
                       data=list(grouped.values()))
    except asyncio.TimeoutError:
        return jsonify(success=False, error="VictoriaMetrics raw fetch timed out"), 504
    except aiohttp.ClientError:
        return jsonify(success=False, error="Cannot connect to VictoriaMetrics"), 502
    except (ValueError, KeyError, TypeError, UnicodeError):
        return jsonify(success=False, error="Invalid response from VictoriaMetrics"), 502
