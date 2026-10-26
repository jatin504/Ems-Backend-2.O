from quart import Blueprint, request, jsonify
from datetime import datetime, time as dtime, timedelta
import os
import sys
import pathlib
from zoneinfo import ZoneInfo  

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
EXTRA = PROJECT_ROOT / "processor" / "src" / "samplings" / "src"
if str(EXTRA) not in sys.path:
    sys.path.insert(0, str(EXTRA))


from processor.src.samplings.src.tinydb_store import query_resolution, query_alarms  # resolved via sys.path

tinydb_bp = Blueprint("tinydb_bp", __name__)


#Helpers
def _get_tz() -> ZoneInfo:
    """Return ZoneInfo from APP_TZ (default: Asia/Kolkata)."""
    tzname = os.getenv("APP_TZ", "Asia/Kolkata")
    try:
        return ZoneInfo(tzname)
    except Exception:
        return ZoneInfo("Asia/Kolkata")


def _parse_hhmm_safe(s: str | None, default: dtime) -> dtime:
    """Parse 'HH:MM' safely; fallback to default on any error."""
    if not s:
        return default
    try:
        hh, mm = s.split(":")
        return dtime(hour=int(hh), minute=int(mm), second=0)
    except Exception:
        return default


def _build_day_window(day: str | None, start_hhmm: str | None, end_hhmm: str | None) -> tuple[str, str]:
    """
    Returns (start_str, end_str) as 'YYYY-MM-DD HH:MM:SS' in APP_TZ.
    `day` may be 'today' | 'yesterday' | None (defaults to today).
    """
    tz = _get_tz()
    now = datetime.now(tz)

    # choose base date
    base_date = now.date()
    if (day or "").lower() == "yesterday":
        base_date = base_date - timedelta(days=1)

    # parse hh:mm (safe)
    start_t = _parse_hhmm_safe(start_hhmm, dtime(0, 0, 0))
    end_t = _parse_hhmm_safe(end_hhmm, dtime(23, 59, 59))

    start_dt = datetime.combine(base_date, start_t, tz)
    end_dt = datetime.combine(base_date, end_t, tz)

    # tinydb_store expects naive strings like "YYYY-MM-DD HH:MM:SS"
    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    return start_str, end_str


# Routes 
@tinydb_bp.get("/resolution/<string:device_id>")
async def get_resolution_today_yesterday(device_id: str):
    """
    Query minute-bucket time-series stored in TinyDB for today/yesterday window.

    Query params:
      day=today|yesterday (default today)
      from=HH:MM  (optional)
      to=HH:MM    (optional)
      columns=comma,separated,fields (optional)  e.g. active_power_kw,volt
      order=asc|desc (default asc)
      limit=int (optional)
    """
    day = request.args.get("day", "today")
    start_str, end_str = _build_day_window(
        day=day,
        start_hhmm=request.args.get("from"),
        end_hhmm=request.args.get("to"),
    )

    order = (request.args.get("order") or "asc").lower()
    if order not in ("asc", "desc"):
        order = "asc"

    limit_q = request.args.get("limit")
    try:
        limit = int(limit_q) if limit_q is not None else None
    except ValueError:
        limit = None

    columns = request.args.get("columns")
    cols = [c.strip() for c in columns.split(",")] if columns else None

    data = query_resolution(
        device_id=device_id,
        startTS=start_str,
        endTS=end_str,
        columns=cols,
        limit=limit,
        order=order,
    )

    return jsonify(
        {
            "device_id": device_id,
            "day": day,
            "startTS": start_str,
            "endTS": end_str,
            "count": len(data),
            "data": data,
        }
    )


@tinydb_bp.get("/alarms/<string:device_id>")
async def get_alarms_today_yesterday(device_id: str):
    """
    Query alarms from TinyDB for today/yesterday window.

    Query params:
      day=today|yesterday (default today)
      from=HH:MM / to=HH:MM  (optional)
      tag=... (optional)
      severity=... (optional)
      alarm_type=ChangeAlarm|ResolutionAlarm (optional)
      order=asc|desc (default desc)
      limit=int (optional)
      columns=comma,separated,fields (optional) e.g. time_stamp,tag_name,value,severity,alarm_type
    """
    day = request.args.get("day", "today")
    start_str, end_str = _build_day_window(
        day=day,
        start_hhmm=request.args.get("from"),
        end_hhmm=request.args.get("to"),
    )

    order = (request.args.get("order") or "desc").lower()
    if order not in ("asc", "desc"):
        order = "desc"

    limit_q = request.args.get("limit")
    try:
        limit = int(limit_q) if limit_q is not None else None
    except ValueError:
        limit = None

    columns = request.args.get("columns")
    cols = [c.strip() for c in columns.split(",")] if columns else None

    data = query_alarms(
        device_id=device_id,
        tag=request.args.get("tag"),
        startTS=start_str,
        endTS=end_str,
        severity=request.args.get("severity"),
        alarm_type=request.args.get("alarm_type"),
        limit=limit,
        order=order,
        columns=cols,
    )

    return jsonify(
        {
            "device_id": device_id,
            "day": day,
            "startTS": start_str,
            "endTS": end_str,
            "count": len(data),
            "data": data,
        }
    )


@tinydb_bp.get("/replay")
async def replay_minute_frames():
    """
    Build minute-wise frames for the SLD 'Replay' feature.

    Query:
      topics      = comma-separated device IDs (example: EnfPadma_Device,Another_Device)
      day         = 'today' | 'yesterday'      (default 'today')
      time        = HH:MM                      (window start, default 00:00)
      minutes     = int                        (default 15)
      columns     = comma,separated,fields     (optional)

    Response:
      {
        topics: [ ...device_ids ],
        startTS, endTS, frameCount,
        frames: [
          { ts: 'YYYY-MM-DD HH:MM:SS',
            byTopic: { <device_id>: { ...columns }, ... } },
          ...
        ]
      }
    """

    device_ids_q = request.args.get("topics", "")
    device_ids = [d.strip() for d in device_ids_q.split(",") if d.strip()]
    if not device_ids:
        return jsonify({"error": "No topics provided"}), 400

    day_q = request.args.get("day", "today")
    start_time_q = request.args.get("time", "00:00")
    minutes_q = request.args.get("minutes", "15")
    try:
        minutes = max(1, int(minutes_q))
    except Exception:
        minutes = 15

    start_str, _ = _build_day_window(day=day_q, start_hhmm=start_time_q, end_hhmm=start_time_q)
    tz = _get_tz()
    start_dt = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
    end_dt = start_dt + timedelta(minutes=minutes)
    end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    columns = request.args.get("columns")
    cols = [c.strip() for c in columns.split(",")] if columns else None

    # --- gather all rows from all topics ---
    combined_rows = []
    for dev in device_ids:
        rows = query_resolution(
            device_id=dev,
            startTS=start_str,
            endTS=end_str,
            columns=cols,
            order="asc",
        )
        for r in rows:
            ts = r.get("time_stamp")
            if not ts:
                continue
            payload = {k: {"value": v} for k, v in r.items() if k != "time_stamp"}
            combined_rows.append({
                "ts": ts,
                "topic": dev,
                "data": payload
            })

    # --- sort globally by timestamp (chronological order) ---
    combined_rows.sort(key=lambda x: x["ts"])

    # --- merge into a continuous flow (optional grouping) ---
    # Each frame = single chronological event, but we can also group same timestamps.
    frames = []
    grouped = {}
    for entry in combined_rows:
        ts = entry["ts"]
        topic = entry["topic"]
        if ts not in grouped:
            grouped[ts] = {"ts": ts, "byTopic": {}}
        grouped[ts]["byTopic"][topic] = entry["data"]

    frames = [grouped[ts] for ts in sorted(grouped.keys())]

    return jsonify(
        {
            "topics": device_ids,
            "day": day_q,
            "startTS": start_str,
            "endTS": end_str,
            "frameCount": len(frames),
            "frames": frames,
        }
    )
