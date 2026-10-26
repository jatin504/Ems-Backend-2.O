# tinydb_store.py  (day-wise files; startTS/endTS for queries; since/until kept as aliases)
import os
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Iterable, Optional, Dict, Any, List, Set, Union
import json

# ============================================================
# Async / Threaded Wrappers for Non-blocking TinyDB Writes
# ============================================================
import asyncio
from concurrent.futures import ThreadPoolExecutor

# --- Optional: load .env so EMS_TINYDB_DIR works from .env ---
try:
    from dotenv import load_dotenv  # pip install python-dotenv
    load_dotenv()
except Exception:
    pass

from tinydb import TinyDB, Query
from tinydb.table import Table

# ============================================================
# Config & Layout
# ============================================================
def _get_base_dir() -> Path:
    raw = os.getenv("EMS_TINYDB_DIR", r"./data/tinydb")
    return Path(raw).expanduser().resolve()

DEFAULT_BASE_DIR = _get_base_dir()
DEVICES_ROOT = DEFAULT_BASE_DIR / "devices"
ALARMS_ROOT = DEFAULT_BASE_DIR / "alarms"

# Per-device allowlists (which tags/columns to store)
DEVICE_ALLOWLISTS: Dict[str, Set[str]] = {}  # {"plant_1_pcs_1": {"time_stamp","active_power",...}}

# TTLs (minutes) — how long to keep data (None = keep forever)
DEVICE_TTL_MINUTES: Dict[str, int] = {}      # resolution
ALARM_TTL_MINUTES: Dict[str, int] = {}       # alarms

# Defaults (keep forever unless overridden)
DEFAULT_RESOLUTION_TTL_MINUTES: Optional[int] = None
DEFAULT_ALARM_TTL_MINUTES: Optional[int] = None

def _ensure_roots() -> None:
    DEVICES_ROOT.mkdir(parents=True, exist_ok=True)
    ALARMS_ROOT.mkdir(parents=True, exist_ok=True)

def _now_utc() -> datetime:
    return datetime.utcnow()

# ============================================================
# Utilities
# ============================================================
TimeLike = Union[str, float, int, None]

def _parse_time_like(ts: TimeLike) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts)
    try:
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                return datetime.strptime(ts, "%Y-%m-%d %H:%M:00")
            except ValueError:
                return datetime.fromisoformat(ts)
    except Exception:
        return None

def _normalize_minute(ts: Union[str, float, int]) -> str:
    dt = _parse_time_like(ts) or _now_utc()
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def _apply_allowlist(row: Dict[str, Any], allowlist: Optional[Iterable[str]]) -> Dict[str, Any]:
    if not allowlist:
        return dict(row)
    allowed = set(allowlist)
    return {k: v for k, v in row.items() if k in allowed}

def _day_key_from_ts(minute_ts: str) -> str:
    # minute_ts: "YYYY-MM-DD HH:MM:00"
    return minute_ts[:10]

def _daterange(d0: date, d1: date) -> List[date]:
    step = 1 if d1 >= d0 else -1
    n = abs((d1 - d0).days) + 1
    return [d0 + timedelta(days=i * step) for i in range(n)]

# ---------- Paths for day-wise files ----------
def _device_day_path(device_id: str, day_key: str) -> Path:
    _ensure_roots()
    base = DEVICES_ROOT / device_id.lower()
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{day_key}.json"

def _alarm_day_path(device_id: str, day_key: str) -> Path:
    _ensure_roots()
    base = ALARMS_ROOT / device_id.lower()
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{day_key}.json"

def _open_db(path: Path) -> TinyDB:
    return TinyDB(path)

# ---------- Optional file-level TTL (whole-day file deletion) ----------
def _get_effective_resolution_ttl_minutes(device_id: str) -> Optional[int]:
    return DEVICE_TTL_MINUTES.get(device_id, DEFAULT_RESOLUTION_TTL_MINUTES)

def _get_effective_alarm_ttl_minutes(device_id: str) -> Optional[int]:
    return ALARM_TTL_MINUTES.get(device_id, DEFAULT_ALARM_TTL_MINUTES)

def _purge_old_day_files(dir_path: Path, ttl_minutes: Optional[int]) -> int:
    if ttl_minutes is None or not dir_path.exists():
        return 0
    cutoff_day = (_now_utc() - timedelta(minutes=ttl_minutes)).date()
    deleted = 0
    for p in dir_path.glob("*.json"):
        try:
            day_dt = datetime.strptime(p.stem, "%Y-%m-%d").date()
        except Exception:
            continue
        if day_dt < cutoff_day:
            try:
                p.unlink()
                deleted += 1
            except Exception:
                pass
    return deleted

# ---------- TinyDB file-shape repair (common corruption) ----------
def _repair_table_if_needed(db_path: Path, table_name: str) -> bool:
    if not db_path.exists():
        return False
    try:
        with db_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return False

    original = payload.get(table_name)
    changed = False

    if isinstance(original, list):
        fixed = {}
        i = 1
        for item in original:
            if isinstance(item, dict):
                fixed[str(i)] = item
                i += 1
        payload[table_name] = fixed
        changed = True
    elif isinstance(original, dict):
        filtered = {str(k): v for k, v in original.items() if isinstance(v, dict)}
        if len(filtered) != len(original):
            payload[table_name] = filtered
            changed = True

    if "_default" not in payload or not isinstance(payload.get("_default"), dict):
        payload["_default"] = {}
        changed = True

    if changed:
        with db_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
    return changed

# ============================================================
# Public API — Resolution/Analog time-series
# ============================================================
def upsert_resolution(
    device_id: str,
    row: Dict[str, Any],
    allow_columns: Optional[Iterable[str]] = None,
    use_device_allowlist: bool = True,
) -> None:
    """Upsert minute-bucket row into the device's day file."""
    if "time_stamp" not in row:
        raise ValueError("row must include 'time_stamp'")

    minute_ts = _normalize_minute(row["time_stamp"])
    day_key = _day_key_from_ts(minute_ts)

    # Optional TTL (delete old day files if configured)
    _purge_old_day_files((DEVICES_ROOT / device_id.lower()), _get_effective_resolution_ttl_minutes(device_id))

    row = dict(row)
    row["time_stamp"] = minute_ts

    effective_allowlist: Optional[Set[str]] = None
    if use_device_allowlist and device_id in DEVICE_ALLOWLISTS:
        effective_allowlist = set(DEVICE_ALLOWLISTS[device_id])
    if allow_columns:
        effective_allowlist = set(allow_columns) if effective_allowlist is None else (effective_allowlist & set(allow_columns))
    if effective_allowlist is not None:
        effective_allowlist.add("time_stamp")
    row = _apply_allowlist(row, effective_allowlist)

    db_path = _device_day_path(device_id, day_key)
    with _open_db(db_path) as db:
        table: Table = db.table("data")
        Q = Query()
        existing = table.get(Q.time_stamp == minute_ts)
        if existing:
            merged = dict(existing)
            merged.update(row)
            table.update(merged, Q.time_stamp == minute_ts)
        else:
            table.insert(row)

def query_resolution(
    device_id: str,
    startTS: TimeLike = None,
    endTS:   TimeLike = None,
    columns: Optional[Iterable[str]] = None,
    limit: Optional[int] = None,
    order: str = "asc",
    # --- backward-compat aliases (deprecated) ---
    since: TimeLike = None,
    until: TimeLike = None,
) -> List[Dict[str, Any]]:
    """
    Read across day files between startTS..endTS (inclusive).
    Aliases: since->startTS, until->endTS (deprecated).
    """
    # Alias mapping if new params not provided
    if startTS is None and since is not None:
        startTS = since
    if endTS is None and until is not None:
        endTS = until

    now = _now_utc()
    sdt = _parse_time_like(startTS) or (now - timedelta(days=1))
    udt = _parse_time_like(endTS) or now

    # Optional TTL cleanup
    _purge_old_day_files((DEVICES_ROOT / device_id.lower()), _get_effective_resolution_ttl_minutes(device_id))

    records: List[Dict[str, Any]] = []
    for d in _daterange(sdt.date(), udt.date()):
        db_path = _device_day_path(device_id, d.strftime("%Y-%m-%d"))
        if not db_path.exists():
            continue
        try:
            with _open_db(db_path) as db:
                part = db.table("data").all()
        except ValueError:
            if _repair_table_if_needed(db_path, "data"):
                with _open_db(db_path) as db:
                    part = db.table("data").all()
            else:
                raise
        records.extend(part)

    # filter, sort, project
    records = [r for r in records if (t := _parse_time_like(r.get("time_stamp"))) and sdt <= t <= udt]
    records.sort(key=lambda r: _parse_time_like(r.get("time_stamp")) or datetime.min, reverse=(order == "desc"))
    if limit is not None and limit >= 0:
        records = records[:limit]
    if columns:
        cols = set(columns) | {"time_stamp"}
        records = [{k: v for k, v in r.items() if k in cols} for r in records]
    return records

# ============================================================
# Public API — Alarms
# ============================================================
def insert_alarm(
    device_id: str,
    row: Dict[str, Any],
    allow_columns: Optional[Iterable[str]] = None,
) -> None:
    """Insert an alarm event into the device's day file. Required: time_stamp, tag_name, value."""
    if "time_stamp" not in row:
        raise ValueError("alarm row must include 'time_stamp'")

    minute_ts = _normalize_minute(row["time_stamp"])
    day_key = _day_key_from_ts(minute_ts)

    _purge_old_day_files((ALARMS_ROOT / device_id.lower()), _get_effective_alarm_ttl_minutes(device_id))

    row = dict(row)
    row["time_stamp"] = minute_ts
    if allow_columns:
        allow = set(allow_columns) | {"time_stamp", "tag_name", "value"}
        row = _apply_allowlist(row, allow)

    db_path = _alarm_day_path(device_id, day_key)
    with _open_db(db_path) as db:
        db.table("alarms").insert(row)

def query_alarms(
    device_id: str,
    tag: Optional[str] = None,
    startTS: TimeLike = None,
    endTS:   TimeLike = None,
    severity: Optional[str] = None,
    alarm_type: Optional[str] = None,
    limit: Optional[int] = None,
    order: str = "desc",
    columns: Optional[Iterable[str]] = None,
    # --- backward-compat aliases (deprecated) ---
    since: TimeLike = None,
    until: TimeLike = None,
) -> List[Dict[str, Any]]:
    """
    Read alarms across day files between startTS..endTS (inclusive).
    Aliases: since->startTS, until->endTS (deprecated).
    """
    # Alias mapping if new params not provided
    if startTS is None and since is not None:
        startTS = since
    if endTS is None and until is not None:
        endTS = until

    now = _now_utc()
    sdt = _parse_time_like(startTS) or (now - timedelta(days=1))
    udt = _parse_time_like(endTS) or now

    _purge_old_day_files((ALARMS_ROOT / device_id.lower()), _get_effective_alarm_ttl_minutes(device_id))

    records: List[Dict[str, Any]] = []
    for d in _daterange(sdt.date(), udt.date()):
        db_path = _alarm_day_path(device_id, d.strftime("%Y-%m-%d"))
        if not db_path.exists():
            continue
        try:
            with _open_db(db_path) as db:
                part = db.table("alarms").all()
        except ValueError:
            if _repair_table_if_needed(db_path, "alarms"):
                with _open_db(db_path) as db:
                    part = db.table("alarms").all()
            else:
                raise
        records.extend(part)

    # filters
    if tag:
        records = [r for r in records if str(r.get("tag_name")) == tag]
    if severity:
        records = [r for r in records if str(r.get("severity")) == severity]
    if alarm_type:
        records = [r for r in records if str(r.get("alarm_type")) == alarm_type]

    records = [r for r in records if (t := _parse_time_like(r.get("time_stamp"))) and sdt <= t <= udt]
    records.sort(key=lambda r: _parse_time_like(r.get("time_stamp")) or datetime.min, reverse=(order == "desc"))
    if limit is not None and limit >= 0:
        records = records[:limit]
    if columns:
        cols = set(columns) | {"time_stamp"}
        records = [{k: v for k, v in r.items() if k in cols} for r in records]
    return records

# ============================================================
# Admin helpers (policies & maintenance)
# ============================================================
def register_device_allowlist(device_id: str, fields: Iterable[str]) -> None:
    DEVICE_ALLOWLISTS[device_id] = set(fields)

def set_device_ttl_minutes(device_id: str, minutes: Optional[int]) -> None:
    if minutes is None:
        DEVICE_TTL_MINUTES.pop(device_id, None)
    else:
        DEVICE_TTL_MINUTES[device_id] = int(minutes)

def set_alarm_ttl_minutes(device_id: str, minutes: Optional[int]) -> None:
    if minutes is None:
        ALARM_TTL_MINUTES.pop(device_id, None)
    else:
        ALARM_TTL_MINUTES[device_id] = int(minutes)

def compact_device(device_id: str) -> None:
    """Not needed with day files; kept for API parity."""
    pass

def compact_alarms(device_id: str) -> None:
    """Not needed with day files; kept for API parity."""
    pass


# Shared executor (2–4 workers are usually enough for TinyDB)
_TINYDB_EXECUTOR = ThreadPoolExecutor(max_workers=4)

async def async_upsert_resolution(*args, **kwargs):
    """Non-blocking threaded upsert for resolution data."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_TINYDB_EXECUTOR, upsert_resolution, *args, **kwargs)

async def async_insert_alarm(*args, **kwargs):
    """Non-blocking threaded insert for alarms."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(_TINYDB_EXECUTOR, insert_alarm, *args, **kwargs)

# ============================================================
# Example quick test
# ============================================================
# if __name__ == "__main__":
    # Optional: os.environ["EMS_TINYDB_DIR"] = r"D:\New folder\tinydb"
    # device_id = "plant_1_block_1_inverter_inverter_1"
    # today = datetime.now().strftime("%Y-%m-%d")

    # upsert_resolution(device_id, {"time_stamp": f"{today} 10:01:10", "active_power_kw": 120, "volt": 392})
    # upsert_resolution(device_id, {"time_stamp": f"{today} 10:01:50", "active_power_kw": 130, "volt": 394})
    # insert_alarm(device_id, {"time_stamp": f"{today} 10:01:00", "tag_name": "Overtemp", "value": 1, "severity": "HIGH"})

    # print("Resolution data:", query_resolution(device_id, startTS=f"{today} 10:30:00", endTS=f"{today} 11:35:00"))
    # print("Alarm data:", query_alarms(device_id, startTS=f"{today} 00:00:00", endTS=f"{today} 23:59:00"))
