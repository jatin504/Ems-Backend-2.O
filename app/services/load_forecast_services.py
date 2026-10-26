import os
import json
import logging
import asyncio
import urllib.request
import urllib.error
from datetime import datetime, date, timedelta
from app.utils.db_utils import DB
import math

logger = logging.getLogger(__name__)

FORECAST_SERVICE_URL = os.getenv("FORECAST_SERVICE_URL")

_table_initialized = False


async def ensure_load_forecast_table(db):
    """Ensures load_forecast table exists in PostgreSQL if not already created."""
    global _table_initialized
    if not _table_initialized:
        table_query = """
        CREATE TABLE IF NOT EXISTS load_forecast (
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            target_ts TIMESTAMP NOT NULL,
            forecast_date DATE NOT NULL,
            forecast_load_kw NUMERIC(12, 2) NOT NULL,
            model_name VARCHAR(100) DEFAULT 'model_clean',
            type VARCHAR(50) DEFAULT 'LOAD',
            CONSTRAINT uq_forecast_point UNIQUE (target_ts, model_name, type)
        );
        CREATE INDEX IF NOT EXISTS idx_load_forecast_target_ts ON load_forecast (target_ts);
        CREATE INDEX IF NOT EXISTS idx_load_forecast_date ON load_forecast (forecast_date);
        """
        try:
            await db.execute(table_query)
            _table_initialized = True
            logger.info("Database table 'load_forecast' verified/initialized.")
        except Exception as e:
            logger.warning("Could not auto-create load_forecast table: %s", e)


def _sync_post_forecast(url: str, payload: dict) -> dict:
    """Internal synchronous HTTP POST caller using standard urllib."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


CORRELATION_HOURS = {
    6: 16,
    10: 20,
    17: 22,
    21: 18,
    23: 16,
}


def classify_tariff_and_state(ts: datetime):
    time_float = ts.hour + ts.minute / 60.0
    if time_float < 6.0 or time_float >= 23.0:
        return 2.00, "off_peak"
    elif (10.0 <= time_float < 13.0) or (18.0 <= time_float < 20.0):
        return 8.00, "peak"
    else:
        return 4.00, "mid_peak"


def calculate_pv_kw(dt: datetime, peak_pv_kw: float = 2000.0) -> float:
    """
    PV generation:
    - Starts at 05:46.
    - Ramps up after sunrise.
    - Peaks at 2000 kW around solar noon.
    - Ramps down toward sunset.
    - Never exceeds 2000 kW.
    """
    start_minutes = 5 * 60 + 46  # 05:46
    current_minutes = dt.hour * 60 + dt.minute

    # No PV generation before 05:46 or after 18:00
    sunset_minutes = 18 * 60

    if current_minutes < start_minutes or current_minutes >= sunset_minutes:
        return 0.0

    # Normalize daylight progress between 0 and 1
    progress = (
        (current_minutes - start_minutes)
        / (sunset_minutes - start_minutes)
    )

    # Smooth solar curve: zero at sunrise/sunset, maximum at midday

    pv_kw = peak_pv_kw * math.sin(math.pi * progress)

    return round(max(0.0, min(pv_kw, peak_pv_kw)), 2)
import random

def calculate_poa(dt: datetime, peak_poa: float = 1000.0) -> float:
    """
    POA irradiance:
    - Starts at 05:46.
    - Ends at 18:00.
    - Normal smooth curve outside 10:00-15:00.
    - More variation between 10:00 and 15:00.
    - POA remains between 0 and peak_poa.
    """
    start_minutes = 5 * 60 + 46
    sunset_minutes = 18 * 60
    current_minutes = dt.hour * 60 + dt.minute

    if current_minutes < start_minutes or current_minutes >= sunset_minutes:
        return 0.0

    progress = (
        (current_minutes - start_minutes)
        / (sunset_minutes - start_minutes)
    )

    # Base smooth solar curve
    base_poa = peak_poa * math.sin(math.pi * progress)

    # Increase variation from 10:00 to 15:00
    if 9 <= dt.hour < 15:
        variation = random.uniform(-50, 50)
        poa = base_poa + variation
    else:
        poa = base_poa

    return round(max(0.0, min(poa, peak_poa)), 2)


def build_and_save_sample_window_json(forecast_points: list) -> str:
    """
    Transforms forecast points into the exact sample_window structure and
    saves it locally on EMS Backend disk using the path configured in .env.
    """
    if not forecast_points:
        return ""

    sample_records = []
    for p in forecast_points:
        ts_str = p.get("timestamp")
        if not ts_str:
            continue
        try:
            if isinstance(ts_str, datetime):
                dt = ts_str
            else:
                dt = datetime.fromisoformat(str(ts_str).strip())
        except Exception:
            continue

        raw_load = p.get("forecast_load_kw") if p.get("forecast_load_kw") is not None else p.get("load_kw", 0.0)
        load_kw = round(float(raw_load), 2)
        pv_kw = calculate_pv_kw(dt, peak_pv_kw=2000.0)
        poa = calculate_poa(dt, peak_poa=1000.0)
        tariff, state = classify_tariff_and_state(dt)
        correlation = CORRELATION_HOURS.get(dt.hour, 0) if dt.minute == 0 else 0

        sample_records.append({
            "load_kw": load_kw,
            "pv_kw": pv_kw,
            "poa": poa,
            "co_relation_percent": correlation,
            "load_state": state,
            "grid_import_limit_kw": 4000,
            "tariff_per_kw": tariff,
            "time_stamp": dt.strftime("%Y-%m-%d %H:%M:%S")
        })

    sample_window_path = os.getenv("SAMPLE_WINDOW_JSON_PATH", "data/sample_window_forecast.json")
    try:
        os.makedirs(os.path.dirname(os.path.abspath(sample_window_path)), exist_ok=True)
        with open(sample_window_path, "w", encoding="utf-8") as f:
            json.dump(sample_records, f, indent=2)
        logger.info("Saved sample window forecast JSON (%d records) to %s", len(sample_records), sample_window_path)
        return sample_window_path
    except Exception as e:
        logger.error("Failed saving sample window JSON to %s: %s", sample_window_path, e)
        return ""


async def call_forecast_service(model_name: str = "model_clean", input_file: str = None) -> dict:
    """
    Wrapper function to call the Load Forecasting Quart service.
    Runs asynchronously and delegates HTTP call to a worker thread.
    """
    base_url = FORECAST_SERVICE_URL or "http://127.0.0.1:8000"
    endpoint = f"{base_url.rstrip('/')}/api/v1/loadforecast"
    payload = {
        "model_name": model_name or "model_clean"
    }
    if input_file:
        payload["input_file"] = input_file

    logger.info("Calling Forecasting Service at %s", endpoint)
    try:
        result = await asyncio.to_thread(_sync_post_forecast, endpoint, payload)
        return result
    except urllib.error.URLError as e:
        logger.error("Failed to connect to Forecasting Service (%s): %s", endpoint, e)
        raise ConnectionError(
            f"Cannot connect to Forecasting Service at {FORECAST_SERVICE_URL}. Is Load Forecasting app running?"
        )
    except Exception as e:
        logger.error("Error calling forecasting service: %s", e)
        raise


async def save_forecast_to_db(forecast_list: list, model_name: str = "model_clean") -> int:
    """
    Saves or updates forecast points into the PostgreSQL `load_forecast` table.
    Uses UPSERT so repeated calls update values cleanly without creating duplicates.
    """
    if not forecast_list:
        return 0

    records = []
    for item in forecast_list:
        ts_str = item.get("timestamp")
        load_val = item.get("forecast_load_kw")
        if not ts_str or load_val is None:
            continue
        try:
            if isinstance(ts_str, datetime):
                dt = ts_str
            else:
                dt = datetime.fromisoformat(str(ts_str).strip())
            forecast_date = dt.date()
            records.append((dt, forecast_date, float(load_val), model_name, "LOAD"))
        except Exception as err:
            logger.warning("Failed to parse forecast point %s: %s", item, err)

    if not records:
        return 0

    insert_query = """
        INSERT INTO load_forecast (target_ts, forecast_date, forecast_load_kw, model_name, type)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT ON CONSTRAINT uq_forecast_point
        DO UPDATE SET
            forecast_load_kw = EXCLUDED.forecast_load_kw,
            forecast_date = EXCLUDED.forecast_date,
            created_at = NOW()
    """
    async with DB() as db:
        await ensure_load_forecast_table(db)
        await db.connection.executemany(insert_query, records)

    logger.info("Saved/updated %d records into PostgreSQL table 'load_forecast'.", len(records))
    return len(records)


async def get_forecast_from_db(target_date: date = None, model_name: str = None) -> list:
    """
    Fetches stored forecast records from PostgreSQL `load_forecast` table.
    """
    query = """
        SELECT id, target_ts, forecast_date, forecast_load_kw, model_name, type, created_at
        FROM load_forecast
    """
    conditions = []
    params = []

    if target_date:
        params.append(target_date)
        conditions.append(f"forecast_date = ${len(params)}")

    if model_name:
        params.append(model_name)
        conditions.append(f"model_name = ${len(params)}")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY target_ts ASC"

    async with DB() as db:
        await ensure_load_forecast_table(db)
        rows = await db.fetch(query, *params)
        return [
            {
                "timestamp": r["target_ts"].isoformat() if r["target_ts"] else None,
                "forecast_load_kw": float(r["forecast_load_kw"]) if r["forecast_load_kw"] is not None else None,
                "model_name": r["model_name"],
                "type": r["type"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]


async def get_strategy_schedules_from_db(target_date: date = None, filter_type: str = "all") -> list:
    """
    Fetches strategy schedules from PostgreSQL `energy_strategy` table for a specific date or filter.
    """
    if target_date is None and filter_type:
        filter_str = str(filter_type).lower()
        if filter_str == "today":
            target_date = date.today()
        elif filter_str in ("tomorrow", "next_day"):
            target_date = date.today() + timedelta(days=1)

    if target_date:
        day_start = datetime.combine(target_date, datetime.min.time())
        day_end = datetime.combine(target_date, datetime.max.time())
        query = """
            SELECT *
            FROM energy_strategy
            WHERE (
                (start_ts >= $1 AND start_ts <= $2)
                OR (end_ts >= $1 AND end_ts <= $2)
                OR (start_ts <= $1 AND (end_ts >= $1 OR end_ts IS NULL))
            )
            ORDER BY start_ts ASC
        """
        params = (day_start, day_end)
    else:
        query = """
            SELECT *
            FROM energy_strategy
            WHERE start_ts IS NOT NULL
            ORDER BY start_ts ASC
        """
        params = ()

    async with DB() as db:
        rows = await db.fetch(query, *params)
        return [
            {
                "id": r["id"],
                "start_ts": r["start_ts"].isoformat() if r["start_ts"] else None,
                "end_ts": r["end_ts"].isoformat() if r["end_ts"] else None,
                "type": r["type"],
                "tariff": r["tariff"],
                "power": float(r["power"]) if r["power"] is not None else None,
                "duration": r["duration"],
                "min_soc": float(r["min_soc"]) if r["min_soc"] is not None else None,
                "max_soc": float(r["max_soc"]) if r["max_soc"] is not None else None,
                "priority": r["priority"],
                "remark": r["remark"],
                "schedule_from": r.get("schedule_from", "INTERNAL") if hasattr(r, "get") else r["schedule_from"],
            }
            for r in rows
        ]


def build_chart_series(forecast_list: list, schedule_list: list) -> dict:
    """
    Transforms forecast points and strategy schedules into timestamp-based
    chart series compatible with Highcharts / ApexCharts for the Load vs BESS widget.
    """
    load_series = []
    charge_series = []
    discharge_series = []

    # 1. Format Forecast points: [[timestamp_ms, load_val], ...]
    for pt in forecast_list:
        ts_str = pt.get("timestamp") or pt.get("target_ts")
        val = pt.get("forecast_load_kw")
        if ts_str and val is not None:
            try:
                dt = datetime.fromisoformat(str(ts_str).strip())
                ts_ms = int(dt.timestamp() * 1000)
                load_series.append([ts_ms, round(float(val), 2)])
            except Exception:
                pass
    load_series.sort(key=lambda x: x[0])

    # 2. Format Schedule events: [[timestamp_ms, power_val], ...]
    total_charge_kwh = 0.0
    total_discharge_kwh = 0.0

    for item in schedule_list:
        s_raw = item.get("start_ts")
        e_raw = item.get("end_ts")
        power = float(item.get("power") or 0.0)
        stype = str(item.get("type") or "").upper()
        dur = item.get("duration")

        if not s_raw or power <= 0:
            continue

        try:
            start_dt = datetime.fromisoformat(str(s_raw).strip())
            if e_raw:
                end_dt = datetime.fromisoformat(str(e_raw).strip())
            elif dur:
                end_dt = start_dt + timedelta(minutes=float(dur))
            else:
                end_dt = start_dt + timedelta(hours=1)
            
            hours = max(0.25, (end_dt - start_dt).total_seconds() / 3600.0)
            energy_kwh = round(power * hours, 2)
            start_ms = int(start_dt.timestamp() * 1000)

            if "CHARGE" in stype and "DISCHARGE" not in stype:
                charge_series.append([start_ms, round(power, 2)])
                total_charge_kwh += energy_kwh
            elif "DISCHARGE" in stype:
                discharge_series.append([start_ms, round(power, 2)])
                total_discharge_kwh += energy_kwh
        except Exception:
            pass

    charge_series.sort(key=lambda x: x[0])
    discharge_series.sort(key=lambda x: x[0])

    peak_load = max([p[1] for p in load_series], default=0.0)

    return {
        "chart_series": {
            "load_forecast": load_series,
            "charge": charge_series,
            "discharge": discharge_series,
        },
        "summary": {
            "total_forecast_points": len(load_series),
            "total_schedules": len(schedule_list),
            "total_charge_kwh": round(total_charge_kwh, 2),
            "total_discharge_kwh": round(total_discharge_kwh, 2),
            "net_discharge_kwh": round(total_discharge_kwh - total_charge_kwh, 2),
            "peak_load_kw": round(peak_load, 2)
        }
    }


async def get_combined_load_and_schedule(
    target_date: date = None,
    filter_param: str = None,
    force_refresh: bool = False,
    input_file: str = None,
    model_name: str = "model_clean"
) -> dict:
    """
    Combined service function to retrieve both Load Forecast and Strategy Schedule in a single call.
    - Saves forecast into `load_forecast` table.
    - Fetches schedules from `energy_strategy` table.
    - Returns combined raw records, aligned chart series for the UI widget, and KPI summaries.
    """
    # 1. Resolve target date
    if not target_date and filter_param:
        f_lower = str(filter_param).lower()
        if f_lower == "today":
            target_date = date.today()
        elif f_lower in ("tomorrow", "next_day"):
            target_date = date.today() + timedelta(days=1)

    # 2. Get Forecast data
    forecast_points = []
    source = "ems_database"

    if target_date and not force_refresh:
        forecast_points = await get_forecast_from_db(target_date=target_date, model_name=model_name)

    if not forecast_points:
        try:
            fc_res = await call_forecast_service(model_name=model_name, input_file=input_file)
            raw_pts = fc_res.get("forecast", [])
            build_and_save_sample_window_json(raw_pts)
            if raw_pts:
                await save_forecast_to_db(raw_pts, model_name=model_name)
                if target_date:
                    forecast_points = [
                        p for p in raw_pts
                        if p.get("timestamp") and datetime.fromisoformat(str(p["timestamp"])).date() == target_date
                    ]
                    if not forecast_points:
                        forecast_points = raw_pts
                else:
                    forecast_points = raw_pts
                source = "forecast_microservice"
        except Exception as e:
            logger.warning("Forecast microservice call failed in combined endpoint: %s", e)
            if not forecast_points:
                forecast_points = await get_forecast_from_db(target_date=target_date, model_name=model_name)
                source = "ems_database_fallback"

    # 3. Get Strategy Schedules
    schedules = await get_strategy_schedules_from_db(target_date=target_date, filter_type=filter_param or "all")

    # 4. Build chart series & summary
    chart_info = build_chart_series(forecast_points, schedules)

    sample_window_path = os.getenv("SAMPLE_WINDOW_JSON_PATH", "data/sample_window_forecast.json")
    if forecast_points and not os.path.exists(sample_window_path):
        build_and_save_sample_window_json(forecast_points)

    return {
        "status": "success",
        "date": str(target_date) if target_date else "all",
        "filter": filter_param or ("today" if target_date == date.today() else "custom"),
        "source": source,
        "sample_window_json_path": sample_window_path if os.path.exists(sample_window_path) else None,
        "forecast": forecast_points,
        "schedule": schedules,
        "chart_series": chart_info["chart_series"],
        "summary": chart_info["summary"]
    }
