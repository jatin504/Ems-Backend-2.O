import logging
from datetime import datetime, date, timedelta
from config.device_config_resolver import DeviceConfigResolver
from controller_layer.strategies.classes.schedule_strategy import ScheduleStrategy
from quart import Blueprint, request, jsonify
from app.services.load_forecast_services import (
    call_forecast_service,
    save_forecast_to_db,
    get_forecast_from_db,
    get_combined_load_and_schedule,
    build_and_save_sample_window_json,
    build_sample_window,
    save_sample_window_json,
)
from app.services.provisional_schedule_service import (
    delete_forecast_and_provisional_schedule,
    get_provisional_schedule,
    save_provisional_schedule,
)

logger = logging.getLogger(__name__)

load_forecast_bp = Blueprint("load_forecast_bp", __name__)


# =====================================================================
# COMBINED ENDPOINT: Load Forecast + Strategy Schedule (1 Single API)
# =====================================================================
@load_forecast_bp.route("/api/load-forecast-schedule", methods=["GET", "POST"])
async def get_load_and_schedule():
    """
    Run the forecast on an unfiltered request and return today's and tomorrow's saved plan.
    A date or day filter reads saved rows. force_refresh also runs the model with a filter.

    Query Params / JSON Body:
      - filter: "today", "tomorrow" / "next_day", "all"
      - date: specific date in YYYY-MM-DD format (e.g. 2026-10-08)
      - force_refresh: "true" / "false" (re-run model even if DB has data)
      - input_file: optional path to historical data CSV
      - model_name: "model_clean" (default) or "model_dirty"

    Response contains:
      - forecast: raw load forecast points list
      - schedule: minute-wise provisional schedule rows
      - chart_series: timestamped provisional values
      - summary: total_charge_kwh, total_discharge_kwh, net_discharge_kwh, peak_load_kw
    """
    try:
        if request.method == "POST":
            body = await request.get_json(silent=True) or {}
            input_file = body.get("input_file")
            filter_param = body.get("filter")
            date_param = body.get("date")
            model_name = body.get("model_name", "model_clean")
            force_refresh = str(body.get("force_refresh", "false")).lower() in ("true", "1")
        else:
            input_file = request.args.get("input_file")
            filter_param = request.args.get("filter")
            date_param = request.args.get("date")
            model_name = request.args.get("model_name", "model_clean")
            force_refresh = request.args.get("force_refresh", "false").lower() in ("true", "1")

        target_date = None
        if date_param:
            try:
                target_date = datetime.strptime(date_param.strip(), "%Y-%m-%d").date()
            except ValueError:
                return jsonify({"error": "Invalid date format, expected YYYY-MM-DD"}), 400

        today = date.today()
        tomorrow = today + timedelta(days=1)
        filter_param = (filter_param or "all").lower()
        if target_date is None:
            if filter_param == "today":
                target_date = today
            elif filter_param in ("tomorrow", "next_day"):
                target_date = tomorrow
            elif filter_param != "all":
                return jsonify({"error": "Invalid filter, expected today, tomorrow or all"}), 400

        source = "ems_database"
        sample_path = None
        if force_refresh or target_date is None:
            try:
                forecast_data = await call_forecast_service(model_name=model_name, input_file=input_file)
            except ConnectionError as error:
                logger.warning("Forecast service unavailable: %s", error)
                source = "ems_database_fallback"
            else:
                points = forecast_data.get("forecast", [])
                samples = build_sample_window(points)
                if samples:
                    sample_path = save_sample_window_json(samples)
                    if not sample_path:
                        raise OSError("Could not save sample window forecast")
                    strategy = ScheduleStrategy(resolver=DeviceConfigResolver(), sample_data=samples)
                    schedule = await strategy.prepare_ems_plan_schedule()
                    await save_provisional_schedule(points, samples, schedule, model_name)
                    source = "forecast_microservice"

        first_day = target_date or today
        last_day = target_date or tomorrow
        forecast = await get_forecast_from_db(target_date=first_day, model_name=model_name)
        if first_day != last_day:
            forecast += await get_forecast_from_db(target_date=last_day, model_name=model_name)
        schedule = await get_provisional_schedule(first_day, last_day, model_name)

        columns = {
            "load_forecast": "forecast_load_kw",
            "poa": "poa",
            "pv": "pv_kw",
            "grid_charge": "grid_charge_kw",
            "discharge": "discharge_kw",
            "bess_power": "bess_power_kw",
            "grid_import": "grid_import_kw",
        }
        chart_series = {name: [] for name in (*columns, "charge")}
        for row in schedule:
            timestamp = int(datetime.fromisoformat(row["target_ts"]).timestamp() * 1000)
            for name, column in columns.items():
                chart_series[name].append([timestamp, row[column]])
            chart_series["charge"].append([timestamp, round(row["charge_kw"] + row["grid_charge_kw"], 2)])

        charge_kwh = sum(value for _, value in chart_series["charge"]) / 60
        discharge_kwh = sum(value for _, value in chart_series["discharge"]) / 60
        return jsonify({
            "status": "success",
            "date": target_date.isoformat() if target_date else "all",
            "filter": filter_param,
            "source": source,
            "sample_window_json_path": sample_path,
            "forecast": forecast,
            "schedule": schedule,
            "chart_series": chart_series,
            "summary": {
                "total_forecast_points": len(forecast),
                "total_schedules": len(schedule),
                "total_charge_kwh": round(charge_kwh, 2),
                "total_discharge_kwh": round(discharge_kwh, 2),
                "net_discharge_kwh": round(discharge_kwh - charge_kwh, 2),
                "peak_load_kw": max((row["forecast_load_kw"] for row in schedule), default=0.0),
            },
        }), 200

    except Exception as e:
        logger.exception("Unexpected error in /api/load-and-schedule: %s", e)
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@load_forecast_bp.route("/api/provisional-schedule/delete", methods=["POST"])
async def delete_schedule_for_date():
    body = await request.get_json(silent=True) or {}
    value = request.args.get("date") or (body.get("date") if isinstance(body, dict) else None)
    try:
        target_date = date.fromisoformat(value)
        if value != target_date.isoformat():
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({"error": "Pass date in YYYY-MM-DD format"}), 400

    forecasts, schedules = await delete_forecast_and_provisional_schedule(target_date)
    return jsonify({
        "date": target_date.isoformat(),
        "deleted_load_forecast_rows": forecasts,
        "deleted_provisional_schedule_rows": schedules,
    }), 200


# =====================================================================
# STANDARD LOAD FORECAST ENDPOINT (Backward Compatible)
# =====================================================================
@load_forecast_bp.route("/api/loadforecast", methods=["GET", "POST"])
async def get_forecast():
    """
    API endpoint in EMS Backend to fetch, store, and serve Load Forecasts.
    - Delegates calculation to the Load Forecasting microservice.
    - Automatically stores/updates predictions into PostgreSQL `load_forecast` table.
    - Supports retrieving cached DB forecasts using ?date=YYYY-MM-DD or ?filter=tomorrow.
    - Supports ?include_schedule=true to return combined response.
    """
    try:
        if request.method == "POST":
            body = await request.get_json(silent=True) or {}
            input_file = body.get("input_file")
            model_name = body.get("model_name") or "model_clean"
            filter_param = body.get("filter")
            date_param = body.get("date")
            include_schedule = str(body.get("include_schedule", "false")).lower() in ("true", "1")
            force_refresh = str(body.get("force_refresh", "false")).lower() in ("true", "1")
        else:
            input_file = request.args.get("input_file")
            model_name = request.args.get("model_name") or "model_clean"
            filter_param = request.args.get("filter")
            date_param = request.args.get("date")
            include_schedule = request.args.get("include_schedule", "false").lower() in ("true", "1")
            force_refresh = request.args.get("force_refresh", "false").lower() in ("true", "1")

        target_date = None
        if date_param:
            try:
                target_date = datetime.strptime(date_param.strip(), "%Y-%m-%d").date()
            except ValueError:
                return jsonify({"error": "Invalid date format, expected YYYY-MM-DD"}), 400
        elif filter_param == "today":
            target_date = date.today()
        elif filter_param in ("tomorrow", "next_day"):
            target_date = date.today() + timedelta(days=1)

        # If include_schedule is requested, forward directly to combined logic
        if include_schedule:
            result = await get_combined_load_and_schedule(
                target_date=target_date,
                filter_param=filter_param,
                force_refresh=force_refresh,
                input_file=input_file,
                model_name=model_name
            )
            return jsonify(result), 200

        # If a specific date or tomorrow is requested and force_refresh is not requested, check DB first
        if target_date and not force_refresh:
            stored_data = await get_forecast_from_db(target_date=target_date, model_name=model_name)
            if stored_data:
                return jsonify({
                    "status": "success",
                    "source": "ems_database",
                    "table_name": "load_forecast",
                    "filter_date": str(target_date),
                    "total_points": len(stored_data),
                    "forecast": stored_data
                }), 200

        # Call the Load Forecasting microservice
        forecast_data = await call_forecast_service(model_name=model_name, input_file=input_file)

        # Persist points into PostgreSQL `load_forecast` table
        forecast_points = forecast_data.get("forecast", [])
        stored_count = await save_forecast_to_db(forecast_points, model_name=model_name)

        # Save Sample Window JSON on EMS Backend end via .env path
        saved_json_path = build_and_save_sample_window_json(forecast_points)

        return jsonify({
            "status": "success",
            "source": "ems_backend_api",
            "stored_in_db": True,
            "records_stored": stored_count,
            "table_name": "load_forecast",
            "sample_window_json_path": saved_json_path,
            "data": forecast_data
        }), 200

    except ConnectionError as ce:
        logger.warning("Forecasting service unavailable, attempting DB fallback: %s", ce)
        fallback_data = await get_forecast_from_db(target_date=target_date, model_name=model_name)
        if fallback_data:
            return jsonify({
                "status": "partial_success",
                "source": "ems_database_fallback",
                "warning": "Forecasting service offline, serving stored DB forecast",
                "table_name": "load_forecast",
                "total_points": len(fallback_data),
                "forecast": fallback_data
            }), 200

        return jsonify({
            "status": "error",
            "error_type": "service_unavailable",
            "message": str(ce)
        }), 503
    except Exception as e:
        logger.exception("Unexpected error in /api/loadforecast: %s", e)
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
