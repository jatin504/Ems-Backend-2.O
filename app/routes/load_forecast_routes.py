import json
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from config.device_config_resolver import DeviceConfigResolver
from controller_layer.strategies.classes.schedule_strategy import ScheduleStrategy
from quart import Blueprint, request, jsonify
from app.services.load_forecast_services import (
    call_forecast_service,
    save_forecast_to_db,
    get_forecast_from_db,
    get_combined_load_and_schedule,
    build_and_save_sample_window_json,
)

logger = logging.getLogger(__name__)

load_forecast_bp = Blueprint("load_forecast_bp", __name__)

# =====================================================================
# COMBINED ENDPOINT: Load Forecast + Strategy Schedule (1 Single API)
# =====================================================================
@load_forecast_bp.route("/api/load-schedule", methods=["GET", "POST"])
@load_forecast_bp.route("/api/load-and-schedule", methods=["GET", "POST"])
@load_forecast_bp.route("/api/forecast-schedule", methods=["GET", "POST"])
@load_forecast_bp.route("/api/load-forecast-schedule", methods=["GET", "POST"])
async def get_load_and_schedule():
    """
    Unified single API endpoint returning BOTH Load Forecast and Strategy Schedule.
    Designed specifically for the single UI widget ("Load vs BESS Schedule").

    Query Params / JSON Body:
      - filter: "today", "tomorrow" / "next_day", "all"
      - date: specific date in YYYY-MM-DD format (e.g. 2026-10-08)
      - force_refresh: "true" / "false" (re-run model even if DB has data)
      - input_file: optional path to historical data CSV
      - model_name: "model_clean" (default) or "model_dirty"

    Response contains:
      - forecast: raw load forecast points list
      - schedule: raw energy strategy schedules list
      - chart_series: Highcharts-ready series for load_forecast, charge, discharge
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

        result = await get_combined_load_and_schedule(
            target_date=target_date,
            filter_param=filter_param,
            force_refresh=force_refresh,
            input_file=input_file,
            model_name=model_name
        )

        result_file = Path("data/sample_window_forecast.json")
        with result_file.open("r", encoding="utf-8") as f:
            data = json.load(f)
        
        device_resolver = DeviceConfigResolver()
        strategy = ScheduleStrategy(resolver=device_resolver, sample_data=data)

        await strategy.prepare_ems_plan_schedule()

        logger.info("SCHEDULE PREPARED.........")

        return jsonify(result), 200

    except Exception as e:
        logger.exception("Unexpected error in /api/load-and-schedule: %s", e)
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


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
