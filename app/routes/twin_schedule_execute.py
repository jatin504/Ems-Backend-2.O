import asyncio
import json
import logging
from pathlib import Path

from quart import Blueprint, request, jsonify

from config.device_config_resolver import DeviceConfigResolver
from controller_layer.strategies.classes.schedule_strategy import ScheduleStrategy

logger = logging.getLogger(__name__)

twin_schedule_exec_bp = Blueprint("twin_schedule_exec", __name__)

# Track running tasks to prevent them from being garbage collected
# and to allow duplicate execution checks.
running_tasks: dict[str, asyncio.Task] = {}

result_file = Path("data/sample_window_forecast.json")
with result_file.open("r", encoding="utf-8") as f:
    data = json.load(f)

device_resolver = DeviceConfigResolver()
strategy = ScheduleStrategy(resolver=device_resolver, sample_data=data)

async def run_schedule(
    channel,
    server_ip,
    server_port,
    slave_id,
    device_id,
    event_number,
):
    try:

        if event_number == 0:
            p = await strategy.prepare_plan_schedule()
            return 


        await strategy.execute(
            channel,
            server_ip,
            server_port,
            slave_id,
            device_id,
            None,
            event_number,
        )

        logger.info(
            "Schedule execution completed for %s",
            device_id,
        )

    except asyncio.CancelledError:
        logger.info(
            "Schedule execution cancelled for %s",
            device_id,
        )
        raise

    except Exception:
        logger.exception(
            "Schedule execution failed for %s",
            device_id,
        )

    finally:
        # Avoid removing a newer task accidentally.
        current_task = asyncio.current_task()

        if running_tasks.get(device_id) is current_task:
            running_tasks.pop(device_id, None)


@twin_schedule_exec_bp.route(
    "/api/twin_schedule_exec",
    methods=["POST"],
)
async def twin_schedule_exec():
    try:
        body = await request.get_json()

        if not isinstance(body, dict):
            return jsonify({
                "success": False,
                "error": "A JSON object is required",
            }), 400

        event_number = body.get("event_number", 1)

        if event_number is None or event_number == "":
            return jsonify({
                "success": False,
                "error": "event_number required",
            }), 400

        device_id = "plant_1_pcs_controller_1"

        # Prevent overlapping executions for the same device.
        existing_task = running_tasks.get(device_id)

        if existing_task and not existing_task.done():
            return jsonify({
                "success": False,
                "message": "Schedule execution is already running",
                "device_id": device_id,
            }), 409

        device_resolver = DeviceConfigResolver()
        device_info = device_resolver.get_device_info_by_id(device_id)

        if not device_info:
            logger.error(
                "Device info not found for %s",
                device_id,
            )
            return jsonify({
                "success": False,
                "error": f"Device info not found: {device_id}",
            }), 404

        task = asyncio.create_task(
            run_schedule(
                device_info["channel"],
                device_info["server_ip"],
                device_info["server_port"],
                device_info["slave_id"],
                device_id,
                event_number,
            )
        )

        running_tasks[device_id] = task

        return jsonify({
            "success": True,
            "message": "Schedule execution started",
            "device_id": device_id,
            "event_number": event_number,
        }), 202

    except Exception:
        logger.exception("Twin Schedule Execution API error")

        return jsonify({
            "success": False,
            "error": "Failed to start schedule execution",
        }), 500


@twin_schedule_exec_bp.route(
    "/api/twin_schedule_stop",
    methods=["POST"],
)
async def twin_schedule_stop():
    try:
        device_id = "plant_1_pcs_controller_1"

        task = running_tasks.get(device_id)

        if task is None or task.done():
            return jsonify({
                "success": False,
                "message": "No schedule execution is running",
                "device_id": device_id,
            }), 404

        task.cancel()

        logger.info(
            "Stop requested for schedule execution: %s",
            device_id,
        )

        return jsonify({
            "success": True,
            "message": "Schedule stop requested",
            "device_id": device_id,
        }), 200

    except Exception:
        logger.exception("Twin Schedule Stop API error")

        return jsonify({
            "success": False,
            "error": "Failed to stop schedule execution",
        }), 500
    