import json
import os

from quart import Blueprint, jsonify


ems_recommendation_bp = Blueprint(
    "ems_recommendation",
    __name__
)


RESULT_JSON_PATH = "D:/New folder/ems_backend_data/schedule_strategy/result.json"


# =========================================================
# FALSE SCHEDULES
# =========================================================
# HELPERS

def is_false(value):
    """
    Handles:
        false
        "false"
        0
        "0"
    """

    return (
        value is False
        or str(value).strip().lower() == "false"
        or value == 0
        or str(value).strip() == "0"
    )


def is_true(value):
    """
    Handles:
        true
        "true"
        1
        "1"
    """

    return (
        value is True
        or str(value).strip().lower() == "true"
        or value == 1
        or str(value).strip() == "1"
    )

@ems_recommendation_bp.get("/api/ems-recommendation")
async def get_ems_recommendation():

    try:

        if not os.path.exists(RESULT_JSON_PATH):

            return jsonify({
                "success": False,
                "message": "result.json not found",
                "results": [],
                "count": 0
            }), 404

        with open(
            RESULT_JSON_PATH,
            "r",
            encoding="utf-8"
        ) as file:

            result = json.load(file)

        schedules = result.get("schedule", [])
        results = result.get("results", [])

        
        # is_pass is inside results

        is_pass = False

        # if results:
        #     is_pass = is_true(
        #         results[0].get("is_pass")
        #     )

        # If schedule PASSED
        # No recommendation required

        # if is_pass:

        #     return jsonify({
        #         "success": True,
        #         "count": 0,
        #         "results": []
        #     })

        # If schedule FAILED
        # Return complete result objects

        failed_results = []

        for result_item in results:

            is_failed= is_false(
                result_item.get("is_pass")
            )

            if is_failed:

                bess_event_decision = result_item.get(
                    "bess_event_decision",
                    {}
                )

                result_item["event_id"] = (
                    bess_event_decision.get("event_id")
                )

                failed_results.append(result_item)

        # Response

        return jsonify({
            "success": True,
            "count": len(failed_results),
            "results": failed_results
        })

    except Exception as e:

        return jsonify({
            "success": False,
            "message": "Failed to read EMS recommendation",
            "error": str(e),
            "results": [],
            "count": 0
        }), 500


# =========================================================
# TRUE SCHEDULES
# =========================================================

@ems_recommendation_bp.get("/api/ems-schedule")
async def get_ems_schedule():

    try:
        if not os.path.exists(RESULT_JSON_PATH):
            return jsonify({
                "success": False,
                "message": "result.json not found",
                "schedule": [],
                "count": 0
            }), 404

        with open(
            RESULT_JSON_PATH,
            "r",
            encoding="utf-8"
        ) as file:
            result = json.load(file)

        schedules = result.get("schedule", [])

        # passed_schedules = [
        #     item
        #     for item in schedules
        #     if item.get("is_pass") is True
        # ]

        return jsonify({
            "success": True,
            "count": len(schedules),
            "schedule": schedules
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "message": "Failed to read EMS schedule",
            "error": str(e),
            "schedule": [],
            "count": 0
        }), 500