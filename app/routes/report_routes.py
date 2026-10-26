from quart import Blueprint, request, jsonify
import json

from model.db import get_async_connection
from app.utils.jwt import decode_access_token
from app.utils.db_utils import DB

report_bp = Blueprint("report_bp", __name__)


# SAVE TEMPLATE
@report_bp.route("/templates", methods=["POST"])
async def save_template():
    body = await request.get_json(force=True)

    report_name = (body or {}).get("reportName")
    if not report_name:
        return jsonify({"ok": False, "error": "reportName is required"}), 400

    schedule = body.get("schedule")
    email = body.get("email") if schedule == "email" else None

    schedule_time = body.get("scheduleTime")
    schedule_time = int(schedule_time) if schedule_time not in (None, "") else None

    #  NEW FIELD (BACKWARD SAFE)
    schedule_minute = body.get("scheduleMinute")
    schedule_minute = int(schedule_minute) if schedule_minute not in (None, "") else 0

    schedule_live = body.get("scheduleLive")
    schedule_live = int(schedule_live) if schedule_live not in (None, "") else None

    user_id = body.get("userId")

    config = {
        "dateType": body.get("selectedDateType"),
        "frequency": body.get("selectedFrequency"),
        "interval": body.get("selectedInterval"),
        "deviceTypes": body.get("selectedDeviceTypes") or [],
        "deviceIds": body.get("selectedDeviceIds") or [],
        "params": body.get("selectedParams") or [],
    }

    if body.get("selectedDateType") == "history":
        config["history"] = {
            "startTs": body.get("historyStartTs"),
            "endTs": body.get("historyEndTs"),
        }

    is_scheduled = schedule_time is not None

    auth = request.headers.get("Authorization", "")
    token_user_id = None
    if auth.lower().startswith("bearer "):
        payload = decode_access_token(auth.split(" ", 1)[1].strip())
        if payload:
            token_user_id = payload.get("user_id")

    async with DB() as conn:
        if schedule == "self":
            user_id = token_user_id or user_id
            if not user_id:
                return jsonify({"ok": False, "error": "Unauthorized"}), 401

            row = await conn.fetchrow("SELECT email FROM users WHERE id=$1", user_id)
            if row:
                email = row["email"]

        await conn.execute(
            """
            INSERT INTO report_table
                (report_name, configuration, user_id, is_scheduled, email, schedule_time, schedule_minute, schedule_live)
            VALUES ($1,$2::jsonb,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (report_name) DO UPDATE SET
                configuration=EXCLUDED.configuration,
                user_id=COALESCE(EXCLUDED.user_id,report_table.user_id),
                is_scheduled=EXCLUDED.is_scheduled,
                email=EXCLUDED.email,
                schedule_time=EXCLUDED.schedule_time,
                schedule_minute=EXCLUDED.schedule_minute,
                schedule_live=EXCLUDED.schedule_live;
            """,
            report_name,
            json.dumps(config),
            user_id,
            is_scheduled,
            email,
            schedule_time,
            schedule_minute,
            schedule_live,
        )

    return jsonify({"ok": True, "reportName": report_name})


# GET ALL
@report_bp.route("/templates", methods=["GET"])
async def get_templates():
    async with DB() as conn:
        rows = await conn.fetch(
            """
            SELECT report_name,email,schedule_time,schedule_minute,schedule_live,configuration
            FROM report_table
            ORDER BY report_name
            """
        )

    return jsonify([
        {
            "reportName": r["report_name"],
            "email": r["email"],
            "scheduleTime": r["schedule_time"],
            "scheduleMinute": r["schedule_minute"],   #  NEW
            "scheduleLive": r["schedule_live"],
            "configuration": r["configuration"],
        }
        for r in rows
    ])


# GET SINGLE
@report_bp.route("/templates/<string:report_name>", methods=["GET"])
async def get_single_template(report_name):
    async with DB() as conn:
        row = await conn.fetchrow(
            """
            SELECT report_name,email,schedule_time,schedule_minute,schedule_live,configuration 
            FROM report_table 
            WHERE report_name=$1
            """,
            report_name,
        )

    if not row:
        return jsonify({"ok": False, "error": "Template not found"}), 404

    return jsonify({
        "ok": True,
        "reportName": row["report_name"],
        "email": row["email"],
        "scheduleTime": row["schedule_time"],
        "scheduleMinute": row["schedule_minute"],   #  NEW
        "scheduleLive": row["schedule_live"],
        "configuration": row["configuration"],
    })


# UPDATE TEMPLATE
@report_bp.route("/templates/<string:report_name>", methods=["PUT"])
async def update_template(report_name):
    try:
        body = await request.get_json(force=True)

        email = body.get("email")

        schedule_time = body.get("scheduleTime")
        schedule_time = int(schedule_time) if schedule_time not in (None, "") else None

        #  NEW FIELD
        schedule_minute = body.get("scheduleMinute")
        schedule_minute = int(schedule_minute) if schedule_minute not in (None, "") else 0

        schedule_live = body.get("scheduleLive")
        schedule_live = int(schedule_live) if schedule_live not in (None, "") else None

        config = {
            "dateType": body.get("selectedDateType"),
            "frequency": body.get("selectedFrequency"),
            "interval": body.get("selectedInterval"),
            "deviceTypes": body.get("selectedDeviceTypes") or [],
            "deviceIds": body.get("selectedDeviceIds") or [],
            "params": body.get("selectedParams") or [],
        }

        if body.get("selectedDateType") == "history":
            config["history"] = {
                "startTs": body.get("historyStartTs"),
                "endTs": body.get("historyEndTs"),
            }

        async with DB() as conn:
            await conn.execute(
                """
                UPDATE report_table
                SET configuration=$1::jsonb,
                    email=$2::text,
                    schedule_time=$3::int,
                    schedule_minute=$4::int,
                    schedule_live=$5::int,
                    is_scheduled = CASE WHEN $3 IS NOT NULL THEN TRUE ELSE FALSE END
                WHERE report_name=$6
                """,
                json.dumps(config),
                email,
                schedule_time,
                schedule_minute,
                schedule_live,
                report_name,
            )

        return jsonify({
            "ok": True,
            "reportName": report_name,
            "message": "Template updated successfully"
        })

    except Exception as e:
        print("UPDATE TEMPLATE ERROR:", str(e))
        return jsonify({"ok": False, "error": str(e)}), 500


# DELETE
@report_bp.route("/templates/<string:report_name>", methods=["DELETE"])
async def delete_template(report_name):
    async with DB() as conn:
        await conn.execute("DELETE FROM report_table WHERE report_name=$1", report_name)

    return jsonify({"ok": True, "deleted": report_name})


# SCHEDULE EMAIL
@report_bp.route("/schedule", methods=["POST"])
async def schedule_report():
    body = await request.get_json(force=True)

    report_name = body.get("reportName")
    email = body.get("email")

    schedule_live = int(body.get("scheduleLive"))
    schedule_time = int(body.get("scheduleTime"))

    #  NEW FIELD
    schedule_minute = int(body.get("scheduleMinute", 0))

    config = {
        "dateType": body.get("selectedDateType", "relative"),
        "frequency": body.get("selectedFrequency", "daily"),
        "interval": body.get("selectedInterval", "15min"),
        "deviceTypes": body.get("selectedDeviceTypes") or [],
        "deviceIds": body.get("selectedDeviceIds") or [],
        "params": body.get("selectedParams") or [],
    }

    user_id = body.get("userId")

    async with DB() as conn:
        await conn.execute(
            """
            INSERT INTO report_table
            (report_name,configuration,user_id,is_scheduled,email,schedule_time,schedule_minute,schedule_live)
            VALUES ($1,$2::jsonb,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (report_name) DO UPDATE SET
                configuration=EXCLUDED.configuration,
                user_id=COALESCE(EXCLUDED.user_id,report_table.user_id),
                is_scheduled=TRUE,
                email=EXCLUDED.email,
                schedule_time=EXCLUDED.schedule_time,
                schedule_minute=EXCLUDED.schedule_minute,
                schedule_live=EXCLUDED.schedule_live;
            """,
            report_name,
            json.dumps(config),
            user_id,
            True,
            email,
            schedule_time,
            schedule_minute,
            schedule_live,
        )

    return jsonify({
        "ok": True,
        "reportName": report_name,
        "message": f"Report scheduled at {schedule_time}:{schedule_minute:02d}"
    })
