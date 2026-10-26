import asyncio
from typing import List, Dict, Any, Optional

from quart import Blueprint, jsonify, request
from model.db import get_dict_cursor

alarms_bp = Blueprint("alarms", __name__)

# =========================================================
# SERVICE FUNCTIONS
# =========================================================

def _fetch_latest_alarms() -> List[Dict[str, Any]]:
    conn = get_dict_cursor()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                device_id,
                tag_name,
                time_stamp,
                severity,
                alarm_type,
                value,
                status,
                comment,
                ack_timestamp,
                clr_timestamp
            FROM (
                SELECT DISTINCT ON (device_id, tag_name)
                    device_id,
                    tag_name,
                    time_stamp,
                    severity,
                    alarm_type,
                    value,
                    status,
                    comment,
                    ack_timestamp,
                    clr_timestamp
                FROM alarms
                WHERE time_stamp >= date_trunc('day', now())
                ORDER BY device_id, tag_name, time_stamp DESC
            ) latest
            WHERE status = 'ACTIVE'
            ORDER BY time_stamp DESC;
            """
        )
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def _fetch_alarm_history(
    limit: int = 1000,
    hours: Optional[int] = None,
    start_ts: Optional[str] = None,
    end_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    conn = get_dict_cursor()
    cur = conn.cursor()
    try:
        # 🔹 Priority 1: startTs + endTs
        if start_ts and end_ts:
            cur.execute(
                """
                SELECT
                    device_id,
                    tag_name,
                    time_stamp,
                    severity,
                    alarm_type,
                    value,
                    status,
                    comment,
                    ack_timestamp,
                    clr_timestamp
                FROM alarms
                WHERE time_stamp BETWEEN %s::timestamp AND %s::timestamp
                ORDER BY time_stamp DESC
                LIMIT %s;
                """,
                (start_ts, end_ts, limit),
            )

        # 🔹 Priority 2: hours-based (existing)
        elif hours:
            cur.execute(
                """
                SELECT
                    device_id,
                    tag_name,
                    time_stamp,
                    severity,
                    alarm_type,
                    value,
                    status,
                    comment,
                    ack_timestamp,
                    clr_timestamp
                FROM alarms
                WHERE time_stamp >= NOW() - (%s || ' hours')::interval
                ORDER BY time_stamp DESC
                LIMIT %s;
                """,
                (hours, limit),
            )

        # 🔹 Priority 3: default
        else:
            cur.execute(
                """
                SELECT
                    device_id,
                    tag_name,
                    time_stamp,
                    severity,
                    alarm_type,
                    value,
                    status,
                    comment,
                    ack_timestamp,
                    clr_timestamp
                FROM alarms
                ORDER BY time_stamp DESC
                LIMIT %s;
                """,
                (limit,),
            )

        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def _update_alarm(
    device_id: str,
    tag_name: str,
    time_stamp: str,   # ISO string
    status: str,
    comment: Optional[str],
) -> bool:
    """
    ACK  -> sets ack_timestamp
    CLR  -> sets clr_timestamp
    """
    conn = get_dict_cursor()
    cur = conn.cursor()
    try:
        if status == "ACK":
            cur.execute(
                """
                UPDATE alarms
                SET
                    status = 'ACK',
                    comment = %s,
                    ack_timestamp = NOW()
                WHERE device_id = %s
                  AND tag_name = %s
                  AND time_stamp = %s::timestamp;
                """,
                (comment, device_id, tag_name, time_stamp),
            )

        elif status == "CLR":
            cur.execute(
                """
                UPDATE alarms
                SET
                    status = 'CLR',
                    comment = %s,
                    clr_timestamp = NOW()
                WHERE device_id = %s
                  AND tag_name = %s
                  AND time_stamp = %s::timestamp;
                """,
                (comment, device_id, tag_name, time_stamp),
            )

        conn.commit()
        return cur.rowcount > 0
    finally:
        cur.close()
        conn.close()

# =========================================================
# API ROUTES
# =========================================================

@alarms_bp.route("/latest", methods=["GET"])
async def api_latest_alarms():
    """
    GET /api/alarms/latest
    """
    rows = await asyncio.to_thread(_fetch_latest_alarms)

    return jsonify(rows)


@alarms_bp.route("/history", methods=["GET"])
async def api_alarm_history():
    """
    GET /api/alarms/history

    Query params supported:
      ?startTs=YYYY-MM-DD HH:MM:SS
      &endTs=YYYY-MM-DD HH:MM:SS
      &hours=24
      &limit=1000
    """
    limit = int(request.args.get("limit", 1000))

    hours = request.args.get("hours")
    hours = int(hours) if hours else None

    start_ts = request.args.get("startTs")
    end_ts = request.args.get("endTs")

    rows = await asyncio.to_thread(
        _fetch_alarm_history,
        limit,
        hours,
        start_ts,
        end_ts,
    )

    return jsonify(rows)


@alarms_bp.route("/update", methods=["POST"])
async def api_update_alarm():
    """
    POST /api/alarms/update

    Body:
    {
      device_id,
      tag_name,
      time_stamp,   // ISO timestamp
      action: "ack" | "clr",
      comment
    }
    """
    data = await request.get_json()

    device_id = data.get("device_id")
    tag_name = data.get("tag_name")
    time_stamp = data.get("time_stamp")
    action = data.get("action")
    comment = data.get("comment")

    if not all([device_id, tag_name, time_stamp, action]):
        return jsonify({"error": "Missing fields"}), 400

    if action not in ("ack", "clr"):
        return jsonify({"error": "Invalid action"}), 400

    status = "ACK" if action == "ack" else "CLR"

    updated = await asyncio.to_thread(
        _update_alarm,
        device_id,
        tag_name,
        time_stamp,
        status,
        comment,
    )
    

    if not updated:
        return jsonify({"error": "Alarm not found"}), 404

    return jsonify({
        "message": "Alarm updated successfully",
        "status": status
    })