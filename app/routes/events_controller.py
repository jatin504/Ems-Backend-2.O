import asyncio
from typing import List, Dict, Any, Optional

from quart import Blueprint, jsonify, request
from model.db import get_dict_cursor

# =========================================================
# NEW EVENT BLUEPRINT
# =========================================================

events_bp = Blueprint("events", __name__)

# =========================================================
# SERVICE FUNCTIONS
# =========================================================

def _fetch_latest_events(
    limit: int = 10,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    conn = get_dict_cursor()
    cur = conn.cursor()

    try:
        cur.execute(
            """
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
            FROM events
            WHERE time_stamp >= date_trunc('day', now())
            ORDER BY device_id, tag_name, time_stamp DESC
            LIMIT %s OFFSET %s;
            """,
            (limit, offset),
        )

        return cur.fetchall()

    finally:
        cur.close()
        conn.close()


def _fetch_event_history(
    limit: int = 1000,
    hours: Optional[int] = None,
    start_ts: Optional[str] = None,
    end_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:

    conn = get_dict_cursor()
    cur = conn.cursor()

    try:

        # =====================================================
        # PRIORITY 1 → startTs + endTs
        # =====================================================

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
                FROM events
                WHERE time_stamp BETWEEN %s::timestamp AND %s::timestamp
                ORDER BY time_stamp DESC
                LIMIT %s;
                """,
                (start_ts, end_ts, limit),
            )

        # =====================================================
        # PRIORITY 2 → hours
        # =====================================================

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
                FROM events
                WHERE time_stamp >= NOW() - (%s || ' hours')::interval
                ORDER BY time_stamp DESC
                LIMIT %s;
                """,
                (hours, limit),
            )

        # =====================================================
        # PRIORITY 3 → default
        # =====================================================

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
                FROM events
                ORDER BY time_stamp DESC
                LIMIT %s;
                """,
                (limit,),
            )

        return cur.fetchall()

    finally:
        cur.close()
        conn.close()


def _update_event(
    device_id: str,
    tag_name: str,
    time_stamp: str,
    status: str,
    comment: Optional[str],
) -> bool:

    conn = get_dict_cursor()
    cur = conn.cursor()

    try:

        # =====================================================
        # ACK EVENT
        # =====================================================

        if status == "ACK":

            cur.execute(
                """
                UPDATE events
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

        # =====================================================
        # CLR EVENT
        # =====================================================

        elif status == "CLR":

            cur.execute(
                """
                UPDATE events
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


@events_bp.route("/latest", methods=["GET"])
async def api_latest_events():
    """
    GET /api/events/latest
    """

    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 10))

    offset = (page - 1) * limit

    rows = await asyncio.to_thread(
        _fetch_latest_events,
        limit,
        offset,
    )

    return jsonify(rows)


@events_bp.route("/history", methods=["GET"])
async def api_event_history():
    """
    GET /api/events/history

    Query Params:
      ?startTs=
      &endTs=
      &hours=
      &limit=
    """

    limit = int(request.args.get("limit", 1000))

    hours = request.args.get("hours")
    hours = int(hours) if hours else None

    start_ts = request.args.get("startTs")
    end_ts = request.args.get("endTs")

    rows = await asyncio.to_thread(
        _fetch_event_history,
        limit,
        hours,
        start_ts,
        end_ts,
    )

    return jsonify(rows)


@events_bp.route("/update", methods=["POST"])
async def api_update_event():
    """
    POST /api/events/update
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
        _update_event,
        device_id,
        tag_name,
        time_stamp,
        status,
        comment,
    )

    if not updated:
        return jsonify({"error": "Event not found"}), 404

    return jsonify(
        {
            "message": "Event updated successfully",
            "status": status,
        }
    )