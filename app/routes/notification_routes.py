import asyncio
from typing import List, Dict, Any, Optional

from quart import Blueprint, jsonify, request
from model.db import get_dict_cursor

notification_bp = Blueprint("notification", __name__)


# SERVICE FUNCTIONS


def _fetch_notification_events(
    page: int = 1,
    page_size: int = 20,
    status: Optional[str] = None,
    search: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    sort_by: Optional[str] = "event_time",          #  ADDED
    sort_order: Optional[str] = "desc"              #  ADDED
) -> Dict[str, Any]:

    conn = get_dict_cursor()
    cur = conn.cursor()

    try:
        offset = (page - 1) * page_size

        query = """
            SELECT
                id,
                event_time,
                source,
                event_type,
                severity,
                title,
                message,
                entity_type,
                entity_id,
                status,
                acknowledged,
                acknowledged_by,
                acknowledged_time,
                cleared_time,
                metadata,
                created_at
            FROM public.notification_event
            WHERE 1=1
        """

        params = []

        #  status filter
        if status:
            query += " AND status = %s"
            params.append(status)

        #  search filter
        if search and search.strip():
            query += """
                AND (
                    LOWER(COALESCE(message, '')) LIKE %s OR
                    LOWER(COALESCE(event_type, '')) LIKE %s OR
                    LOWER(COALESCE(entity_id, '')) LIKE %s OR
                    LOWER(COALESCE(title, '')) LIKE %s OR
                    LOWER(COALESCE(source, '')) LIKE %s OR
                    LOWER(COALESCE(severity, '')) LIKE %s OR
                    LOWER(COALESCE(status, '')) LIKE %s OR
                    LOWER(COALESCE(entity_type, '')) LIKE %s OR
                    CAST(metadata AS TEXT) ILIKE %s
                )
            """

            search_term = f"%{search.lower()}%"
            params.extend([
                search_term, search_term, search_term,
                search_term, search_term, search_term,
                search_term, search_term,
                search_term
            ])

        #  date filter
        if from_date:
            query += " AND event_time >= %s"
            params.append(from_date)

        if to_date:
            query += " AND event_time <= %s"
            params.append(to_date)

        #  total count
        count_query = f"SELECT COUNT(*) FROM ({query}) AS total"
        cur.execute(count_query, tuple(params))
        total = cur.fetchone()["count"]

        #  sorting logic  ADDED
        valid_columns = ["event_time", "created_at", "severity", "status"]
        if sort_by not in valid_columns:
            sort_by = "event_time"

        sort_order = "ASC" if sort_order.lower() == "asc" else "DESC"

        #  pagination + sorting
        query += f" ORDER BY {sort_by} {sort_order} LIMIT %s OFFSET %s"
        params.extend([page_size, offset])

        cur.execute(query, tuple(params))
        rows = cur.fetchall()

        return {
            "data": rows,
            "total": total,
            "page": page,
            "page_size": page_size
        }

    finally:
        cur.close()
        conn.close()



# ACK FUNCTION


def _ack_notification(event_id: int, user: str) -> bool:
    conn = get_dict_cursor()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            UPDATE public.notification_event
            SET
                acknowledged = TRUE,
                acknowledged_by = %s,
                acknowledged_time = NOW()
            WHERE id = %s;
            """,
            (user, event_id),
        )

        conn.commit()
        return cur.rowcount > 0

    finally:
        cur.close()
        conn.close()



# CLEAR FUNCTION (optional)


def _clear_notification(event_id: int, user: str) -> bool:
    conn = get_dict_cursor()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            UPDATE public.notification_event
            SET
                status = 'cleared',
                cleared_time = NOW()
            WHERE id = %s;
            """,
            (event_id,),
        )

        conn.commit()
        return cur.rowcount > 0

    finally:
        cur.close()
        conn.close()



# API ROUTES


#  GET LIST
@notification_bp.route("/list", methods=["GET"])
async def api_notification_events():
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 20))
    status = request.args.get("status")
    search = request.args.get("search")
    from_date = request.args.get("fromDate")
    to_date = request.args.get("toDate")

    #  ADDED sorting params
    sort_by = request.args.get("sortBy", "event_time")
    sort_order = request.args.get("sortOrder", "desc")

    result = await asyncio.to_thread(
        _fetch_notification_events,
        page,
        page_size,
        status,
        search,
        from_date,
        to_date,
        sort_by,          #  ADDED
        sort_order        #  ADDED
    )

    return jsonify(result)


#  ACK API
@notification_bp.route("/ack", methods=["POST"])
async def api_ack_notification():
    data = await request.get_json()

    event_id = data.get("id")
    user = data.get("user", "system")

    if not event_id:
        return jsonify({"error": "Missing id"}), 400

    updated = await asyncio.to_thread(_ack_notification, event_id, user)

    if not updated:
        return jsonify({"error": "Notification not found"}), 404

    return jsonify({
        "message": "Acknowledged successfully"
    })


#  CLEAR API
@notification_bp.route("/clear", methods=["POST"])
async def api_clear_notification():
    data = await request.get_json()

    event_id = data.get("id")

    if not event_id:
        return jsonify({"error": "Missing id"}), 400

    updated = await asyncio.to_thread(_clear_notification, event_id, "system")

    if not updated:
        return jsonify({"error": "Notification not found"}), 404

    return jsonify({
        "message": "Cleared successfully"
    })


#  UNACK COUNT API
@notification_bp.route("/unack-count", methods=["GET"])
async def get_unack_count():
    conn = get_dict_cursor()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT COUNT(*) as count
            FROM public.notification_event
            WHERE acknowledged = FALSE
        """)
        count = cur.fetchone()["count"]
        return jsonify({"count": count})
    finally:
        cur.close()
        conn.close()