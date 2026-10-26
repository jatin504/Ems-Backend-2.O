import asyncio
from typing import List, Dict, Any, Optional

from quart import Blueprint, jsonify, request
from model.db import get_dict_cursor

dashboard_logs_bp = Blueprint("dashboard_logs", __name__)


def _fetch_dashboard_logs(
    limit: int = 200,
    search: Optional[str] = None,
    start_ts: Optional[str] = None,
    end_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:

    conn = get_dict_cursor()
    cur = conn.cursor()

    try:

        base_query = """
            SELECT
                id,
                event_time,
                action,
                control_type,
                module,
                username,
                status
            FROM dashboard_logs
            WHERE 1=1
        """

        params = []

        # SEARCH
        if search:
            base_query += """
                AND (
                    username ILIKE %s
                    OR action ILIKE %s
                    OR module ILIKE %s
                )
            """
            params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

        # DATE RANGE (FULL DAY SAFE)
        if start_ts and end_ts:
            base_query += """
                AND event_time BETWEEN %s::timestamp AND %s::timestamp
            """
            params.extend([start_ts, end_ts])

        base_query += """
            ORDER BY event_time DESC
            LIMIT %s
        """
        
        params.append(limit)

        cur.execute(base_query, tuple(params))

        return cur.fetchall()

    finally:
        cur.close()
        conn.close()


@dashboard_logs_bp.route("/api/dashboard/logs", methods=["GET"])
async def api_dashboard_logs():

    limit = int(request.args.get("limit", 200))
    search = request.args.get("search")

    start_ts = request.args.get("startTs")
    end_ts = request.args.get("endTs")

    rows = await asyncio.to_thread(
        _fetch_dashboard_logs,
        limit,
        search,
        start_ts,
        end_ts,
    )

    return jsonify(rows)