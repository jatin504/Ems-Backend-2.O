import asyncio
from typing import List, Dict, Any, Optional

from quart import Blueprint, jsonify, request
from model.db import get_dict_cursor

report_delivery_log_bp = Blueprint("report_delivery_log", __name__)


def _fetch_report_logs(
    limit: int,
    offset: int,
    search: Optional[str],
    start_ts: Optional[str],
    end_ts: Optional[str],
) -> Dict[str, Any]:

    conn = get_dict_cursor()
    cur = conn.cursor()

    try:

        base_query = """
            FROM report_delivery_log
            WHERE 1=1
        """

        params = []

        # ✅ SEARCH
        if search:
            base_query += """
                AND (
                    report_name ILIKE %s
                    OR status ILIKE %s
                    OR COALESCE(error,'') ILIKE %s
                )
            """
            params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

        # ✅ DATE RANGE
        if start_ts and end_ts:
            base_query += """
                AND sent_at BETWEEN %s::timestamp AND %s::timestamp
            """
            params.extend([start_ts, end_ts])

        # ✅ TOTAL COUNT
        count_query = "SELECT COUNT(*) " + base_query
        cur.execute(count_query, tuple(params))
        total = cur.fetchone()["count"]

        # ✅ DATA QUERY
        data_query = """
            SELECT
                id,
                report_name,
                run_date,
                sent_at,
                status,
                error
        """ + base_query + """
            ORDER BY sent_at DESC
            LIMIT %s OFFSET %s
        """

        params.extend([limit, offset])

        cur.execute(data_query, tuple(params))
        rows = cur.fetchall()

        return {
            "logs": rows,
            "pagination": {
                "total": total,
                "page_size": limit
            }
        }

    finally:
        cur.close()
        conn.close()


@report_delivery_log_bp.route("/report-delivery-logs", methods=["GET"])
async def api_report_delivery_logs():

    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 10))

    search = request.args.get("search")
    start_ts = request.args.get("startTs")
    end_ts = request.args.get("endTs")

    offset = (page - 1) * limit

    result = await asyncio.to_thread(
        _fetch_report_logs,
        limit,
        offset,
        search,
        start_ts,
        end_ts,
    )

    return jsonify(result)