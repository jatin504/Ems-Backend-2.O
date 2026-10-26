import asyncio
from typing import List, Dict, Any, Optional

from quart import Blueprint, jsonify, request
from model.db import get_dict_cursor


backend_health_logs_bp = Blueprint(
    "backend_health_logs",
    __name__,
)


def _fetch_backend_health_logs(
    page: int = 1,
    limit: int = 10,
    search: Optional[str] = None,
    start_ts: Optional[str] = None,
    end_ts: Optional[str] = None,
):
    conn = get_dict_cursor()
    cur = conn.cursor()

    try:
        # -----------------------------------------
        # OFFSET
        # -----------------------------------------
        offset = (page - 1) * limit

        # -----------------------------------------
        # BASE QUERY
        # -----------------------------------------
        base_query = """
            FROM (
                -- PRIMARY BACKEND HEALTH
                SELECT
                    id,
                    'PRIMARY BACKEND HEALTH' AS log_type,
                    down_at AS event_time,
                    down_at,
                    up_at,
                    duration_seconds,
                    status,
                    NULL::TEXT AS backend_name,
                    created_at
                FROM primary_backend_health

                UNION ALL

                -- BACKEND SELF HEALTH
                SELECT
                    id,
                    'BACKEND SELF HEALTH' AS log_type,
                    checked_at AS event_time,
                    NULL::TIMESTAMPTZ AS down_at,
                    NULL::TIMESTAMPTZ AS up_at,
                    NULL::DOUBLE PRECISION AS duration_seconds,
                    status,
                    backend_name,
                    checked_at AS created_at
                FROM backend_self_health
            ) AS health_logs
            WHERE 1=1
        """

        params = []

        # -----------------------------------------
        # SEARCH
        # -----------------------------------------
        if search:
            base_query += """
                AND (
                    log_type ILIKE %s
                    OR status::text ILIKE %s
                    OR backend_name ILIKE %s
                )
            """

            search_value = f"%{search}%"

            params.extend([
                search_value,
                search_value,
                search_value,
            ])

        # -----------------------------------------
        # START DATE
        # -----------------------------------------
        if start_ts:
            base_query += """
                AND event_time >= %s::timestamptz
            """

            params.append(start_ts)

        # -----------------------------------------
        # END DATE
        # -----------------------------------------
        if end_ts:
            base_query += """
                AND event_time <= %s::timestamptz
            """

            params.append(end_ts)

        # -----------------------------------------
        # TOTAL COUNT
        # -----------------------------------------
        count_query = f"""
            SELECT COUNT(*)
            {base_query}
        """

        cur.execute(
            count_query,
            tuple(params)
        )

        count_result = cur.fetchone()

        if isinstance(count_result, dict):
            total = int(count_result["count"])
        else:
            total = int(count_result[0])

        # -----------------------------------------
        # FETCH PAGINATED DATA
        # -----------------------------------------
        data_query = f"""
            SELECT
                id,
                log_type,
                event_time,
                down_at,
                up_at,
                duration_seconds,
                status,
                backend_name,
                created_at
            {base_query}
            ORDER BY event_time DESC
            LIMIT %s
            OFFSET %s
        """

        data_params = params + [
            limit,
            offset,
        ]

        cur.execute(
            data_query,
            tuple(data_params)
        )

        rows = cur.fetchall()

        # -----------------------------------------
        # TOTAL PAGES
        # -----------------------------------------
        total_pages = (
            (total + limit - 1) // limit
            if total > 0
            else 0
        )

        return rows, total, total_pages

    finally:
        cur.close()
        conn.close()


@backend_health_logs_bp.route(
    "/logs",
    methods=["GET"]
)
async def api_backend_health_logs():

    try:
        # -----------------------------------------
        # PAGE
        # -----------------------------------------
        page = int(
            request.args.get("page", 1)
        )

        # -----------------------------------------
        # LIMIT
        # -----------------------------------------
        limit = int(
            request.args.get("limit", 10)
        )

        # Safety
        page = max(page, 1)
        limit = min(
            max(limit, 1),
            1000
        )

        # -----------------------------------------
        # FILTERS
        # -----------------------------------------
        search = request.args.get(
            "search"
        )

        start_ts = request.args.get(
            "startTs"
        )

        end_ts = request.args.get(
            "endTs"
        )

        # -----------------------------------------
        # DATABASE
        # -----------------------------------------
        rows, total, total_pages = (
            await asyncio.to_thread(
                _fetch_backend_health_logs,
                page,
                limit,
                search,
                start_ts,
                end_ts,
            )
        )

        # -----------------------------------------
        # RESPONSE
        # -----------------------------------------
        return jsonify({
            "success": True,

            "data": rows,

            "pagination": {
                "page": page,
                "limit": limit,
                "total": total,
                "total_pages": total_pages,
            },
        })

    except ValueError:
        return jsonify({
            "success": False,
            "error": "Invalid page or limit.",
        }), 400

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
        }), 500