import asyncio
from typing import List, Dict, Any, Optional

from quart import Blueprint, jsonify, request
from model.db import get_dict_cursor


# =========================================================
# STRATEGY LOGS BLUEPRINT
# =========================================================

strategy_logs_bp = Blueprint(
    "strategy_logs",
    __name__,
)


# =========================================================
# SETPOINT WRITES BLUEPRINT
# =========================================================

setpoint_writes_bp = Blueprint(
    "setpoint_writes",
    __name__,
)


# =========================================================
# STRATEGY LOGS
# =========================================================

def _fetch_strategy_logs(
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
                strategy_id,
                started_at,
                finished_at,
                split_part(
                    decision_snapshot->>'text',
                    '.',
                    1
                ) AS decision_snapshot,
                error_text
            FROM strategy_execution_logs
            WHERE 1=1
        """

        params = []

        if search:
            base_query += """
                AND (
                    decision_snapshot::text ILIKE %s
                    OR strategy_id::text ILIKE %s
                )
            """

            params.extend([
                f"%{search}%",
                f"%{search}%",
            ])

        if start_ts and end_ts:
            base_query += """
                AND started_at BETWEEN %s::timestamp
                AND %s::timestamp
            """

            params.extend([
                start_ts,
                end_ts,
            ])

        base_query += """
            ORDER BY started_at DESC
            LIMIT %s
        """

        params.append(limit)

        cur.execute(
            base_query,
            tuple(params),
        )

        return cur.fetchall()

    finally:
        cur.close()
        conn.close()


@strategy_logs_bp.route("/logs", methods=["GET"])
async def api_strategy_logs():

    limit = int(
        request.args.get("limit", 200)
    )

    search = request.args.get("search")

    start_ts = request.args.get("startTs")
    end_ts = request.args.get("endTs")

    rows = await asyncio.to_thread(
        _fetch_strategy_logs,
        limit,
        search,
        start_ts,
        end_ts,
    )

    return jsonify(rows)


# =========================================================
# SETPOINT WRITES / OPEN CONTROL
# =========================================================

def _fetch_setpoint_writes(
    limit: int = 100,
    offset: int = 0,
    device_id: Optional[str] = None,
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
                strategy_id,
                timestamp,
                register_mode,
                setpoint_value,
                unit,
                device_id,
                success,
                response_text
            FROM setpoint_writes
            WHERE 1=1
        """

        params = []

        if device_id:
            base_query += """
                AND device_id = %s
            """

            params.append(device_id)

        if search:
            base_query += """
                AND (
                    response_text ILIKE %s
                    OR device_id::text ILIKE %s
                    OR strategy_id::text ILIKE %s
                )
            """

            params.extend([
                f"%{search}%",
                f"%{search}%",
                f"%{search}%",
            ])

        if start_ts and end_ts:
            base_query += """
                AND timestamp >= %s::timestamp
                AND timestamp <= %s::timestamp
            """

            params.extend([
                start_ts,
                end_ts,
            ])

        base_query += """
            ORDER BY timestamp DESC
            LIMIT %s OFFSET %s
        """

        params.extend([
            limit,
            offset,
        ])

        cur.execute(
            base_query,
            tuple(params),
        )

        return cur.fetchall()

    finally:
        cur.close()
        conn.close()


@setpoint_writes_bp.route(
    "/setpoint-writes",
    methods=["GET"],
)
async def api_setpoint_writes():

    limit = int(
        request.args.get("limit", 100)
    )

    page = int(
        request.args.get("page", 1)
    )

    device_id = request.args.get("device_id")

    search = request.args.get("search")

    start_ts = request.args.get("startTs")

    end_ts = request.args.get("endTs")

    offset = (page - 1) * limit

    rows = await asyncio.to_thread(
        _fetch_setpoint_writes,
        limit,
        offset,
        device_id,
        search,
        start_ts,
        end_ts,
    )

    return jsonify(rows)