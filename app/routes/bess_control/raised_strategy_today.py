from quart import Blueprint, jsonify
from app.utils.db_utils import DB

from .init_bp import bess_control_bp

@bess_control_bp.route("/strategy_raised_today")
async def raised_strategies_today():
    """
    Get list of strategies that raised today with timestamps.
    """

    query = """
        SELECT
            strategy_name,
            MIN(raised_at) AS first_raised_at,
            MAX(raised_at) AS last_raised_at,
            ROUND(
                EXTRACT(EPOCH FROM (NOW() - MIN(raised_at))) / 3600,
                2
            ) AS hours_raised_so_far
        FROM ems_strategy_flags
        WHERE raised_date = CURRENT_DATE
          AND flag_value = 1
        GROUP BY strategy_name
        ORDER BY first_raised_at;
    """

    async with DB() as db:
        rows = await db.fetch(query)

    result = []
    for r in rows:
        result.append({
            "strategy_name": r["strategy_name"],
            "first_raised_at": r["first_raised_at"].isoformat(),
            "last_raised_at": r["last_raised_at"].isoformat(),
            "hours_raised_so_far": float(r["hours_raised_so_far"])
        })

    return jsonify(result)
