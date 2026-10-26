from quart import Blueprint, request, jsonify
import json
from app.utils.db_utils import DB
from datetime import datetime

market_bids_bp = Blueprint(
    "market_bids",
    __name__,
    url_prefix="/api/market-bids"
)

@market_bids_bp.route("", methods=["GET"])
async def get_market_bids():
    """
    Fetch market bids for a given trade date.
    Query Param:
        trade_date (YYYY-MM-DD)
    """

    trade_date = request.args.get("trade_date")

    if not trade_date:
        return jsonify({
            "status": "error",
            "message": "trade_date is required (YYYY-MM-DD)"
        }), 400

    # Validate date format
    try:
        trade_date_obj = datetime.strptime(trade_date, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({
            "status": "error",
            "message": "Invalid trade_date format. Use YYYY-MM-DD"
        }), 400

    query = """
        SELECT *
        FROM market_bid
        WHERE trade_date = $1
        ORDER BY created_at DESC
    """

    async with DB() as db:
        rows = await db.fetch(query, trade_date_obj)

    # Convert asyncpg.Record → dict
    data = [dict(row) for row in rows]

    return jsonify({
        "status": "success",
        "trade_date": trade_date,
        "count": len(data),
        "data": data
    }), 200
