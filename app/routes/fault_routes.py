from quart import Blueprint, jsonify
from app.utils.db_utils import DB

fault_bp = Blueprint("fault_bp", __name__)

@fault_bp.route("/emergency-last7days", methods=["GET"])
async def emergency_fault_last7days():

    query = """
        SELECT 
            DATE(time_stamp) as day,
            COUNT(*) as count
        FROM public.alarms
        WHERE 
            severity ILIKE 'critical' AND tag_name != 'Fault'
            AND time_stamp >= NOW() - INTERVAL '7 days'
        GROUP BY day
        ORDER BY day;
    """

    async with DB() as db:
        rows = await db.fetch(query)

    result = []

    for r in rows:
        result.append({
            "date": str(r["day"]),
            "count": r["count"]
        })

    return jsonify(result)