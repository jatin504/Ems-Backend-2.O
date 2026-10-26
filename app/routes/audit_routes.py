from quart import Blueprint, request, jsonify
from app.utils.db_utils import DB

audit_bp = Blueprint("audit", __name__)


@audit_bp.route("/api/audit/logs", methods=["GET"])
async def get_audit_logs():

    # =========================
    # QUERY PARAMS (ALARM STYLE)
    # =========================
    q = (request.args.get("q") or "").strip().lower()
    startTs = request.args.get("startTs")
    endTs = request.args.get("endTs")

    # =========================
    # PAGINATION
    # =========================
    try:
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 50))
    except ValueError:
        page = 1
        limit = 50

    if page < 1:
        page = 1
    if limit < 1:
        limit = 50
    if limit > 500:
        limit = 500

    offset = (page - 1) * limit

    # =========================
    # WHERE CLAUSE
    # =========================
    where_clause = "WHERE 1=1"
    params = []

    # 🔎 SEARCH
    if q:
        idx = len(params) + 1
        where_clause += f"""
            AND (
                LOWER(username) LIKE ${idx}
                OR LOWER(message) LIKE ${idx}
                OR LOWER(ip) LIKE ${idx}
            )
        """
        params.append(f"%{q}%")

    # 📅 SAME FILTER AS ALARM HISTORY (TIMESTAMP RANGE)
    if startTs:
        where_clause += f" AND ts >= ${len(params)+1}"
        params.append(startTs)

    if endTs:
        where_clause += f" AND ts <= ${len(params)+1}"
        params.append(endTs)

    # =========================
    # COUNT QUERY
    # =========================
    count_sql = f"""
        SELECT COUNT(*) as total
        FROM audit_logs
        {where_clause}
    """

    # =========================
    # DATA QUERY
    # =========================
    data_sql = f"""
        SELECT id, ts, message, username, ip
        FROM audit_logs
        {where_clause}
        ORDER BY ts DESC
        LIMIT ${len(params)+1}
        OFFSET ${len(params)+2}
    """

    params_with_pagination = params + [limit, offset]

    # =========================
    # DB EXECUTION
    # =========================
    async with DB() as db:
        count_result = await db.fetch(count_sql, *params)
        total = count_result[0]["total"] if count_result else 0

        rows = await db.fetch(data_sql, *params_with_pagination)

    # =========================
    # RESPONSE FORMAT
    # =========================
    logs = [
        {
            "id": r["id"],
            "ts": r["ts"].isoformat() if r["ts"] else None,
            "message": r["message"],
            "user": r.get("username"),
            "ip": r.get("ip"),
        }
        for r in rows
    ]

    total_pages = (total + limit - 1) // limit

    return jsonify({
        "logs": logs,
        "pagination": {
            "page": page,
            "limit": limit,
            "total": total,
            "total_pages": total_pages
        }
    }), 200