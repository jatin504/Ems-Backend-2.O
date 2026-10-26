from quart import request, jsonify
from app.utils.jwt import decode_access_token
from app.utils.db_utils import DB

async def require_permission(action: str):

    auth = request.headers.get("Authorization", "")
    if not auth.lower().startswith("bearer "):
        return None, (jsonify({"message": "Missing Authorization"}), 401)

    token = auth.split(" ", 1)[1].strip()
    payload = decode_access_token(token)

    if not payload:
        return None, (jsonify({"message": "Invalid token"}), 401)

    role = (payload.get("role") or "").lower()

    # admin bypass
    if role in ("admin", "cogneta"):
        return {"role": role}, None

    sql = f"""
        SELECT {action}
        FROM role_permissions
        WHERE LOWER(role_name)=LOWER($1)
        LIMIT 1
    """

    async with DB() as db:
        row = await db.fetchrow(sql, role)

    if not row or not row[action]:
        return None, (jsonify({"message": "Permission denied"}), 403)

    return {"role": role}, None

