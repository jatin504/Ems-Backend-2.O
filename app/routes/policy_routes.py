from quart import Blueprint, request, jsonify
from app.utils.auth import require_auth
from model.db import get_connection

bp = Blueprint("policy", __name__, url_prefix="/api")

VALID_ROLES = {"admin", "manager", "supervisor", "engineer", "operator", "guest"}

def _row_to_json(cols, row):
    """Serialize DB row -> JSON dict, forcing updated_at to ISO 8601 UTC ('Z')."""
    rec = dict(zip(cols, row))
    ua = rec.get("updated_at")
    if ua is not None:
        try:
            rec["updated_at"] = ua.isoformat() + "Z"  # treat stored value as UTC
        except Exception:
            rec["updated_at"] = str(ua)
    return rec

@bp.get("/policy")
@require_auth()
async def get_policy_route():
    role = (request.args.get("role") or "").lower().strip()
    if role not in VALID_ROLES:
        return jsonify({"error": "invalid role"}), 400

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT role_name, min_length, min_numeric, min_alpha, min_special,
                   expiry_days, auto_logoff_minutes, lock_after_attempts, updated_at
            FROM security_policies WHERE role_name=%s
            """,
            (role,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "not found"}), 404
        cols = [d[0] for d in cur.description]
        return jsonify(_row_to_json(cols, row))
    finally:
        cur.close()
        conn.close()

@bp.put("/policy")
@require_auth(roles={"admin"})
async def put_policy_route():
    data = await request.get_json()
    role = str(data.get("role", "")).lower().strip()
    if role not in VALID_ROLES:
        return jsonify({"error": "invalid role"}), 400

    def iv(name, lo, hi, default):
        try:
            v = int(data.get(name, default))
        except Exception:
            v = default
        if v < lo or v > hi:
            raise ValueError(f"{name} out of range")
        return v

    try:
        payload = {
            "role": role,
            "min_length": iv("min_length", 4, 128, 8),
            "min_numeric": iv("min_numeric", 0, 10, 1),
            "min_alpha": iv("min_alpha", 0, 10, 1),
            "min_special": iv("min_special", 0, 10, 1),
            "expiry_days": iv("expiry_days", 0, 3650, 60),
            "auto_logoff_minutes": iv("auto_logoff_minutes", 0, 24 * 60, 10),
            "lock_after_attempts": iv("lock_after_attempts", 0, 20, 3),
        }
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    conn = get_connection()
    cur = conn.cursor()
    try:
        # Normalize updated_at to UTC even if column is TIMESTAMP WITHOUT TIME ZONE
        cur.execute(
            """
            INSERT INTO security_policies(
              role_name, min_length, min_numeric, min_alpha, min_special,
              expiry_days, auto_logoff_minutes, lock_after_attempts, updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s, (NOW() AT TIME ZONE 'UTC'))
            ON CONFLICT (role_name) DO UPDATE SET
              min_length=EXCLUDED.min_length,
              min_numeric=EXCLUDED.min_numeric,
              min_alpha=EXCLUDED.min_alpha,
              min_special=EXCLUDED.min_special,
              expiry_days=EXCLUDED.expiry_days,
              auto_logoff_minutes=EXCLUDED.auto_logoff_minutes,
              lock_after_attempts=EXCLUDED.lock_after_attempts,
              updated_at=(NOW() AT TIME ZONE 'UTC')
            """,
            (
                payload["role"],
                payload["min_length"],
                payload["min_numeric"],
                payload["min_alpha"],
                payload["min_special"],
                payload["expiry_days"],
                payload["auto_logoff_minutes"],
                payload["lock_after_attempts"],
            ),
        )
        conn.commit()

        cur.execute(
            """
            SELECT role_name, min_length, min_numeric, min_alpha, min_special,
                   expiry_days, auto_logoff_minutes, lock_after_attempts, updated_at
            FROM security_policies WHERE role_name=%s
            """,
            (role,),
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        return jsonify(_row_to_json(cols, row))
    finally:
        cur.close()
        conn.close()
