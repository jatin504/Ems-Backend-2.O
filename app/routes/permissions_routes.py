import json
from quart import Blueprint, request, jsonify
from app.utils.auth import require_auth
from model.db import get_connection

bp = Blueprint("permissions", __name__, url_prefix="/api")

VALID_ROLES = {"admin", "manager", "supervisor", "engineer", "operator", "guest", "cogneta"}


# ================= GET =================
@bp.get("/permissions")
@require_auth()
async def get_permissions_route():
    """Get all role permissions."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT role_name, permissions, updated_at
            FROM role_permissions
            ORDER BY role_name
            """
        )
        rows = cur.fetchall()

        result = []

        for role_name, permissions, updated_at in rows:
            perm = permissions or {}

            result.append({
                "role_name": role_name,
                "can_create_user": perm.get("can_create_user", False),
                "can_edit_user": perm.get("can_edit_user", False),
                "can_delete_user": perm.get("can_delete_user", False),
                "can_settings_user": perm.get("can_settings_user", False),
                "can_control_bess": perm.get("can_control_bess", False),
                "can_control_service": perm.get("can_control_service", False),
                "can_control_switchgear": perm.get("can_control_switchgear", False),
                "can_control_plant_config": perm.get("can_control_plant_config", False),
                "updated_at": updated_at.isoformat() + "Z" if updated_at else None
            })

        return jsonify(result)

    finally:
        cur.close()
        conn.close()


# ================= PUT =================
@bp.put("/permissions")
@require_auth(roles={"admin"})
async def put_permissions_route():
    """Update role permissions (admin only)."""

    data = await request.get_json()

    if not isinstance(data, list):
        return jsonify({"error": "Expected array of permissions"}), 400

    conn = get_connection()
    cur = conn.cursor()

    try:
        for perm in data:
            role = str(perm.get("role_name", "")).lower().strip()

            if role not in VALID_ROLES:
                return jsonify({"error": f"Invalid role: {role}"}), 400

            # 🔥 FIX — UI flat payload → JSON build
            permissions = {
                "can_create_user": bool(perm.get("can_create_user")),
                "can_edit_user": bool(perm.get("can_edit_user")),
                "can_delete_user": bool(perm.get("can_delete_user")),
                "can_settings_user": bool(perm.get("can_settings_user")),
                "can_control_bess": bool(perm.get("can_control_bess")),
                "can_control_service": bool(perm.get("can_control_service")),
                "can_control_switchgear": bool(perm.get("can_control_switchgear")),
                "can_control_plant_config": bool(perm.get("can_control_plant_config")),
            }

            #  Role overrides
            if role == "cogneta":
                for k in permissions:
                    permissions[k] = True

            elif role == "admin":
                permissions["can_create_user"] = True
                permissions["can_edit_user"] = True
                permissions["can_delete_user"] = True
                permissions["can_settings_user"] = True
                permissions["can_control_bess"] = True
                permissions["can_control_service"] = True
                permissions["can_control_switchgear"] = True
                permissions["can_control_plant_config"] = False

            cur.execute(
                """
                INSERT INTO role_permissions(role_name, permissions, updated_at)
                VALUES (%s,%s,(NOW() AT TIME ZONE 'UTC'))
                ON CONFLICT (role_name) DO UPDATE SET
                  permissions=EXCLUDED.permissions,
                  updated_at=(NOW() AT TIME ZONE 'UTC')
                """,
                (role, json.dumps(permissions)),
            )

        conn.commit()

        # 🔥 IMPORTANT FIX — Return FLAT JSON (same as GET)
        cur.execute(
            """
            SELECT role_name, permissions, updated_at
            FROM role_permissions
            ORDER BY role_name
            """
        )

        rows = cur.fetchall()
        result = []

        for role_name, permissions, updated_at in rows:
            perm = permissions or {}

            result.append({
                "role_name": role_name,
                "can_create_user": perm.get("can_create_user", False),
                "can_edit_user": perm.get("can_edit_user", False),
                "can_delete_user": perm.get("can_delete_user", False),
                "can_settings_user": perm.get("can_settings_user", False),
                "can_control_bess": perm.get("can_control_bess", False),
                "can_control_service": perm.get("can_control_service", False),
                "can_control_switchgear": perm.get("can_control_switchgear", False),
                "can_control_plant_config": perm.get("can_control_plant_config", False),
                "updated_at": updated_at.isoformat() + "Z" if updated_at else None
            })

        return jsonify(result)

    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
        cur.close()
        conn.close()