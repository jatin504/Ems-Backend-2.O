from quart import Blueprint, request, jsonify
from app.utils.dashboard_logger import insert_dashboard_log
from app.utils.auth import hash_password, verify_password  # bcrypt helpers
from app.utils.db_utils import DB
from utils.db_utils import A_DB_REDUNDANT
from app.utils.jwt import decode_access_token
from app.utils.rbac import require_permission
from utils.redundant_db import is_redundant_enabled
import re
import os
import logging

users_bp = Blueprint("users", __name__)
logger = logging.getLogger(__name__)  

# ---------------- Password strength check ----------------
PASSWORD_RE = re.compile(r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z0-9]).{8,}$")
def is_strong_password(p: str) -> bool:
    return bool(PASSWORD_RE.match(p or ""))

def _get_auth_user():
    """
    Returns dict {"id": int, "role": str} or (None, error_response)
    """
    auth = request.headers.get("Authorization", "")
    if not auth.lower().startswith("bearer "):
        return None, (jsonify({"message": "Missing or invalid Authorization header"}), 401)
    token = auth.split(" ", 1)[1].strip()
    payload = decode_access_token(token)
    if not payload:
        return None, (jsonify({"message": "Invalid or expired token"}), 401)

    user = {"id": payload.get("user_id"), "role": (payload.get("role") or "").lower()}
    if not user["id"]:
        return None, (jsonify({"message": "Invalid token payload"}), 401)
    return user, None


# Default Cogneta user auto-seed 
async def _seed_default_user():
    """
    Boot-time seed: ensure role + default user exist based on .env.
    Safe to run multiple times.
    Controlled by DEFAULT_USER_ENABLE=true|false.
    """
    enable = (os.getenv("DEFAULT_USER_ENABLE", "true").lower() == "true")
    if not enable:
        logger.info("[seed] DEFAULT_USER_ENABLE=false; skipping default user seed")
        return

    email = (os.getenv("DEFAULT_USER_EMAIL") or "").strip()
    password = os.getenv("DEFAULT_USER_PASSWORD") or ""
    phone = (os.getenv("DEFAULT_USER_PHONE") or "").strip()
    username = (os.getenv("DEFAULT_USER_USERNAME") or "cogneta").strip()
    role_name = (os.getenv("DEFAULT_ROLE_NAME") or "cogneta").strip().lower()

    if not email or not password or not phone:
        logger.warning("[seed] Missing DEFAULT_USER_EMAIL/PASSWORD/PHONE; skipping")
        return

    # SQL statements (same style as the rest of this file, using $1 params)
    ensure_role_sql = """
        INSERT INTO roles (name)
        VALUES (LOWER($1))
        ON CONFLICT (name) DO NOTHING
    """
    get_user_sql = "SELECT id FROM users WHERE email = $1"
    create_user_sql = """
        INSERT INTO users (username, email, phone, password_hash, password_updated_at)
        VALUES ($1, $2, $3, $4, NOW())
        RETURNING id
    """
    get_role_id_sql = "SELECT id FROM roles WHERE name = LOWER($1)"
    map_role_sql = """
        INSERT INTO user_roles (user_id, role_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
    """

    try:
        # -------- PRIMARY --------
        async with DB() as db:
            # Ensure role exists
            await db.fetch(ensure_role_sql, role_name)

            # If user already exists, skip
            rows = await db.fetch(get_user_sql, email)
            if rows:
                logger.info(f"[seed] Default user already present: {email}")
                return

            # Create user
            pw_hash = hash_password(password)
            new_user = await db.fetch(create_user_sql, username, email, phone, pw_hash)
            user_id = new_user[0]["id"]

            # Map role
            role_row = await db.fetch(get_role_id_sql, role_name)
            role_id = role_row[0]["id"]
            await db.fetch(map_role_sql, user_id, role_id)

            logger.info(f"[seed] Created default user {email} with role '{role_name}' on PRIMARY")

        # -------- STANDBY --------
        if is_redundant_enabled():
            try:
                async with A_DB_REDUNDANT() as db2:
                    # Ensure role exists
                    await db2.fetch(ensure_role_sql, role_name)

                    # If user already exists, skip
                    rows = await db2.fetch(get_user_sql, email)
                    if rows:
                        logger.info(f"[seed] Default user already present on STANDBY: {email}")
                        return

                    # Create user
                    pw_hash = hash_password(password)
                    new_user = await db2.fetch(create_user_sql, username, email, phone, pw_hash)
                    user_id = new_user[0]["id"]

                    # Map role
                    role_row = await db2.fetch(get_role_id_sql, role_name)
                    role_id = role_row[0]["id"]
                    await db2.fetch(map_role_sql, user_id, role_id)

                    logger.info(f"[seed] Created default user {email} with role '{role_name}' on STANDBY")
            except Exception as e:
                logger.error(f"[seed] Error seeding default user on STANDBY: {e}")
                logger.warning("[seed] Continuing with PRIMARY only...")

    except Exception as e:
        logger.exception("[seed] Error seeding default user")



# ---------------- Create User ----------------
@users_bp.route("/api/users", methods=["POST"])
async def create_user():

    user, err = await require_permission("can_create_user")
    if err:
        return err

    try:
        data = await request.get_json()
        username = (data.get("username") or "").strip()
        email = (data.get("email") or "").strip()
        password = data.get("password") or ""
        role = (data.get("role") or "viewer").strip()
        phone = (data.get("phone") or "").strip()

        if not username or not email or not password:
            return jsonify({"message": "Missing required fields"}), 400

        if not is_strong_password(password):
            return jsonify({
                "message": "Weak password. Must be 8+ chars with uppercase, lowercase, number, and symbol."
            }), 400

        hashed_pw = hash_password(password)

        query_user = "SELECT id FROM users WHERE email=$1 OR username=$2"
        insert_user = """
            INSERT INTO users (username, email, password_hash, phone, password_updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            RETURNING id
        """
        insert_role_if_missing = """
            INSERT INTO roles (name)
            VALUES (LOWER($1))
            ON CONFLICT (name) DO NOTHING
        """
        insert_user_role = """
            INSERT INTO user_roles (user_id, role_id)
            VALUES ($1, (SELECT id FROM roles WHERE LOWER(name)=LOWER($2)))
        """

        # -------- PRIMARY --------
        async with DB() as db:
            existing = await db.fetch(query_user, email, username)
            if existing:
                return jsonify({"message": "User already exists"}), 400

            await db.fetch(insert_role_if_missing, role)
            row = await db.fetch(insert_user, username, email, hashed_pw, phone)
            user_id = row[0]["id"]
            await db.fetch(insert_user_role, user_id, role)

        # -------- STANDBY --------
        if is_redundant_enabled():
            try:
                async with A_DB_REDUNDANT() as db2:
                    existing = await db2.fetch(query_user, email, username)
                    if existing:
                        logger.warning(f"User {email} already exists on STANDBY, skipping creation")
                    else:
                        await db2.fetch(insert_role_if_missing, role)
                        row = await db2.fetch(insert_user, username, email, hashed_pw, phone)
                        user_id_standby = row[0]["id"]
                        await db2.fetch(insert_user_role, user_id_standby, role)
                        logger.info(f"User {email} created on STANDBY database")
            except Exception as e:
                logger.error(f"Error creating user on STANDBY database: {e}")
                logger.warning("Continuing with PRIMARY only...")

        return jsonify({"message": "User created", "user_id": user_id}), 201

    except Exception as e:
        print("Error creating user:", e)
        return jsonify({"message": "Internal server error", "error": str(e)}), 500


# ---------------- Get All Users ----------------
@users_bp.route("/api/users", methods=["GET"])
async def get_users():
    try:
        query = """
            SELECT u.id, u.username, u.email, u.phone, u.is_blocked, r.name AS role
            FROM users u
            LEFT JOIN user_roles ur ON u.id = ur.user_id
            LEFT JOIN roles r ON ur.role_id = r.id
            ORDER BY u.id ASC
        """

        async with DB() as db:
            rows = await db.fetch(query)

        users = []
        for row in rows:
            username = row["username"] or ""

            # split by underscore
            parts = username.split("_", 1)

            first_name = parts[0]
            last_name = parts[1] if len(parts) > 1 else ""

            users.append({
                "id": row["id"],
                "first_name": first_name,
                "last_name": last_name,
                "email": row["email"],
                "phone": row.get("phone") or "",
                "role": row["role"] or "viewer",
                "is_blocked": (row["is_blocked"] if "is_blocked" in row else False),
            })

        return jsonify(users), 200

    except Exception as e:
        print("Error fetching users:", e)
        return jsonify([]), 500


# ---------------- Update User (Edit) ----------------
@users_bp.route("/api/users/<int:user_id>", methods=["PUT"])
async def update_user(user_id: int):
    """
    Expects JSON: { "username": str, "email": str, "role": str, "phone": str }
    """
    user, err = await require_permission("can_edit_user")
    if err:
        return err

    try:
        data = await request.get_json()
        username = (data.get("username") or "").strip()
        email = (data.get("email") or "").strip()
        role = (data.get("role") or "viewer").strip()
        phone = (data.get("phone") or "").strip()

        if not username or not email:
            return jsonify({"message": "username and email are required"}), 400

        # Prevent duplicate email/username for other users
        check_dupe = """
            SELECT 1 FROM users
            WHERE (LOWER(username)=LOWER($1) OR LOWER(email)=LOWER($2)) AND id<>$3
        """
        update_user_sql = "UPDATE users SET username=$1, email=$2, phone=$3 WHERE id=$4 RETURNING id"

        ensure_role_sql = """
            INSERT INTO roles (name)
            VALUES (LOWER($1))
            ON CONFLICT (name) DO NOTHING
        """
        delete_user_roles = "DELETE FROM user_roles WHERE user_id=$1"
        insert_user_role = """
            INSERT INTO user_roles (user_id, role_id)
            VALUES ($1, (SELECT id FROM roles WHERE LOWER(name)=LOWER($2)))
        """

        async with DB() as db:
            dupe = await db.fetch(check_dupe, username, email, user_id)
            if dupe:
                return jsonify({"message": "username or email already in use"}), 400

            updated = await db.fetch(update_user_sql, username, email, phone, user_id)
            if not updated:
                return jsonify({"message": "User not found"}), 404

            await db.fetch(ensure_role_sql, role)
            await db.fetch(delete_user_roles, user_id)
            await db.fetch(insert_user_role, user_id, role)

        return jsonify({"message": "User updated"}), 200

    except Exception as e:
        print("Error updating user:", e)
        return jsonify({"message": "Internal server error", "error": str(e)}), 500


# ---------------- Block / Unblock User ----------------
@users_bp.route("/api/users/<int:user_id>/block", methods=["PATCH"])
async def block_user(user_id: int):
    """
    Expects JSON: { "is_blocked": true|false }
    """
    user, err = await require_permission("can_settings_user")
    if err:
        return err

    try:
        data = await request.get_json()
        is_blocked = bool(data.get("is_blocked", False))

        sql = "UPDATE users SET is_blocked=$1 WHERE id=$2 RETURNING id"
        async with DB() as db:
            row = await db.fetch(sql, is_blocked, user_id)

        if not row:
            return jsonify({"message": "User not found"}), 404

        return jsonify({"message": "Block status updated", "is_blocked": is_blocked}), 200

    except Exception as e:
        print("Error updating block status:", e)
        return jsonify({"message": "Internal server error", "error": str(e)}), 500


# ---------------- Delete User ----------------
@users_bp.route("/api/users/<int:user_id>", methods=["DELETE"])
async def delete_user(user_id: int):
    user, err = await require_permission("can_delete_user")
    if err:
        return err

    try:
        del_roles = "DELETE FROM user_roles WHERE user_id=$1"
        del_user = "DELETE FROM users WHERE id=$1 RETURNING id"

        async with DB() as db:
            await db.fetch(del_roles, user_id)
            gone = await db.fetch(del_user, user_id)

        if not gone:
            return jsonify({"message": "User not found"}), 404

        return jsonify({"message": "User deleted"}), 200

    except Exception as e:
        print("Error deleting user:", e)
        return jsonify({"message": "Internal server error", "error": str(e)}), 500


# ---------------- NEW: Change own password (requires current) ----------------
@users_bp.route("/api/change-password", methods=["POST"])
async def change_password():
    """
    Body: { "current_password": str, "new_password": str }
    Auth: Bearer token (self)
    """
    user, err = _get_auth_user()
    if err:
        return err

    try:
        data = await request.get_json()
        current_password = (data.get("current_password") or "")
        new_password = (data.get("new_password") or "")

        if not current_password or not new_password:
            return jsonify({"message": "Missing required fields"}), 400

        if not is_strong_password(new_password):
            return jsonify({"message": "Weak password. Must be 8+ chars with uppercase, lowercase, number, and symbol."}), 400

        async with DB() as db:
            rows = await db.fetch("SELECT id, password_hash FROM users WHERE id=$1 LIMIT 1", user["id"])
            if not rows:
                return jsonify({"message": "User not found"}), 404

            me = rows[0]
            if not verify_password(current_password, me["password_hash"]):
                return jsonify({"message": "Incorrect current password"}), 400

            new_hash = hash_password(new_password)
            updated = await db.fetch("UPDATE users \n                SET password_hash=$1,\n                    password_updated_at=NOW()\n                WHERE id=$2 RETURNING id", new_hash, user["id"])
            if not updated:
                return jsonify({"message": "Update failed"}), 500

        return jsonify({"message": "Password updated successfully"}), 200

    except Exception as e:
        print("Error in change_password:", e)
        return jsonify({"message": "Internal server error", "error": str(e)}), 500


# ---------------- NEW: Admin sets another user's password ----------------
@users_bp.route("/api/users/set-password", methods=["POST"])
async def admin_set_password():
    """
    Body: { "identifier": "<username or email>", "new_password": str }
    Auth: Bearer token (must be admin)
    """
    user, err = _get_auth_user()
    if err:
        return err
    if (user["role"] or "") != "admin":
        return jsonify({"message": "Forbidden"}), 403

    try:
        data = await request.get_json()
        identifier = (data.get("identifier") or "").strip()
        new_password = (data.get("new_password") or "")

        if not identifier or not new_password:
            return jsonify({"message": "Missing required fields"}), 400

        if not is_strong_password(new_password):
            return jsonify({"message": "Weak password. Must be 8+ chars with uppercase, lowercase, number, and symbol."}), 400

        lookup_sql = """
            SELECT id FROM users
            WHERE LOWER(username)=LOWER($1) OR LOWER(email)=LOWER($1)
            LIMIT 1
        """
        async with DB() as db:
            rows = await db.fetch(lookup_sql, identifier)
            if not rows:
                return jsonify({"message": "User not found"}), 404

            target_id = rows[0]["id"]
            new_hash = hash_password(new_password)
            updated = await db.fetch("UPDATE users \n                SET password_hash=$1,\n                    password_updated_at=NOW()\n                WHERE id=$2 RETURNING id", new_hash, target_id)
            if not updated:
                return jsonify({"message": "Update failed"}), 500

        return jsonify({"message": "Password set successfully for the user"}), 200

    except Exception as e:
        print("Error in admin_set_password:", e)
        return jsonify({"message": "Internal server error", "error": str(e)}), 500


#  CORS 
@users_bp.after_app_request
async def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "http://localhost:4445"
    response.headers["Vary"] = "Origin"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
    return response

# Preflight endpoints
@users_bp.route("/api/users", methods=["OPTIONS"])
@users_bp.route("/api/users/<int:user_id>", methods=["OPTIONS"])
@users_bp.route("/api/users/<int:user_id>/block", methods=["OPTIONS"])
@users_bp.route("/api/change-password", methods=["OPTIONS"])
@users_bp.route("/api/users/set-password", methods=["OPTIONS"])
async def users_cors_preflight(**kwargs):
    return "", 204



@users_bp.before_app_serving
async def _seed_on_startup():
    await _seed_default_user()


# ---------------- VERIFY PASSWORD FOR CONTROL ACTIONS ----------------
@users_bp.route("/api/verify-password", methods=["POST"])
async def verify_password_for_action():

    user, err = _get_auth_user()
    if err:
        return err

    try:
        data = await request.get_json()

        password = (data.get("password") or "").strip()

        #  NEW CONTEXT FROM FRONTEND
        control_type = (data.get("control_type") or "SERVICE").upper()
        module = data.get("module") or "UNKNOWN"

        if not password:
            return jsonify({"message": "Password required"}), 400

        async with DB() as db:
            rows = await db.fetch(
                "SELECT password_hash, username FROM users WHERE id=$1 LIMIT 1",
                user["id"],
            )

            if not rows:
                return jsonify({"message": "User not found"}), 404

            db_hash = rows[0]["password_hash"]
            username = rows[0]["username"]

            if not verify_password(password, db_hash):

                await insert_dashboard_log(
                    action="PASSWORD_VERIFY",
                    control_type=control_type,
                    module=module,
                    username=username,
                    ip=request.headers.get("X-Forwarded-For", request.remote_addr),
                    status="FAILED"
                )

                return jsonify({"message": "Invalid password"}), 401

            await insert_dashboard_log(
                action="PASSWORD_VERIFY",
                control_type=control_type,
                module=module,
                username=username,
                ip=request.headers.get("X-Forwarded-For", request.remote_addr),
                status="SUCCESS"
            )

        return jsonify({"message": "Password verified"}), 200

    except Exception as e:
        print("verify_password_for_action error:", e)
        return jsonify({"message": "Internal server error"}), 500

