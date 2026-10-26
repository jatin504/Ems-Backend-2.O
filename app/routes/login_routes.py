# app/routes/login_routes.py
from quart import Blueprint, request, jsonify
from app.utils.auth import verify_password
from app.utils.jwt import generate_token
from app.utils.db_utils import DB
from app.utils.audit_logger import insert_audit_log  
import logging
from datetime import datetime, timedelta
from app.utils.email_service import send_email
from app.utils.auth import hash_password
import random, string

logger = logging.getLogger(__name__)

def generate_password(length=10):
    chars = string.ascii_letters + string.digits + "@#$%"
    return ''.join(random.choice(chars) for _ in range(length))

login_bp = Blueprint("login", __name__)


@login_bp.route("/api/login", methods=["POST"])
async def login_user():
    data = await request.get_json()
    identifier = (data.get("identifier") or "").strip()   # username ya email
    password   = data.get("password") or ""
    
    # logger.info(f"FROM BODY: {data}")

    if not identifier or not password:
        return jsonify({"message": "Missing credentials"}), 400

    query = """
        SELECT u.id,
               u.username,
               u.email,
               u.password_hash,
               COALESCE(u.is_blocked, FALSE) AS is_blocked,
               COALESCE(u.failed_attempts, 0) AS failed_attempts,
               u.lock_until,
               r.name AS role
        FROM users u
        JOIN user_roles ur ON u.id = ur.user_id
        JOIN roles r ON ur.role_id = r.id
        WHERE LOWER(u.username)=LOWER($1) OR LOWER(u.email)=LOWER($1)
        LIMIT 1
    """

    async with DB() as db:
        rows = await db.fetch(query, identifier)

    # USER NOT FOUND → LOGIN_FAILED LOG
    if not rows:
        await insert_audit_log(
            "LOGIN_FAILED",
            identifier,
            request.headers.get("X-Forwarded-For", request.remote_addr)
        )
        return jsonify({"message": "User not found"}), 404

    user = rows[0]

    # Check account locked
    if user["lock_until"] and user["lock_until"] > datetime.utcnow():
        remaining_seconds = (user["lock_until"] - datetime.utcnow()).total_seconds()
        remaining_minutes = int(remaining_seconds // 60)

        return jsonify({
            "message": f"Account locked. Try again after {remaining_minutes} minutes"
        }), 403

    if user["is_blocked"]:
        return jsonify({"message": "Your account is blocked"}), 403

    # WRONG PASSWORD → LOGIN_FAILED LOG
    if not verify_password(password, user["password_hash"]):

        async with DB() as db:

            failed_attempts = user["failed_attempts"] + 1

            #  policy fetch
            policy = await db.fetch("""
                SELECT lock_after_attempts
                FROM security_policies
                WHERE role_name=$1
            """, user["role"])

            max_attempts = policy[0]["lock_after_attempts"]

            lock_until = None

            # LOCK CONDITION
            if failed_attempts >= max_attempts:
                lock_until = datetime.utcnow() + timedelta(minutes=10)  # FIXED 10 MIN

                #  GET ALL ADMINS EMAIL
                admins = await db.fetch("""
                    SELECT u.email
                    FROM users u
                    JOIN user_roles ur ON u.id = ur.user_id
                    JOIN roles r ON ur.role_id = r.id
                    WHERE r.name IN ('admin','cogneta')
                """)

                #  SEND EMAIL TO ALL ADMINS
                for admin in admins:
                    send_email(
                        admin["email"],
                        "Account Locked Alert 🚨",
                        f"""
User Account Locked!

Username: {user['username']}
Email: {user['email']}

Reason: Multiple failed login attempts ({failed_attempts})

Account will be unlocked after 10 minutes.

IP Address: {request.headers.get("X-Forwarded-For", request.remote_addr)}

- EMS Security System
"""
                    )

            await db.execute("""
                UPDATE users
                SET failed_attempts=$1,
                    lock_until=$2
                WHERE id=$3
            """, failed_attempts, lock_until, user["id"])

        await insert_audit_log(
            "LOGIN_FAILED",
            user["username"],
            request.headers.get("X-Forwarded-For", request.remote_addr)
        )

        return jsonify({"message": "Incorrect password"}), 401

    # LOGIN SUCCESS → LOGON LOG
    async with DB() as db:
        await db.execute("""
            UPDATE users
            SET failed_attempts=0,
                lock_until=NULL
            WHERE id=$1
        """, user["id"])

    token = generate_token({"user_id": user["id"], "role": user["role"]})

    await insert_audit_log(
        "LOGON",
        user["username"],
        request.headers.get("X-Forwarded-For", request.remote_addr)
    )

    return jsonify({"access_token": token}), 200


#NEW LOGOUT API 
@login_bp.route("/api/logout", methods=["POST"])
async def logout_user():
    data = await request.get_json()
    username = (data.get("username") or "-")

    await insert_audit_log(
        "LOGOFF",
        username,
        request.headers.get("X-Forwarded-For", request.remote_addr)
    )

    return jsonify({"message": "Logged out"}), 200


@login_bp.route("/api/forgot-password", methods=["POST"])
async def forgot_password():
    data = await request.get_json()
    identifier = (data.get("identifier") or "").strip()

    if not identifier:
        return jsonify({"message": "Identifier required"}), 400

    async with DB() as db:
        rows = await db.fetch("""
            SELECT u.id, u.email, u.username, r.name as role
            FROM users u
            JOIN user_roles ur ON u.id = ur.user_id
            JOIN roles r ON ur.role_id = r.id
            WHERE LOWER(u.username)=LOWER($1) OR LOWER(u.email)=LOWER($1)
            LIMIT 1
        """, identifier)

        if not rows:
            return jsonify({"message": "User not found"}), 404

        user = rows[0]

        # ✅ CASE 1 → ADMIN / COGNETA
        if user["role"] in ["admin", "cogneta"]:

            new_password = generate_password()
            hashed = hash_password(new_password)

            await db.execute("""
                UPDATE users
                SET password_hash=$1
                WHERE id=$2
            """, hashed, user["id"])

            send_email(
                user["email"],
                "Password Reset",
                f"""
Hello {user['username']},

Your new password is: {new_password}

Please login and change it.

- EMS System
"""
            )

            return jsonify({"message": "New password sent to your email"}), 200

        # ✅ CASE 2 → NORMAL USER
        else:

            admins = await db.fetch("""
                SELECT u.email
                FROM users u
                JOIN user_roles ur ON u.id = ur.user_id
                JOIN roles r ON ur.role_id = r.id
                WHERE r.name IN ('admin','cogneta')
            """)

            for admin in admins:
                send_email(
                    admin["email"],
                    "Password Reset Request",
                    f"""
User {user['username']} ({user['email']})
has requested a password reset.
"""
                )

            return jsonify({"message": "Request sent to admin"}), 200
