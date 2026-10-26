# app/utils/auth.py
import re
import functools
from quart import request, jsonify, g
from app.utils.jwt import decode_access_token  # uses your jwt utils
import bcrypt

# ---- existing helpers (kept) ----
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode()

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode())

# ---- strong password policy (same as user creation) ----
PASSWORD_RE = re.compile(r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z0-9]).{8,}$")
def is_strong_password(p: str) -> bool:
    return bool(PASSWORD_RE.match(p or ""))

# ---- JWT guard ----
def require_auth(roles: set[str] | None = None):
    """
    @require_auth()                 -> any logged-in user
    @require_auth(roles={'admin'})  -> only admins
    Needs header: Authorization: Bearer <token>
    """
    roles = roles or set()

    def decorator(view_fn):
        @functools.wraps(view_fn)
        async def wrapper(*args, **kwargs):
            auth = request.headers.get("Authorization", "")
            if not auth.lower().startswith("bearer "):
                return jsonify({"message": "Missing or invalid Authorization header"}), 401
            token = auth.split(" ", 1)[1].strip()
            payload = decode_access_token(token)
            if not payload:
                return jsonify({"message": "Invalid or expired token"}), 401

            # token created in login: {"user_id": ..., "role": ...}
            g.user = {"id": payload.get("user_id"), "role": (payload.get("role") or "").lower()}

            if roles and g.user["role"] not in {r.lower() for r in roles}:
                return jsonify({"message": "Forbidden"}), 403

            return await view_fn(*args, **kwargs)
        return wrapper
    return decorator
