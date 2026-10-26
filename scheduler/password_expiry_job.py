from datetime import datetime, timedelta
from app.utils.db_utils import DB
from app.utils.email_service import send_email
from app.utils.auth import hash_password
import random, string


def generate_password(length=10):
    chars = string.ascii_letters + string.digits + "@#$%"
    return ''.join(random.choice(chars) for _ in range(length))


async def password_expiry_job():
    async with DB() as db:

        users = await db.fetch("""
            SELECT u.id, u.username, u.email, u.password_updated_at, r.name as role
            FROM users u
            JOIN user_roles ur ON u.id = ur.user_id
            JOIN roles r ON ur.role_id = r.id
        """)

        for user in users:

            if not user["password_updated_at"]:
                continue

            # 🔹 policy fetch
            policy = await db.fetch("""
                SELECT expiry_days
                FROM security_policies
                WHERE role_name=$1
            """, user["role"])

            if not policy:
                continue

            expiry_days = policy[0]["expiry_days"]
            expiry_date = user["password_updated_at"] + timedelta(days=expiry_days)
            now = datetime.utcnow()

            days_left = (expiry_date - now).days

            # 🟡 WARNING (3 days before)
            if 0 < days_left <= 3:
                send_email(
                    user["email"],
                    "Password Expiry Warning ⚠️",
                    f"""
Hello {user['username']},

Your password will expire in {days_left} days.

Please change it before expiry.

- EMS System
"""
                )

            # 🔴 EXPIRED → AUTO RESET
            if now > expiry_date:

                new_password = generate_password()
                hashed = hash_password(new_password)

                await db.execute("""
                    UPDATE users
                    SET password_hash=$1,
                        password_updated_at=$2
                    WHERE id=$3
                """, hashed, datetime.utcnow(), user["id"])

                send_email(
                    user["email"],
                    "Password Auto Reset 🔒",
                    f"""
                    Hello {user['username']},

                    Your password has expired.

                    New Password: {new_password}

                    Please login and change it immediately.

                    - EMS Security
                    """
                )