from app.utils.db_utils import DB

async def insert_audit_log(message: str, username: str, ip: str):
    sql = """
        INSERT INTO audit_logs (message, username, ip)
        VALUES ($1, $2, $3)
    """
    try:
        async with DB() as db:
            await db.fetch(sql, message, username or "-", ip or "-")
    except Exception as e:
        print("Audit log error:", e)
