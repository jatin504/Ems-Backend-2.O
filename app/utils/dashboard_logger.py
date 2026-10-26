from app.utils.db_utils import DB
import json

async def insert_dashboard_log(
    action: str,
    control_type: str,
    module: str,
    username: str,
    ip: str,
    status: str,
    payload: dict | None = None
):
    sql = """
        INSERT INTO dashboard_logs
        (action, control_type, module, username, ip, status, payload)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
    """

    try:
        async with DB() as db:
            await db.fetch(
                sql,
                action,
                control_type,
                module,
                username,
                ip,
                status,
                json.dumps(payload) if payload else None
            )
    except Exception as e:
        print("Dashboard log error:", e)