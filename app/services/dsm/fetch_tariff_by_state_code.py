from app.utils.db_utils import DB


async def fetch_tariff_by_state_code(state_code):

    query = """
        SELECT * FROM public.state_tariff_snapshot
        WHERE STATE_CODE = $1
        ORDER BY TS DESC
        LIMIT 1
    """
    result = None

    async with DB() as db:
        result = await db.fetch(query, state_code)

    if result:
        return [dict(record) for record in result]
    else:
        return []