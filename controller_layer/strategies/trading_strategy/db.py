import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

DB_POOL = None


async def init_db():
    global DB_POOL

    DB_POOL = await asyncpg.create_pool(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        min_size=2,
        max_size=10
    )


async def fetch_one(query, *args):
    if DB_POOL is None:
        await init_db()
        
    async with DB_POOL.acquire() as conn:
        return await conn.fetchrow(query, *args)


async def fetch_all(query, *args):
    if DB_POOL is None:
        await init_db()

    async with DB_POOL.acquire() as conn:
        return await conn.fetch(query, *args)


async def execute(query, *args):
    if DB_POOL is None:
        await init_db()
    
    async with DB_POOL.acquire() as conn:
        return await conn.execute(query, *args)
