from model import get_dict_cursor
from contextlib import ContextDecorator


class DB(ContextDecorator):
    """
    A reusable PostgreSQL database connection utility using psycopg2.
    Uses RealDictCursor for dict-style row access.
    """

    def __init__(self, config=None):

        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = get_dict_cursor()
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            if exc_type is None:  # No exception
                self.conn.commit()
            else:
                self.conn.rollback()
            self.conn.close()

    def execute(self, query, params=None):
        """Execute a query"""
        self.cursor.execute(query, params or ())

    def fetchone(self):
        """Fetch one row"""
        return self.cursor.fetchone()

    def fetchall(self):
        """Fetch all rows"""
        return self.cursor.fetchall()

    def commit(self):
        """Commit transaction manually"""
        if self.conn:
            self.conn.commit()

    def rollback(self):
        """Rollback transaction manually"""
        if self.conn:
            self.conn.rollback()



import asyncpg
from contextlib import AsyncContextDecorator
from .db_config import db_config
import logging

logger = logging.getLogger(__name__)

# Global pool
DB_POOL = None
STANDBY_DB_POOL = None

async def init_db():
    global DB_POOL
    # try:
    if DB_POOL is None:
        DB_POOL = await asyncpg.create_pool(**db_config())
    # except Exception as e:
    #     logger.info(f"Error in init_DB(): {e}")
    # logger.info(
    #     f"[DB] Pool initialized on loop {id(asyncio.get_running_loop())}"
    # )


async def close_db():
    global DB_POOL, STANDBY_DB_POOL

    if DB_POOL is not None:
        await DB_POOL.close()
        DB_POOL = None

    if STANDBY_DB_POOL is not None:
        await STANDBY_DB_POOL.close()
        STANDBY_DB_POOL = None

class A_DB(AsyncContextDecorator):

    def __init__(self):
        self.primary_config = db_config("primary")
        self.standby_config = db_config("standby")

    async def __aenter__(self):
        global DB_POOL, STANDBY_DB_POOL

        if DB_POOL is None:
            raise RuntimeError(
                "DB_POOL has not been initialized. "
                "Call init_db() during application startup."
            )

        try:
            if DB_POOL is None:
                DB_POOL = await asyncpg.create_pool(**self.primary_config)

            self.pool = DB_POOL
            self.connection = await self.pool.acquire()
            self.mode = "PRIMARY"
        except Exception as e:
            logger.error(" PRIMARY DB DOWN — switching to STANDBY")
            logger.error(f"Error details: {e}")
            logger.info("🔁 Using STANDBY DB")

            if STANDBY_DB_POOL is None:
                STANDBY_DB_POOL = await asyncpg.create_pool(**self.standby_config)

            self.pool = STANDBY_DB_POOL
            self.connection = await self.pool.acquire()
            self.mode = "STANDBY"

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.pool.release(self.connection)

    async def fetch(self, query: str, *params):
        return await self.connection.fetch(query, *params)

    async def fetchrow(self, query: str, *params):
        return await self.connection.fetchrow(query, *params)

    async def execute(self, query: str, *params):
        return await self.connection.execute(query, *params)

    async def executemany(self, query: str, *params):
        return await self.connection.executemany(query, *params)


# ================= REDUNDANT DB =================
REDUNDANT_DB_POOL = None


class A_DB_REDUNDANT(AsyncContextDecorator):

    def __init__(self):
        self.config = db_config("standby") 

    async def __aenter__(self):
        global REDUNDANT_DB_POOL
        if REDUNDANT_DB_POOL is None:
            REDUNDANT_DB_POOL = await asyncpg.create_pool(**self.config)
        self.connection = await REDUNDANT_DB_POOL.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await REDUNDANT_DB_POOL.release(self.connection)

    async def execute(self, query: str, *params):
        return await self.connection.execute(query, *params)

    async def fetch(self, query: str, *params):
        return await self.connection.fetch(query, *params)

    async def fetchrow(self, query: str, *params):
        return await self.connection.fetchrow(query, *params)
