import asyncpg
from contextlib import AsyncContextDecorator
from quart import current_app

# Global pool
DB_POOL = None


class DB(AsyncContextDecorator):
    """
    Async DB utility that reuses a global connection pool.
    """

    def __init__(self, config=None):
        self.config = config or current_app.db_config

    async def __aenter__(self):
        global DB_POOL
        if DB_POOL is None:
            DB_POOL = await asyncpg.create_pool(**self.config)
        self.connection = await DB_POOL.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await DB_POOL.release(self.connection)

    async def fetch(self, query: str, *params):
        return await self.connection.fetch(query, *params)
    
    async def fetchrow(self, query: str, *params):
        return await self.connection.fetchrow(query, *params)
    
    async def execute(self, query: str, *params):
        return await self.connection.execute(query, *params)
