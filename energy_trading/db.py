import psycopg2
from psycopg2.extras import RealDictCursor
import asyncpg

class DictRecord(asyncpg.Record):
    def __getitem__(self, key):
        if isinstance(key, str):
            return super().get(key)
        return super().__getitem__(key)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "myplantdb",
    "user": "postgres",
    "password": "postgres"
}
 

async def get_async_connection():
    """Get an asyncpg connection pool for myplantdb."""
    return await asyncpg.create_pool(**DB_CONFIG, min_size=1, max_size=10)

async def get_async_dict_cursor():
    """Get an asyncpg connection pool for myplantdb with dict-like records."""
    return await asyncpg.create_pool(**DB_CONFIG, min_size=1, max_size=10, record_class=DictRecord)