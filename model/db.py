import psycopg2
from psycopg2.extras import RealDictCursor
import asyncpg
import os

from utils.db_config import db_config   # 


# ================= PRIMARY DB =================
def get_connection():
    role = os.getenv("DB_ROLE", "primary")
    config = db_config(role)
    return psycopg2.connect(**config)

def get_dict_cursor():
    role = os.getenv("DB_ROLE", "primary")
    config = db_config(role)
    return psycopg2.connect(**config, cursor_factory=RealDictCursor)


# ================= CENTER DB (UNCHANGED) =================
DB_CONFIG_Center = {
    "host": "localhost",
    "port": 5432,
    "dbname": "mycenterdb",
    "user": "postgres",
    "password": "postgres"
}

def get_connection_center():
    return psycopg2.connect(**DB_CONFIG_Center)

def get_dict_cursor_center():
    return psycopg2.connect(**DB_CONFIG_Center, cursor_factory=RealDictCursor)


# ================= ASYNC =================
class DictRecord(asyncpg.Record):
    def __getitem__(self, key):
        if isinstance(key, str):
            return super().get(key)
        return super().__getitem__(key)


async def get_async_connection():
    role = os.getenv("DB_ROLE", "primary")
    config = db_config(role)
    return await asyncpg.create_pool(**config, min_size=1, max_size=10)

async def get_async_dict_cursor():
    role = os.getenv("DB_ROLE", "primary")
    config = db_config(role)
    return await asyncpg.create_pool(**config, min_size=1, max_size=10, record_class=DictRecord)


async def get_async_connection_center():
    return await asyncpg.create_pool(**DB_CONFIG_Center, min_size=1, max_size=10)

async def get_async_dict_cursor_center():
    return await asyncpg.create_pool(**DB_CONFIG_Center, min_size=1, max_size=10, record_class=True)


# ================= REDUNDANT DB =================
def get_connection_redundant():
    config = db_config("standby")   # 
    return psycopg2.connect(**config)