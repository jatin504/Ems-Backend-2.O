import asyncio
from runtime.environment_const import TIMESERIES_DB

DB_WRITE_QUEUE = asyncio.Queue(maxsize=1_800_000)
VICTORIA_DB_WRITE_QUEUE = asyncio.Queue(maxsize=200_000)


GARBAGE_QUEUE = asyncio.Queue(maxsize=5000)


def get_timeseries_backend():
    backend = (TIMESERIES_DB or "").strip().upper()
    if backend not in {"POSTGRES", "VICTORIA"}:
        raise ValueError("TIMESERIES_DB must be POSTGRES or VICTORIA")
    return backend


def get_timeseries_write_queue():
    return DB_WRITE_QUEUE if get_timeseries_backend() == "POSTGRES" else VICTORIA_DB_WRITE_QUEUE
