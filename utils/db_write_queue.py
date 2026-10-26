import asyncio

DB_WRITE_QUEUE = asyncio.Queue(maxsize=1_800_000)
VICTORIA_DB_WRITE_QUEUE = asyncio.Queue(maxsize=200_000)


GARBAGE_QUEUE = asyncio.Queue(maxsize=5000)