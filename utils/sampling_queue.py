import asyncio

SAMPLING_QUEUE = asyncio.Queue(maxsize=1_800_000)
