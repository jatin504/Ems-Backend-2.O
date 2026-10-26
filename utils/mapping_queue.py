import asyncio

import queue

import multiprocessing

ASYNC_MAPPING_QUEUE = asyncio.Queue(maxsize=400000)
MP_MAPPING_QUEUE = multiprocessing.Queue(maxsize=400000)
