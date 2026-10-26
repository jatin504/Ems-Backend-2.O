from multiprocessing import Queue

LATEST_TELEMETRY_QUEUE = Queue(maxsize=1_800_000)
