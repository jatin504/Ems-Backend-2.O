"""Dedicated non-propagating, asynchronous raw historian logs."""
import atexit
import logging
from logging.handlers import QueueHandler, QueueListener, TimedRotatingFileHandler
import os
from pathlib import Path
from queue import SimpleQueue
import threading

_lock = threading.Lock()
_listeners = {}


def close_raw_logs():
    with _lock:
        for logger, listener, handler in _listeners.values():
            listener.stop()
            handler.close()
            for queued in list(logger.handlers):
                logger.removeHandler(queued)
                queued.close()
        _listeners.clear()


atexit.register(close_raw_logs)


def raw_logger():
    """Configure once per folder; never send raw records to root/app handlers."""
    directory = os.getenv("EMS_RAW_LOG_DIR")
    if not directory:
        raise ValueError("Set EMS_RAW_LOG_DIR to the raw insertion log folder")
    folder = Path(directory).expanduser().resolve()
    with _lock:
        if folder in _listeners:
            return _listeners[folder][0]
        folder.mkdir(parents=True, exist_ok=True)
        logger = logging.getLogger("ems.raw_insertion." + str(folder))
        logger.setLevel(logging.INFO)
        logger.propagate = False
        handler = TimedRotatingFileHandler(folder / "raw_data_insertaion.log", when="midnight",
                                          backupCount=14, encoding="utf-8", delay=True)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        queue = SimpleQueue()
        logger.addHandler(QueueHandler(queue))
        listener = QueueListener(queue, handler, respect_handler_level=True)
        listener.start()
        _listeners[folder] = (logger, listener, handler)
        return logger
