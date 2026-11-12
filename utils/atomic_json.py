"""Publish a complete JSON replacement in the same directory as its target.

This prevents partial-file reads. It does not make a read/modify/write transaction
atomic; mutations must still have one owner or hold a transaction-level lock.
"""
import json
import os
from pathlib import Path
import tempfile
import time


def atomic_write_json(path, data, indent=2):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, indent=indent)
            stream.flush()
            os.fsync(stream.fileno())
        # Windows may briefly deny replacement while another process reads.
        for attempt in range(6):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt == 5:
                    raise
                time.sleep(0.02 * (attempt + 1))
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
