"""Shared bounded queue between mapped telemetry and the raw VM writer."""
import asyncio
from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()


def positive_int(name, default):
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(name + " must be a positive integer")
    return value


def positive_float(name, default):
    value = float(os.getenv(name, str(default)))
    if not 0 < value < float("inf"):
        raise ValueError(name + " must be positive and finite")
    return value


@dataclass(frozen=True, slots=True)
class RawPacket:
    """One device reading; samples contain (metric name, numeric value) pairs."""
    device: str
    timestamp: int
    samples: tuple[tuple[str, int | float], ...]


# Full queue makes producers wait; queued packets are never evicted.
RAW_VM_QUEUE = asyncio.Queue(maxsize=positive_int("EMS_RAW_QUEUE_PACKETS", 2000))
