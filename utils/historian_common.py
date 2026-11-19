"""Shared UTC sample-month label for the existing EMS Victoria writers."""
from datetime import datetime, timezone


def record_month(timestamp_ms):
    """Use the actual sample timestamp so the approved month-scoped archive works."""
    return datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).strftime("%Y-%m")
