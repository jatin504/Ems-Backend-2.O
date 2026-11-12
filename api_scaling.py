"""Shared routing policy for the proxy generator and read-only API workers.

Only audited read handlers may run on replicas. GET alone is not sufficient:
several existing GET handlers write files or execute schedules.
"""

from hypercorn.logging import Logger


class QuietReadinessLogger(Logger):
    """Keep readiness failures visible without logging every successful probe."""

    async def access(self, request, response, request_time):
        if (request.get("method") == "GET" and request.get("path") == "/health/ready"
                and response is not None and response.get("status") == 200):
            return
        await super().access(request, response, request_time)

READ_PREFIXES = ("/latest/", "/history/")
READ_POST_PATHS = ("/api/timeseries-data", "/api/online_devices")
HEALTH_PATHS = ("/health", "/ping", "/health/ready")


def replica_allowed(method, path):
    if method in {"GET", "HEAD"}:
        return path in HEALTH_PATHS or path.startswith(READ_PREFIXES)
    if method == "POST":
        return path in READ_POST_PATHS
    if method == "OPTIONS":
        return path in READ_POST_PATHS or path.startswith(READ_PREFIXES)
    return False
