"""One consistent CORS policy for both frontend deployments."""
import os
from urllib.parse import urlsplit

from quart_cors import cors

# Explicit EMS frontend allowlist. Add real deployed frontend origins here,
# or override this list with EMS_CORS_ORIGINS in the deployment environment.
# Do not copy unrelated third-party domains from example configurations.
ALLOWED_ORIGINS = [
    "http://localhost:4445",
    "http://localhost:5000",
    "http://127.0.0.1:4445",
    "http://127.0.0.1:5000",
]


def configure_cors(app):
    configured = os.getenv("EMS_CORS_ORIGINS")
    origins = ([value.strip() for value in configured.split(",") if value.strip()]
               if configured is not None else ALLOWED_ORIGINS.copy())
    if not origins:
        raise ValueError("EMS_CORS_ORIGINS must list the frontend origins")
    for origin in origins:
        parsed = urlsplit(origin)
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname or
                parsed.path or parsed.query or parsed.fragment or parsed.username or parsed.password):
            raise ValueError("CORS origins must be exact http(s) origins without paths")
    cors(app, allow_origin=origins, allow_credentials=True,
         allow_headers=["Content-Type", "Authorization", "X-Internal-API-Key"],
         allow_methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
         expose_headers=["X-EMS-Instance"])
