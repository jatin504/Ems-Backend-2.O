# REST API SERVER
import asyncio
import os
import signal
from hypercorn.asyncio import serve
from hypercorn.config import Config
from app import create_app
from api_scaling import QuietReadinessLogger

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

async def main():
    app, broadcaster = create_app()

    config = Config()
    config.bind = [os.getenv("EMS_API_BIND", "0.0.0.0:5179")]
    config.use_reloader = False
    config.errorlog = "-"
    config.accesslog = "-"
    config.logger_class = QuietReadinessLogger
    config.graceful_timeout = 20

    shutdown = asyncio.Event()
    # Windows supervisors send CTRL_BREAK to the worker's process group.
    # Explicit handlers also give Linux SIGTERM a graceful shutdown path.
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: shutdown.set())
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, lambda *_: shutdown.set())
    await serve(app, config, shutdown_trigger=shutdown.wait)

if __name__ == "__main__":
    asyncio.run(main())
