# REST API SERVER
import asyncio
from hypercorn.asyncio import serve
from hypercorn.config import Config
from app import create_app

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

async def main():
    app, broadcaster = create_app()

    config = Config()
    config.bind = ["0.0.0.0:5179"]
    config.use_reloader = False
    config.errorlog = "-"
    config.accesslog = "-"

    await serve(app, config)

if __name__ == "__main__":
    asyncio.run(main())
