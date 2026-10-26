import aiohttp
import logging

logger = logging.getLogger(__name__)

class SSEPublisher:
    def __init__(self, rest_api_url: str, api_key):
        self.base_url = rest_api_url.rstrip("/")
        self.api_key = api_key
        self.session = None

    async def start(self):
        """Initialize the HTTP session"""
        if self.session is None:
            self.session = aiohttp.ClientSession()
            logger.info("SSEPublisher session started")

    async def stop(self):
        """Close the HTTP session"""
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("SSEPublisher session stopped")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.stop()

    async def publish(self, topic: str, data: dict):
        if not self.session:
            logger.warning("Session not initialized, auto-starting...")
            await self.start()
            
        async with self.session.post(
            f"{self.base_url}/stream/internal/publish",
            json={"topic": topic, "data": data},
            headers={"X-Internal-API-Key": self.api_key},
            timeout=aiohttp.ClientTimeout(total=2)
        ) as resp:
            if resp.status != 200:
                logger.warning(f"Publish failed: {resp.status}")