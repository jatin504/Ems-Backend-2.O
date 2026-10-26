import logging
import asyncio
import contextlib
from pymodbus.client import AsyncModbusTcpClient
from controller_layer.writers.file_writer import FileService

logger = logging.getLogger(__name__)

class ModbusClientManager:
    _lock = None
    _pools = {}  # key -> {"queue": asyncio.Queue, "active_count": int}
    _max_pool_size = 1  # Max concurrent connections per unique key (adjust as needed)

    @classmethod
    def initialize(cls, loop, max_pool_size=1):
        global MAIN_LOOP
        MAIN_LOOP = loop
        cls._lock = asyncio.Lock()
        cls._max_pool_size = max_pool_size
        cls._pools = {}

    @classmethod
    async def get_client(cls, channel_id, server_ip, server_port, retries):
        key = f"channel_{channel_id}_{server_ip}:{server_port}"
        
        # Ensure pool exists for this key
        async with cls._lock:
            if key not in cls._pools:
                cls._pools[key] = {
                    "queue": asyncio.Queue(),
                    "active_count": 0
                }
        
        pool = cls._pools[key]
        
        # 1. Try to reuse an existing idle client from the pool
        try:
            client = pool["queue"].get_nowait()
            if client.connected:
                return client
            else:
                client.close()
                async with cls._lock:
                    pool["active_count"] -= 1
        except asyncio.QueueEmpty:
            pass
        
        # 2. If no idle client, check if we can create a new one under the limit
        created_new = False
        async with cls._lock:
            if pool["active_count"] < cls._max_pool_size:
                pool["active_count"] += 1
                created_new = True
        
        if created_new:
            client = AsyncModbusTcpClient(host=server_ip, port=server_port, retries=retries, timeout=0.5, reconnect_delay=3.0)
            await client.connect()
            if not client.connected:
                async with cls._lock:
                    pool["active_count"] -= 1
                await FileService.update_flag("is_protocol_healthy", 0)
                raise ConnectionError(f"Failed to connect to {key}")
            
            await FileService.update_flag("is_protocol_healthy", 1)
            logger.info(f"Created new Modbus client for {key}. Active: {pool['active_count']}/{cls._max_pool_size}")
            return client
        
        # 3. Pool is full, wait for an idle client to be returned by another task
        client = await pool["queue"].get()
        if not client.connected:
            # Attempt to reconnect if the pooled client dropped
            await client.connect()
            if not client.connected:
                # If it fails, decrement count and recursively try to get/create again
                async with cls._lock:
                    pool["active_count"] -= 1
                return await cls.get_client(channel_id, server_ip, server_port, retries)
        
        return client

    @classmethod
    def release_client(cls, channel_id, server_ip, server_port, client):
        key = f"channel_{channel_id}_{server_ip}:{server_port}"
        if key in cls._pools:
            cls._pools[key]["queue"].put_nowait(client)

    @classmethod
    @contextlib.asynccontextmanager
    async def client_context(cls, channel_id, server_ip, server_port, retries):
        """Context manager to safely acquire and release a Modbus client."""
        client = await cls.get_client(channel_id, server_ip, server_port, retries)
        try:
            yield client
        finally:
            cls.release_client(channel_id, server_ip, server_port, client)

    @classmethod
    @contextlib.asynccontextmanager
    async def write_client_context(cls, channel_id, server_ip, server_port, retries):
        """Dedicated ephemeral connection for writing to avoid polluting the read pool."""
        key = f"channel_{channel_id}_{server_ip}:{server_port}"
        client = AsyncModbusTcpClient(host=server_ip, port=server_port, retries=retries, timeout=0.5)
        try:
            await client.connect()
            if not client.connected:
                raise ConnectionError(f"Failed to connect to {key}")
            logger.info(f"Created ephemeral Modbus write client for {key}")
            yield client
        finally:
            client.close()

    @classmethod
    async def close_all(cls):
        async with cls._lock:
            for key, pool in cls._pools.items():
                while not pool["queue"].empty():
                    client = pool["queue"].get_nowait()
                    client.close()
                logger.info(f"Closed all Modbus clients for {key}")