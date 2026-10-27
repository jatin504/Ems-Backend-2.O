"""Selectable latest-snapshot storage; history remains in the existing databases."""
import asyncio
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv
from filelock import FileLock

load_dotenv()


class TelemetryStore:
    """Keep the existing JSON files or use Valkey for latest device snapshots."""
    def __init__(self, stage, base_dir=None, client=None):
        # Validate configuration before creating a backend; JSON needs no server/client.
        if stage not in {"mapped", "sampled"}:
            raise ValueError("Unknown telemetry stage")
        self.stage = stage
        # "backend" here means the snapshot storage engine, not the whole EMS app.
        # Default to JSON; these values do not hot-reload while the process runs.
        self.backend = os.getenv("EMS_CACHE_BACKEND", "json").strip().lower()
        if self.backend not in {"json", "valkey"}:
            raise ValueError("EMS_CACHE_BACKEND must be json or valkey")
        # TTL affects whole Valkey device keys only: 0 means keep until overwritten.
        # Each successful positive-TTL write refreshes that key's idle timeout.
        self.ttl = int(os.getenv("EMS_CACHE_TTL_SECONDS", "0"))
        if self.ttl < 0:
            raise ValueError("EMS_CACHE_TTL_SECONDS must be >= 0")
        self.prefix = os.getenv("EMS_VALKEY_PREFIX", "ems:telemetry")
        if not self.prefix:
            raise ValueError("EMS_VALKEY_PREFIX must not be empty")
        self.base_dir = Path(base_dir) if base_dir else None
        self.client = client
        # Import the optional client only when Valkey is selected in the environment.
        if self.backend == "valkey" and self.client is None:
            import valkey
            from valkey.backoff import NoBackoff
            from valkey.retry import Retry
            self.client = valkey.Valkey.from_url(
                os.getenv("EMS_VALKEY_URL", "valkey://localhost:6379/0"),
                # Decode hash fields as strings; values are JSON-decoded in read().
                decode_responses=True,
                # Bound cache network waits; retry count below is deliberately zero.
                socket_connect_timeout=1, socket_timeout=1,
                retry=Retry(NoBackoff(), 0),
                retry_on_timeout=False, max_connections=32,
            )

    def key(self, device_id):
        """Separate mapped/sampled values and escape device identifiers in keys."""
        # Example: ems:telemetry:mapped:plant_1_pcs_pcs_1.
        # Escaping prevents separators/path characters in IDs changing the layout.
        return f"{self.prefix}:{self.stage}:{quote(str(device_id), safe='')}"

    def _path(self, device_id):
        """Preserve mapped JSON paths and store sampled snapshots separately."""
        if self.base_dir is None:
            from runtime_paths import DataPaths
            folder = "latest_telemetry" if self.stage == "mapped" else "sampled_telemetry"
            self.base_dir = DataPaths.resolve(folder)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        return self.base_dir / (quote(str(device_id), safe="") + ".json")

    def merge(self, device_id, values):
        """Merge only supplied tags so other register ranges remain available."""
        if not values:
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if self.backend == "valkey":
            # One transaction: no read-modify-write race between mapping workers.
            # JSON encoding here creates strings inside a Valkey hash, not files.
            # "data:" distinguishes tag fields from the hash's update timestamp.
            fields = {"data:" + k: json.dumps(v) for k, v in values.items()}
            fields["time_stamp"] = now
            with self.client.pipeline(transaction=True) as pipe:
                # HSET updates supplied tags without deleting other register blocks.
                # These commands are queued locally until pipe.execute() below.
                pipe.hset(self.key(device_id), mapping=fields)
                if self.ttl:
                    pipe.expire(self.key(device_id), self.ttl)
                else:
                    pipe.persist(self.key(device_id))
                # Actual Valkey transaction execution: HSET plus expiry/persist.
                pipe.execute()
            # Prevent also executing the JSON branch: only one backend is selected.
            return
        path = self._path(device_id)
        # Lock across processes; replace the JSON atomically after a durable flush.
        with FileLock(str(path) + ".lock", timeout=5):
            # Read-modify-write under the lock to avoid losing another writer's tags.
            data = self.read(device_id) or {}
            data.update(values)
            fd, temporary = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as stream:
                    json.dump({"time_stamp": now, "data": data}, stream, indent=2)
                    # Push Python's buffers to the OS, then request a disk flush.
                    stream.flush()
                    os.fsync(stream.fileno())
                # Readers see a complete old or new JSON file, not a partial write.
                os.replace(temporary, path)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)

    def read(self, device_id):
        """Return the same tag dictionary regardless of the configured backend."""
        if self.backend == "valkey":
            # Actual Valkey read. No automatic fallback to a previous JSON session.
            fields = self.client.hgetall(self.key(device_id))
            if not fields:
                return None
            # Remove "data:" and recover the original Python tag objects/scalars.
            # The hash-level time_stamp is intentionally not returned as a tag.
            return {k[5:]: json.loads(v) for k, v in fields.items() if k.startswith("data:")}
        path = self._path(device_id)
        if not path.exists():
            return None
        # Same return shape as Valkey: only the tag dictionary, not the file envelope.
        with path.open(encoding="utf-8") as stream:
            return json.load(stream)["data"]

    async def amerge(self, device_id, values):
        """Async write wrapper used by PostMapper through LatestTelemetryManager."""
        # Keep blocking filesystem/network I/O away from the telemetry event loop.
        await asyncio.to_thread(self.merge, device_id, values)

    async def aread(self, device_id):
        """Async read wrapper used by GET /stream; does not create/change values."""
        # Async API readers share the same decoded snapshot shape.
        return await asyncio.to_thread(self.read, device_id)
