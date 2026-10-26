import os
import json
import asyncio
from pathlib import Path
from datetime import datetime, timedelta
from utils.db_write_queue import GARBAGE_QUEUE

import logging

logger = logging.getLogger(__name__)


class GarbageFileWriter:

    def __init__(self):
        HERE = os.path.dirname(os.path.abspath(__file__))
        BASE = os.getenv("LOG_DIR", os.path.join(HERE, "logs"))
        self.base_dir = Path(f"{BASE}/garbage")
        self.base_dir.mkdir(parents=True, exist_ok=True)

        print(f"GarbageFileWriter initialized. Writing to: {self.base_dir}")

    def _current_file(self):
        today = datetime.now().strftime("%Y-%m-%d")
        return self.base_dir / f"garbage_{today}.jsonl"

    def _cleanup_old_files(self):
        try:
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)

            for file in self.base_dir.glob("garbage_*.jsonl"):

                try:
                    file_date = datetime.strptime(
                        file.stem.replace("garbage_", ""),
                        "%Y-%m-%d"
                    ).date()

                    if file_date not in (today, yesterday):
                        file.unlink()

                except Exception:
                    continue

        except Exception as e:
            logger.error(f"Garbage cleanup failed: {e}")

    async def start(self):

        while True:

            try:
                batch = []

                for _ in range(5000):
                    try:
                        batch.append(GARBAGE_QUEUE.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                if batch:

                    file_path = self._current_file()

                    with open(file_path, "a") as f:

                        for record in batch:

                            record["logged_at"] = datetime.now().isoformat()

                            f.write(
                                json.dumps(
                                    record,
                                    default=str
                                ) + "\n"
                            )

                    logger.info(
                        f"GarbageWriter wrote {len(batch)} records"
                    )

                self._cleanup_old_files()

            except Exception as e:
                logger.error(
                    f"Garbage writer failed: {e}"
                )

            await asyncio.sleep(60)