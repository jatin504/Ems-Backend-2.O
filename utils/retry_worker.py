import asyncio
import json
import asyncpg
import os
import csv
from datetime import datetime
from utils.db_config import db_config

FAILED_FILE = "failed_writes.csv"

def fix_params(params):
    new_params = []
    for p in params:
        if isinstance(p, str) and "T" in p:
            try:
                p = datetime.fromisoformat(p)
            except:
                pass
        new_params.append(p)
    return new_params

async def retry_worker():
    while True:
        try:
            if not os.path.exists(FAILED_FILE):
                await asyncio.sleep(5)
                continue

            # try connect primary
            config = db_config("primary")
            conn = await asyncpg.connect(**config)

            print("PRIMARY BACK → syncing data...")

            with open(FAILED_FILE, "r", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            failed_rows = []

            for row in rows:
                try:
                    params = json.loads(row["params"])
                    params = fix_params(params)

                    await conn.execute(row["query"], *params)
                except Exception as e:
                    print("retry failed:", e)
                    failed_rows.append(row)

            with open(FAILED_FILE, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["query", "params"])
                writer.writeheader()
                for r in failed_rows:
                    writer.writerow(r)

            await conn.close()

        except Exception:
            print("⏳ PRIMARY still down... retry later")

        await asyncio.sleep(5)