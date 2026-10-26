# main.py
"""
Entrypoint script that:
- loads CSV into grid_req
- fetches today's grid req rows
- schedules them into energy_strategy (idempotent)
- loops at configured interval


Environment variables used:
- GRID_REQ_CSV_PATH: path to CSV to import (optional)
- LOOP_INTERVAL_SECONDS: how often main loop runs (default: 300)


Run with: python main.py
"""
import os
import asyncio
import logging
from dotenv import load_dotenv


from dsm.retrieve_req import load_csv_to_grid_req, fetch_today_grid_reqs
from dsm.schedule import schedule_grid_reqs_to_energy_strategy

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - [%(device_id)s] %(message)s",
)
logger = logging.getLogger(__name__)

async def main_loop():
    load_dotenv()  # Load .env file

    csv_path = os.getenv("GRID_REQ_CSV_PATH")
    loop_interval = int(os.getenv("LOOP_INTERVAL_SECONDS", "300"))

    while True:
        try:
            logger.info("Loading CSV into grid_req...")
            inserted_rows = await load_csv_to_grid_req(csv_path)
            logger.info(f"Inserted {inserted_rows} rows into grid_req.")

            logger.info("Fetching today's grid requirements...")
            today_reqs = await fetch_today_grid_reqs()
            logger.info(f"Fetched {len(today_reqs)} grid requirements for today.")

            logger.info("Scheduling grid requirements into energy strategy...")
            scheduled_count = await schedule_grid_reqs_to_energy_strategy(today_reqs)
            logger.info(f"Scheduled {scheduled_count} grid requirements into energy strategy.")

        except Exception as e:
            logger.error(f"Error in main loop: {e}")

        logger.info(f"Sleeping for {loop_interval} seconds before next iteration...")
        await asyncio.sleep(loop_interval)

if __name__ == "__main__":
    asyncio.run(main_loop())