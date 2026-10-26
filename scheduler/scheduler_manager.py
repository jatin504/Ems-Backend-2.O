import json
import logging
import os
import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from scheduler.report_dispatcher import schedule_report_dispatcher
from scheduler.password_expiry_job import password_expiry_job

from .scheduler_executor import bess_scheduler_loop, scrape_and_insert_tariff, scrape_all_states_to_snapshot
from .update_schedule_metadata import update_schedule_metadata
import asyncio
from processor import ComputationEngine

logger = logging.getLogger(__name__)
scheduler = BackgroundScheduler()
# scheduler.start()


def run_bess_strategy_task():
    asyncio.run(bess_scheduler_loop())


def run_update_schedule_task():
    asyncio.run(update_schedule_metadata())

# COmputations
compution = ComputationEngine()
def run_computation_plant_device():
    compution.execute_computation_functions()

def run_password_expiry_task():
    asyncio.run(password_expiry_job())

def scheduler_manager_loop():
    now = datetime.datetime.now(datetime.timezone.utc)
    logger.info(f"[Scheduler Manager] Starting at {now}")

    if not scheduler.running:
        scheduler.start()
        logger.info(
            "[Scheduler] Starting APScheduler"
        )

    scheduler.add_job(
        run_bess_strategy_task,
        trigger=IntervalTrigger(seconds=10),
        id="bess_strategy_job",
        replace_existing=True,
    )

    # Plant Device Computations
    # scheduler.add_job(
    #     run_computation_plant_device,
    #     trigger=IntervalTrigger(seconds=10),
    #     id="plant_device_computation_layer",
    #     replace_existing=True,
    # )


    scheduler.add_job(
        run_update_schedule_task,
        trigger=IntervalTrigger(seconds=10),
        id="update_schedule_metadata_job",
        replace_existing=True,
    )

    # scheduler.add_job(
    #     scrape_and_insert_tariff,
    #     trigger=CronTrigger(minute="0,15,30,45"),
    #     id="tariff_scraper_job",
    #     replace_existing=True,
    # )

    # scheduler.add_job(
    #     scrape_and_insert_tariff,
    #     trigger=IntervalTrigger(minutes=1),  # MHA → energy_strategy every 15m
    #     id="tariff_scraper_mha_job",
    #     replace_existing=True,
    # )
    #scheduler.add_job(
    #    scrape_all_states_to_snapshot,
    #    trigger=IntervalTrigger(minutes=1),  # All states → snapshot every 15m
    #    id="tariff_snapshot_all_states_job",
    #    replace_existing=True,
    #)

    # Report Email Dispatcher
    schedule_report_dispatcher(scheduler)

    scheduler.add_job(
        run_password_expiry_task,
        trigger=IntervalTrigger(hours=24),  # daily
        id="password_expiry_job",
        replace_existing=True,
    )

    logger.info("[Scheduler Manager] Jobs scheduled (BESS & metadata updater)")

def stop_scheduler():

    logger.info(
        "[Scheduler] Stopping APScheduler"
    )

    try:

        if scheduler.running:
            scheduler.shutdown(
                wait=False
            )

    except Exception as e:

        logger.exception(
            f"[Scheduler] Shutdown failed: {e}"
        )
