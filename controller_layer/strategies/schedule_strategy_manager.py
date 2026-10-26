import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger

from config.device_config_resolver import DeviceConfigResolver
from controller_layer.strategies.classes.schedule_runner import ScheduleRunner
from metadata.metadata_resolver import MetadataResolver

# Strategy imports
from ..strategies.bess_strategies import BessBasicStrategy, ToUSmartStrategy, ToUFlatStrategy, PeakShavingStrategy

from utils.loop_manager import get_main_loop

from LogColor import LogColor

logger = logging.getLogger(__name__)

# Global task deduplication
scheduled_task_ids = set()

# APScheduler singleton
scheduler = BackgroundScheduler()
scheduler.start()

# Calling main loop
main_loop = get_main_loop()

class StrategyScheduleManager:

    def __init__(self):
        self.device_resolver = DeviceConfigResolver()
        self.metadata_resolver = MetadataResolver()

    # ----------------------------------------------------------------------
    # 1. Detect strategy from schedule contents
    # ----------------------------------------------------------------------
    def detect_strategy(self, schedule: dict):
        has_duration = "duration_minutes" in schedule
        has_tariff = "tariff" in schedule
        has_load = "load_power" in schedule
        source = "schedule_from" in schedule

        # ----- Extrnal Schedule ---------
        if schedule["schedule_from"].lower() == "external":
            return "EXTERNAL_SCHEDULER"

        # ---- TOU SMART / FLAT ----
        if has_duration and has_tariff:
            tariffs = schedule["tariff"]

            # If tariff array contains different values, it's SMART
            if len(set(tariffs)) > 1:
                return "TOU_SMART"
            else:
                return "TOU_FLAT"

        # ---- PEAK SHAVING ----
        if has_load:
            return "PEAK_SHAVING"

        # ---- BASIC STRATEGY ----
        return "BASIC"

    # ----------------------------------------------------------------------
    # 2. Return correct class handler
    # ----------------------------------------------------------------------
    def get_strategy_handler(self, strategy_name: str):
        mapping = {
            "BASIC": BessBasicStrategy,
            "PEAK_SHAVING": PeakShavingStrategy,
            "TOU_SMART": ToUSmartStrategy,
            "TOU_FLAT": ToUFlatStrategy,
            "EXTERNAL_SCHEDULER": ScheduleRunner
        }
        return mapping[strategy_name]

    # ----------------------------------------------------------------------
    # 3. Schedule execution using APScheduler
    # ----------------------------------------------------------------------
    def schedule_strategy_execution(self, strategy_class, schedule, device_info, signal):

        start_ts = schedule.get("startTs")
        if not start_ts:
            logger.error("[Strategy Manager] Schedule missing startTs field. Skipping.")
            return

        try:
            start_dt = datetime.strptime(start_ts, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            logger.error(f"[Strategy Manager] Invalid startTs format: {start_ts}. Skipping.")
            return

        job_id = f"{device_info['device_id']}_{start_ts}_{signal}"

        if job_id in scheduled_task_ids:
            logger.info(f"{LogColor.BLUE} [{job_id}] Already scheduled. Skipping. {LogColor.RESET}")
            return
        
        scheduled_task_ids.add(job_id)

        try:

            scheduler.add_job(
                func=strategy_class.run_coroutine_wrapper,
                trigger=DateTrigger(run_date=start_dt),
                kwargs={
                    "schedule": schedule,
                    "device_info": device_info,
                    "signal": signal
                },
                id=job_id,
                misfire_grace_time=30
            )
            logger.info(f"{LogColor.CYAN} [Scheduled] {strategy_class.__name__} | {signal.upper()} | Job ID: {job_id} {LogColor.RESET}")

        except Exception as e:
            logger.error(f"[Strategy Manager] Failed to schedule job {job_id}: {e}")
            return

    # ----------------------------------------------------------------------
    # 4. Main function: auto-detect & schedule everything
    # ----------------------------------------------------------------------
    async def run(self, device_id: str):

        device_info = self.device_resolver.get_device_info_by_id(device_id)
        if not device_info:
            logger.error(f"[Strategy Manager] Device info not found for {device_id}")
            return

        schedules = self.metadata_resolver.get_active_schedules()

        if not schedules:
            logger.info("[Strategy Manager] No active schedules found.")
            return

        logger.info(f"[Strategy Manager] Found {len(schedules)} active schedules.")

        for schedule in schedules:

            # --- Extract signal from schedule's original category ---
            signal = schedule.get("type")   # "charge" or "discharge"
            if signal not in ("charge", "discharge"):
                logger.warning(f"[Strategy Manager] Unknown schedule type: {signal}, skipping...")
                continue

            # --- Detect strategy dynamically ---
            strategy_type = self.detect_strategy(schedule)
            logger.info(f"[Schedule {schedule['startTs']}] Strategy Detected = {strategy_type} | Signal = {signal}")

            strategy_class = self.get_strategy_handler(strategy_type)

            # --- Schedule for execution ---
            self.schedule_strategy_execution(
                strategy_class=strategy_class,
                schedule=schedule,
                device_info=device_info,
                signal=signal
            )

