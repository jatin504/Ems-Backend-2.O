from datetime import datetime, timezone, timedelta
import logging
from controller_layer.strategies.classes.common.ScheduleRunner import bess_decision
from controller_layer.strategies.constants import StrategyID
from device_config_helper import DeviceConfigHelper
from utils import ModbusWriter
from typing import List, Dict
from ..charging_strategies import ChargingBasicStrategy, ScheduleRunnerCharging
from ..dicharging_strategies import DischargeBasicStrategy, ScheduleRunnerDischarging
from statics.controller_resolver import ControllerStaticResolver
from metadata.metadata_resolver import MetadataResolver
from latest_telemetry_resolver import LatestTelemetryResolver
from ...writers import EMSControlStateManager
from ..cycle_management import get_today_cycles
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from functools import partial
from LogColor import LogColor
import pytz, asyncio
from pathlib import Path
import json
from utils.loop_manager import get_main_loop

from model.db import get_async_connection

from ..load_bess_params_setting import load_bess_params_setting
from ..cycle_management import load_cycle_config

# model
from model import ExecutionLog

# utils
from utils.db_utils import A_DB

# Repos'
from repository import create_execution_log, finish_execution_log

# Strategy Flag Controller
from ...writers.strategy_flags_controller import StrategyFlagsController

from runtime_paths import DataPaths


logger = logging.getLogger(__name__)

# Global task deduplication
scheduled_task_ids = set()

# APScheduler singleton
scheduler = BackgroundScheduler()
scheduler.start()

# Timezone
IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.utc

# asyncScheduler = AsyncIOScheduler()
# asyncScheduler.start()

# Calling main loop
main_loop = get_main_loop()

class ScheduleRunner:
    """
    Basic BESS Control Strategy.
    Charges when production exceeds demand and discharges when demand exceeds production,
    subject to SoC thresholds.
    """
    def __init__(self):
        pass

    
    @staticmethod
    def run_coroutine_wrapper(**kwargs):
        logger.info(
            f"[Strategy] Wrapper fired | loop_running={main_loop.is_running()} "
            f"| loop_closed={main_loop.is_closed()}"
        )

        try:
            future = asyncio.run_coroutine_threadsafe(
                ScheduleRunner.run_schedule(**kwargs),
                main_loop
            )

            logger.info("[Strategy] Coroutine submitted to main loop")

            def done_callback(f):
                try:
                    result = f.result()
                    logger.info(f"[Strategy DONE] Result: {result}")
                except Exception:
                    logger.exception("[Strategy ERROR] Coroutine failed")

            future.add_done_callback(done_callback)

        except Exception:
            logger.exception("[Strategy] Failed to submit coroutine")

    @staticmethod
    async def run_schedule(
        schedule: dict,
        device_info: dict,
        signal: str
    ):
        """
        Executed by APScheduler when schedule start time triggers.
        This function MUST NOT create new jobs here.
        """

        try:

            # Extract Device Infos
            device_id = device_info["device_id"]
            channel = device_info["channel"]
            server_ip = device_info["server_ip"]
            server_port = device_info["server_port"]
            slave_id = device_info["slave_id"]

            logger.info(f"{LogColor.CYAN} [MANUAL] Executing schedule for device {device_id} {LogColor.RESET}")

            # --------------------------------------------
            resolver = LatestTelemetryResolver() 
            controller_static = ControllerStaticResolver()
            # -------------------------------------------- 

            # Flag Raiser
            ess_control_state_manager = EMSControlStateManager()
            strategy_flags_controller = StrategyFlagsController(
                ess_control_state_manager = ess_control_state_manager,
            )
            await strategy_flags_controller.raise_strategy_flags_once_per_day(
                strategy_name="external",
                flags={
                    "enable_tou_tariff_strategy": 0,
                    "enable_capped_dynamic_strategy": 0,
                    "enable_peak_shaving_strategy": 0,
                    "enable_basic_strategy": 0,
                    "internal_controlling_executing": 0,
                    "external_schedule_executing": 1,
                }
            )

            logger.info(f"{LogColor.CYAN} [MANUAL SCHEDULE] Flag is Raised {LogColor.RESET}")

            # -------------------------------------------------
            # 0. Load BESS Decision 
            # -------------------------------------------------

            decision_config = await bess_decision(device_id)

            (
                prevent_charging,
                block_discharging,
                soc
            ) = (
                decision_config["prevent_charging"],
                decision_config["block_discharging"],
                decision_config["soc"]
            )

            # ---------------------------------------
            # 1. Extract schedule fields
            # ---------------------------------------
            startTs = schedule.get("startTs")
            endTs = schedule.get("endTs")
            power_kw = schedule.get("power")
            min_soc = schedule.get("min_soc")
            max_soc = schedule.get("max_soc")

            # if not all([startTs, endTs]):
            #     logger.error(f"[SCHEDULE] Invalid schedule payload: {schedule}")
            #     return

            # ---------------------------------------
            # 2. Get live values
            # ---------------------------------------
            # device_data = resolver.get_latest_device_data(device_id)
            # if not device_data:
            #     logger.error(f"[SCHEDULE] No live data for {device_id}")
            #     return
            
            # soc = device_data["SoC"]["value"]

            # ---------------------------------------
            # 3. Smart decision-making
            # ---------------------------------------
            if signal == "discharge":
                # DISCHARGE LOGIC
                logger.info(f"{LogColor.CYAN} [SCHEDULE] Discharge event triggered {LogColor.RESET}")

                # if block_discharging:
                #     logger.info("[SCHEDULE] Discharging blocked by decision logic.")
                #     return

                await ScheduleRunnerDischarging(
                    resolver=resolver,
                    controller_static=controller_static,
                    power_kw=power_kw,
                    startTs=startTs,
                    endTs=endTs,
                    sch_min_soc=min_soc,
                    sch_max_soc=max_soc,
                    device_id=device_id,
                    channel=channel,    
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    extra_info={
                        "strategy_id" : StrategyID.MANUAL_SCHEDULE.value,
                        "authority": "external"
                    }
                )

            else:
                # CHARGE LOGIC
                logger.info(f"{LogColor.CYAN} [SCHEDULE] Charge event triggered {LogColor.RESET}")

                # if prevent_charging:
                #     logger.info("[SCHEDULE] Charging prevented by decision logic.")
                #     return

                await ScheduleRunnerCharging(
                    resolver=resolver,
                    controller_static=controller_static,
                    power_kw=power_kw,
                    startTs=startTs,
                    endTs=endTs,
                    sch_min_soc=min_soc,
                    sch_max_soc=max_soc,
                    device_id=device_id,
                    channel=channel,    
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    extra_info={
                        "strategy_id" : StrategyID.MANUAL_SCHEDULE.value,
                        "authority": "external"
                    }
                )

            # External Schedule Flag set down
            # await strategy_flags_controller.raise_strategy_flags_once_per_day(
            #     strategy_name="external",
            #     flags={
            #         "external_schedule_executing": 0,
            #     }
            # )

            logger.info("[SCHEDULE] Completed.")
        except Exception as e:
            logger.error(f"{LogColor.RED} [SCHEDULE] Error executing schedule: {e} {LogColor.RESET}")
            return