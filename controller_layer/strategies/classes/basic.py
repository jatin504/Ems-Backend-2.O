from datetime import datetime, timezone, timedelta
import logging
from device_config_helper import DeviceConfigHelper
from utils import ModbusWriter
from typing import List, Dict
from ..charging_strategies import ChargingBasicStrategy, tou_charging, PeakShavingChargingStrategy, flat_tou_charging, smart_tou_charging, smart_tou_schedule_charging, flat_tou_schedule_charging
from ..dicharging_strategies import DischargeBasicStrategy, TouDischargingSmartStrategy, TouDischargingFlatStrategy, PeakShavingDischargeStrategy
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

class BessBasicStrategy:
    """
    Basic BESS Control Strategy.
    Charges when production exceeds demand and discharges when demand exceeds production,
    subject to SoC thresholds.
    """
    def __init__(self, resolver):
        # self.resolver = resolver  # Used to resolve device and meter data
        self.resolver = LatestTelemetryResolver()
        self.controller_static = ControllerStaticResolver()
        self.metadata_resolver = MetadataResolver()
        self.ess_control_state_manager = EMSControlStateManager()
        self.strategy_flags_controller = StrategyFlagsController(
            ess_control_state_manager = self.ess_control_state_manager,
        )

    async def execute(self, channel, server_ip, server_port, slave_id, device_id, signal):
        """
        Execute the basic BESS control strategy.
        """


        strategy_name = "BASIC"
        strategy_id = 4

        logger.info(f"{LogColor.RED}[BasicStrategy] Starting strategy execution...{LogColor.RESET}")

        # -------------------------------------------------
        # Raise EMS Status Flag fot Strategy
        # -------------------------------------------------        

            # await self.ess_control_state_manager.raise_flag("enable_capped_dynamic_strategy", 1)
            # await self.ess_control_state_manager.raise_flag("enable_tou_tariff_strategy", 0)
            # await self.ess_control_state_manager.raise_flag("enable_peak_shaving_strategy", 0)
        try:

            await self.strategy_flags_controller.raise_strategy_flags_once_per_day(
                strategy_name=strategy_name,
                flags={
                    "enable_basic_strategy": 1,
                    "enable_capped_dynamic_strategy": 0,
                    "enable_tou_tariff_strategy": 0,
                    "enable_peak_shaving_strategy": 0
                }
            )
        except Exception as e:
            logger.info(f"{LogColor.RED} Error whilte raising EMS STATUS FLAG {e} {LogColor.RESET}")
            return

        try:

            try: 

                decision_config = await BessBasicStrategy.bess_decision(device_id=device_id)

                (
                    prevent_charging,
                    aggressively_charge,
                    block_discharging,
                    backup_soc,
                    min_soc,
                    max_soc,
                    normal_recovery_soc,
                    cycles_used_today,
                    max_cycles_per_day,
                    battery_capacity,
                    max_discharge_kw,
                    max_charge_kw,
                    arbitrage_cfg,
                    soc_cfg,
                    bess_power,
                    current_tariff,
                    soc,
                    disable_aggrassive_charging,
                    disable_arbitrage,
                    disable_low_price_charging
                ) = (
                    decision_config["prevent_charging"],
                    decision_config["aggressively_charge"],
                    decision_config["block_discharging"],
                    decision_config["backup_soc"],
                    decision_config["min_soc"],
                    decision_config["max_soc"],
                    decision_config["normal_recovery_soc"],
                    decision_config["cycles_used_today"],
                    decision_config["max_cycles_per_day"],
                    decision_config["battery_capacity"],
                    decision_config["max_discharge_kw"],
                    decision_config["max_charge_kw"],
                    decision_config["arbitrage_cfg"],
                    decision_config["soc_cfg"],
                    decision_config["bess_power"],
                    decision_config["current_tariff"],
                    decision_config["soc"],
                    decision_config["disable_aggrassive_charging"],
                    decision_config["disable_arbitrage"],
                    decision_config["disable_low_price_charging"]
                )
            except Exception as e:
                logger.info(f"{LogColor.RED} Error loading live params: {e} {LogColor.RESET}")
                return

            # check (gen. power > threshold kw) or (any inv curtailment mode one) and setpoint is < 100%
            #     then charge battery (first take the remaining avaiable power present then check how much i need to charge then take decision according to balancing and rewrite set point and charge bettery)
            meter_device_id = "plant_1_poi_meter_1"

            # Fetch Meter data
            # meter_device_data = await self.resolver.get_latest_device_data(meter_device_id)

            pv_power = 7000 #meter_device_data["power"]["value"]
            threshold_kw = 5000 # eg.
		
            # function that return True if any inv curtailment mode is on
            is_curtailment_on = await BessBasicStrategy.check_inv_curtailment()

            setpoint_from_controller = 80 # eg.
            pv_capacity = 600  # self.controller_static.get('pv_capacity_kw')

            if (not prevent_charging
                and (pv_power > threshold_kw or is_curtailment_on)
                and setpoint_from_controller < 100):

                logger.info(f"{LogColor.GREEN} In excess charging strategy... {LogColor.RESET}")
                
                # Calculate how much charge
                charge_power = min((100 - setpoint_from_controller) * pv_capacity, battery_capacity * (max_soc - 60) / 100, max_charge_kw)  # kW
                pv_setpoint_change = charge_power / pv_capacity * 100  # percentage change to setpoint
                new_pv_setpoint = 95 #setpoint_from_controller + pv_setpoint_change

                logger.info(f"{LogColor.GREEN} Charge Power= {charge_power}kW New PV Setpoint= {new_pv_setpoint}% {LogColor.RESET}")

                await ChargingBasicStrategy(
                    resolver=self.resolver,
                    controller_static=self.controller_static,
                    power_kw=charge_power,
                    startTs=None,
                    endTs=None,
                    device_id=device_id,
                    channel=channel,    
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    extra_info={
                        "strategy_id" : 4,
                        "excess_charge": True,
                        "pv_setpoint_adjustment": {
                            "original_setpoint": setpoint_from_controller,
                            "new_setpoint": new_pv_setpoint,
                            "change": pv_setpoint_change
                        }
                    }
                )

                

        except Exception as e:
            logger.error(f"{LogColor.RED}Error executing BESS strategy: {e}{LogColor.RESET}")
            raise

    @staticmethod
    async def run_discharge_strategy(
        resolver,
        controller_static,
        power,
        startTs,
        endTs,
        device_id,
        channel,
        server_ip,
        server_port,
        slave_id
    ):
        strategy = await DischargeBasicStrategy(
            resolver=resolver,
            controller_static=controller_static,
            power_kw=power,
            startTs=startTs,
            endTs=endTs,
            device_id=device_id,
            channel=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id
        )

        return strategy
    
    @staticmethod
    def run_coroutine_wrapper(**kwargs):
        """Submit async job to main loop."""
        future = asyncio.run_coroutine_threadsafe(
            BessBasicStrategy.run_schedule(**kwargs),   # <-- async method
            main_loop
        )

        future.add_done_callback(
            lambda f: print(
                f"[Strategy DONE] Result: {f.result()}"
            ) if not f.exception() else print(
                f"[Strategy ERROR] {f.exception()}"
            )
        )

    @staticmethod
    async def check_inv_curtailment() -> bool:
        """
        IF any of the Inverter in curtailmenet it returns True else False
        """

        device_config = DeviceConfigHelper()
        resolver = LatestTelemetryResolver() 

        inv_ids = device_config.get_device_ids_by_device_type("inverter")

        for id in inv_ids:
            try:
                data = await resolver.get_latest_device_data(id)
                curtailment_mode = data.get("curtailment_mode", {}).get("value", 0)
                if curtailment_mode != 0:
                    return True
            except Exception as e:
                logger.error(f"Error checking curtailment for {id}: {e}")
                continue

        return False
    
    @staticmethod
    async def bess_decision(
        device_id: str
    ):
        """
        CENTRALIZED SAFETY + TARIF + CYCLE DECISION LOGIC FOR BESS Strategy

        :param device_id: The ID of the BESS device.
        :return:
        Returns a dict of decisions that any strategy can use.
        """
        
        resolver = LatestTelemetryResolver() 
        controller_static = ControllerStaticResolver() 
        # metadata_resolver = MetadataResolver() 
        # ess_control_state_manager = EMSControlStateManager()
        # smart_tou = ToUSmartStrategy.load_smart_tou_settings()

        # ----------------------------
        # LOAD CONFIGS
        # ----------------------------

        bess_cfg = load_bess_params_setting()
        cycle_cfg = load_cycle_config()

        min_soc = bess_cfg["min_soc"]
        max_soc = bess_cfg["max_soc"]
        backup_soc = bess_cfg["backup_soc"]
        battery_capacity = controller_static.get('battery_capacity')
        max_discharge_kw = bess_cfg.get("max_discharge_kw")
        max_charge_kw = bess_cfg.get("max_charge_kw")

        arbitrage_cfg = cycle_cfg["price_logic_overrides"]
        soc_cfg = cycle_cfg["soc_safety_management"]
        lockout_cfg = cycle_cfg["lockout_behaviour"]
        price_config = cycle_cfg["price_logic_overrides"]
        notify_cfg = cycle_cfg["notifications"]

        max_cycles_per_day = cycle_cfg["max_cycles_per_day"]
        cycles_used_today = await get_today_cycles()

        # Derived values
        min_safe_soc = min_soc
        normal_recovery_soc = backup_soc

        disable_aggrassive_charging = price_config["disable_aggrassive_charging"]
        disable_arbitrage = price_config["disable_arbitrage"]
        disable_low_price_charging = price_config["disable_low_price_charging"]
        disable_peak_shaving = price_config["disable_peak_shaving"]

        # ---------------------------------------------------
        # Read latest device data
        # ---------------------------------------------------
        try:
            device_data = resolver.get_latest_device_data(device_id)

            soc = device_data["SoC"]["value"]
            bess_power = float(device_data["ac_active_power"]["value"])
            current_tariff = 3# float(self.metadata_resolver.get_latest_tariff())

            logger.info(f"{LogColor.BLUE} [BASIC Strategy] SoC: {soc}, Current Tariff: {current_tariff} {LogColor.RESET}")
        except Exception as e:
            logger.info(f"{LogColor.RED} Error loading live params: {e} {LogColor.RESET}")

        # ----------------------------
        # OUTPUT FLAGS
        # ----------------------------
        prevent_charging = False
        aggressively_charge = False
        block_discharging = False

        # ----------------------------
        # 2. MAX CYCLES ENFORCEMENT
        # ----------------------------
        if cycles_used_today >= max_cycles_per_day:
            logger.warning(f"[CYCLE] Max cycles reached → Blocking discharge")
            block_discharging = True

            if lockout_cfg["block_economic_charging"]:
                prevent_charging = True

        # ----------------------------
        # 3. SOC SAFETY MANAGEMENT
        # ----------------------------
        if soc < min_safe_soc:
            logger.warning(f"[SAFETY] SOC < minimum → forcing safety charging")

            if lockout_cfg["allow_safety_charging"]:
                prevent_charging = False
                aggressively_charge = True
                block_discharging = True

        # ----------------------------
        # 4. RESERVE BAND CHARGING
        # ----------------------------
        reserve_soc_threshold = backup_soc

        if soc < reserve_soc_threshold and lockout_cfg["allow_reserve_charging"]:
            prevent_charging = False  # allow charging anytime to reach reserve

        # ----------------------------
        # 5. OPTIONAL NOTIFICATIONS
        # ----------------------------
        if notify_cfg["notify_at_80_percent_usage"]:
            usage_percent = (cycles_used_today / max_cycles_per_day if max_cycles_per_day != 0 else 1) * 100
            if usage_percent >= 80:
                logger.info(f"[NOTICE] 80% cycles consumed today")

        return {
            "prevent_charging": prevent_charging,
            "aggressively_charge": aggressively_charge,
            "block_discharging": block_discharging,
            "backup_soc": backup_soc,
            "min_soc" : min_soc,
            "max_soc" : max_soc,
            "normal_recovery_soc": normal_recovery_soc,
            "cycles_used_today": cycles_used_today,
            "max_cycles_per_day": max_cycles_per_day,
            "battery_capacity" : battery_capacity,
            "max_discharge_kw" : max_discharge_kw,
            "max_charge_kw" : max_charge_kw,
            "arbitrage_cfg" : arbitrage_cfg,
            "soc_cfg" : soc_cfg,
            "bess_power" : bess_power,
            "current_tariff" : current_tariff,
            "soc" : soc,
            "disable_low_price_charging" : disable_low_price_charging,
            "disable_arbitrage" : disable_arbitrage,
            "disable_aggrassive_charging" : disable_aggrassive_charging
        }

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

            # --------------------------------------------
            resolver = LatestTelemetryResolver() 
            controller_static = ControllerStaticResolver()
            # -------------------------------------------- 

            # Extract Device Infos
            device_id = device_info["device_id"]
            channel = device_info["channel"]
            server_ip = device_info["server_ip"]
            server_port = device_info["server_port"]
            slave_id = device_info["slave_id"]

            logger.info(f"{LogColor.CYAN} [BESS BASIC] Executing schedule for device {device_id} {LogColor.RESET}")

            # -------------------------------------------------
            # 0. Load BESS Decision 
            # -------------------------------------------------

            decision_config = await BessBasicStrategy.bess_decision(device_id)

            (
                prevent_charging,
                block_discharging,
                backup_soc,
                min_soc,
                max_soc,
                battery_capacity,
                max_discharge_kw,
                bess_power,
                soc
            ) = (
                decision_config["prevent_charging"],
                decision_config["block_discharging"],
                decision_config["backup_soc"],
                decision_config["min_soc"],
                decision_config["max_soc"],
                decision_config["battery_capacity"],
                decision_config["max_discharge_kw"],
                decision_config["bess_power"],
                decision_config["soc"]
            )

            # ---------------------------------------
            # 1. Extract schedule fields
            # ---------------------------------------
            startTs = schedule.get("startTs")
            endTs = schedule.get("endTs")
            power_kw = schedule.get("power")

            if not all([startTs, endTs]):
                logger.error(f"[BESS BASIC] Invalid schedule payload: {schedule}")
                return

            # ---------------------------------------
            # 2. Get live values
            # ---------------------------------------
            device_data = resolver.get_latest_device_data(device_id)
            if not device_data:
                logger.error(f"[BESS BASIC] No live data for {device_id}")
                return
            
            # soc = device_data["SoC"]["value"]

            # ---------------------------------------
            # 3. Smart decision-making
            # ---------------------------------------
            if signal == "discharge":
                # DISCHARGE LOGIC
                logger.info(f"{LogColor.CYAN} [BESS BASIC] Discharge event triggered {LogColor.RESET}")

                if block_discharging:
                    logger.info("[BESS BASIC] Discharging blocked by decision logic.")
                    return

                await DischargeBasicStrategy(
                    resolver=resolver,
                    controller_static=controller_static,
                    power_kw=power_kw,
                    startTs=startTs,
                    endTs=endTs,
                    device_id=device_id,
                    channel=channel,
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id
                )

            else:
                # CHARGE LOGIC
                logger.info(f"{LogColor.CYAN} [BESS BASIC] Charge event triggered {LogColor.RESET}")

                # if prevent_charging:
                #     logger.info("[BESS BASIC] Charging prevented by decision logic.")
                #     return

                await ChargingBasicStrategy(
                    resolver=resolver,
                    controller_static=controller_static,
                    power_kw=power_kw,
                    startTs=startTs,
                    endTs=endTs,
                    device_id=device_id,
                    channel=channel,    
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    extra_info={
                        "strategy_id" : 4
                    }
                )
                

            logger.info("[BESS BASIC] No action required. Conditions not met.")
        except Exception as e:
            logger.error(f"{LogColor.RED} [BESS BASIC] Error executing schedule: {e} {LogColor.RESET}")
            return