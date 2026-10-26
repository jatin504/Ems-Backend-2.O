from datetime import datetime, timezone, timedelta
import logging
from .peak_shaving import PeakShavingStrategy
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

class ToUSmartStrategy:
    def __init__(self, resolver):
        self.resolver = LatestTelemetryResolver()
        self.controller_static = ControllerStaticResolver()
        self.metadata_resolver = MetadataResolver()
        self.ess_control_state_manager = EMSControlStateManager()
        self.strategy_flags_controller = StrategyFlagsController(
            ess_control_state_manager = self.ess_control_state_manager,
        )

    async def execute(self, channel, server_ip, server_port, slave_id, device_id, signal):
        """
        Smart strategy execution: calculates how much energy to discharge per time slot based on tariff weighting.

        :param total_dischargeable_energy: Total energy (kWh) that can be discharged
        :param tariffs: List of tariff values per slot (e.g. [3.5, 2.5, 2.5])
        :param durations_minutes: List of durations (e.g. [60, 60, 60]) for each slot
        :param ramp_rate_kw_per_min: Ramp rate limit
        :param power_rating_kw: Max discharge power of BESS
        """

        logger.info(f"[TouSmartStrategy] Starting strategy execution...")

        strategy_id = 1
        strategy_name = "Capped Dynamic"

        # -------------------------------------------------
        # Raise EMS Status Flag fot Strategy
        # -------------------------------------------------
        try:

            # await self.ess_control_state_manager.raise_flag("enable_capped_dynamic_strategy", 1)
            # await self.ess_control_state_manager.raise_flag("enable_tou_tariff_strategy", 0)
            # await self.ess_control_state_manager.raise_flag("enable_peak_shaving_strategy", 0)

            await self.strategy_flags_controller.raise_strategy_flags_once_per_day(
                strategy_name="capped_dynamic",
                flags={
                    "enable_capped_dynamic_strategy": 1,
                    "enable_tou_tariff_strategy": 0,
                    "enable_basic_strategy": 0,
                    "enable_peak_shaving_strategy": 0
                }
            )
        except Exception as e:
            logger.info(f"{LogColor.RED} Error whilte raising EMS STATUS FLAG {e} {LogColor.RESET}")

        # -------------------------------------------------
        # Load BESS Decision 
        # -------------------------------------------------

        decision_config = await ToUSmartStrategy.bess_decision(device_id)

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
            refresh_interval_minutes,
            arbitrage_threshold,
            battery_capacity,
            max_discharge_kw,
            arbitrage_cfg,
            soc_cfg,
            bess_power,
            rules,
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
            decision_config["refresh_interval_minutes"],
            decision_config["arbitrage_threshold"],
            decision_config["battery_capacity"],
            decision_config["max_discharge_kw"],
            decision_config["arbitrage_cfg"],
            decision_config["soc_cfg"],
            decision_config["bess_power"],
            decision_config["rules"],
            decision_config["current_tariff"],
            decision_config["soc"],
            decision_config["disable_aggrassive_charging"],
            decision_config["disable_arbitrage"],
            decision_config["disable_low_price_charging"]
        )

        # ----------------------------------------
        # BESS Smart Tou Charging Condition
        # ----------------------------------------

        # Loss factor and diff tariff from metadata
        min_discharge_tariff = 3 #self.metadata_resolver.get_discharge_tariff_stats().get("min", 0)
        loss_percent = (100 - self.controller_static.get("efficiency")) / 100
        diff_tariff = self.controller_static.get("diff_tariff")
        
        # ---------------------------------
        # Inserting Starting Execution Log
        # ---------------------------------
        try :

        
            pool = await get_async_connection()
            async with pool.acquire() as conn:

                # create execution log
                exec_log = ExecutionLog(strategy_id=strategy_id, started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), input_snapshot={"live": {"soc" : soc, "current_tariff" : current_tariff}, "config": {}, "flags": {}})
                exec_id = await create_execution_log(conn, exec_log)
            await pool.close()
        except Exception as e:
            logger.info(f"{LogColor.RED} Error While Creating Log Exec. {e} {LogColor.RESET}")


        # --------------------------------------------------
        # Peak Shaving as Constraints
        # --------------------------------------------------

        if not arbitrage_cfg["disable_peak_shaving"]:
            await PeakShavingStrategy.peak_shaving_as_constraint(
                device_id=device_id,
                poi_meter_id="plant_1_poi_meter_1",
                channel=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                block_discharging=block_discharging
            )

        # ---------------------------------------------------
        # Decision Logic
        # ---------------------------------------------------

        last_discharge_tariffs = [2,3,4] # last 24 hours dicharge tariff
        last_charge_tariffs = [1,2,3] # last 24 hours charge tariff

        current_tariff = 10 # current tariff instance
        # ---------------------------------------
        # 1. DISCHARGE → When profitable
        # ---------------------------------------
        # Profit if today's price is higher than future price or diff threshold

        def safe_avg(values):
            return sum(values) / len(values) if values else 0

        historical_spread = safe_avg(last_discharge_tariffs) - safe_avg(last_charge_tariffs)

        #current_tariff >= historical_spread + arbitrage_threshold
        should_discharge = (
            current_tariff - historical_spread >= arbitrage_threshold if not disable_arbitrage else True
            and rules["allow_discharge_when_profitable"]
            and soc > min_soc
            and cycles_used_today <= max_cycles_per_day
            and not block_discharging
        )

        if should_discharge:
            logger.info(f"{LogColor.BLUE} [TOU-SMART] Condition Met → Discharge Battery {LogColor.RESET}")

            result = await TouDischargingSmartStrategy(
                resolver=self.resolver,
                controller_static=self.controller_static,
                tariffs=current_tariff,
                durations_minutes=refresh_interval_minutes,
                startTs=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                endTs=(datetime.now()+timedelta(minutes=refresh_interval_minutes)).strftime("%Y-%m-%d %H:%M:%S"),
                device_id=device_id,
                channel=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id
            )

            pool = await get_async_connection()
            async with pool.acquire() as conn:
                await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
            logger.info(f"[TouSmartStrategy] Strategy execution completed.")
            return result
        
        # ---------------------------------------
        # 2. AGGRESSIVE CHARGING (below floor)
        # ---------------------------------------
        should_charge = (
            aggressively_charge
            and soc < max_soc
            and not prevent_charging
            and not disable_aggrassive_charging
        )
        if should_charge:
            logger.info("[TOU-SMART] Aggressive Charge Triggered")
            result = await smart_tou_charging(
                resolver=self.resolver,
                controller_static=self.controller_static,
                soc=soc,
                power=bess_power,
                current_tariff=current_tariff,
                channel=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
                battery_capacity=battery_capacity,
                extra_info={"exec_id" : exec_id}
            )
            pool = await get_async_connection()
            async with pool.acquire() as conn:
                await finish_execution_log(conn, exec_id, finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), decision_snapshot=result if result else {}, error_text="")
            logger.info(f"[TouSmartStrategy] Strategy execution completed.")
            return result
        else:
            logger.info("[TOU-SMART] SOC at Max → Cannot Charge")
            pool = await get_async_connection()
            async with pool.acquire() as conn:
                await finish_execution_log(conn, exec_id, finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), decision_snapshot=result if result else {}, error_text="[TOU-SMART] SOC at Max → Cannot Charge")
            # logger.info(f"[TouSmartStrategy] Strategy execution completed.")
            # return {}


        # ---------------------------------------
        # 3. NORMAL CHARGING (if price low)
        # ---------------------------------------
        threshold_tariff = min_discharge_tariff - (min_discharge_tariff * loss_percent) - diff_tariff
        logger.info(f"[TouCharging] Calculated Tariff Threshold: {threshold_tariff:.2f} (Current Tariff: {current_tariff})")

        should_charge = (
            not prevent_charging
            and soc < backup_soc
            and current_tariff <= threshold_tariff
            and not disable_low_price_charging
        )

        if should_charge:
            logger.info("[TOU-SMART] Condition Met → Normal Charging")

            result = await smart_tou_charging(
                resolver=self.resolver,
                controller_static=self.controller_static,
                soc=soc,
                power=bess_power,
                current_tariff=current_tariff,
                channel=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
                battery_capacity=battery_capacity,
                extra_info={"strategy_id" : strategy_id}
            )
        
            pool = await get_async_connection()
            async with pool.acquire() as conn:
                await finish_execution_log(conn, exec_id, finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), decision_snapshot=result if result else {}, error_text="")
            logger.info(f"[TouSmartStrategy] Strategy execution completed.")
            return result
        
        pool = await get_async_connection()
        async with pool.acquire() as conn:
            await finish_execution_log(conn, exec_id, finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), decision_snapshot={}, error_text="")
        logger.info(f"[TouSmartStrategy] Strategy execution completed.")
        return
    
    @staticmethod
    async def run_discharge_strategy(
        resolver,
        controller_static,
        tariffs,
        durations_minutes,
        startTs,
        endTs,
        device_id,
        channel,
        server_ip,
        server_port,
        slave_id
    ):
        strategy = await TouDischargingSmartStrategy(
            resolver=resolver,
            controller_static=controller_static,
            tariffs=tariffs,
            durations_minutes=durations_minutes,
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
            ToUSmartStrategy.run_schedule(**kwargs),   # <-- async method
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
    async def simulate_dynamic_cap_day(
        controller_device_id
    ):
        resolver = LatestTelemetryResolver() 
        controller_static = ControllerStaticResolver() 


        # ---------------------------------------------------
        # Load Smart TOU Config
        # ---------------------------------------------------

        config = ToUSmartStrategy.load_smart_tou_settings()

        # ----------------------------
        # LOAD CONFIGS
        # ----------------------------

        bess_cfg = load_bess_params_setting()
        cycle_cfg = load_cycle_config()

        min_soc = bess_cfg["min_soc"]
        max_soc = bess_cfg["max_soc"]
        backup_soc = bess_cfg["backup_soc"]
        battery_capacity = controller_static.get('battery_capacity')
        max_charge_kw = bess_cfg.get("max_charge_kw")
        max_discharge_kw = bess_cfg.get("max_discharge_kw")
        efficiency = controller_static.get("efficiency")
        efficiency = efficiency if efficiency <= 1 else efficiency * 0.01

        lockout_cfg = cycle_cfg["lockout_behaviour"]

        max_cycles_per_day = cycle_cfg["max_cycles_per_day"]
        cycles_used_today = 1#await get_today_cycles()


        device_data = resolver.get_latest_device_data(controller_device_id)
        initial_soc = float(device_data["SoC"]["value"])

        INTERVAL_HOURS = 0.25  # 15 minutes

        tariffs = [
            # 00:00–06:00 (24 blocks) → 2
            2,2,2,2, 2,2,2,2, 2,2,2,2, 2,2,2,2, 2,2,2,2, 2,2,2,2,

            # 06:00–10:00 (16 blocks) → 5
            5,5,5,5, 5,5,5,5, 5,5,5,5, 5,5,5,5,

            # 10:00–17:00 (28 blocks) → 4
            4,4,4,4, 4,4,4,4, 4,4,4,4, 4,4,4,4,
            4,4,4,4, 4,4,4,4, 4,4,4,4, 4,4,4,4,

            # 17:00–22:00 (20 blocks) → 12 (evening peak)
            12,12,12,12, 12,12,12,12, 12,12,12,12,
            12,12,12,12, 12,12,12,12,

            # 22:00–24:00 (8 blocks) → 6
            6,6,6,6, 6,6,6,6
        ]

        def safe_avg(values):
            return sum(values) / len(values) if values else 0

        price_cap = config["caps_and_thresholds"]["price_cap_threshold"]
        price_floor = config["caps_and_thresholds"]["price_floor_threshold"]
        arbitrage_threshold = config["caps_and_thresholds"]["arbitrage_threshold"]

        soc = initial_soc
        total_charge = 0
        total_discharge = 0
        energy_cost = 0
        energy_revenue = 0
        cycles = cycles_used_today

        timeline = []

        last_charge_tariffs = [2.5, 2.0, 3.5]
        avg_charge_price = safe_avg(last_charge_tariffs)

        for block, current_tariff in enumerate(tariffs):

            prevent_charging = False
            aggressively_charge = False
            block_discharging = False

            soc_before = soc
            action = "idle"
            energy_charged = 0
            energy_discharged = 0

            # ----------------------------
            # 1. PRICE CAP / FLOOR
            # ----------------------------
            if current_tariff >= price_cap:
                prevent_charging = True

            if current_tariff <= price_floor:
                aggressively_charge = True

            # ----------------------------
            # 2. MAX CYCLES
            # ----------------------------
            if cycles >= max_cycles_per_day:
                block_discharging = True
                if lockout_cfg["block_economic_charging"]:
                    prevent_charging = True

            # ----------------------------
            # 3. SOC SAFETY
            # ----------------------------
            if soc < min_soc:
                if lockout_cfg["allow_safety_charging"]:
                    prevent_charging = False
                    aggressively_charge = True
                    block_discharging = True

            # ----------------------------
            # 4. RESERVE BAND
            # ----------------------------
            if soc < backup_soc and lockout_cfg["allow_reserve_charging"]:
                prevent_charging = False

            # ----------------------------
            # 5. ECONOMIC DISCHARGE
            # ----------------------------
            expected_spread = current_tariff - avg_charge_price

            if (
                expected_spread >= arbitrage_threshold
                and not block_discharging
                and soc > backup_soc
            ):

                max_power = min(
                    max_discharge_kw,
                    ((soc - backup_soc) / 100) * battery_capacity / INTERVAL_HOURS
                )

                energy_discharged = max_power * INTERVAL_HOURS

                if energy_discharged > 0:
                    soc -= (energy_discharged / battery_capacity) * 100
                    total_discharge += energy_discharged
                    energy_revenue += energy_discharged * current_tariff * efficiency
                    cycles += energy_discharged / battery_capacity
                    action = "discharge"

            # ----------------------------
            # 6. ECONOMIC CHARGE
            # ----------------------------
            elif not prevent_charging:

                if aggressively_charge or current_tariff <= avg_charge_price:

                    max_power = min(
                        max_charge_kw,
                        ((100 - soc) / 100) * battery_capacity / INTERVAL_HOURS
                    )

                    energy_charged = max_power * INTERVAL_HOURS

                    if energy_charged > 0:
                        soc += (energy_charged / battery_capacity) * 100
                        total_charge += energy_charged
                        energy_cost += energy_charged * current_tariff / efficiency
                        cycles += energy_charged / battery_capacity
                        action = "charge"

            timeline.append({
                "block": block,
                "price": current_tariff,
                "action": action,
                "soc_before": round(soc_before, 2),
                "soc_after": round(soc, 2),
                "energy_charged_kwh": round(energy_charged, 2),
                "energy_discharged_kwh": round(energy_discharged, 2),
            })

        return {
            "initial_soc": initial_soc,
            "final_soc": round(soc, 2),
            "total_charge_kwh": round(total_charge, 2),
            "total_discharge_kwh": round(total_discharge, 2),
            "energy_cost": round(energy_cost, 2),
            "energy_revenue": round(energy_revenue, 2),
            "net_profit": round(energy_revenue - energy_cost, 2),
            "cycles_used_today": round(cycles, 2),
            "timeline": timeline
        }
    
    @staticmethod
    async def bess_decision(
        device_id: str
    ):
        """
        CENTRALIZED SAFETY + TARIF + CYCLE DECISION LOGIC FOR Smart TOU Strategy

        :param device_id: The ID of the BESS device.
        :return:
        Returns a dict of decisions that any strategy can use.
        """
        
        resolver = LatestTelemetryResolver() 
        controller_static = ControllerStaticResolver() 
        # metadata_resolver = MetadataResolver() 
        # ess_control_state_manager = EMSControlStateManager()
        smart_tou = ToUSmartStrategy.load_smart_tou_settings()

        # ---------------------------------------------------
        # Load Smart TOU Config
        # ---------------------------------------------------

        price_source = smart_tou["price_source"]["selected_source_name"]
        refresh_interval_minutes = smart_tou["price_source"]["refresh_interval_minutes"]
        price_cap = smart_tou["caps_and_thresholds"]["price_cap_threshold"]
        price_floor = smart_tou["caps_and_thresholds"]["price_floor_threshold"]
        arbitrage_threshold = smart_tou["caps_and_thresholds"]["arbitrage_threshold"]
        rules = smart_tou["optimization_rule"]
        
        logger.info(f"{LogColor.BLUE} [TouSmartStrategy] Price Source = {price_source} {LogColor.RESET}")
        logger.info(f"{LogColor.BLUE} [TouSmartStrategy] Rules = {rules} {LogColor.RESET}")

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

        # ---------------------------------------------------
        # Read latest device data
        # ---------------------------------------------------
        try:
            device_data = resolver.get_latest_device_data(device_id)

            soc = device_data["SoC"]["value"]
            bess_power = float(device_data["ac_active_power"]["value"])
            current_tariff = 3# float(self.metadata_resolver.get_latest_tariff())

            logger.info(f"{LogColor.BLUE} [TouSmartStrategy] SoC: {soc}, Current Tariff: {current_tariff} {LogColor.RESET}")
        except Exception as e:
            logger.info(f"{LogColor.RED} Error loading live params: {e} {LogColor.RESET}")

        # ----------------------------
        # OUTPUT FLAGS
        # ----------------------------
        prevent_charging = False
        aggressively_charge = False
        block_discharging = False

        # ----------------------------
        # 1. PRICE CAP / FLOOR BEHAVIOUR
        # ----------------------------

        if current_tariff >= price_cap:
            prevent_charging = True

        if current_tariff <= price_floor:
            aggressively_charge = True

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
            usage_percent = (cycles_used_today / max_cycles_per_day) * 100
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
            "refresh_interval_minutes" : refresh_interval_minutes,
            "arbitrage_threshold" : arbitrage_threshold,
            "battery_capacity" : battery_capacity,
            "max_discharge_kw" : max_discharge_kw,
            "arbitrage_cfg" : arbitrage_cfg,
            "soc_cfg" : soc_cfg,
            "bess_power" : bess_power,
            "rules" : rules,
            "current_tariff" : current_tariff,
            "soc" : soc,
            "disable_low_price_charging" : disable_low_price_charging,
            "disable_arbitrage" : disable_arbitrage,
            "disable_aggrassive_charging" : disable_aggrassive_charging
        }


    @staticmethod
    def generate_bess_arbitrage_schedule(
        tariff_data,
        current_soc,
        bess_capacity_kwh,
        max_power_kw,
        config
    ):
        """
        tariff_data = [
            {"startTs": "...", "endTs": "...", "price": 3.4},
            {"startTs": "...", "endTs": "...", "price": 2.1},
            ...
        ]
        
        config = JSON config dictionary
        """

        price_floor = config["caps_and_thresholds"]["price_floor_threshold"]
        price_cap = config["caps_and_thresholds"]["price_cap_threshold"]
        arbitrage_threshold = config["caps_and_thresholds"]["arbitrage_threshold"]
        max_cycles = config["caps_and_thresholds"]["max_cycles_per_day"]

        aggressive_charge = config["optimization_rule"][
            "Aggressively_charge_when_price_below_price_floor_threshold"
        ]
        prevent_charge_high = config["optimization_rule"][
            "prevent_charging_when_price_above_price_cap_threshold"
        ]
        allow_discharge = config["optimization_rule"]["allow_discharge_when_profitable"]

        schedule = []

        # STEP 1: sort data by price
        sorted_by_price = sorted(tariff_data, key=lambda x: x["price"])

        # Cheapest slots for charging
        num_charge_slots = max_cycles
        cheap_slots = sorted_by_price[:num_charge_slots]

        # Highest slots for discharging
        expensive_slots = sorted_by_price[-num_charge_slots:]

        for slot in tariff_data:

            price = slot["price"]

            decision = "idle"
            power = 0

            # RULE 1: Forced charge when extremely low price
            if aggressive_charge and price <= price_floor:
                decision = "charge"
                power = max_power_kw

            # RULE 2: Forced discharge when extremely high price
            elif allow_discharge and price >= price_cap:
                decision = "discharge"
                power = max_power_kw

            # RULE 3: Cheap relative interval → schedule charge
            elif slot in cheap_slots and current_soc < 100:
                decision = "charge"
                power = max_power_kw

            # RULE 4: Expensive interval → schedule discharge
            elif slot in expensive_slots and current_soc > 0:
                decision = "discharge"
                power = max_power_kw

            # SOC-based sanity
            if current_soc <= 10:
                decision = "charge"
            elif current_soc >= 90:
                decision = "discharge"

            # Update SOC (simple assumption: interval = 1 hour)
            if decision == "charge":
                current_soc += (power / bess_capacity_kwh) * 100
                if current_soc > 100:
                    current_soc = 100

            elif decision == "discharge":
                current_soc -= (power / bess_capacity_kwh) * 100
                if current_soc < 0:
                    current_soc = 0

            schedule.append({
                "startTs": slot["startTs"],
                "endTs": slot["endTs"],
                "price": price,
                "decision": decision,
                "power_kw": power,
                "soc_after": current_soc
            })

        return schedule


    @staticmethod
    def load_smart_tou_settings(CONFIG_JSON = "controller_layer/strategies/strategy_configuration/SmartTou.json"):
        """
        Loads SmartTOU settings from SmartTou.json and returns a structured dict
        containing:
            - price source mode (resolved string)
            - refresh interval
            - caps & thresholds
            - optimization rules
        """
        path = DataPaths.resolve(CONFIG_JSON)

        if not path.exists():
            raise FileNotFoundError(f"{CONFIG_JSON} not found")

        with open(path, "r") as f:
            config = json.load(f)

        # -----------------------------
        # Price Source Section
        # -----------------------------
        ps = config.get("price_source", {})

        price_source_option_map = ps.get("price_source_options", {})
        price_source_default = ps.get("price_source_default", "0")

        # Resolve actual readable source name
        price_source_name = price_source_option_map.get(price_source_default, "unknown")

        refresh_interval_min = ps.get("refresh_interval_minutes", 60)

        # -----------------------------
        # Thresholds Section
        # -----------------------------
        caps = config.get("caps_and_thresholds", {})

        price_cap = caps.get("price_cap_threshold")
        price_floor = caps.get("price_floor_threshold")
        arbitrage_threshold = caps.get("arbitrage_threshold")
        max_cycles = caps.get("max_cycles_per_day")

        # -----------------------------
        # Optimization Rules Section
        # -----------------------------
        rules = config.get("optimization_rule", {})

        return {
            "price_source": {
                "selected_source_code": price_source_default,
                "selected_source_name": price_source_name,
                "refresh_interval_minutes": refresh_interval_min
            },
            "caps_and_thresholds": {
                "price_cap_threshold": price_cap,
                "price_floor_threshold": price_floor,
                "arbitrage_threshold": arbitrage_threshold,
                "max_cycles_per_day": max_cycles
            },
            "optimization_rule": rules
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

            logger.info(f"{LogColor.CYAN} [Smart TOU] Executing schedule for device {device_id} {LogColor.RESET}")

            # -------------------------------------------------
            # 0. Load BESS Decision 
            # -------------------------------------------------

            decision_config = await ToUSmartStrategy.bess_decision(device_id)

            (
                prevent_charging,
                block_discharging,
                backup_soc,
                min_soc,
                max_soc,
                cycle_used_today,
                refresh_interval_minutes,
                battery_capacity,
                max_discharge_kw,
                bess_power,
                current_tariff,
                soc
            ) = (
                decision_config["prevent_charging"],
                decision_config["block_discharging"],
                decision_config["backup_soc"],
                decision_config["min_soc"],
                decision_config["max_soc"],
                decision_config["cycles_used_today"],
                decision_config["refresh_interval_minutes"],
                decision_config["battery_capacity"],
                decision_config["max_discharge_kw"],
                decision_config["bess_power"],
                decision_config["current_tariff"],
                decision_config["soc"]
            )

            # ---------------------------------------
            # 1. Extract schedule fields
            # ---------------------------------------
            tariffs = schedule.get("tariff")
            durations_minutes = schedule.get("duration_minutes")
            startTs = schedule.get("startTs")
            endTs = schedule.get("endTs")
            power_kw = schedule.get("power")

            if not all([tariffs, durations_minutes, startTs, endTs]):
                logger.error(f"[Smart TOU] Invalid schedule payload: {schedule}")
                return

            # ---------------------------------------
            # 2. Get live values
            # ---------------------------------------
            device_data = resolver.get_latest_device_data(device_id)
            if not device_data:
                logger.error(f"[Smart TOU] No live data for {device_id}")
                return
            
            # soc = device_data["SoC"]["value"]

            # ---------------------------------------
            # 3. Smart decision-making
            # ---------------------------------------
            if signal == "discharge":
                # DISCHARGE LOGIC
                logger.info(f"{LogColor.CYAN} [Smart TOU] Discharge event triggered {LogColor.RESET}")

                if block_discharging:
                    logger.info("[Smart TOU] Discharging blocked by decision logic.")
                    return

                await TouDischargingSmartStrategy(
                    resolver=resolver,
                    controller_static=controller_static,
                    tariffs=tariffs,
                    durations_minutes=durations_minutes,
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
                logger.info(f"{LogColor.CYAN} [Smart TOU] Charge event triggered {LogColor.RESET}")

                if prevent_charging:
                    logger.info("[Smart TOU] Charging prevented by decision logic.")
                    return

                await smart_tou_schedule_charging(
                    resolver=resolver,
                    controller_static=controller_static,
                    soc=soc,    
                    power=power_kw,
                    current_tariff=current_tariff,
                    channel=channel,    
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    device_id=device_id,
                    battery_capacity=battery_capacity,
                    extra_info={
                        "strategy_id" : 1
                    }
                )
                

            logger.info("[Smart TOU] No action required. Conditions not met.")
        except Exception as e:
            logger.error(f"{LogColor.RED} [Smart TOU] Error executing schedule: {e} {LogColor.RESET}")
            return
