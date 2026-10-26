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

class ToUFlatStrategy:
    def __init__(self, resolver):
        self.resolver = LatestTelemetryResolver()
        self.controller_static = ControllerStaticResolver()
        self.metadata_resolver = MetadataResolver()
        self.strategy_config_path = DataPaths.resolve("controller_layer/strategies/strategy_configuration/FlatTou.json")
        self.ess_control_state_manager = EMSControlStateManager()
        self.strategy_flags_controller = StrategyFlagsController(
            ess_control_state_manager = self.ess_control_state_manager,
        )
        # Global registry
        self.running_flat_tou = {}

    async def execute(self, channel, server_ip, server_port, slave_id, device_id, signal):
        """
        Flat strategy execution: calculates how much energy to discharge per time slot based on tariff weighting.

        :param total_dischargeable_energy: Total energy (kWh) that can be discharged
        :param tariffs: List of tariff values per slot (e.g. [3.5, 2.5, 2.5])
        :param durations_minutes: List of durations (e.g. [60, 60, 60]) for each slot
        :param ramp_rate_kw_per_min: Ramp rate limit
        :param power_rating_kw: Max discharge power of BESS
        """
        now = datetime.now()
        logger.info(f"{LogColor.BLUE} {now} [TouFlatStrategy] Starting strategy execution... {LogColor.RESET}")

        strategy_id = 2
        strategy_name = "ToU Tariff"

        # -------------------------------------------------
        # Raise EMS Status Flag fot Strategy
        # -------------------------------------------------
        try:

            # await self.ess_control_state_manager.raise_flag("enable_tou_tariff_strategy", 1)
            # await self.ess_control_state_manager.raise_flag("enable_capped_dynamic_strategy", 0)
            # await self.ess_control_state_manager.raise_flag("enable_peak_shaving_strategy", 0)

            await self.strategy_flags_controller.raise_strategy_flags_once_per_day(
                strategy_name="flat_tou",
                flags={
                    "enable_tou_tariff_strategy": 1,
                    "enable_capped_dynamic_strategy": 0,
                    "enable_peak_shaving_strategy": 0,
                    "enable_basic_strategy": 0,
                }
            )

        except Exception as e:
            logger.info(f"{LogColor.RED} Error whilte raising EMS STATUS FLAG {e} {LogColor.RESET}")

        # -------------------------------------------------
        # Load BESS Decision
        # -------------------------------------------------   
        try:

            config = await ToUFlatStrategy.bess_decision(device_id)

            (
                prevent_charging,
                block_discharging,
                price_cfg,
                backup_soc,
                min_soc,
                max_soc,
                bess_power,
                battery_capacity,
                min_discharge_tariff,
                loss_percent,
                diff_tariff,
                chargeAH,
                current_tariff,
                soc,
                disable_low_price_charging,
                state,
                start_dt,
                end_dt,
                price,
                discharge_during_peak,
                allow_charging_during_off_peak,
                allow_grid_charging_if_price_below,
                allow_charging_only_from_solar
            ) = (
                config["prevent_charging"],
                config["block_discharging"],
                config["price_cfg"],
                config["backup_soc"],
                config["min_soc"],
                config["max_soc"],
                config["bess_power"],
                config["battery_capacity"],
                config["min_discharge_tariff"],
                config["loss_percent"],
                config["diff_tariff"],
                config["chargeAH"],
                config["current_tariff"],
                config["soc"],
                config["disable_low_price_charging"],
                config["tou_state"],
                config["start_ts"],
                config["end_ts"],
                config["price"],
                config["discharge_during_peak"],
                config["allow_charging_during_off_peak"],
                config["allow_grid_charging_if_price_below"],
                config["allow_charging_only_from_solar"]
            )
        
        except Exception as e:
            logger.info(f"{LogColor.RED} Error loading live params: {e} {LogColor.RESET}")
            return
        
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
        try:

            if not price_cfg["disable_peak_shaving"]:
                await PeakShavingStrategy.peak_shaving_as_constraint(
                    device_id=device_id,
                    poi_meter_id="plant_1_poi_meter_1",
                    channel=channel,
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    block_discharging=block_discharging
                )
                logger.info(f"{LogColor.BLUE} Peak Shaving as Constrained completed in Flat TOU Strategy {LogColor.RESET}")
            else:
                logger.info(f"{LogColor.BLUE} Peak Shaving Disabled in Flat TOU Strategy {LogColor.RESET}")

        except Exception as e:
            logger.info(f"{LogColor.RED} Error in Peak Shaving Constraint: {e} {LogColor.RESET}")

        # Unique execution key
        key = f"{device_id}_{state}"

        # Check if already running
        if key in self.running_flat_tou:
            entry = self.running_flat_tou[key]

            # If same window is already running → skip
            if entry["startTs"] == start_dt and entry["endTs"] == end_dt:
                logger.info(f"[FLAT-TOU] {state.upper()} already running for {device_id}. Skipping.")
                return

            # If old entry expired, clean it
            if datetime.now() > entry["expires"]:
                del self.running_flat_tou[key]
            else:
                logger.info(f"[FLAT-TOU] {state.upper()} job active. Skipping.")
                return

        if state == "peak" and discharge_during_peak == True and not block_discharging:
            logger.info(f'{LogColor.BLUE} {now}  IM IN PEAK FLAT Discharge {LogColor.RESET}')

            # Register the running strategy
            self.running_flat_tou[key] = {
                "state": state,
                "startTs": start_dt,
                "endTs": end_dt,
                "job_started": True,
                "expires": end_dt + timedelta(minutes=2)  # cleanup buffer
            }

            # Execute peak discharging strategy
            result = await TouDischargingFlatStrategy(
                resolver=self.resolver,
                controller_static=self.controller_static,
                tariffs=[price],
                durations_minutes=[(end_dt - start_dt).seconds // 60],
                startTs=start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                endTs=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                device_id=device_id,
                channel=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id
            )

            # Clean running state after completion
            key = f"{device_id}_peak"      # or pass state argument
            if key in self.running_flat_tou:
                del self.running_flat_tou[key]

            pool = await get_async_connection()
            async with pool.acquire() as conn:
                await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
            logger.info("FLAT TOU discharge completed. Registry cleaned.")
            return result

        
        if state == "off_peak" and allow_charging_during_off_peak and not prevent_charging:
            logger.info(f"{LogColor.BLUE} off_peak flat TOU {LogColor.RESET}")

            # Register the running strategy
            self.running_flat_tou[key] = {
                "state": state,
                "startTs": start_dt,
                "endTs": end_dt,
                "job_started": True,
                "expires": end_dt + timedelta(minutes=2)  # cleanup buffer
            }

            result = await flat_tou_charging(
                resolver=self.resolver,
                controller_static=self.controller_static,
                soc=soc, 
                power=chargeAH, 
                current_tariff=current_tariff,
                channel=channel, 
                server_ip=server_ip, 
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
                battery_capacity=battery_capacity
            )

            # Clean running state after completion
            key = f"{device_id}_off_peak"      # or pass state argument
            if key in self.running_flat_tou:
                del self.running_flat_tou[key]

            pool = await get_async_connection()
            async with pool.acquire() as conn:
                await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
            logger.info("FLAT TOU charge completed. Registry cleaned.")
            return result
        
        if config is None and current_tariff <= allow_grid_charging_if_price_below and not disable_low_price_charging:

            logger.info(f"{LogColor.BLUE} Grid Charging Flat TOU {LogColor.RESET}")

            result = await flat_tou_charging(
                resolver=self.resolver,
                controller_static=self.controller_static,
                soc=soc, 
                power=chargeAH, 
                current_tariff=current_tariff,
                channel=channel, 
                server_ip=server_ip, 
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
                battery_capacity=battery_capacity
            )

            pool = await get_async_connection()
            async with pool.acquire() as conn:
                await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
            logger.info("FLAT TOU charge completed. Registry cleaned.")
            return result
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
                battery_capacity=battery_capacity
            )

            pool = await get_async_connection()
            async with pool.acquire() as conn:
                await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
            logger.info("FLAT TOU charge completed. Registry cleaned.")
            return result
      
        
        logger.info(f"[TouFlatStrategy] Strategy execution completed.")
        return
    
    @staticmethod
    def get_current_tou_state(tou_config: dict):
        """
        Determine current state (off_peak, mid_peak, peak) and return
        the state info with startTs, endTs, price, and optimization rules.
        """
        now = datetime.now()
        current_time = now.time()
        
        setting = tou_config.get("setting", {})
        optimization = tou_config.get("optimization_rule", {})

        def time_in_range(start_str, end_str, check_time):
            start = datetime.strptime(start_str, "%H:%M").time()
            end = datetime.strptime(end_str, "%H:%M").time()

            # Handle ranges that cross midnight
            if start <= end:
                return start <= check_time <= end
            else:
                return check_time >= start or check_time <= end

        result = {}

        # Iterate each TOU type (off_peak, mid_peak, peak)
        for state_name, ranges in setting.items():
            for r in ranges:
                if time_in_range(r["start_time"], r["end_time"], current_time):
                    # Build today's datetime timestamps
                    start_dt = datetime.combine(now.date(), datetime.strptime(r["start_time"], "%H:%M").time())
                    end_dt = datetime.combine(now.date(), datetime.strptime(r["end_time"], "%H:%M").time())

                    result = {
                        "state": state_name,
                        "startTs": start_dt,
                        "endTs": end_dt,
                        "price": r["price"],
                        "optimization_rule": optimization
                    }
                    return result

        return None  # If nothing matches (should not normally happen)

    @staticmethod
    async def check_inv_is_available() -> bool:
        """
        IF any of the Inverter in curtailmenet it returns True else False
        """

        device_config = DeviceConfigHelper()
        resolver = LatestTelemetryResolver() 

        inv_ids = device_config.get_device_ids_by_device_type("inverter")

        for id in inv_ids:
            try:
                data = await resolver.get_latest_device_data(id)
                curtailment_mode = data.get("ac_active_power", {}).get("value", 0)
                if curtailment_mode != 0:
                    return True
            except Exception as e:
                logger.error(f"Error checking curtailment for {id}: {e}")
                continue

        return False

    @staticmethod
    def simulate_tou_day(
        controller_device_id: str
    ):
        """
        Simulates one full ToU day for BESS.

        Returns:
            dict with timeline, final SOC, revenue, energy stats.
        """

        # ============================================================
        # 1. Load Peak Shaving Settings
        # ============================================================
        cfg_path = DataPaths.resolve("controller_layer/strategies/strategy_configuration/FlatTou.json")
        with open(cfg_path) as f:
            tou_json = json.load(f)

        # ============================================================
        # 2. Live BESS Data
        # ============================================================
        resolver = LatestTelemetryResolver()
        device_data = resolver.get_latest_device_data(controller_device_id)
        initial_soc = float(device_data["SoC"]["value"])

        # ============================================================
        # 3. Load BESS Config
        # ============================================================
        controller_static = ControllerStaticResolver()
        bess = load_bess_params_setting()
        min_soc      = bess["min_soc"]
        max_soc      = bess["max_soc"]
        battery_capacity = controller_static.get("battery_capacity")
        efficiency = controller_static.get("efficiency")
        efficiency = efficiency if efficiency <= 1 else efficiency * 0.01
        max_charge_kw = bess.get("max_charge_kw")
        max_discharge_kw = bess.get("max_discharge_kw")

        result = {
            "initial_soc": initial_soc,
            "final_soc": None,
            "total_charge_kwh": 0,
            "total_discharge_kwh": 0,
            "energy_cost": 0,
            "energy_revenue": 0,
            "net_profit": 0,
            "timeline": []
        }

        soc = initial_soc
        reserve_soc = tou_json["optimization_rule"]["reserve_soc"]

        settings = tou_json["setting"]
        rules = tou_json["optimization_rule"]

        # Combine all windows into single list
        windows = []
        for state in ["mid_peak", "peak", "off_peak"]:
            for block in settings.get(state, []):
                windows.append({
                    "state": state,
                    "start": block["start_time"],
                    "end": block["end_time"],
                    "price": block["price"]
                })

        # Helper to compute hours between HH:MM
        def hours_between(start_str, end_str):
            fmt = "%H:%M"
            start = datetime.strptime(start_str, fmt)
            end = datetime.strptime(end_str, fmt)
            if end < start:
                end += timedelta(days=1)
            return (end - start).seconds / 3600

        for window in windows:

            duration = hours_between(window["start"], window["end"])
            price = window["price"]
            state = window["state"]

            soc_before = soc
            energy_charged = 0
            energy_discharged = 0
            action = "idle"

            available_energy = (soc - reserve_soc) / 100 * battery_capacity
            remaining_capacity = (100 - soc) / 100 * battery_capacity

            # ---------------------------
            # DISCHARGE DURING PEAK
            # ---------------------------
            if state == "peak" and rules["discharging_during_peak"]:

                max_possible_discharge = min(
                    max_discharge_kw * duration,
                    available_energy
                )

                if max_possible_discharge > 0:
                    energy_discharged = max_possible_discharge
                    soc -= (energy_discharged / battery_capacity) * 100
                    result["total_discharge_kwh"] += energy_discharged
                    result["energy_revenue"] += energy_discharged * price * efficiency
                    action = "discharge"

            # ---------------------------
            # CHARGE DURING OFF PEAK
            # ---------------------------
            elif state == "off_peak" and rules["allow_charging_during_off_peak"]:

                if rules["allow_grid_charging_if_price_below"] is not None:
                    if price <= rules["allow_grid_charging_if_price_below"]:

                        max_possible_charge = min(
                            max_charge_kw * duration,
                            remaining_capacity
                        )

                        if max_possible_charge > 0:
                            energy_charged = max_possible_charge
                            soc += (energy_charged / battery_capacity) * 100
                            result["total_charge_kwh"] += energy_charged
                            result["energy_cost"] += energy_charged * price / efficiency
                            action = "charge"

            # ---------------------------
            # RECORD WINDOW
            # ---------------------------
            result["timeline"].append({
                "state": state,
                "start": window["start"],
                "end": window["end"],
                "price": price,
                "action": action,
                "duration_hours": duration,
                "energy_charged_kwh": round(energy_charged, 2),
                "energy_discharged_kwh": round(energy_discharged, 2),
                "soc_before_percent": round(soc_before, 2),
                "soc_after_percent": round(soc, 2)
            })

        result["final_soc"] = round(soc, 2)
        result["net_profit"] = round(
            result["energy_revenue"] - result["energy_cost"], 2
        )

        return result
    
    @staticmethod
    async def bess_decision(device_id: str):
        """
        Static helper to centrally load all flags, thresholds and config
        required by Flat ToU Strategy in a single call.
        
        Returns a structured dictionary.
        """

        result = {}

        # ---------------------------------------------------
        # 1. Load BESS Params
        # ---------------------------------------------------
        bess = load_bess_params_setting()
        result["min_soc"] = bess["min_soc"]
        result["max_soc"] = bess["max_soc"]
        result["max_discharge_kw"] = bess.get("max_discharge_kw")
        result["backup_soc"] = bess.get("backup_soc")

        # ---------------------------------------------------
        # 2. Load Cycle Config & Daily Usage
        # ---------------------------------------------------
        cycle_cfg = load_cycle_config()
        result["max_cycles_per_day"] = cycle_cfg["max_cycles_per_day"]
        result["soc_cfg"] = cycle_cfg["soc_safety_management"]
        result["lockout_cfg"] = cycle_cfg["lockout_behaviour"]
        result["price_cfg"] = cycle_cfg["price_logic_overrides"]
        result["notify_cfg"] = cycle_cfg["notifications"]

        result["cycle_used_today"] = await get_today_cycles()

        result["disable_low_price_charging"] = result["price_cfg"]["disable_low_price_charging"]

        # ---------------------------------------------------
        # 3. Get latest device values
        # ---------------------------------------------------
        resolver = LatestTelemetryResolver()
        data = resolver.get_latest_device_data(device_id)

        try:
            result["soc"] = float(data["SoC"]["value"])
            result["bess_power"] = float(data["ac_active_power"]["value"])
            result["chargeAH"] = float(data["ChargeAH"]["value"])
        except:
            result["soc"] = 50  # fallback

        # tariff metadata
        metadata = MetadataResolver()
        try:
            stats = metadata.get_discharge_tariff_stats()
            result["min_discharge_tariff"] = stats.get("min", 0)
            result["current_tariff"] = float(metadata.get_latest_tariff())
        except:
            result["current_tariff"] = 10

        # ---------------------------------------------------
        # 4. Controller Static Params
        # ---------------------------------------------------
        controller_static = ControllerStaticResolver()

        result["battery_capacity"] = controller_static.get("battery_capacity")
        eff = controller_static.get("efficiency")
        result["loss_percent"] = (100 - eff) / 100
        result["diff_tariff"] = controller_static.get("diff_tariff")

        # ---------------------------------------------------
        # 5. Strategy JSON (Flat ToU)
        # ---------------------------------------------------
        cfg_path = DataPaths.resolve("controller_layer/strategies/strategy_configuration/FlatTou.json")
        with open(cfg_path) as f:
            tou_data = json.load(f)

        info = ToUFlatStrategy.get_current_tou_state(tou_data)

        # timestamp and state
        result.update({
            "tou_state": info["state"],
            "start_ts": info["startTs"],
            "end_ts": info["endTs"],
            "price" : info["price"]
        })

        # optimization flags
        opt = info["optimization_rule"]
        result.update({
            "discharge_during_peak": opt.get("discharging_during_peak"),
            "allow_charging_during_off_peak": opt.get("allow_charging_during_off_peak"),
            "allow_charging_only_from_solar": opt.get("allow_charging_only_from_solar"),
            "allow_grid_charging_if_price_below": opt.get("allow_grid_charging_if_price_below"),
            "reserve_soc": opt.get("reserve_soc"),
        })

        # ---------------------------------------------------
        # 6. Final computed logic flags (safety + lockout)
        # ---------------------------------------------------
        result["block_discharging"] = False
        result["prevent_charging"] = False
        result["aggressively_charge"] = False

        # MAX CYCLES lockout
        if result["cycle_used_today"] >= result["max_cycles_per_day"]:
            result["block_discharging"] = True
            if result["lockout_cfg"]["block_economic_charging"]:
                result["prevent_charging"] = True
        
        # Charging only from Solar
        if result["allow_charging_only_from_solar"]:
            is_inv_avail = await ToUFlatStrategy.check_inv_is_available
            if is_inv_avail:
                result["prevent_charging"] = False
            else:
                result["prevent_charging"] = True
        else: 
            result["prevent_charging"] = True

        # SOC safety
        if result["soc"] < result["min_soc"]:
            if result["lockout_cfg"]["allow_safety_charging"]:
                result["prevent_charging"] = False
                result["aggressively_charge"] = True
                result["block_discharging"] = True

        # Reserve threshold
        if result["reserve_soc"] and result["soc"] < result["reserve_soc"]:
            if result["lockout_cfg"]["allow_reserve_charging"]:
                result["prevent_charging"] = False

        # RETURN FINAL MASTER CONFIG
        return result

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

            logger.info(f"{LogColor.CYAN} [Flat TOU] Executing schedule for device {device_id} {LogColor.RESET}")

            # -------------------------------------------------
            # 0. Load BESS Decision 
            # -------------------------------------------------

            try:

                config = await ToUFlatStrategy.bess_decision(device_id)

                (
                    prevent_charging,
                    block_discharging,
                    backup_soc,
                    min_soc,
                    max_soc,
                    bess_power,
                    battery_capacity,
                    current_tariff,
                    soc
                ) = (
                    config["prevent_charging"],
                    config["block_discharging"],
                    config["backup_soc"],
                    config["min_soc"],
                    config["max_soc"],
                    config["bess_power"],
                    config["battery_capacity"],
                    config["current_tariff"],
                    config["soc"]
                )
            
            except Exception as e:
                logger.info(f"{LogColor.RED} Error loading live params: {e} {LogColor.RESET}")
                return

            # ---------------------------------------
            # 1. Extract schedule fields
            # ---------------------------------------
            tariffs = schedule.get("tariff")
            durations_minutes = schedule.get("duration_minutes")
            startTs = schedule.get("startTs")
            endTs = schedule.get("endTs")
            power_kw = schedule.get("power")

            if not all([tariffs, durations_minutes, startTs, endTs]):
                logger.error(f"[Flat TOU] Invalid schedule payload: {schedule}")
                return

            # ---------------------------------------
            # 2. Get live values
            # ---------------------------------------
            device_data = resolver.get_latest_device_data(device_id)
            if not device_data:
                logger.error(f"[Flat TOU] No live data for {device_id}")
                return
            
            # soc = device_data["SoC"]["value"]

            # ---------------------------------------
            # 3. Flat decision-making
            # ---------------------------------------
            if signal == "discharge":
                # DISCHARGE LOGIC
                logger.info(f"{LogColor.CYAN} [Flat TOU] Discharge event triggered {LogColor.RESET}")

                if block_discharging:
                    logger.info("[Flat TOU] Discharging blocked by decision logic.")
                    return

                await TouDischargingFlatStrategy(
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
                logger.info(f"{LogColor.CYAN} [Flat TOU] Charge event triggered {LogColor.RESET}")

                if prevent_charging:
                    logger.info("[Flat TOU] Charging prevented by decision logic.")
                    return

                await flat_tou_schedule_charging(
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
                        "strategy_id" : 2
                    }
                )

            logger.info("[Flat TOU] No action required. Conditions not met.")
        except Exception as e:
            logger.error(f"{LogColor.RED} [Flat TOU] Error executing schedule: {e} {LogColor.RESET}")
            return

