from datetime import datetime, timezone, timedelta
import logging
from controller_layer.strategies.classes.common.PeakShavingStrategyType7 import bess_decision as bess_decision7
from controller_layer.strategies.dg_control import DGStartControl, DGStopControl
from controller_layer.cloud_detection.detect import CloudDetection
from controller_layer.strategies.grif_forming_following_control import gfl_gfm_mode_selection, smart_mode_controller
from controller_layer.strategies.tags import Tags
from controller_layer.strategies.charging_strategies import PeakShavingChargingStrategyType7, PeakShavingChargingStrategy
from controller_layer.strategies.dicharging_strategies import PeakShavingDischargeStrategyType7, PeakShavingDischargeStrategy
from statics.controller_resolver import ControllerStaticResolver
from latest_telemetry_resolver import LatestTelemetryResolver
from ...writers import EMSControlStateManager
from ..cycle_management import get_today_cycles
from apscheduler.schedulers.background import BackgroundScheduler
from LogColor import LogColor
import pytz
import json
from utils.loop_manager import get_main_loop

from model.db import get_async_connection

from ..load_bess_params_setting import get_c_rate, load_bess_params_setting
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

class PeakShavingStrategy:
    def __init__(self, resolver):
        self.resolver = LatestTelemetryResolver()
        self.controller_static = ControllerStaticResolver()
        self.ess_control_state_manager = EMSControlStateManager()
        self.strategy_flags_controller = StrategyFlagsController(
            ess_control_state_manager = self.ess_control_state_manager,
        )
        # self.metadata_resolver = MetadataResolver()

    async def execute(self, channel, server_ip, server_port, slave_id, device_id, signal):
        """
        Peak Shaving strategy execution: calculates how much energy to charge / discharge per time slot based on Load.

        """

        logger.info(f"{LogColor.MAGENTA} Starting [PeakShavingStrategy] executing... {LogColor.RESET}")

        strategy_id = 3
        strategy_name = "Peak Shaving"

        # -------------------------------------------------
        # Raise EMS Status Flag fot Strategy
        # -------------------------------------------------
        try:

            # await self.ess_control_state_manager.raise_flag("enable_peak_shaving_strategy", 1)
            # await self.ess_control_state_manager.raise_flag("enable_tou_tariff_strategy", 0)
            # await self.ess_control_state_manager.raise_flag("enable_capped_dynamic_strategy", 0)

            await self.strategy_flags_controller.raise_strategy_flags_once_per_day(
                strategy_name="flat_tou",
                flags={
                    "enable_tou_tariff_strategy": 0,
                    "enable_capped_dynamic_strategy": 0,
                    "enable_peak_shaving_strategy": 1,
                    "enable_basic_strategy": 0,
                }
            ) 

        except Exception as e:
            logger.info(f"{LogColor.RED} Error whilte raising EMS STATUS FLAG {e} {LogColor.RESET}")

        # -------------------------------------------------
        # Load BESS Decision
        # -------------------------------------------------
        poi_meter_id = "plant_1_poi_meter_1"
        try:
            
            config = await PeakShavingStrategy.bess_decision(device_id, poi_meter_id)

            (
                prevent_charging,
                block_discharging,
                hard_limit_kw,
                soft_limit_kw,
                threshold_kw,
                max_discharge_kw,
                max_usable_kw,
                mode,
                forecast_source,
                current_load_kw,
                soc,
                max_soc,
                enforce_soft,
                force_hard,
                next_peak_window,
                pv_power_kw,
                bess_power_kw,
                cloud_details,
            ) = (
                config["prevent_charging"],
                config["block_discharging"],
                config["hard_limit_kw"],
                config["soft_limit_kw"],
                config["threshold_kw"],
                config["max_discharge_kw"],
                config["max_usable_kw"],
                config["mode"],
                config["forecast_source"],
                config["current_load_kw"],
                config["soc"],
                config["max_soc"],
                config["enforce_soft"],
                config["force_hard"],
                config["next_peak_window"],
                config["pv_power_kw"],
                config["bess_power_kw"],
                config["cloud_details"],
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
                exec_log = ExecutionLog(strategy_id=strategy_id, started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), input_snapshot={"live": {"soc" : soc, "current_load" : current_load_kw}, "config": {}, "flags": {}})
                exec_id = await create_execution_log(conn, exec_log)
            await pool.close()
        except Exception as e:
            logger.info(f"{LogColor.RED} Error While Creating Log Exec. {e} {LogColor.RESET}")


        # -----------------------------------------------------------------------------
        # ***************************** EQUATIONS *************************************
        # -----------------------------------------------------------------------------

        grid_power = current_load_kw - pv_power_kw - bess_power_kw

        pv_surplus_power_kw = pv_power_kw - current_load_kw

        # -----------------------------------------------------------------------------
        # Cloud Detection
        # -----------------------------------------------------------------------------

        if cloud_details.get("is_cloud_detected"):
            logger.info(f"{LogColor.YELLOW} Cloud Detected! Setpoint: {cloud_details.get('setpoint_kw', 0)} kW | Current Stage: {cloud_details.get('current_stage')} | Dispatch Percent: {cloud_details.get('dispatch_percent')}% {LogColor.RESET}")
            grid_power = grid_power - (cloud_details.get('setpoint_kw', 0) * cloud_details.get('dispatch_percent'))
        
        # ------------------------------------------------------------------------------
        # ***************************** CONTROLLED PEAK SHAVING ************************
        # ------------------------------------------------------------------------------

        # threshold_kw => Peak Shaving Threshold
        # Soft Limit => Safe Limit
        # Hard Limit => Contractual Limit

        logger.info(
            f"Grid Power: {grid_power} | "
            f"Current Load: {current_load_kw} | "
            f"PV Power: {pv_power_kw} | "
            f"BESS Power: {bess_power_kw} | "
            f"PV Surplus: {pv_surplus_power_kw} | "
            f"Threshold kW: {threshold_kw} | "
            f"Soft Limit kW: {soft_limit_kw} | "
            f"Block Discharge: {block_discharging} | "
        )

        result = None

        # Set Target Limit kW
        target_limit = None

        # Determine which limit is active
        if threshold_kw < grid_power < soft_limit_kw:
            target_limit = threshold_kw

        elif soft_limit_kw < grid_power < hard_limit_kw:
            if enforce_soft:
                target_limit = soft_limit_kw
            else:
                target_limit = hard_limit_kw

        elif grid_power > hard_limit_kw:
            target_limit = hard_limit_kw

        if target_limit is None:
        
            if pv_surplus_power_kw > 0:
            
                if soc >= max_soc:
                    logger.info(
                        f"{LogColor.MAGENTA}PV surplus available but battery is full. "
                        f"Curtailing PV.{LogColor.RESET}"
                    )

                    result = await PeakShavingStrategy.pv_power_curtailment(
                        pv_power_kw,
                        pv_surplus_power_kw,
                        grid_power,
                        current_load_kw
                    )

                elif not prevent_charging:

                    logger.info(
                        f"{LogColor.MAGENTA}Charging battery from PV surplus "
                        f"({pv_surplus_power_kw:.2f} kW).{LogColor.RESET}"
                    )

                    result = await PeakShavingChargingStrategy(
                        resolver=self.resolver,
                        controller_static=self.controller_static,
                        device_id=device_id,
                        channel=channel,
                        server_ip=server_ip,
                        server_port=server_port,
                        slave_id=slave_id,
                        available_kw=pv_surplus_power_kw,
                        max_soc=max_soc
                    )

        else :

            required_kw = grid_power - target_limit
            discharge_kw = min(required_kw, max_usable_kw, max_discharge_kw)

            logger.info(
                f"(Required={required_kw:.2f}, Available={discharge_kw:.2f}) "
            )

            # Battery cannot support required load
            # if discharge_kw < required_kw:
            #     block_discharging = True

                
            # main Execution
            if not block_discharging:

                logger.info(
                    f"Peak shaving using BESS | "
                    f"Grid={grid_power:.2f}, "
                    f"Target={target_limit:.2f}, "
                    f"Required={required_kw:.2f}, "
                    f"Discharge={discharge_kw:.2f}"
                )

                result = await PeakShavingDischargeStrategy(
                    resolver=self.resolver,
                    controller_static=self.controller_static,
                    server_ip=server_ip,
                    server_port=server_port,
                    channel=channel,
                    slave_id=slave_id,
                    device_id=device_id,
                    excess_load=discharge_kw,
                    target_limit=target_limit,
                    poi_meter_id=poi_meter_id,
                    extra_info={"force_hard": force_hard}
                )

        # Store result into DB
        pool = await get_async_connection()
        async with pool.acquire() as conn:
            await finish_execution_log(
                conn,
                exec_id,
                finished_at=datetime.now(),
                decision_snapshot=result if result else {"text": "Peak Shaving is normal. No action. Strategy execution completed"},
                error_text=""
            )
        await pool.close()

        logger.info("[PeakShavingStrategyType] Strategy execution completed.")
        return


        # -----------------------------------------------------------------------
        # ***************************** PREDICTIVE PEAK SHAVING ******************
        # -----------------------------------------------------------------------
        # if mode == "predective_peak_shaving":

        #     logger.info(f"{LogColor.YELLOW} Predictive Peak Shaving Active | Source = {forecast_source} {LogColor.RESET}")

        #     # Example: AI forecast pull (you can plug real API)
        #     predicted_load = None

        #     if forecast_source == "ai_forecast":
        #         predicted_load = self.forecasting_engine.get_predicted_peak()  # <--- your method needed
        #     elif forecast_source == "manual_input":
        #         predicted_load = self.controller_static.get("manual_peak_load")
        #     else:
        #         logger.info("No forecast available.")
        #         return

        #     logger.info(f"{LogColor.CYAN} Predicted Load: {predicted_load} kW | Threshold: {threshold_kw} kW {LogColor.RESET}")

        #     if predicted_load and predicted_load > threshold_kw and not block_discharging:
        #         required_kw = predicted_load - threshold_kw
        #         discharge_kw = min(required_kw, max_bess_discharge_kw, max_discharge_kw)

        #         logger.info(f"{LogColor.RED} Predicted peak high → Pre-discharge {discharge_kw:.2f} kW {LogColor.RESET}")

        #         result = await PeakShavingDischargeStrategy(
        #             resolver=self.resolver,
        #             controller_static=self.controller_static,
        #             server_ip=server_ip,
        #             server_port=server_port,
        #             channel=channel,
        #             slave_id=slave_id,
        #             device_id=device_id,
        #             excess_load=discharge_kw,
        #             poi_meter_id=poi_meter_id
        #         )
        #         pool = await get_async_connection()
        #         async with pool.acquire() as conn:
        #             await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
        #         logger.info(f"[PeakShavingStrategy] Strategy execution completed.")
        #         return result

        #     logger.info(f"{LogColor.GREEN} Predicted load normal. No action. {LogColor.RESET}")
        #     pool = await get_async_connection()
        #     async with pool.acquire() as conn:
        #         await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot={"text": "Predicted load normal. No action."}, error_text="")
        #     logger.info(f"[PeakShavingStrategy] Strategy execution completed.")
        #     return result
    
    @staticmethod
    async def pv_power_curtailment(
        pv_power_kw: float,
        pv_surplus_power_kw: float,
        grid_power: float,
        current_load_kw: float
    ):
        """
            This Function is responsible fot PV Curtailment.
            Condition: Load Power is less then PV Power but battery is at MAX. Action Curtailment of PV
        """

        logger.info(f"{LogColor.MAGENTA} [PV Curtailment] in execution... {LogColor.RESET}")

        grid_threshold_kw = -150
        pv_setpoint = pv_power_kw
        if grid_power < grid_threshold_kw:
            pv_setpoint = current_load_kw + (grid_threshold_kw * -1)
        

        return {}

    @staticmethod
    def load_peak_shaving_settings(json_path="controller_layer/strategies/strategy_configuration/PeakShaving.json") -> dict:
        """
        Load peak shaving settings and return resolved values.
        
        Returns:
            dict containing:
                - peak_shaving_mode (string)
                - forcast_source (string)
                - threshold_kw
                - reserve_soc_percent
                - max_discharge_power_kw
                - advanced_settings (dict)
        """
        path = DataPaths.resolve(json_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {json_path}")

        with open(path, "r") as f:
            data = json.load(f)

        # Resolve mode name
        mode_key = data.get("peak_shaving_mode")
        mode = data.get("peak_shaving_mode_option", {}).get(mode_key)

        # Resolve forecast source
        source_key = data.get("forcast_source")
        source = data.get("forcast_source_option", {}).get(source_key)

        # Resolve Load Windows
        windows = data.get("windows", {})
        current_window = PeakShavingStrategy.get_current_load_window_state(windows)
        next_peak_window = PeakShavingStrategy.get_next_peak_window_state(windows)
        next_load_window = PeakShavingStrategy.get_next_load_window(windows)

        return {
            "peak_shaving_mode": mode,
            "peak_shaving_mode_key": mode_key,
            "forcast_source": source,
            "forcast_source_key": source_key,
            "threshold_kw": data.get("peak_shaving_threshold_kw"),
            "reserve_soc_percent": data.get("reserve_soc_percent"),
            "max_discharge_power_kw": data.get("max_discharge_power_kw"),
            "advanced_settings": data.get("advanced_settings", {}),
            "load_windows" : windows,
            "current_window" : current_window,
            "next_peak_window" : next_peak_window,
            "next_load_window" : next_load_window
        }

    @staticmethod
    def get_next_load_window(windows: dict):
        """
        Returns next upcoming load window (off_peak, mid_peak, peak).
        """

        now = datetime.now()
        today = now.date()
        next_window = None

        for state_name, ranges in windows.items():
            for r in ranges:
                start_time = datetime.strptime(r["start_time"], "%H:%M").time()
                end_time = datetime.strptime(r["end_time"], "%H:%M").time()

                start_dt = datetime.combine(today, start_time)
                end_dt = datetime.combine(today, end_time)

                # Handle midnight crossing
                if start_time > end_time:
                    end_dt += timedelta(days=1)

                # If already passed → move to tomorrow
                if start_dt <= now:
                    start_dt += timedelta(days=1)
                    end_dt += timedelta(days=1)

                if not next_window or start_dt < next_window["startTs"]:
                    next_window = {
                        "state": state_name,
                        "startTs": start_dt,
                        "endTs": end_dt,
                        "load": r["load"]
                    }

        return next_window
    
    @staticmethod
    def get_next_peak_window_state(windows: dict):
        """
        Find the next upcoming peak window.
        Returns state info with startTs, endTs, load.
        """

        now = datetime.now()
        today = now.date()
        next_peak = None

        peak_ranges = windows.get("peak", [])

        for r in peak_ranges:
            start_time = datetime.strptime(r["start_time"], "%H:%M").time()
            end_time = datetime.strptime(r["end_time"], "%H:%M").time()

            start_dt = datetime.combine(today, start_time)
            end_dt = datetime.combine(today, end_time)

            # Handle midnight crossing
            if start_time > end_time:
                end_dt += timedelta(days=1)

            # If already passed today → move to tomorrow
            if start_dt <= now:
                start_dt += timedelta(days=1)
                end_dt += timedelta(days=1)

            # Keep the earliest upcoming peak
            if not next_peak or start_dt < next_peak["startTs"]:
                next_peak = {
                    "state": "peak",
                    "startTs": start_dt,
                    "endTs": end_dt,
                    "load": r["load"]
                }

        return next_peak
    
    @staticmethod
    def get_current_load_window_state(windows: dict):
        """
        Determine current state (off_peak, mid_peak, peak) and return
        the state info with startTs, endTs, price, and optimization rules.
        """
        now = datetime.now()
        current_time = now.time()
        
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
        for state_name, ranges in windows.items():
            for r in ranges:
                if time_in_range(r["start_time"], r["end_time"], current_time):
                    # Build today's datetime timestamps
                    start_dt = datetime.combine(now.date(), datetime.strptime(r["start_time"], "%H:%M").time())
                    end_dt = datetime.combine(now.date(), datetime.strptime(r["end_time"], "%H:%M").time())

                    result = {
                        "state": state_name,
                        "startTs": start_dt,
                        "endTs": end_dt,
                        "load": r["load"]
                    }
                    return result

        return None  # If nothing matches (should not normally happen)
  
    @staticmethod
    async def bess_decision(device_id: str, poi_meter_id: str):
        """
        CENTRALIZED DECISION ENGINE
        Reads configs + live data + cycle data and returns one decision dict.
        """

        result = {}

        # ============================================================
        # 1. Load Peak Shaving Settings
        # ============================================================
        cfg = PeakShavingStrategy.load_peak_shaving_settings()

        result["mode"]                  = cfg["peak_shaving_mode"]
        result["threshold_kw"]          = cfg["threshold_kw"]
        result["forecast_source"]       = cfg["forcast_source"]

        result["load_windows"]          = cfg["load_windows"]
        result["current_window"]        = cfg["current_window"]
        result["next_peak_window"]      = cfg["next_peak_window"]
        result["next_load_window"]      = cfg["next_load_window"]
        
        adv = cfg["advanced_settings"]

        result["soft_limit_kw"]         = adv["soft_limit_kw"]
        result["hard_limit_kw"]         = adv["hard_limit_kw"]
        result["enforce_soft"]          = adv["maintain_load_under_soft_limit_using_bess"]
        result["force_hard"]            = adv["force_control_to_stay_under_hard_limit"]

        # ============================================================
        # 2. Read POI Load (Always needed)
        # ============================================================
        resolver = LatestTelemetryResolver()
        poi_data = resolver.get_latest_device_data(poi_meter_id)
        result["current_load_kw"] = float(poi_data[Tags.Load]["value"])
        result["pv_power_kw"] = float(poi_data[Tags.PV_POWER]["value"])

        # ============================================================
        # 3. Load BESS Config
        # ============================================================
        controller_static = ControllerStaticResolver()
        bess = load_bess_params_setting()
        result["min_soc"]      = bess["min_soc"]
        result["max_soc"]      = bess["max_soc"]
        result["backup_soc"]   = bess["backup_soc"]
        result["battery_capacity"] = controller_static.get("battery_capacity")
        result["load_rise_kw_per_sec"] = controller_static.get("load_rise_kw_per_sec")
        result["pv_drop_kw_sec"] = controller_static.get("pv_drop_kw_sec")
        result["pv_capacity_kw"] = controller_static.get("pv_capacity_kw")
        result["ems_response_time_delay_sec"] = controller_static.get("ems_response_time_delay_sec")
        efficiency = controller_static.get("efficiency")
        result["efficiency"] = efficiency if efficiency <= 1 else efficiency * 0.01
        result["max_discharge_kw"] = bess.get("max_discharge_kw")

        # ============================================================
        # 4. Live BESS Data
        # ============================================================
        device_data = resolver.get_latest_device_data(device_id)

        result["soc"] = float(device_data[Tags.BATTERY_SOC]["value"])
        result["bess_power_kw"] = float(device_data[Tags.BATTERY_POWER]["value"])
        result["charge_ah"] = float(device_data[Tags.BATTERY_ENERGY]["value"])
        result["battery_temperature"] = device_data.get("Battery_Temperature", {}).get("value", 25)

        # Usable Discharge Power of Battery
        result["max_usable_kw"] = get_c_rate(result["battery_temperature"], bess) * (result["battery_capacity"] * (result["soc"] - result["min_soc"]) / 100) * result["efficiency"]

        # ============================================================
        # 5. Cycle & Safety Management
        # ============================================================
        cycle_cfg            = load_cycle_config()

        result["cycle_used_today"]       = 4#await get_today_cycles()
        result["max_cycles_per_day"]     = cycle_cfg["max_cycles_per_day"]

        result["soc_safety"]             = cycle_cfg["soc_safety_management"]
        result["lockout"]                = cycle_cfg["lockout_behaviour"]
        result["price_cfg"]              = cycle_cfg["price_logic_overrides"]

        # Derived flags
        result["disable_low_price_charging"] = result["price_cfg"]["disable_low_price_charging"]
        result["disable_peak_shaving"]       = result["price_cfg"]["disable_peak_shaving"]

        # ============================================================
        # 6. Block / Safety Logic Precomputation
        # ============================================================

        # 6.1 Cycle Limit
        result["block_discharging"] = (
            result["cycle_used_today"] >= result["max_cycles_per_day"]
        )

        # 6.2 SOC Safe
        min_safe_soc = result["min_soc"]
        soc = result["soc"]
        result["aggressively_charge"] = False
        result["prevent_charging"] = False

        if soc < min_safe_soc:
            result["block_discharging"] = True
            result["aggressively_charge"] = True
            result["prevent_charging"] = False   # allow charging
            result["target_soc"] = result["backup_soc"]

        # RESERVE
        reserve = result["backup_soc"]
        if soc < reserve and result["lockout"]["allow_reserve_charging"]:
            result["prevent_charging"] = False

        # ============================================================
        # 7. Dynamic Peak Shaving Threshold Computation
        # ============================================================
        try:

            computation_result = PeakShavingStrategy.calculate_dynamic_peak_shaving(
                windows=result["load_windows"],
                battery_capacity=result["battery_capacity"],
                current_soc=result["soc"],
                max_soc=result["max_soc"],
                efficiency=result["efficiency"],
                contractual_limit_kw=result["hard_limit_kw"],
                load_rise_kw_per_sec=result["load_rise_kw_per_sec"],
                pv_drop_kw_sec=result["pv_drop_kw_sec"],
                ems_response_time_delay_sec=result["ems_response_time_delay_sec"]
            )
        except Exception as e:
            logger.info(f"Error While Dynamic Writes: {e}")

        result["threshold_kw"] = computation_result.get("dynamic_ps_limit_kw", result["threshold_kw"])
        result["soft_limit_kw"] = computation_result.get("soft_limit_kw", result["soft_limit_kw"])

        # ============================================================
        # Cloud Detection
        # ============================================================
        result["cloud_details"] = CloudDetection.detect()

        return result
    
    @staticmethod
    async def fetch_yesterday_power_data(
        conn,
        load_table: str,
        pv_table: str,
        bess_table: str,
        load_col: str,
        pv_col: str,
        bess_col: str,
        ts_col: str = "timestamp"
    ):
        """
        Fetch yesterday's load, pv and bess power values.
        """

        yesterday_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        yesterday_end = yesterday_start + timedelta(days=1)

        query = f"""
        SELECT 
            l.{ts_col} as ts,
            l.{load_col} as load_power,
            p.{pv_col} as pv_power,
            b.{bess_col} as bess_power
        FROM {load_table} l
        LEFT JOIN {pv_table} p ON l.{ts_col} = p.{ts_col}
        LEFT JOIN {bess_table} b ON l.{ts_col} = b.{ts_col}
        WHERE l.{ts_col} >= $1
        AND l.{ts_col} < $2
        ORDER BY l.{ts_col}
        """

        rows = await conn.fetch(query, yesterday_start, yesterday_end)

        result = []

        for r in rows:
            result.append({
                "ts": r["ts"].isoformat(),
                "load_kw": r["load_power"] or 0,
                "pv_kw": r["pv_power"] or 0,
                "bess_kw": r["bess_power"] or 0
            })

        return result
     
    @staticmethod
    async def store_daily_snapshot(config):
        import os
        import json

        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        day_before_yesterday = yesterday - timedelta(days=1)

        cache_dir = "/data/cache"
        os.makedirs(cache_dir, exist_ok=True)

        day_before_yesterday_file = f"{cache_dir}/{day_before_yesterday}_snapshot.json"
        yesterday_file = f"{cache_dir}/{yesterday}_snapshot.json"

        # -------------------------------------------------
        # If today's snapshot already exists → SKIP
        # -------------------------------------------------
        if os.path.exists(yesterday_file):
            print(f"[SNAPSHOT] yesterday's snapshot already exists → Skipping")
            return yesterday_file

        # -------------------------------------------------
        # If day_before_yesterday file exists → DELETE
        # -------------------------------------------------
        if os.path.exists(day_before_yesterday_file):
            print(f"[SNAPSHOT] Removing old snapshot file")
            os.remove(day_before_yesterday_file)

        # -------------------------------------------------
        # Fetch yesterday data
        # -------------------------------------------------

        async with A_DB as conn:
            data = await PeakShavingStrategy.fetch_yesterday_power_data(
                conn,
                load_table=config["load_table"],
                pv_table=config["pv_table"],
                bess_table=config["bess_table"],
                load_col=config["load_column"],
                pv_col=config["pv_column"],
                bess_col=config["bess_column"]
            )

        with open(yesterday_file, "w") as f:
            json.dump(data, f)

        return yesterday_file
    
    @staticmethod
    def calculate_dynamic_peak_shaving(
        windows: dict,
        battery_capacity: float,
        current_soc: float,
        max_soc: float,
        efficiency: float,
        contractual_limit_kw: float,
        load_rise_kw_per_sec,
        pv_drop_kw_sec,
        ems_response_time_delay_sec
    ):
        """
        Calculates dynamic peak shaving threshold and energy sufficiency.
        """

        now = datetime.now()

        # Get next peak window
        peak_window = PeakShavingStrategy.get_next_peak_window_state(windows=windows)

        if not peak_window:
            return {"error": "No upcoming peak window found"}

        # Ensure we are subtracting datetime objects, not tuples
        start = peak_window["startTs"]
        end = peak_window["endTs"]

        # If they accidentally became tuples, grab the first element
        if isinstance(start, tuple): start = start[0]
        if isinstance(end, tuple): end = end[0]

        # Calculate duration in hours
        peak_hours = (end - start).total_seconds() / 3600
        # Usable energy (kWh)
        usable_energy_kwh = (
            battery_capacity * ((max_soc - current_soc) / 100) * efficiency
        )

        # p_max calculation
        p_max_kw = min(usable_energy_kwh / peak_hours, peak_window["load"] - contractual_limit_kw) if peak_hours > 0 else 1

        # Dynamic PS limit
        dynamic_threshold_limit_kw = contractual_limit_kw - p_max_kw


        # Worst case net ramp rate
        r_net = load_rise_kw_per_sec + pv_drop_kw_sec
        overshoot = r_net * ems_response_time_delay_sec

        # Soft Limit Calculation
        soft_limit_kw = contractual_limit_kw - overshoot  # If include safety factor then (1.5 * overshoot)

        # Writing Dynamic Thresholds KW
        PeakShavingStrategy.write_dynamic_threshold_to_json(dynamic_threshold_limit_kw, soft_limit_kw)

        # Required energy to shave full peak
        required_energy_kwh = p_max_kw * peak_hours

        sufficient = usable_energy_kwh >= required_energy_kwh
        shortfall_kwh = 0 if sufficient else required_energy_kwh - usable_energy_kwh

        # Get next window
        next_window = PeakShavingStrategy.get_next_load_window(windows)

        charging_possible = False
        charging_sufficient = None
        max_possible_charge_kwh = 0

        if next_window and next_window["state"] == "off_peak":
            # Ensure we are subtracting datetime objects, not tuples
            start = next_window["startTs"]
            end = next_window["endTs"]

            # If they accidentally became tuples, grab the first element
            if isinstance(start, tuple): start = start[0]
            if isinstance(end, tuple): end = end[0]

            # Calculate duration in hours
            charge_hours = (
               end  - start
            ).total_seconds() / 3600

            # Max possible charge during off-peak
            max_possible_charge_kwh = min(
                (battery_capacity * efficiency), contractual_limit_kw - next_window["load"]
            ) * (charge_hours)

            charging_possible = True
            charging_sufficient = max_possible_charge_kwh >= shortfall_kwh

        return {
            "next_peak_window": peak_window,
            "peak_duration_hours": peak_hours,
            "usable_energy_kwh": usable_energy_kwh,
            "p_max_kw": p_max_kw,
            "contractual_limit_kw": contractual_limit_kw,
            "dynamic_ps_limit_kw": dynamic_threshold_limit_kw,
            "soft_limit_kw": soft_limit_kw,
            "energy_sufficient": sufficient,
            "energy_shortfall_kwh": shortfall_kwh,
            "next_window": next_window,
            "charging_possible_in_next_window": charging_possible,
            "charging_sufficient_for_shortfall": charging_sufficient,
            "max_possible_charge_kwh_during_shorfall": max_possible_charge_kwh
        }
    
    @staticmethod
    def write_dynamic_threshold_to_json(
        dynamic_threshold_kw: float,
        soft_limit_kw: float
    ):
        """
        Writes dynamic peak shaving threshold into JSON file
        under key: 'peak_shaving_threshold_kw'
        """
        json_path="controller_layer/strategies/strategy_configuration/PeakShaving.json"

        # Resolve path
        path = DataPaths.resolve(json_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {json_path}")

        # Load existing JSON
        with open(path, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                raise ValueError(f"Invalid JSON format in {json_path}")

        # Update threshold
        data["peak_shaving_threshold_kw"] = round(dynamic_threshold_kw, 3)
        data["advanced_settings"]["soft_limit_kw"] = round(soft_limit_kw, 3)

        # Write back safely
        with open(path, "w") as f:
            json.dump(data, f, indent=4)

        return {
            "status": "success",
            "written_threshold_kw": dynamic_threshold_kw,
            "file": str(path)
        }
    
    @staticmethod
    def simulate_daily_optimization(controller_device_id: str):
        """
        Simulates EMS peak shaving + charging strategy
        for all remaining windows today.
        """

        # ============================================================
        # 1. Load Peak Shaving Settings
        # ============================================================
        cfg = PeakShavingStrategy.load_peak_shaving_settings()

        windows = cfg["load_windows"]
        contractual_limit_kw = cfg["advanced_settings"]["hard_limit_kw"]

        # ============================================================
        # 2. Live BESS Data
        # ============================================================
        resolver = LatestTelemetryResolver()
        device_data = resolver.get_latest_device_data(controller_device_id)
        current_soc = float(device_data["SoC"]["value"])

        # ============================================================
        # 3. Load BESS Config
        # ============================================================
        controller_static = ControllerStaticResolver()
        bess = load_bess_params_setting()
        min_soc      = bess["min_soc"]
        max_soc      = bess["max_soc"]
        battery_capacity = controller_static.get("battery_capacity")
        load_rise_kw_per_sec = controller_static.get("load_rise_kw_per_sec")
        pv_drop_kw_sec = controller_static.get("pv_drop_kw_sec")
        ems_response_time_delay_sec = controller_static.get("ems_response_time_delay_sec")
        efficiency = controller_static.get("efficiency")
        efficiency = efficiency if efficiency <= 1 else efficiency * 0.01
        max_charge_power_kw = bess.get("max_charge_kw")
        max_discharge_power_kw = bess.get("max_discharge_kw")

        now = datetime.now()
        today = now.date()

        results = []

        # Flatten and sort all windows
        all_windows = []

        for state, ranges in windows.items():
            for r in ranges:
                start_time = datetime.strptime(r["start_time"], "%H:%M").time()
                end_time = datetime.strptime(r["end_time"], "%H:%M").time()

                start_dt = datetime.combine(today, start_time)
                end_dt = datetime.combine(today, end_time)

                if start_time > end_time:
                    end_dt = end_dt.replace(day=end_dt.day + 1)

                if end_dt > now:  # only future windows
                    all_windows.append({
                        "state": state,
                        "start": start_dt,
                        "end": end_dt,
                        "load": r["load"]
                    })

        all_windows.sort(key=lambda x: x["start"])

        soc = current_soc
        total_energy_discharged = 0

        for window in all_windows:

            duration_hours = (window["end"] - window["start"]).total_seconds() / 3600
            if duration_hours <= 0:
                continue

            soc_before = soc

            usable_energy_kwh = (
                battery_capacity * ((soc - min_soc) / 100) * efficiency
            )

            action = "idle"
            p_max_kw = 0
            dynamic_threshold = None
            soft_limit_kw = None
            shortfall_kwh = 0
            energy_used_kwh = 0
            energy_charged_kwh = 0

            # PEAK WINDOW → DISCHARGE
            if window["state"] == "peak":

                if usable_energy_kwh > 0:

                    p_max_kw = usable_energy_kwh / duration_hours
                    p_max_kw = min(p_max_kw, window["load"] - contractual_limit_kw, max_discharge_power_kw)

                    energy_used_kwh = p_max_kw * duration_hours
                    total_energy_discharged = total_energy_discharged + energy_charged_kwh

                    dynamic_threshold = contractual_limit_kw - p_max_kw

                    # Worst case net ramp rate
                    r_net = load_rise_kw_per_sec + pv_drop_kw_sec
                    overshoot = r_net * ems_response_time_delay_sec

                    # Soft Limit Calculation
                    soft_limit_kw = contractual_limit_kw - overshoot  # If include safety factor then (1.5 * overshoot)

                    soc_drop_percent = (energy_used_kwh / battery_capacity) * 100
                    soc -= soc_drop_percent

                    action = "discharge"

                else:
                    shortfall_kwh = battery_capacity * ((min_soc - soc) / 100)

            # OFF-PEAK WINDOW → CHARGE
            elif window["state"] == "off_peak":

                available_room_kwh = (
                    battery_capacity * ((max_soc - soc) / 100) * efficiency
                )

                if available_room_kwh > 0:

                    charge_power = min(max_charge_power_kw, available_room_kwh / duration_hours, contractual_limit_kw - window["load"])

                    energy_charged_kwh = charge_power * duration_hours * efficiency

                    energy_charged_kwh = min(energy_charged_kwh, available_room_kwh)

                    soc_rise_percent = (energy_charged_kwh / battery_capacity) * 100
                    soc += soc_rise_percent

                    action = "charge"

            # MID PEAK → Optional (you can customize)
            else:
                action = "idle"

            soc = max(min(soc, max_soc), min_soc)

            results.append({
                "window_state": window["state"],
                "start": window["start"],
                "end": window["end"],
                "load": window["load"],
                "duration_hours": duration_hours,
                "soc_before_percent": round(soc_before, 2),
                "soc_after_percent": round(soc, 2),
                "action": action,
                "p_max_kw": round(p_max_kw, 2),
                "dynamic_threshold_kw": round(dynamic_threshold, 2) if dynamic_threshold else None,
                "soft_limit_kw": round(soft_limit_kw, 2) if soft_limit_kw else None,
                "energy_used_kwh": round(energy_used_kwh, 2),
                "energy_charged_kwh": round(energy_charged_kwh, 2),
                "shortfall_kwh": round(shortfall_kwh, 2),
            })

        return {
            "simulation_start": now,
            "initial_soc": current_soc,
            "final_soc": round(soc, 2),
            "total_energy_discharged": total_energy_discharged,
            "windows_simulated": len(results),
            "timeline": results
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

            logger.info(f"{LogColor.CYAN} [Peak Shaving] Executing schedule for device {device_id} {LogColor.RESET}")

            # -------------------------------------------------
            # 0. Load BESS Decision 
            # -------------------------------------------------
            poi_meter_id = "plant_1_poi_meter_1"
            config = await PeakShavingStrategy.bess_decision(device_id, poi_meter_id)

            (
                prevent_charging,
                block_discharging,
                hard_limit_kw,
                soft_limit_kw,
                threshold_kw,
                max_discharge_kw,
                mode,
                forecast_source,
                current_load_kw,
                max_bess_discharge_kw,
                soc,
                max_soc
            ) = (
                config["prevent_charging"],
                config["block_discharging"],
                config["hard_limit_kw"],
                config["soft_limit_kw"],
                config["threshold_kw"],
                config["max_discharge_power_kw"],
                config["mode"],
                config["forecast_source"],
                config["current_load_kw"],
                config["max_bess_discharge_kw"],
                config["soc"],
                config["max_soc"]
            )

            # ---------------------------------------
            # 1. Extract schedule fields
            # ---------------------------------------
            load = schedule.get("load_power", threshold_kw)
            power_kw = schedule.get("power")

            if not all([load, power_kw]):
                logger.error(f"[Peak Shaving] Invalid schedule payload: {schedule}")
                return

            # ---------------------------------------
            # 2. Get live values
            # ---------------------------------------
            device_data = resolver.get_latest_device_data(device_id)
            if not device_data:
                logger.error(f"[Peak Shaving] No live data for {device_id}")
                return
            
            # soc = device_data["SoC"]["value"]

            # ---------------------------------------
            # 3. Smart decision-making
            # ---------------------------------------
            if signal == "discharge":
                # DISCHARGE LOGIC
                logger.info(f"{LogColor.CYAN} [Peak Shaving] Discharge event triggered {LogColor.RESET}")

                if block_discharging:
                    logger.info("[Peak Shaving] Discharging blocked by decision logic.")
                    return
                
                required_kw = current_load_kw - load
                discharge_kw = min(required_kw, max_bess_discharge_kw, max_discharge_kw)

                await PeakShavingDischargeStrategy(
                    resolver=resolver,
                    controller_static=controller_static,
                    server_ip=server_ip,
                    server_port=server_port,
                    channel=channel,
                    slave_id=slave_id,
                    device_id=device_id,
                    excess_load=discharge_kw,
                    poi_meter_id=poi_meter_id
                )

            else:
                # CHARGE LOGIC
                logger.info(f"{LogColor.CYAN} [Peak Shaving] Charge event triggered {LogColor.RESET}")

                if prevent_charging:
                    logger.info("[Peak Shaving] Charging prevented by decision logic.")
                    return
                
                available_kw = current_load_kw - threshold_kw

                logger.info(f"{LogColor.MAGENTA} Available kw is : {available_kw} {LogColor.RESET}")

                await PeakShavingChargingStrategy(
                    resolver=resolver,
                    controller_static=controller_static,
                    device_id=device_id,
                    channel=channel,
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    available_kw=available_kw,
                    max_soc=max_soc,
                    extra_info={"strategy_id": 3}
                )
                

            logger.info("[Peak Shaving] No action required. Conditions not met.")
        except Exception as e:
            logger.error(f"{LogColor.RED} [Peak Shaving] Error executing schedule: {e} {LogColor.RESET}")
            return

    @staticmethod
    async def peak_shaving_as_constraint(device_id: str, poi_meter_id: str, block_discharging: bool, server_ip: str, server_port: int, channel: int, slave_id: int):
        """
        CENTRALIZED Peak Shaving as Constraint ENGINE
        Reads configs + live data and if Condition met, it discharge to reduce load else returns no action.
        """

        try:
            logger.info(f"{LogColor.MAGENTA} Starting [PeakShaving as Constraint] executing... {LogColor.RESET}")

            strategy_id = 3
            # ---------------------------------
            # Inserting Starting Execution Log
            # ---------------------------------
            # try :
                
            #     pool = await get_async_connection()
            #     async with pool.acquire() as conn:

            #         # create execution log
            #         exec_log = ExecutionLog(strategy_id=strategy_id, started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), input_snapshot={"live": {"soc" : soc, "current_load" : current_load_kw}, "config": {}, "flags": {}})
            #         exec_id = await create_execution_log(conn, exec_log)
            #     await pool.close()
            # except Exception as e:
            #     logger.info(f"{LogColor.RED} Error While Creating Log Exec. {e} {LogColor.RESET}")

            config = await PeakShavingStrategy.bess_decision(device_id, poi_meter_id)

            (
                threshold_kw,
                hard_limit_kw,
                soft_limit_kw,
                max_discharge_kw,
                current_load_kw,
                max_bess_discharge_kw,
                pv_power_kw,
                bess_power_kw,
                force_hard,
                enforce_soft,
                prevent_charging,
                soc,
                max_soc
            ) = (

                config["threshold_kw"],
                config["hard_limit_kw"],
                config["soft_limit_kw"],
                config["max_discharge_power_kw"],
                config["current_load_kw"],
                config["max_bess_discharge_kw"],
                config["pv_power_kw"],
                config["bess_power_kw"],
                config["force_hard"],
                config["enforce_soft"],
                config["prevent_charging"],
                config["soc"],
                config["max_soc"]
            )

            # -----------------------------------------------------------------------------
            # ***************************** EQUATIONS *************************************
            # -----------------------------------------------------------------------------

            grid_power = current_load_kw - pv_power_kw - bess_power_kw

            pv_surplus_power_kw = pv_power_kw - current_load_kw

            if grid_power > threshold_kw and not block_discharging:
                # Reduce load to soft limit
                required_kw = grid_power - threshold_kw
                discharge_kw = min(required_kw, max_bess_discharge_kw, max_discharge_kw)

                logger.info(f"{LogColor.RED} Above Peak Shaving Threshold limit → Discharging {discharge_kw:.2f} kW {LogColor.RESET}")

                result = await PeakShavingDischargeStrategy(
                    resolver=LatestTelemetryResolver(),
                    controller_static=ControllerStaticResolver(),
                    server_ip=server_ip,
                    server_port=server_port,
                    channel=channel,
                    slave_id=slave_id,
                    device_id=device_id,
                    excess_load=discharge_kw,
                    poi_meter_id=poi_meter_id,
                    extra_info={"force_hard": force_hard}
                )
                # pool = await get_async_connection()
                # async with pool.acquire() as conn:
                #     await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
                logger.info(f"[PeakShaving as Constraint] Strategy execution completed.")
                return result
            
            elif grid_power > soft_limit_kw and enforce_soft and not block_discharging:
                required_kw = grid_power - soft_limit_kw
                discharge_kw = min(required_kw, max_bess_discharge_kw, max_discharge_kw)

                logger.info(f"{LogColor.RED} Above soft limit → Discharging {discharge_kw:.2f} kW {LogColor.RESET}")

                result = await PeakShavingDischargeStrategy(
                    resolver=LatestTelemetryResolver(),
                    controller_static=ControllerStaticResolver(),
                    server_ip=server_ip,
                    server_port=server_port,
                    channel=channel,
                    slave_id=slave_id,
                    device_id=device_id,
                    excess_load=discharge_kw,
                    poi_meter_id=poi_meter_id,
                    extra_info={"force_hard": force_hard}
                )
                # pool = await get_async_connection()
                # async with pool.acquire() as conn:
                #     await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
                logger.info(f"[PeakShaving as Constraint] Strategy execution completed.")
                return result
            
            elif grid_power > hard_limit_kw and not block_discharging:
                required_kw = grid_power - hard_limit_kw
                discharge_kw = min(required_kw, max_bess_discharge_kw, max_discharge_kw)

                logger.info(f"{LogColor.RED} Above Hard limit → Discharging {discharge_kw:.2f} kW {LogColor.RESET}")

                result = await PeakShavingDischargeStrategy(
                    resolver=LatestTelemetryResolver(),
                    controller_static=ControllerStaticResolver(),
                    server_ip=server_ip,
                    server_port=server_port,
                    channel=channel,
                    slave_id=slave_id,
                    device_id=device_id,
                    excess_load=discharge_kw,
                    poi_meter_id=poi_meter_id,
                    extra_info={"force_hard": force_hard}
                )
                # pool = await get_async_connection()
                # async with pool.acquire() as conn:
                #     await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
                logger.info(f"[PeakShaving as Constraint] Strategy execution completed.")
                return result


            if pv_surplus_power_kw > 0 and not prevent_charging:
                logger.info(f"{LogColor.MAGENTA} Available kw is : {pv_surplus_power_kw} {LogColor.RESET}")
                
                result = await PeakShavingChargingStrategy(
                    resolver=LatestTelemetryResolver(),
                    controller_static=ControllerStaticResolver(),
                    device_id=device_id,
                    channel=channel,
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    available_kw=pv_surplus_power_kw,
                    max_soc=max_soc
                )
                # pool = await get_async_connection()
                # async with pool.acquire() as conn:
                #     await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
                # logger.info(f"[PeakShavingStrategy] Strategy execution completed.")
                return result
            
            elif pv_surplus_power_kw > 0 and soc >= max_soc:
                logger.info(f"{LogColor.MAGENTA} Load Power is less then PV Power but battery is at MAX. Action Curtailment of PV {LogColor.RESET}")
                return await PeakShavingStrategy.pv_power_curtailment(pv_power_kw, pv_surplus_power_kw, grid_power, current_load_kw)

        except Exception as e:
            logger.info(f"{LogColor.RED} Error in Peak Shaving as Constraint: {e} {LogColor.RESET}")
            return


class PeakShavingStrategyType7:
    def __init__(self, resolver):
        self.resolver = LatestTelemetryResolver()
        self.controller_static = ControllerStaticResolver()
        self.ess_control_state_manager = EMSControlStateManager()
        self.strategy_flags_controller = StrategyFlagsController(
            ess_control_state_manager = self.ess_control_state_manager,
        )
        # self.metadata_resolver = MetadataResolver()

    async def execute(self, channel, server_ip, server_port, slave_id, device_id, signal):
        """
        Peak Shaving strategy execution: calculates how much energy to charge / discharge per time slot based on Load.

        """

        logger.info(f"{LogColor.MAGENTA} Starting [PeakShavingStrategyType7] executing... {LogColor.RESET}")

        strategy_id = 3
        strategy_name = "Peak Shaving"

        # -------------------------------------------------
        # Raise EMS Status Flag fot Strategy
        # -------------------------------------------------
        try:

            # await self.ess_control_state_manager.raise_flag("enable_peak_shaving_strategy", 1)
            # await self.ess_control_state_manager.raise_flag("enable_tou_tariff_strategy", 0)
            # await self.ess_control_state_manager.raise_flag("enable_capped_dynamic_strategy", 0)

            await self.strategy_flags_controller.raise_strategy_flags_once_per_day(
                strategy_name="flat_tou",
                flags={
                    "enable_tou_tariff_strategy": 0,
                    "enable_capped_dynamic_strategy": 0,
                    "enable_peak_shaving_strategy": 1,
                    "enable_basic_strategy": 0,
                }
            ) 

        except Exception as e:
            logger.info(f"{LogColor.RED} Error whilte raising EMS STATUS FLAG {e} {LogColor.RESET}")

        # -------------------------------------------------
        # Load BESS Decision
        # -------------------------------------------------
        poi_meter_id = "plant_1_poi_meter_1"
        try:
            
            config = await PeakShavingStrategyType7.bess_decision(device_id, poi_meter_id)

            (
                prevent_charging,
                block_discharging,
                hard_limit_kw,
                soft_limit_kw,
                threshold_kw,
                max_discharge_kw,
                max_usable_kw,
                mode,
                forecast_source,
                current_load_kw,
                soc,
                max_soc,
                enforce_soft,
                force_hard,
                next_peak_window,
                pv_power_kw,
                bess_power_kw,
                dg_power_kw,
                optimal_diesel_output,
                dg_load_percent,
                bess_gfm_transition_threshold,
                bess_gfl_transition_threshold,
                allow_charging_during_off_peak,
                current_window,
            ) = (
                config["prevent_charging"],
                config["block_discharging"],
                config["hard_limit_kw"],
                config["soft_limit_kw"],
                config["threshold_kw"],
                config["max_discharge_kw"],
                config["max_usable_kw"],
                config["mode"],
                config["forecast_source"],
                config["current_load_kw"],
                config["soc"],
                config["max_soc"],
                config["enforce_soft"],
                config["force_hard"],
                config["next_peak_window"],
                config["pv_power_kw"],
                config["bess_power_kw"],
                config["dg_power_kw"],
                config["optimal_diesel_output"],
                config["dg_load_percent"],
                config["bess_gfm_transition_threshold"],
                config["bess_gfl_transition_threshold"],
                config["allow_charging_during_off_peak"],
                config["current_window"],
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
                exec_log = ExecutionLog(strategy_id=strategy_id, started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), input_snapshot={"live": {"soc" : soc, "current_load" : current_load_kw}, "config": {}, "flags": {}})
                exec_id = await create_execution_log(conn, exec_log)
            await pool.close()
        except Exception as e:
            logger.info(f"{LogColor.RED} Error While Creating Log Exec. {e} {LogColor.RESET}")

        # -----------------------------------------------------------------------------
        # ***************************** EQUATIONS *************************************
        # -----------------------------------------------------------------------------

        grid_power = current_load_kw - pv_power_kw - bess_power_kw - dg_power_kw

        pv_surplus_power_kw = pv_power_kw - current_load_kw

        # ------------------------------------------------------------------------------
        # ***************************** SMART CONTROLLER MODE ************************
        # ------------------------------------------------------------------------------

        try:
            await smart_mode_controller(self.resolver)

        except Exception as e:
            logger.info(f"{LogColor.RED} Error While Execute [SmartModeController]: {e} {LogColor.RESET}")

        
        # ------------------------------------------------------------------------------
        # ***************************** CONTROLLED PEAK SHAVING ************************
        # ------------------------------------------------------------------------------

        # threshold_kw => Peak Shaving Threshold
        # Soft Limit => Safe Limit
        # Hard Limit => Contractual Limit

        logger.info(
            f"Grid Power: {grid_power} | "
            f"Current Load: {current_load_kw} | "
            f"PV Power: {pv_power_kw} | "
            f"BESS Power: {bess_power_kw} | "
            f"DG Power: {dg_power_kw} | "
            f"PV Surplus: {pv_surplus_power_kw} | "
            f"Threshold kW: {threshold_kw} | "
            f"Soft Limit kW: {soft_limit_kw} | "
            f"Block Discharge: {block_discharging} | "
            f"Prevent Charging: {prevent_charging} | "
            # f"Current Window: {current_window} | "
        )

        result = None

        # Set Target Limit kW
        target_limit = None

        # Determine which limit is active
        if threshold_kw < grid_power < soft_limit_kw:
            target_limit = threshold_kw

        elif soft_limit_kw < grid_power < hard_limit_kw:
            if enforce_soft:
                target_limit = soft_limit_kw
            else:
                target_limit = hard_limit_kw

        elif grid_power > hard_limit_kw:
            target_limit = hard_limit_kw

        if target_limit is None:

            logger.info("target limit is none!!")

            if pv_surplus_power_kw > 0:
            
                if soc >= max_soc:
                    logger.info(
                        f"{LogColor.MAGENTA}PV surplus available but battery is full. "
                        f"Curtailing PV.{LogColor.RESET}"
                    )

                    result = await PeakShavingStrategyType7.pv_power_curtailment(
                        pv_power_kw,
                        pv_surplus_power_kw,
                        grid_power,
                        current_load_kw
                    )

                elif not prevent_charging:

                    logger.info(
                        f"{LogColor.MAGENTA}Charging battery from PV surplus "
                        f"({pv_surplus_power_kw:.2f} kW).{LogColor.RESET}"
                    )

                    result = await PeakShavingChargingStrategyType7(
                        resolver=self.resolver,
                        controller_static=self.controller_static,
                        device_id=device_id,
                        channel=channel,
                        server_ip=server_ip,
                        server_port=server_port,
                        slave_id=slave_id,
                        available_kw=pv_surplus_power_kw,
                        max_soc=max_soc
                    )

            elif dg_load_percent < 0.3:

                if soc >= max_soc:

                    logger.info(
                        f"{LogColor.MAGENTA}DG running below optimal load but battery is full. "
                        f"Stopping DG.{LogColor.RESET}"
                    )

                    result = await DGStopControl(
                        resolver=self.resolver,
                        controller_static=self.controller_static,
                        server_ip=server_ip,
                        server_port=server_port,
                        channel=channel,
                        slave_id=slave_id,
                        device_id=device_id,
                        status=0,
                        load=(grid_power - threshold_kw)
                    )

                elif not prevent_charging:

                    available_kw = optimal_diesel_output - current_load_kw

                    logger.info(
                        f"{LogColor.MAGENTA}Charging battery using DG spare capacity "
                        f"({available_kw:.2f} kW).{LogColor.RESET}"
                    )

                    result = await PeakShavingChargingStrategyType7(
                        resolver=self.resolver,
                        controller_static=self.controller_static,
                        device_id=device_id,
                        channel=channel,
                        server_ip=server_ip,
                        server_port=server_port,
                        slave_id=slave_id,
                        available_kw=available_kw,
                        max_soc=max_soc
                    )

            elif current_window is not None and current_window["state"] == "off-peak" and allow_charging_during_off_peak and not prevent_charging:
                    available_kw = threshold_kw + pv_power_kw - current_load_kw

                    if available_kw > 0:
                        logger.info(
                            f"{LogColor.MAGENTA}Charging battery from utility during off-peak "
                            f"({available_kw:.2f} kW).{LogColor.RESET}"
                        )

                        result = await PeakShavingChargingStrategyType7(
                            resolver=self.resolver,
                            controller_static=self.controller_static,
                            device_id=device_id,
                            channel=channel,
                            server_ip=server_ip,
                            server_port=server_port,
                            slave_id=slave_id,
                            available_kw=available_kw,
                            max_soc=max_soc
                        )

        else :

            required_kw = grid_power - target_limit
            discharge_kw = min(required_kw, max_usable_kw, max_discharge_kw)

            logger.info(
                f"(Required={required_kw:.2f}, Available={discharge_kw:.2f}) "
            )

            # Battery cannot support required load
            if discharge_kw < required_kw:
                block_discharging = True

            # GFL -> GFM Transition SoC threshold
            # if soc < bess_gfm_transition_threshold:
            #     block_discharging = False
                
            # main Execution
            if not block_discharging:

                logger.info(
                    f"Peak shaving using BESS | "
                    f"Grid={grid_power:.2f}, "
                    f"Target={target_limit:.2f}, "
                    f"Required={required_kw:.2f}, "
                    f"Discharge={discharge_kw:.2f}"
                )

                # await gfl_gfm_mode_selection(
                #     resolver=self.resolver,
                #     mode="GFM",
                #     device_type="BESS"
                # )

                result = await PeakShavingDischargeStrategyType7(
                    resolver=self.resolver,
                    controller_static=self.controller_static,
                    server_ip=server_ip,
                    server_port=server_port,
                    channel=channel,
                    slave_id=slave_id,
                    device_id=device_id,
                    excess_load=discharge_kw,
                    target_limit=target_limit,
                    poi_meter_id=poi_meter_id,
                    extra_info={"force_hard": force_hard}
                )

            else:

                logger.info(
                    f"BESS capacity insufficient "
                    f"(Required={required_kw:.2f}, Available={discharge_kw:.2f}) "
                    f"Starting DG."
                )

                # await gfl_gfm_mode_selection(
                #     resolver=self.resolver,
                #     mode="GFM",
                #     device_type="DG"
                # )

                result = await DGStartControl(
                    resolver=self.resolver,
                    controller_static=self.controller_static,
                    server_ip=server_ip,
                    server_port=server_port,
                    channel=channel,
                    slave_id=slave_id,
                    device_id=device_id,
                    status=1,
                    load=required_kw,
                )

        # Store result into DB
        pool = await get_async_connection()
        async with pool.acquire() as conn:
            await finish_execution_log(
                conn,
                exec_id,
                finished_at=datetime.now(),
                decision_snapshot=result if result else {"text": "Peak Shaving is normal. No action. Strategy execution completed"},
                error_text=""
            )
        await pool.close()

        logger.info("[PeakShavingStrategyType7] Strategy execution completed.")
        return

        # -----------------------------------------------------------------------
        # ***************************** PREDICTIVE PEAK SHAVING ******************
        # -----------------------------------------------------------------------
        # if mode == "predective_peak_shaving":

        #     logger.info(f"{LogColor.YELLOW} Predictive Peak Shaving Active | Source = {forecast_source} {LogColor.RESET}")

        #     # Example: AI forecast pull (you can plug real API)
        #     predicted_load = None

        #     if forecast_source == "ai_forecast":
        #         predicted_load = self.forecasting_engine.get_predicted_peak()  # <--- your method needed
        #     elif forecast_source == "manual_input":
        #         predicted_load = self.controller_static.get("manual_peak_load")
        #     else:
        #         logger.info("No forecast available.")
        #         return

        #     logger.info(f"{LogColor.CYAN} Predicted Load: {predicted_load} kW | Threshold: {threshold_kw} kW {LogColor.RESET}")

        #     if predicted_load and predicted_load > threshold_kw and not block_discharging:
        #         required_kw = predicted_load - threshold_kw
        #         discharge_kw = min(required_kw, max_usable_kw, max_discharge_kw)

        #         logger.info(f"{LogColor.RED} Predicted peak high → Pre-discharge {discharge_kw:.2f} kW {LogColor.RESET}")

        #         result = await PeakShavingDischargeStrategy(
        #             resolver=self.resolver,
        #             controller_static=self.controller_static,
        #             server_ip=server_ip,
        #             server_port=server_port,
        #             channel=channel,
        #             slave_id=slave_id,
        #             device_id=device_id,
        #             excess_load=discharge_kw,
        #             poi_meter_id=poi_meter_id
        #         )
        #         pool = await get_async_connection()
        #         async with pool.acquire() as conn:
        #             await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
        #         logger.info(f"[PeakShavingStrategyType7] Strategy execution completed.")
        #         return result

        #     logger.info(f"{LogColor.GREEN} Predicted load normal. No action. {LogColor.RESET}")
        #     pool = await get_async_connection()
        #     async with pool.acquire() as conn:
        #         await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot={"text": "Predicted load normal. No action."}, error_text="")
        #     logger.info(f"[PeakShavingStrategyType7] Strategy execution completed.")
        #     return result
    
    @staticmethod
    async def pv_power_curtailment(
        pv_power_kw: float,
        pv_surplus_power_kw: float,
        grid_power: float,
        current_load_kw: float
    ):
        """
            This Function is responsible fot PV Curtailment.
            Condition: Load Power is less then PV Power but battery is at MAX. Action Curtailment of PV
        """

        logger.info(f"{LogColor.MAGENTA} [PV Curtailment] in execution... {LogColor.RESET}")

        grid_threshold_kw = -150
        pv_setpoint = pv_power_kw
        if grid_power < grid_threshold_kw:
            pv_setpoint = current_load_kw + (grid_threshold_kw * -1)
        

        return {}

    @staticmethod
    def load_peak_shaving_settings(json_path="controller_layer/strategies/strategy_configuration/PeakShaving.json") -> dict:
        """
        Load peak shaving settings and return resolved values.
        
        Returns:
            dict containing:
                - peak_shaving_mode (string)
                - forcast_source (string)
                - threshold_kw
                - reserve_soc_percent
                - advanced_settings (dict)
        """
        path = DataPaths.resolve(json_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {json_path}")

        with open(path, "r") as f:
            data = json.load(f)

        # Resolve mode name
        mode_key = data.get("peak_shaving_mode")
        mode = data.get("peak_shaving_mode_option", {}).get(mode_key)

        # Resolve forecast source
        source_key = data.get("forcast_source")
        source = data.get("forcast_source_option", {}).get(source_key)

        # Resolve Load Windows
        windows = data.get("windows", {})
        current_window = PeakShavingStrategyType7.get_current_load_window_state(windows)
        next_peak_window = PeakShavingStrategyType7.get_next_peak_window_state(windows)
        next_load_window = PeakShavingStrategyType7.get_next_load_window(windows)

        return {
            "peak_shaving_mode": mode,
            "peak_shaving_mode_key": mode_key,
            "forcast_source": source,
            "forcast_source_key": source_key,
            "threshold_kw": data.get("peak_shaving_threshold_kw"),
            "reserve_soc_percent": data.get("reserve_soc_percent"),
            "advanced_settings": data.get("advanced_settings", {}),
            "optimization_rules": data.get("optimization_rules", {}),
            "load_windows" : windows,
            "current_window" : current_window,
            "next_peak_window" : next_peak_window,
            "next_load_window" : next_load_window,
            "bess_gfl_transition_threshold": data.get("bess_gfl_transition_threshold"),
            "bess_gfm_transition_threshold": data.get("bess_gfm_transition_threshold")
        }

    @staticmethod
    def get_next_load_window(windows: dict):
        """
        Returns next upcoming load window (off_peak, mid_peak, peak).
        """

        now = datetime.now()
        today = now.date()
        next_window = None

        for state_name, ranges in windows.items():
            for r in ranges:
                start_time = datetime.strptime(r["start_time"], "%H:%M").time()
                end_time = datetime.strptime(r["end_time"], "%H:%M").time()

                start_dt = datetime.combine(today, start_time)
                end_dt = datetime.combine(today, end_time)

                # Handle midnight crossing
                if start_time > end_time:
                    end_dt += timedelta(days=1)

                # If already passed → move to tomorrow
                if start_dt <= now:
                    start_dt += timedelta(days=1)
                    end_dt += timedelta(days=1)

                if not next_window or start_dt < next_window["startTs"]:
                    next_window = {
                        "state": state_name,
                        "startTs": start_dt,
                        "endTs": end_dt,
                        "load": r["load"]
                    }

        return next_window
    
    @staticmethod
    def get_next_peak_window_state(windows: dict):
        """
        Find the next upcoming peak window.
        Returns state info with startTs, endTs, load.
        """

        now = datetime.now()
        today = now.date()
        next_peak = None

        peak_ranges = windows.get("peak", [])

        for r in peak_ranges:
            start_time = datetime.strptime(r["start_time"], "%H:%M").time()
            end_time = datetime.strptime(r["end_time"], "%H:%M").time()

            start_dt = datetime.combine(today, start_time)
            end_dt = datetime.combine(today, end_time)

            # Handle midnight crossing
            if start_time > end_time:
                end_dt += timedelta(days=1)

            # If already passed today → move to tomorrow
            if start_dt <= now:
                start_dt += timedelta(days=1)
                end_dt += timedelta(days=1)

            # Keep the earliest upcoming peak
            if not next_peak or start_dt < next_peak["startTs"]:
                next_peak = {
                    "state": "peak",
                    "startTs": start_dt,
                    "endTs": end_dt,
                    "load": r["load"]
                }

        return next_peak
    
    @staticmethod
    def get_current_load_window_state(windows: dict):
        """
        Determine current state (off_peak, mid_peak, peak) and return
        the state info with startTs, endTs, price, and optimization rules.
        """
        now = datetime.now()
        current_time = now.time()
        
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
        for state_name, ranges in windows.items():
            for r in ranges:
                if time_in_range(r["start_time"], r["end_time"], current_time):
                    # Build today's datetime timestamps
                    start_dt = datetime.combine(now.date(), datetime.strptime(r["start_time"], "%H:%M").time())
                    end_dt = datetime.combine(now.date(), datetime.strptime(r["end_time"], "%H:%M").time())

                    result = {
                        "state": state_name,
                        "startTs": start_dt,
                        "endTs": end_dt,
                        "load": r["load"]
                    }
                    return result

        return None  # If nothing matches (should not normally happen)

    @staticmethod
    async def bess_decision(device_id: str, poi_meter_id: str):
        """
        CENTRALIZED DECISION ENGINE
        Reads configs + live data + cycle data and returns one decision dict.
        """

        result = await bess_decision7(device_id, poi_meter_id)

        return result
  
    @staticmethod
    async def p_bess_decision(device_id: str, poi_meter_id: str):
        """
        CENTRALIZED DECISION ENGINE
        Reads configs + live data + cycle data and returns one decision dict.
        """

        result = {}

        # ============================================================
        # 1. Load Peak Shaving Settings
        # ============================================================
        cfg = PeakShavingStrategyType7.load_peak_shaving_settings()

        result["mode"]                  = cfg["peak_shaving_mode"]
        result["threshold_kw"]          = cfg["threshold_kw"]
        result["forecast_source"]       = cfg["forcast_source"]

        result["load_windows"]          = cfg["load_windows"]
        result["current_window"]        = cfg["current_window"]
        result["next_peak_window"]      = cfg["next_peak_window"]
        result["next_load_window"]      = cfg["next_load_window"]
        
        adv = cfg["advanced_settings"]

        result["soft_limit_kw"]         = adv["soft_limit_kw"]
        result["hard_limit_kw"]         = adv["hard_limit_kw"]
        result["enforce_soft"]          = adv["maintain_load_under_soft_limit_using_bess"]
        result["force_hard"]            = adv["force_control_to_stay_under_hard_limit"]

        result["bess_gfl_transition_threshold"] = cfg["bess_gfl_transition_threshold"]
        result["bess_gfm_transition_threshold"] = cfg["bess_gfm_transition_threshold"]

        opti = cfg["optimization_rules"]

        result["allow_charging_during_off_peak"] = opti["allow_charging_during_off_peak"]
        result["allow_charging_only_from_solar"] = opti["allow_charging_only_from_solar"]
        result["discharge_only_during_peak"] = opti["discharge_only_during_peak"]

        # ============================================================
        # 2. Read POI Load (Always needed)
        # ============================================================
        resolver = LatestTelemetryResolver()
        poi_data = resolver.get_latest_device_data(poi_meter_id)
        result["current_load_kw"] = float(poi_data[Tags.Load]["value"])
        result["pv_power_kw"] = float(poi_data[Tags.PV_POWER]["value"]) # from solar meter
        # DG Power
        result["dg_power_kw"] = 0
        result["optimal_diesel_output"] = result["dg_power_kw"] * 0.7      # Diesel_Rated_Power × Target_Loading (%) 
        result["dg_load_percent"] = 0.3

        # ============================================================
        # 3. Load BESS Config
        # ============================================================
        controller_static = ControllerStaticResolver()
        bess = load_bess_params_setting()
        result["min_soc"]      = bess["min_soc"]
        result["max_soc"]      = bess["max_soc"]
        result["backup_soc"]   = bess["backup_soc"]
        result["battery_capacity"] = controller_static.get("battery_capacity")
        result["load_rise_kw_per_sec"] = controller_static.get("load_rise_kw_per_sec")
        result["pv_drop_kw_sec"] = controller_static.get("pv_drop_kw_sec")
        result["ems_response_time_delay_sec"] = controller_static.get("ems_response_time_delay_sec")
        efficiency = controller_static.get("efficiency")
        result["efficiency"] = efficiency if efficiency <= 1 else efficiency * 0.01
        result["max_discharge_kw"] = bess.get("max_discharge_kw")
        result["max_charge_kw"] = bess.get("max_charge_kw")


        # ============================================================
        # 4. Live BESS Data
        # ============================================================
        device_data = resolver.get_latest_device_data(device_id)

        result["soc"] = float(device_data[Tags.BATTERY_SOC]["value"])
        result["bess_power_kw"] = float(device_data[Tags.BATTERY_POWER]["value"])
        result["charge_ah"] = float(device_data[Tags.BATTERY_ENERGY]["value"])
        result["battery_temperature"] = device_data.get("Battery_Temperature", {}).get("value", 25)

        # Usable Discharge Power of Battery
        result["max_usable_kw"] = get_c_rate(result["battery_temperature"], bess) * (result["battery_capacity"] * (result["soc"] - result["min_soc"]) / 100) * result["efficiency"]

        # ============================================================
        # 5. Cycle & Safety Management
        # ============================================================
        cycle_cfg            = load_cycle_config()

        result["cycle_used_today"]       = 4#await get_today_cycles()
        result["max_cycles_per_day"]     = cycle_cfg["max_cycles_per_day"]

        result["soc_safety"]             = cycle_cfg["soc_safety_management"]
        result["lockout"]                = cycle_cfg["lockout_behaviour"]
        result["price_cfg"]              = cycle_cfg["price_logic_overrides"]

        # Derived flags
        result["disable_low_price_charging"] = result["price_cfg"]["disable_low_price_charging"]
        result["disable_peak_shaving"]       = result["price_cfg"]["disable_peak_shaving"]

        # ============================================================
        # 6. Block / Safety Logic Precomputation
        # ============================================================

        # 6.1 Cycle Limit
        result["block_discharging"] = (
            result["cycle_used_today"] >= result["max_cycles_per_day"]
        )

        # 6.2 SOC Safe
        min_safe_soc = result["min_soc"]
        soc = result["soc"]
        result["aggressively_charge"] = False
        result["prevent_charging"] = False

        if soc < min_safe_soc :
            result["block_discharging"] = True
            result["aggressively_charge"] = True
            result["prevent_charging"] = False   # allow charging
            result["target_soc"] = result["backup_soc"]

        # RESERVE
        reserve = result["backup_soc"]
        if soc < reserve and result["lockout"]["allow_reserve_charging"]:
            result["prevent_charging"] = False

        # ============================================================
        # 7. Dynamic Peak Shaving Threshold Computation
        # ============================================================
        try:

            computation_result = PeakShavingStrategyType7.calculate_dynamic_peak_shaving(
                windows=result["load_windows"],
                battery_capacity=result["battery_capacity"],
                current_soc=result["soc"],
                max_soc=result["max_soc"],
                efficiency=result["efficiency"],
                contractual_limit_kw=result["hard_limit_kw"],
                load_rise_kw_per_sec=result["load_rise_kw_per_sec"],
                pv_drop_kw_sec=result["pv_drop_kw_sec"],
                ems_response_time_delay_sec=result["ems_response_time_delay_sec"]
            )
        except Exception as e:
            logger.info(f"Error While Dynamic Writes: {e}")

        result["threshold_kw"] = computation_result.get("dynamic_ps_limit_kw", result["threshold_kw"])
        result["soft_limit_kw"] = computation_result.get("soft_limit_kw", result["soft_limit_kw"])

        return result
    
    @staticmethod
    async def fetch_yesterday_power_data(
        conn,
        load_table: str,
        pv_table: str,
        bess_table: str,
        load_col: str,
        pv_col: str,
        bess_col: str,
        ts_col: str = "timestamp"
    ):
        """
        Fetch yesterday's load, pv and bess power values.
        """

        yesterday_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        yesterday_end = yesterday_start + timedelta(days=1)

        query = f"""
        SELECT 
            l.{ts_col} as ts,
            l.{load_col} as load_power,
            p.{pv_col} as pv_power,
            b.{bess_col} as bess_power
        FROM {load_table} l
        LEFT JOIN {pv_table} p ON l.{ts_col} = p.{ts_col}
        LEFT JOIN {bess_table} b ON l.{ts_col} = b.{ts_col}
        WHERE l.{ts_col} >= $1
        AND l.{ts_col} < $2
        ORDER BY l.{ts_col}
        """

        rows = await conn.fetch(query, yesterday_start, yesterday_end)

        result = []

        for r in rows:
            result.append({
                "ts": r["ts"].isoformat(),
                "load_kw": r["load_power"] or 0,
                "pv_kw": r["pv_power"] or 0,
                "bess_kw": r["bess_power"] or 0
            })

        return result
     
    @staticmethod
    async def store_daily_snapshot(config):
        import os
        import json

        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        day_before_yesterday = yesterday - timedelta(days=1)

        cache_dir = "/data/cache"
        os.makedirs(cache_dir, exist_ok=True)

        day_before_yesterday_file = f"{cache_dir}/{day_before_yesterday}_snapshot.json"
        yesterday_file = f"{cache_dir}/{yesterday}_snapshot.json"

        # -------------------------------------------------
        # If today's snapshot already exists → SKIP
        # -------------------------------------------------
        if os.path.exists(yesterday_file):
            print(f"[SNAPSHOT] yesterday's snapshot already exists → Skipping")
            return yesterday_file

        # -------------------------------------------------
        # If day_before_yesterday file exists → DELETE
        # -------------------------------------------------
        if os.path.exists(day_before_yesterday_file):
            print(f"[SNAPSHOT] Removing old snapshot file")
            os.remove(day_before_yesterday_file)

        # -------------------------------------------------
        # Fetch yesterday data
        # -------------------------------------------------

        async with A_DB as conn:
            data = await PeakShavingStrategyType7.fetch_yesterday_power_data(
                conn,
                load_table=config["load_table"],
                pv_table=config["pv_table"],
                bess_table=config["bess_table"],
                load_col=config["load_column"],
                pv_col=config["pv_column"],
                bess_col=config["bess_column"]
            )

        with open(yesterday_file, "w") as f:
            json.dump(data, f)

        return yesterday_file
    
    @staticmethod
    def calculate_dynamic_peak_shaving(
        windows: dict,
        battery_capacity: float,
        current_soc: float,
        max_soc: float,
        efficiency: float,
        contractual_limit_kw: float,
        load_rise_kw_per_sec,
        pv_drop_kw_sec,
        ems_response_time_delay_sec
    ):
        """
        Calculates dynamic peak shaving threshold and energy sufficiency.
        """

        now = datetime.now()

        # Get next peak window
        peak_window = PeakShavingStrategyType7.get_next_peak_window_state(windows=windows)

        if not peak_window:
            return {"error": "No upcoming peak window found"}

        # Ensure we are subtracting datetime objects, not tuples
        start = peak_window["startTs"]
        end = peak_window["endTs"]

        # If they accidentally became tuples, grab the first element
        if isinstance(start, tuple): start = start[0]
        if isinstance(end, tuple): end = end[0]

        # Calculate duration in hours
        peak_hours = (end - start).total_seconds() / 3600
        # Usable energy (kWh)
        usable_energy_kwh = (
            battery_capacity * ((max_soc - current_soc) / 100) * efficiency
        )

        # p_max calculation
        p_max_kw = min(usable_energy_kwh / peak_hours, peak_window["load"] - contractual_limit_kw) if peak_hours > 0 else 1

        # Dynamic PS limit
        dynamic_threshold_limit_kw = contractual_limit_kw - p_max_kw


        # Worst case net ramp rate
        r_net = load_rise_kw_per_sec + pv_drop_kw_sec
        overshoot = r_net * ems_response_time_delay_sec

        # Soft Limit Calculation
        soft_limit_kw = contractual_limit_kw - overshoot  # If include safety factor then (1.5 * overshoot)

        # Writing Dynamic Thresholds KW
        PeakShavingStrategyType7.write_dynamic_threshold_to_json(dynamic_threshold_limit_kw, soft_limit_kw)

        # Required energy to shave full peak
        required_energy_kwh = p_max_kw * peak_hours

        sufficient = usable_energy_kwh >= required_energy_kwh
        shortfall_kwh = 0 if sufficient else required_energy_kwh - usable_energy_kwh

        # Get next window
        next_window = PeakShavingStrategyType7.get_next_load_window(windows)

        charging_possible = False
        charging_sufficient = None
        max_possible_charge_kwh = 0

        if next_window and next_window["state"] == "off_peak":
            # Ensure we are subtracting datetime objects, not tuples
            start = next_window["startTs"]
            end = next_window["endTs"]

            # If they accidentally became tuples, grab the first element
            if isinstance(start, tuple): start = start[0]
            if isinstance(end, tuple): end = end[0]

            # Calculate duration in hours
            charge_hours = (
               end  - start
            ).total_seconds() / 3600

            # Max possible charge during off-peak
            max_possible_charge_kwh = min(
                (battery_capacity * efficiency), contractual_limit_kw - next_window["load"]
            ) * (charge_hours)

            charging_possible = True
            charging_sufficient = max_possible_charge_kwh >= shortfall_kwh

        return {
            "next_peak_window": peak_window,
            "peak_duration_hours": peak_hours,
            "usable_energy_kwh": usable_energy_kwh,
            "p_max_kw": p_max_kw,
            "contractual_limit_kw": contractual_limit_kw,
            "dynamic_ps_limit_kw": dynamic_threshold_limit_kw,
            "soft_limit_kw": soft_limit_kw,
            "energy_sufficient": sufficient,
            "energy_shortfall_kwh": shortfall_kwh,
            "next_window": next_window,
            "charging_possible_in_next_window": charging_possible,
            "charging_sufficient_for_shortfall": charging_sufficient,
            "max_possible_charge_kwh_during_shorfall": max_possible_charge_kwh
        }
    
    @staticmethod
    def write_dynamic_threshold_to_json(
        dynamic_threshold_kw: float,
        soft_limit_kw: float
    ):
        """
        Writes dynamic peak shaving threshold into JSON file
        under key: 'peak_shaving_threshold_kw'
        """
        json_path="controller_layer/strategies/strategy_configuration/PeakShaving.json"

        # Resolve path
        path = DataPaths.resolve(json_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {json_path}")

        # Load existing JSON
        with open(path, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                raise ValueError(f"Invalid JSON format in {json_path}")

        # Update threshold
        data["peak_shaving_threshold_kw"] = round(dynamic_threshold_kw, 3)
        data["advanced_settings"]["soft_limit_kw"] = round(soft_limit_kw, 3)

        # Write back safely
        with open(path, "w") as f:
            json.dump(data, f, indent=4)

        return {
            "status": "success",
            "written_threshold_kw": dynamic_threshold_kw,
            "file": str(path)
        }
    
    @staticmethod
    def simulate_daily_optimization(controller_device_id: str):
        """
        Simulates EMS peak shaving + charging strategy
        for all remaining windows today.
        """

        # ============================================================
        # 1. Load Peak Shaving Settings
        # ============================================================
        cfg = PeakShavingStrategyType7.load_peak_shaving_settings()

        windows = cfg["load_windows"]
        contractual_limit_kw = cfg["advanced_settings"]["hard_limit_kw"]

        # ============================================================
        # 2. Live BESS Data
        # ============================================================
        resolver = LatestTelemetryResolver()
        device_data = resolver.get_latest_device_data(controller_device_id)
        current_soc = float(device_data["SoC"]["value"])

        # ============================================================
        # 3. Load BESS Config
        # ============================================================
        controller_static = ControllerStaticResolver()
        bess = load_bess_params_setting()
        min_soc      = bess["min_soc"]
        max_soc      = bess["max_soc"]
        battery_capacity = controller_static.get("battery_capacity")
        load_rise_kw_per_sec = controller_static.get("load_rise_kw_per_sec")
        pv_drop_kw_sec = controller_static.get("pv_drop_kw_sec")
        ems_response_time_delay_sec = controller_static.get("ems_response_time_delay_sec")
        efficiency = controller_static.get("efficiency")
        efficiency = efficiency if efficiency <= 1 else efficiency * 0.01
        max_charge_power_kw = bess.get("max_charge_kw")
        max_discharge_power_kw = bess.get("max_discharge_kw")

        now = datetime.now()
        today = now.date()

        results = []

        # Flatten and sort all windows
        all_windows = []

        for state, ranges in windows.items():
            for r in ranges:
                start_time = datetime.strptime(r["start_time"], "%H:%M").time()
                end_time = datetime.strptime(r["end_time"], "%H:%M").time()

                start_dt = datetime.combine(today, start_time)
                end_dt = datetime.combine(today, end_time)

                if start_time > end_time:
                    end_dt = end_dt.replace(day=end_dt.day + 1)

                if end_dt > now:  # only future windows
                    all_windows.append({
                        "state": state,
                        "start": start_dt,
                        "end": end_dt,
                        "load": r["load"]
                    })

        all_windows.sort(key=lambda x: x["start"])

        soc = current_soc
        total_energy_discharged = 0

        for window in all_windows:

            duration_hours = (window["end"] - window["start"]).total_seconds() / 3600
            if duration_hours <= 0:
                continue

            soc_before = soc

            usable_energy_kwh = (
                battery_capacity * ((soc - min_soc) / 100) * efficiency
            )

            action = "idle"
            p_max_kw = 0
            dynamic_threshold = None
            soft_limit_kw = None
            shortfall_kwh = 0
            energy_used_kwh = 0
            energy_charged_kwh = 0

            # PEAK WINDOW → DISCHARGE
            if window["state"] == "peak":

                if usable_energy_kwh > 0:

                    p_max_kw = usable_energy_kwh / duration_hours
                    p_max_kw = min(p_max_kw, window["load"] - contractual_limit_kw, max_discharge_power_kw)

                    energy_used_kwh = p_max_kw * duration_hours
                    total_energy_discharged = total_energy_discharged + energy_charged_kwh

                    dynamic_threshold = contractual_limit_kw - p_max_kw

                    # Worst case net ramp rate
                    r_net = load_rise_kw_per_sec + pv_drop_kw_sec
                    overshoot = r_net * ems_response_time_delay_sec

                    # Soft Limit Calculation
                    soft_limit_kw = contractual_limit_kw - overshoot  # If include safety factor then (1.5 * overshoot)

                    soc_drop_percent = (energy_used_kwh / battery_capacity) * 100
                    soc -= soc_drop_percent

                    action = "discharge"

                else:
                    shortfall_kwh = battery_capacity * ((min_soc - soc) / 100)

            # OFF-PEAK WINDOW → CHARGE
            elif window["state"] == "off_peak":

                available_room_kwh = (
                    battery_capacity * ((max_soc - soc) / 100) * efficiency
                )

                if available_room_kwh > 0:

                    charge_power = min(max_charge_power_kw, available_room_kwh / duration_hours, contractual_limit_kw - window["load"])

                    energy_charged_kwh = charge_power * duration_hours * efficiency

                    energy_charged_kwh = min(energy_charged_kwh, available_room_kwh)

                    soc_rise_percent = (energy_charged_kwh / battery_capacity) * 100
                    soc += soc_rise_percent

                    action = "charge"

            # MID PEAK → Optional (you can customize)
            else:
                action = "idle"

            soc = max(min(soc, max_soc), min_soc)

            results.append({
                "window_state": window["state"],
                "start": window["start"],
                "end": window["end"],
                "load": window["load"],
                "duration_hours": duration_hours,
                "soc_before_percent": round(soc_before, 2),
                "soc_after_percent": round(soc, 2),
                "action": action,
                "p_max_kw": round(p_max_kw, 2),
                "dynamic_threshold_kw": round(dynamic_threshold, 2) if dynamic_threshold else None,
                "soft_limit_kw": round(soft_limit_kw, 2) if soft_limit_kw else None,
                "energy_used_kwh": round(energy_used_kwh, 2),
                "energy_charged_kwh": round(energy_charged_kwh, 2),
                "shortfall_kwh": round(shortfall_kwh, 2),
            })

        return {
            "simulation_start": now,
            "initial_soc": current_soc,
            "final_soc": round(soc, 2),
            "total_energy_discharged": total_energy_discharged,
            "windows_simulated": len(results),
            "timeline": results
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

            logger.info(f"{LogColor.CYAN} [Peak Shaving] Executing schedule for device {device_id} {LogColor.RESET}")

            # -------------------------------------------------
            # 0. Load BESS Decision 
            # -------------------------------------------------
            poi_meter_id = "plant_1_poi_meter_1"
            config = await PeakShavingStrategyType7.bess_decision(device_id, poi_meter_id)

            (
                prevent_charging,
                block_discharging,
                hard_limit_kw,
                soft_limit_kw,
                threshold_kw,
                max_discharge_kw,
                mode,
                forecast_source,
                current_load_kw,
                max_bess_discharge_kw,
                soc,
                max_soc
            ) = (
                config["prevent_charging"],
                config["block_discharging"],
                config["hard_limit_kw"],
                config["soft_limit_kw"],
                config["threshold_kw"],
                config["max_discharge_power_kw"],
                config["mode"],
                config["forecast_source"],
                config["current_load_kw"],
                config["max_bess_discharge_kw"],
                config["soc"],
                config["max_soc"]
            )

            # ---------------------------------------
            # 1. Extract schedule fields
            # ---------------------------------------
            load = schedule.get("load_power", threshold_kw)
            power_kw = schedule.get("power")

            if not all([load, power_kw]):
                logger.error(f"[Peak Shaving] Invalid schedule payload: {schedule}")
                return

            # ---------------------------------------
            # 2. Get live values
            # ---------------------------------------
            device_data = resolver.get_latest_device_data(device_id)
            if not device_data:
                logger.error(f"[Peak Shaving] No live data for {device_id}")
                return
            
            # soc = device_data["SoC"]["value"]

            # ---------------------------------------
            # 3. Smart decision-making
            # ---------------------------------------
            if signal == "discharge":
                # DISCHARGE LOGIC
                logger.info(f"{LogColor.CYAN} [Peak Shaving] Discharge event triggered {LogColor.RESET}")

                if block_discharging:
                    logger.info("[Peak Shaving] Discharging blocked by decision logic.")
                    return
                
                required_kw = current_load_kw - load
                discharge_kw = min(required_kw, max_bess_discharge_kw, max_discharge_kw)

                await PeakShavingDischargeStrategy(
                    resolver=resolver,
                    controller_static=controller_static,
                    server_ip=server_ip,
                    server_port=server_port,
                    channel=channel,
                    slave_id=slave_id,
                    device_id=device_id,
                    excess_load=discharge_kw,
                    poi_meter_id=poi_meter_id
                )

            else:
                # CHARGE LOGIC
                logger.info(f"{LogColor.CYAN} [Peak Shaving] Charge event triggered {LogColor.RESET}")

                if prevent_charging:
                    logger.info("[Peak Shaving] Charging prevented by decision logic.")
                    return
                
                available_kw = current_load_kw - threshold_kw

                logger.info(f"{LogColor.MAGENTA} Available kw is : {available_kw} {LogColor.RESET}")

                await PeakShavingChargingStrategy(
                    resolver=resolver,
                    controller_static=controller_static,
                    device_id=device_id,
                    channel=channel,
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    available_kw=available_kw,
                    max_soc=max_soc,
                    extra_info={"strategy_id": 3}
                )
                

            logger.info("[Peak Shaving] No action required. Conditions not met.")
        except Exception as e:
            logger.error(f"{LogColor.RED} [Peak Shaving] Error executing schedule: {e} {LogColor.RESET}")
            return

    @staticmethod
    async def peak_shaving_as_constraint(device_id: str, poi_meter_id: str, block_discharging: bool, server_ip: str, server_port: int, channel: int, slave_id: int):
        """
        CENTRALIZED Peak Shaving as Constraint ENGINE
        Reads configs + live data and if Condition met, it discharge to reduce load else returns no action.
        """

        try:
            logger.info(f"{LogColor.MAGENTA} Starting [PeakShaving as Constraint] executing... {LogColor.RESET}")

            strategy_id = 3
            # ---------------------------------
            # Inserting Starting Execution Log
            # ---------------------------------
            # try :
                
            #     pool = await get_async_connection()
            #     async with pool.acquire() as conn:

            #         # create execution log
            #         exec_log = ExecutionLog(strategy_id=strategy_id, started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), input_snapshot={"live": {"soc" : soc, "current_load" : current_load_kw}, "config": {}, "flags": {}})
            #         exec_id = await create_execution_log(conn, exec_log)
            #     await pool.close()
            # except Exception as e:
            #     logger.info(f"{LogColor.RED} Error While Creating Log Exec. {e} {LogColor.RESET}")

            config = await PeakShavingStrategyType7.bess_decision(device_id, poi_meter_id)

            (
                threshold_kw,
                hard_limit_kw,
                soft_limit_kw,
                max_discharge_kw,
                current_load_kw,
                max_bess_discharge_kw,
                pv_power_kw,
                bess_power_kw,
                force_hard,
                enforce_soft,
                prevent_charging,
                soc,
                max_soc
            ) = (

                config["threshold_kw"],
                config["hard_limit_kw"],
                config["soft_limit_kw"],
                config["max_discharge_power_kw"],
                config["current_load_kw"],
                config["max_bess_discharge_kw"],
                config["pv_power_kw"],
                config["bess_power_kw"],
                config["force_hard"],
                config["enforce_soft"],
                config["prevent_charging"],
                config["soc"],
                config["max_soc"]
            )

            # -----------------------------------------------------------------------------
            # ***************************** EQUATIONS *************************************
            # -----------------------------------------------------------------------------

            grid_power = current_load_kw - pv_power_kw - bess_power_kw

            pv_surplus_power_kw = pv_power_kw - current_load_kw

            if grid_power > threshold_kw and not block_discharging:
                # Reduce load to soft limit
                required_kw = grid_power - threshold_kw
                discharge_kw = min(required_kw, max_bess_discharge_kw, max_discharge_kw)

                logger.info(f"{LogColor.RED} Above Peak Shaving Threshold limit → Discharging {discharge_kw:.2f} kW {LogColor.RESET}")

                result = await PeakShavingDischargeStrategy(
                    resolver=LatestTelemetryResolver(),
                    controller_static=ControllerStaticResolver(),
                    server_ip=server_ip,
                    server_port=server_port,
                    channel=channel,
                    slave_id=slave_id,
                    device_id=device_id,
                    excess_load=discharge_kw,
                    poi_meter_id=poi_meter_id,
                    extra_info={"force_hard": force_hard}
                )
                # pool = await get_async_connection()
                # async with pool.acquire() as conn:
                #     await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
                logger.info(f"[PeakShaving as Constraint] Strategy execution completed.")
                return result
            
            elif grid_power > soft_limit_kw and enforce_soft and not block_discharging:
                required_kw = grid_power - soft_limit_kw
                discharge_kw = min(required_kw, max_bess_discharge_kw, max_discharge_kw)

                logger.info(f"{LogColor.RED} Above soft limit → Discharging {discharge_kw:.2f} kW {LogColor.RESET}")

                result = await PeakShavingDischargeStrategy(
                    resolver=LatestTelemetryResolver(),
                    controller_static=ControllerStaticResolver(),
                    server_ip=server_ip,
                    server_port=server_port,
                    channel=channel,
                    slave_id=slave_id,
                    device_id=device_id,
                    excess_load=discharge_kw,
                    poi_meter_id=poi_meter_id,
                    extra_info={"force_hard": force_hard}
                )
                # pool = await get_async_connection()
                # async with pool.acquire() as conn:
                #     await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
                logger.info(f"[PeakShaving as Constraint] Strategy execution completed.")
                return result
            
            elif grid_power > hard_limit_kw and not block_discharging:
                required_kw = grid_power - hard_limit_kw
                discharge_kw = min(required_kw, max_bess_discharge_kw, max_discharge_kw)

                logger.info(f"{LogColor.RED} Above Hard limit → Discharging {discharge_kw:.2f} kW {LogColor.RESET}")

                result = await PeakShavingDischargeStrategy(
                    resolver=LatestTelemetryResolver(),
                    controller_static=ControllerStaticResolver(),
                    server_ip=server_ip,
                    server_port=server_port,
                    channel=channel,
                    slave_id=slave_id,
                    device_id=device_id,
                    excess_load=discharge_kw,
                    poi_meter_id=poi_meter_id,
                    extra_info={"force_hard": force_hard}
                )
                # pool = await get_async_connection()
                # async with pool.acquire() as conn:
                #     await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
                logger.info(f"[PeakShaving as Constraint] Strategy execution completed.")
                return result


            if pv_surplus_power_kw > 0 and not prevent_charging:
                logger.info(f"{LogColor.MAGENTA} Available kw is : {pv_surplus_power_kw} {LogColor.RESET}")
                
                result = await PeakShavingChargingStrategy(
                    resolver=LatestTelemetryResolver(),
                    controller_static=ControllerStaticResolver(),
                    device_id=device_id,
                    channel=channel,
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    available_kw=pv_surplus_power_kw,
                    max_soc=max_soc
                )
                # pool = await get_async_connection()
                # async with pool.acquire() as conn:
                #     await finish_execution_log(conn, exec_id, finished_at=datetime.now(), decision_snapshot=result if result else {}, error_text="")
                # logger.info(f"[PeakShavingStrategyType7] Strategy execution completed.")
                return result
            
            elif pv_surplus_power_kw > 0 and soc >= max_soc:
                logger.info(f"{LogColor.MAGENTA} Load Power is less then PV Power but battery is at MAX. Action Curtailment of PV {LogColor.RESET}")
                return await PeakShavingStrategyType7.pv_power_curtailment(pv_power_kw, pv_surplus_power_kw, grid_power, current_load_kw)

        except Exception as e:
            logger.info(f"{LogColor.RED} Error in Peak Shaving as Constraint: {e} {LogColor.RESET}")
            return
