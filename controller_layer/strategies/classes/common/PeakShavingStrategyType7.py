

import json
import logging
from datetime import datetime, timedelta

from controller_layer.cloud_detection.detect import CloudDetection
from controller_layer.strategies.cycle_management.load_cycle_config import load_cycle_config
from controller_layer.strategies.load_bess_params_setting import get_c_rate, load_bess_params_setting
from controller_layer.strategies.tags import Tags
from latest_telemetry_resolver import LatestTelemetryResolver
from runtime_paths import DataPaths
from statics.controller_resolver import ControllerStaticResolver

logger = logging.getLogger(__name__)

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
        current_window = get_current_load_window_state(windows)
        next_peak_window = get_next_peak_window_state(windows)
        next_load_window = get_next_load_window(windows)

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

async def bess_decision(device_id: str, poi_meter_id: str):
        """
        CENTRALIZED DECISION ENGINE
        Reads configs + live data + cycle data and returns one decision dict.
        """

        result = {}

        # ============================================================
        # 1. Load Peak Shaving Settings
        # ============================================================
        cfg = load_peak_shaving_settings()

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
        # result["pv_capacity_kw"] = controller_static.get("pv_capacity_kw")
        result["ems_response_time_delay_sec"] = controller_static.get("ems_response_time_delay_sec")
        efficiency = controller_static.get("efficiency")
        result["efficiency"] = efficiency if efficiency <= 1 else efficiency * 0.01
        result["max_charge_kw"] = bess.get("max_charge_kw")
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

        # BMS Signals
        result["bms_charge_signal"] = float((device_data[Tags.CHARGE_SIGNAL]["value"]))
        result["bms_discharge_signal"] = float((device_data[Tags.DISCHARGE_SIGNAL]["value"]))
        

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

        # Ideal Operating Range 30-40 deg C
        # Normal Charge Range 0-45 deg C
        # Normal Discharge Range -20to55 deg C
        # Module avg Temp 25 +- 5 deg C
        # HVAC : 20-25 deg C

        result["prevent_charging"] = (
            bess["charge_min_threshold"] > result["battery_temperature"] or result["battery_temperature"] > bess["charge_max_threshold"]
        )

        result["block_discharging"] = (
            bess["discharge_min_threshold"] > result["battery_temperature"] or result["battery_temperature"] > bess["discharge_max_threshold"]
        )

        if not result["prevent_charging"]:
            result["prevent_charging"] = (
                result["bms_charge_signal"] == 0
            )

        if not result["block_discharging"]:
            result["block_discharging"] = (
                result["bms_discharge_signal"] == 0
            )
        # ============================================================
        # 7. Dynamic Peak Shaving Threshold Computation
        # ============================================================
        try:

            computation_result = calculate_dynamic_peak_shaving(
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

        result["threshold_kw"] = 9000# computation_result.get("dynamic_ps_limit_kw", result["threshold_kw"])
        result["soft_limit_kw"] = computation_result.get("soft_limit_kw", result["soft_limit_kw"])

        # ============================================================
        # Cloud Detection
        # ============================================================
        result["cloud_details"] = CloudDetection.detect()

        return result

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
        peak_window = get_next_peak_window_state(windows=windows)

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
        write_dynamic_threshold_to_json(dynamic_threshold_limit_kw, soft_limit_kw)

        # Required energy to shave full peak
        required_energy_kwh = p_max_kw * peak_hours

        sufficient = usable_energy_kwh >= required_energy_kwh
        shortfall_kwh = 0 if sufficient else required_energy_kwh - usable_energy_kwh

        # Get next window
        next_window = get_next_load_window(windows)

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