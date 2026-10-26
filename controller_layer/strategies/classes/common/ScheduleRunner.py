

import json
import logging
from datetime import datetime, timedelta

from LogColor import LogColor
from controller_layer.cloud_detection.detect import CloudDetection
from controller_layer.strategies.cycle_management.get_today_cycle import get_today_cycles
from controller_layer.strategies.cycle_management.load_cycle_config import load_cycle_config
from controller_layer.strategies.load_bess_params_setting import get_c_rate, load_bess_params_setting
from controller_layer.strategies.tags import Tags
from latest_telemetry_resolver import LatestTelemetryResolver
from runtime_paths import DataPaths
from statics.controller_resolver import ControllerStaticResolver

logger = logging.getLogger(__name__)

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

        efficiency = controller_static.get("efficiency")
        efficiency = efficiency if efficiency <= 1 else efficiency * 0.01

        arbitrage_cfg = cycle_cfg["price_logic_overrides"]
        soc_cfg = cycle_cfg["soc_safety_management"]
        lockout_cfg = cycle_cfg["lockout_behaviour"]
        price_config = cycle_cfg["price_logic_overrides"]
        notify_cfg = cycle_cfg["notifications"]

        max_cycles_per_day = cycle_cfg["max_cycles_per_day"]
        cycles_used_today = 4#await get_today_cycles()

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

            soc = float(device_data[Tags.BATTERY_SOC]["value"])
            bess_power = float(device_data[Tags.BATTERY_POWER]["value"])
            current_tariff = 3# float(self.metadata_resolver.get_latest_tariff())
            battery_temperature = device_data.get("Battery_Temperature", {}).get("value", 25)
        
            # Usable Discharge Power of Battery
            max_usable_kw = get_c_rate(battery_temperature, bess_cfg) * (battery_capacity * (soc - min_soc) / 100) * efficiency

            # BMS Signals
            bms_charge_signal = float((device_data[Tags.CHARGE_SIGNAL]["value"]))
            bms_discharge_signal = float((device_data[Tags.DISCHARGE_SIGNAL]["value"]))            

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


        prevent_charging = (
                bess_cfg["charge_min_threshold"] > battery_temperature or battery_temperature > bess_cfg["charge_max_threshold"]
        )

        block_discharging = (
                bess_cfg["discharge_min_threshold"] > battery_temperature or battery_temperature > bess_cfg["discharge_max_threshold"]
        )

        if not prevent_charging:
            prevent_charging = (
                bms_charge_signal == 0
            )

        if not block_discharging:
            block_discharging = (
                bms_discharge_signal == 0
            )

        # ----------------------------
        # 5. OPTIONAL NOTIFICATIONS
        # ----------------------------
        if notify_cfg["notify_at_80_percent_usage"]:
            usage_percent = (cycles_used_today / max_cycles_per_day if max_cycles_per_day != 0 else 1) * 100
            if usage_percent >= 80:
                logger.info(f"[NOTICE] 80% cycles consumed today")

        # ============================================================
        # Cloud Detection
        # ============================================================
        cloud_details = CloudDetection.detect()

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
            "efficiency": efficiency,
            "arbitrage_cfg" : arbitrage_cfg,
            "soc_cfg" : soc_cfg,
            "bess_power_kw" : bess_power,
            "current_tariff" : current_tariff,
            "soc" : soc,
            "battery_temperature": battery_temperature,
            "disable_low_price_charging" : disable_low_price_charging,
            "disable_arbitrage" : disable_arbitrage,
            "disable_aggrassive_charging" : disable_aggrassive_charging,
            "cloud_details": cloud_details,
            "max_usable_kw": max_usable_kw
        }