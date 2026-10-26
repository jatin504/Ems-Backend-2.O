import asyncio
from latest_telemetry_resolver import LatestTelemetryResolver

from utils import ModbusWriter
from ..authority.authority_manager import AuthorityManager

# db
from model import get_async_connection

# Model
from model import SetpointWrite

# Repo'
from repository import write_setpoint_record

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from LogColor import LogColor

from runtime_paths import DataPaths

logger = logging.getLogger(__name__)


class ManualOverrideManager:

    MANUAL_FILE = DataPaths.resolve("controller_layer/manual_override/manual_override_config.json")

    @staticmethod
    def load_manual_config() -> dict:
        """Load manual override configuration."""
        if not ManualOverrideManager.MANUAL_FILE.exists():
            return {"enabled": False}

        try:
            with open(ManualOverrideManager.MANUAL_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"[ManualOverride] Failed to load config: {e}")
            return {"enabled": False}

    @staticmethod
    async def run_manual_override(channel, server_ip, server_port, slave_id, device_id):
        """
        Continuous manual control loop (highest priority).
        Stops automatically when SoC hits min/max limits.
        """

        cfg = ManualOverrideManager.load_manual_config()
        if not cfg.get("enabled", False):
            logger.info("[ManualOverride] Disabled. Exiting manual loop.")
            return
        
        # fetch latest device data
        resolver = LatestTelemetryResolver() 
        device_data = resolver.get_latest_device_data(device_id)

        mode = cfg.get("type", "charge")   # "charge" or "discharge"
        setpoint_kw = cfg.get("setpoint_kw", 0)
        min_soc    = cfg.get("min_soc", 0)
        max_soc    = cfg.get("max_soc", 100)
        ramp       = cfg.get("ramp_rate_kw_per_min", 1000)

        logger.info(
            f"{LogColor.MAGENTA} [ManualOverride] Starting MANUAL {mode.upper()} {LogColor.RESET} "
            f"Setpoint={setpoint_kw} kW  SoC Range=({min_soc}-{max_soc})%"
        )

        # 🔹 Get register definitions from device map
        charge_register        = device_data["ChargeAH"]["register"]
        charge_data_type       = device_data["ChargeAH"]["dataType"]

        # discharge_register     = device_data["DischargeAH"]["register"]
        # discharge_data_type    = device_data["DischargeAH"]["dataType"]

        power_mode_register    = device_data["PowerMode"]["register"]
        power_mode_data_type   = device_data["PowerMode"]["dataType"]

        try:

            # Continuous LOOP
            while True:

                # Check proirity from Authority Manager
                proceed = AuthorityManager.can_proceed("manual")
                if not proceed:
                    logger.info("{LogColor.RED}[AUTHORITY] Execution paused due to priority. {LogColor.RESET}")
                    return

                soc   = device_data["SoC"]["value"]
                power = device_data["ac_active_power"]["value"]

                logger.info(
                    f"{LogColor.MAGENTA}[ManualOverride] Mode={mode} SoC={soc}%  {LogColor.RESET}"
                    f"{LogColor.MAGENTA}Range=({min_soc}–{max_soc}) Power={power}kW {LogColor.RESET}"
                )

                # -------------------------
                # STOP CONDITIONS
                # -------------------------

                if mode == "charge" and soc >= max_soc:
                    logger.info(f" {LogColor.MAGENTA} [ManualOverride] Max SoC reached ({soc}%). Stopping charge. {LogColor.MAGENTA}")
                    await ManualOverrideManager._write_stop_setpoints(
                        channel, server_ip, server_port, slave_id,
                        power_mode_register, power_mode_data_type,
                        charge_register, charge_data_type
                    )
                    await ManualOverrideManager._log_setpoint(
                        device_id, 0, "Charging stopped due to max SoC"
                    )
                    # Disable manual override in config file
                    ManualOverrideManager._disable_manual_config()
                    return {
                        "status": "manualStopped",
                        "final_soc": soc,
                        "manual_disabled": True
                    }


                if mode == "discharge" and soc <= min_soc:
                    logger.info(f"{LogColor.MAGENTA} [ManualOverride] Min SoC reached ({soc}%). Stopping discharge. {LogColor.RESET}")
                    await ManualOverrideManager._write_stop_setpoints(
                        channel, server_ip, server_port, slave_id,
                        power_mode_register, power_mode_data_type,
                        charge_register, charge_data_type
                    )
                    await ManualOverrideManager._log_setpoint(
                        device_id, 0, "Discharge stopped due to min SoC"
                    )
                    # Disable manual override in config file
                    ManualOverrideManager._disable_manual_config()
                    return {
                        "status": "manualStopped",
                        "final_soc": soc,
                        "manual_disabled": True
                    }


                # -------------------------
                # ACTIVE CONTROL
                # -------------------------

                # ramp protection
                actual_kw =  setpoint_kw #min(setpoint_kw, ramp * (1/60))  # per second

                power_mode_value = 1 if mode == "charge" else 0
                register_to_write = charge_register if mode == "charge" else charge_register
                datatype_to_write = charge_data_type if mode == "charge" else charge_data_type

                # Write mode
                await ModbusWriter.write_to_register(
                    channel_id=channel,
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    register=power_mode_register,
                    data_type=power_mode_data_type,
                    value=power_mode_value
                )

                # Write setpoint
                await ModbusWriter.write_to_register(
                    channel_id=channel,
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    register=register_to_write,
                    data_type=datatype_to_write,
                    value=actual_kw  
                )

                await ManualOverrideManager._log_setpoint(
                    device_id, actual_kw, f"Manual {mode} update"
                )

                await asyncio.sleep(1)  # polling interval

        except Exception as e:
            logger.info(f"{LogColor.RED} [ManualOverride] Error in manual override loop: {e} {LogColor.RESET}")
            return
    # ---------------------------------------
    # HELPERS
    # ---------------------------------------

    @staticmethod
    async def _write_stop_setpoints(
        channel, server_ip, server_port, slave_id,
        power_mode_register, power_mode_type,
        setpoint_register, setpoint_type
    ):
        """Write ZERO setpoints to stop operations."""
        await ModbusWriter.write_to_register(
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=power_mode_register,
            data_type=power_mode_type,
            value=0
        )
        await ModbusWriter.write_to_register(
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=setpoint_register,
            data_type=setpoint_type,
            value=0
        )

    @staticmethod
    def _disable_manual_config():
        """Set enabled=false in manual_override_config.json."""
        try:
            cfg = ManualOverrideManager.load_manual_config()
            cfg["enabled"] = False

            with open(ManualOverrideManager.MANUAL_FILE, "w") as f:
                json.dump(cfg, f, indent=4)

            logger.info("[ManualOverride] Disabled manual override in config file.")

        except Exception as e:
            logger.error(f"[ManualOverride] Failed to disable manual override: {e}")


    @staticmethod
    async def _log_setpoint(device_id, kw, comment):
        """Write DB record."""
        record = SetpointWrite(
            strategy_id=1,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            register_mode=0,
            setpoint_value=kw,
            unit="kW",
            device_id=device_id,
            success=True,
            response_text=comment
        )

        pool = await get_async_connection()
        async with pool.acquire() as conn:
            await write_setpoint_record(conn, record)
