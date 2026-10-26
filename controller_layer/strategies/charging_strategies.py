import logging
from controller_layer.strategies.classes.common.PeakShavingStrategy import bess_decision
from controller_layer.strategies.classes.common.ScheduleRunner import bess_decision as bess_decision_schedule
from controller_layer.strategies.classes.common.PeakShavingStrategyType7 import bess_decision as bess_decisionType7
from controller_layer.strategies.constants import DeviceID, StrategyID
from controller_layer.strategies.tags import Tags
from utils import ModbusWriter
from typing import Dict, List

logger = logging.getLogger(__name__)


import pytz
import asyncio
from datetime import datetime, timezone
from apscheduler.triggers.date import DateTrigger
from apscheduler.schedulers.background import BackgroundScheduler
from LogColor import LogColor
from utils.loop_manager import get_main_loop

from .load_bess_params_setting import load_bess_params_setting, get_c_rate

from ..authority.authority_manager import AuthorityManager

# db
from model import get_async_connection

# Model
from model import SetpointWrite

# Repo'
from repository import write_setpoint_record

# Global Timezone
IST = pytz.timezone("Asia/Kolkata")

# Timezone
IST = pytz.timezone("Asia/Kolkata")


# Global task deduplication
scheduled_task_ids = set()

# APScheduler singleton
scheduler = BackgroundScheduler()
# scheduler = AsyncIOScheduler()
scheduler.start()

# Calling main loop
main_loop = get_main_loop()

# --------------------------------------------------------------------------------
# **************** Charging means import from somewhere means -ive ***************
# --------------------------------------------------------------------------------

async def ChargingBasicStrategy(
    resolver,
    controller_static,
    power_kw,
    startTs,
    endTs,
    device_id,
    channel,
    server_ip,
    server_port,
    slave_id,
    extra_info=None,
    authority="internal",
):
    """
    Basic charging strategy - POWER based (Option B).
    Writes a positive charge setpoint and loops until actual charging power reaches
    the requested setpoint within tolerance. Stops also on authority takeover or safety.
    """
    logger.info(f"{LogColor.MAGENTA} BASIC CHARGE (POWER-BASED) STARTED {LogColor.RESET}")

    # get device snapshot
    device_data = resolver.get_latest_device_data(device_id)
    if not device_data:
        logger.error("Missing device data")
        return {"error": "Missing device data"}
    
    # if Excess Charge is True
    if extra_info and extra_info.get("excess_charge", False):
        logger.info(f"{LogColor.YELLOW} Excess charge flag is True - overriding power to max_charge_kw {LogColor.RESET}")
        
        # Registers ------ later will be for PV
        power_mode_register = device_data["PowerMode"]["register"]
        power_mode_data_type = device_data["PowerMode"]["dataType"]
        charge_register = device_data["DischargeAH"]["register"] 
        charge_data_type = device_data["DischargeAH"]["dataType"]

        # Pre-flight authority check
        if not AuthorityManager.can_proceed(authority):
            logger.info(f"{LogColor.RED}[AUTHORITY] Blocked — cannot start charging{LogColor.RESET}")
            return {"status": "blocked", "reason": "authority"}
        
        # Write Mode=1 then setpoint
        await ModbusWriter.write_to_register(
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=power_mode_register,
            data_type=power_mode_data_type,
            value=1
        )
        # PV setpoint
        await ModbusWriter.write_to_register(
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=charge_register,
            data_type=charge_data_type,
            value=extra_info.get("pv_setpoint_adjustment", {}).get("new_setpoint", 0)  # default to 0 if missing
        )


    efficiency = controller_static.get("efficiency")
    power_rating_kw = controller_static.get("power_rating_kw")
    battery_capacity = controller_static.get("battery_capacity")


    device_data = resolver.get_latest_device_data(device_id)
    current_soc = device_data["SoC"]["value"]   #70  # Ideally from 

    # Battery Temperature
    battery_temp = device_data.get("Battery_Temperature", {}).get("value", 25)  # default to 25°C if not available

    # call bess settings
    bess_params = load_bess_params_setting()

    min_soc = bess_params["min_soc"]
    max_soc = bess_params["max_soc"]
    max_charge_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (max_soc - min_soc) / 100)
    max_usable_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (max_soc - min_soc) / 100) * efficiency

    # Clamp requested power
    requested_kw = min(abs(power_kw), max_charge_kw, max_usable_kw)  # positive for charge

    # Registers
    power_mode_register = device_data["PowerMode"]["register"]
    power_mode_data_type = device_data["PowerMode"]["dataType"]
    charge_register = device_data["ChargeAH"]["register"]
    charge_data_type = device_data["ChargeAH"]["dataType"]

    intial_power = 200 #device_data["ac_active_power"]["value"]

    # Pre-flight authority check
    if not AuthorityManager.can_proceed(authority):
        logger.info(f"{LogColor.RED}[AUTHORITY] Blocked — cannot start charging{LogColor.RESET}")
        return {"status": "blocked", "reason": "authority"}

    # Write Mode=1 then setpoint
    await ModbusWriter.write_to_register(
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        register=power_mode_register,
        data_type=power_mode_data_type,
        value=1
    )
    await ModbusWriter.write_to_register(
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        register=charge_register,
        data_type=charge_data_type,
        value=requested_kw
    )

    # Audit write
    sp_rec = SetpointWrite(
        strategy_id=4,
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        register_mode=1,
        setpoint_value=requested_kw,
        unit="kW",
        device_id=device_id,
        success=True,
        response_text="Initial basic charge setpoint written"
    )
    pool = await get_async_connection()
    async with pool.acquire() as conn:
        await write_setpoint_record(conn, sp_rec)

    logger.info(f"{LogColor.GREEN}[BasicCharge] Wrote initial charge setpoint {requested_kw:.2f} kW {LogColor.RESET}")

    # Poll loop: stop when actual power reaches requested_kw within tolerance
    achieved = False
    last_actual = None
    while True:
        # authority check
        if not AuthorityManager.can_proceed(authority):
            logger.info(f"{LogColor.RED}[AUTHORITY] Charge paused/stopped by higher priority{LogColor.RESET}")
            break

        # refresh device values
        device_data = resolver.get_latest_device_data(device_id)
        if not device_data:
            logger.error("Device data missing during loop — aborting")
            break

        soc = device_data.get("SoC", {}).get("value")
        actual_power = device_data.get("ac_active_power", {}).get("value")  # adjust tag if needed
        last_actual = actual_power

        logger.info(f"[BasicCharge] ActualPower={actual_power} kW | Target={requested_kw} kW | SoC={soc}%")

        # safety stop if SOC reached/exceeded
        if soc is not None and soc >= max_soc:
            logger.info("[BasicCharge] Max SOC reached — stopping charge for safety")
            break

        # achieved condition (within tolerance)
        if actual_power is not None and actual_power >= (intial_power + requested_kw):
            achieved = True
            logger.info(f"[BasicCharge] Target power achieved: actual={actual_power:.2f} kW")
            break

        await asyncio.sleep(1)

    # Final stop: set mode=0 and setpoint=0
    try:
        logger.info(f"{LogColor.GREEN}[BasicCharge] writing stop setpoints {LogColor.RESET}")
        await ModbusWriter.write_to_register(
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=power_mode_register,
            data_type=power_mode_data_type,
            value=0
        )
        await ModbusWriter.write_to_register(
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=charge_register,
            data_type=charge_data_type,
            value=0
        )
    except Exception as e:
        logger.exception(f"{LogColor.RED}[BasicCharge] Error writing stop setpoints: {e} {LogColor.RESET}")

    # Audit final stop
    sp_rec = SetpointWrite(
        strategy_id=4,
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        register_mode=0,
        setpoint_value=0,
        unit="kW",
        device_id=device_id,
        success=True,
        response_text="Stopped basic charge (power-based end)"
    )
    async with pool.acquire() as conn:
        await write_setpoint_record(conn, sp_rec)

    return {
        "status": "completed" if achieved else "stopped",
        "achieved": achieved,
        "final_actual_power": last_actual,
        "requested_power": requested_kw,
        "final_soc": soc
    }


async def tou_charging(
    resolver,
    controller_static,
    soc,
    power,
    current_tariff,
    channel, 
    server_ip, 
    server_port, 
    slave_id,
    device_id,
    min_discharge_tariff,
    loss_percent,
    diff_tariff,
    battery_capacity,
):
    """
    Flat ToU Charging Trigger

    Triggers charge setpoint if SoC < 30%, power ≥ 0, and tariff is below calculated threshold.

    Returns:
        avg power
    """
    logger.info(f"[TouCharging] Checking charge condition with SoC={soc}%, Power={power}kW, Tariff={current_tariff}")

    battery_capacity = controller_static.get("battery_capacity")

    # Battery Temperature
    data = resolver.get_latest_device_data(device_id)
    battery_temp = data.get("Battery_Temperature", {}).get("value", 25)  # default to 25°C if not available

    # call bess settings
    bess_params = load_bess_params_setting()

    min_soc = bess_params["min_soc"]
    max_soc = bess_params["max_soc"]
    max_charge_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (max_soc - min_soc) / 100)

    threshold_tariff = min_discharge_tariff - (min_discharge_tariff * loss_percent) - diff_tariff
    logger.info(f"[TouCharging] Calculated Tariff Threshold: {threshold_tariff:.2f} (Current Tariff: {current_tariff})")

    if current_tariff <= threshold_tariff:
        logger.info("[TouCharging] Charging condition met. Writing charge setpoint...")
        charge_setpoint_kw = min(battery_capacity * 0.8, max_charge_kw) # 0.8 -> 80% of battery capacity
        # Get the register address and data type for PowerMode
        power_mode_register = data["PowerMode"]["register"]
        power_mode_data_type = data["PowerMode"]["dataType"]

        charge_register = data["ChargeAH"]["register"]
        charge_data_type = data["ChargeAH"]["dataType"]

        # active mode 1 to write
        await ModbusWriter.write_to_register(
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=power_mode_register,
            data_type=power_mode_data_type,
            value=1
        )

        # charge setpoint in kw
        await ModbusWriter.write_to_register(
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=charge_register,
            data_type=charge_data_type,
            value=-charge_setpoint_kw
        )

        logger.info(f"[TouCharging] Charging setpoint of {charge_setpoint_kw} kW written.")
        return charge_setpoint_kw

    logger.info("[TouCharging] Current tariff above threshold. No charging triggered.")
    return current_tariff

async def smart_tou_charging(
    resolver,
    controller_static,
    soc,
    power,
    current_tariff,
    channel, 
    server_ip, 
    server_port, 
    slave_id,
    device_id,
    battery_capacity,
    extra_info
):
    """
    Smart ToU Charging Trigger

    Triggers charge setpoint if SoC < 30%, power ≥ 0, and tariff is below calculated threshold.

    Returns:
        avg power
    """
    logger.info(f"[TouCharging] Checking charge condition with SoC={soc}%, Power={power}kW, Tariff={current_tariff}")

    battery_capacity = controller_static.get("battery_capacity")

    # Battery Temperature
    data = resolver.get_latest_device_data(device_id)
    battery_temp = data.get("Battery_Temperature", {}).get("value", 25)  # default to 25°C if not available

    # call bess settings
    bess_params = load_bess_params_setting()

    min_soc = bess_params["min_soc"]
    max_soc = bess_params["max_soc"]
    max_charge_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (max_soc - min_soc) / 100)

    logger.info("[TouCharging] Charging condition met. Writing charge setpoint...")
    charge_setpoint_kw = min(battery_capacity * 0.8, max_charge_kw) # 0.8 -> 80% of battery capacity
    # Get the register address and data type for PowerMode
    power_mode_register = data["PowerMode"]["register"]
    power_mode_data_type = data["PowerMode"]["dataType"]

    charge_register = data["ChargeAH"]["register"]
    charge_data_type = data["ChargeAH"]["dataType"]

    # Check proirity from Authority Manager
    proceed = AuthorityManager.can_proceed("internal")
    if not proceed:
        logger.info(f"{LogColor.RED} [AUTHORITY] Execution paused due to Priority. {LogColor.RESET}")
        return

    # active mode 1 to write
    await ModbusWriter.write_to_register(
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        register=power_mode_register,
        data_type=power_mode_data_type,
        value=1
    )
    # charge setpoint in kw
    await ModbusWriter.write_to_register(
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        register=charge_register,
        data_type=charge_data_type,
        value=-charge_setpoint_kw
    )

    logger.info(f"[TouCharging] Charging setpoint of {charge_setpoint_kw} kW written.")
    sp_rec = SetpointWrite(strategy_id=extra_info.get("strategy_id"), timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), register_mode=1, setpoint_value=-charge_setpoint_kw, unit="kW", device_id=device_id, success=True, response_text="Setpoint has been written")
    pool = await get_async_connection()
    async with pool.acquire() as conn:
        await write_setpoint_record(conn, sp_rec)
    return charge_setpoint_kw


async def smart_tou_schedule_charging(
    resolver,
    controller_static,
    soc,
    power,
    current_tariff,
    channel, 
    server_ip, 
    server_port, 
    slave_id,
    device_id,
    battery_capacity,
    extra_info
):
    """
    Smart ToU Charging Trigger

    Triggers charge setpoint if SoC < 30%, power ≥ 0, and tariff is below calculated threshold.

    Returns:
        avg power
    """
    logger.info(f"{LogColor.CYAN}[TouCharging] Checking charge condition with SoC={soc}%, Power={power}kW, Tariff={current_tariff} {LogColor.RESET}")

    battery_capacity = controller_static.get("battery_capacity")

    # Battery Temperature
    device_data = resolver.get_latest_device_data(device_id)
    battery_temp = device_data.get("Battery_Temperature", {}).get("value", 25)  # default to 25°C if not available

    # call bess settings
    bess_params = load_bess_params_setting()

    min_soc = bess_params["min_soc"]
    max_soc = bess_params["max_soc"]
    max_charge_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (max_soc - min_soc) / 100)

    # Get the register address and data type for PowerMode
    charge_register = device_data["ChargeAH"]["register"]
    charge_data_type = device_data["ChargeAH"]["dataType"]
    
    power_mode_register = device_data["PowerMode"]["register"]
    power_mode_data_type = device_data["PowerMode"]["dataType"]


    inital_power = device_data["ac_active_power"]["value"]

    # Loop until target SoC reached
    while True:

        device_data = resolver.get_latest_device_data(device_id)

        soc = device_data["SoC"]["value"]
        active_power = device_data["ac_active_power"]["value"]

        logger.info(f"{LogColor.CYAN} [TouCharging] Current SoC={soc}%, Target={max_soc}%, Power={active_power} kW {LogColor.RESET}")

        # Check proirity from Authority Manager
        proceed = AuthorityManager.can_proceed(extra_info.get("authority", "internal"))
        if not proceed:
            logger.info(f"{LogColor.RED} [AUTHORITY] Execution paused due to Priority. {LogColor.RESET}")
            return

        # Stop if target reached
        if soc >= max_soc or active_power >= (inital_power + power):
            logger.info(f"[TouCharging] Target SoC reached ({soc}%). Stopping charging...")

            # Write stop: PowerMode=0 & Charge=0
            await ModbusWriter.write_to_register(
                channel_id=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                register=power_mode_register,
                data_type=power_mode_data_type,
                value=0
            )
            await ModbusWriter.write_to_register(
                channel_id=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                register=charge_register,
                data_type=charge_data_type,
                value=0
            )

            logger.info(f"[TouCharging] Charging setpoint of {charge_setpoint_kw} kW written.")

            # Save DB record
            sp_rec = SetpointWrite(
                strategy_id=extra_info.get("strategy_id"),
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                register_mode=1,
                setpoint_value=0,
                unit="kW",
                device_id=device_id,
                success=True,
                response_text="Charging stopped (target SoC reached from schedules)"
            )

            pool = await get_async_connection()
            async with pool.acquire() as conn:
                await write_setpoint_record(conn, sp_rec)

            return {
                    "status": "charging stopped",
                    "final_soc": soc,
                }  # Final SoC
        
        # Compute charge setpoint
        charge_setpoint_kw = min(
            battery_capacity * 0.8,
            max_charge_kw,
            power if power is not None else max_charge_kw
        )

        logger.info(f"{LogColor.CYAN}[TouCharging] Writing charge {charge_setpoint_kw} kW {LogColor.RESET}")

        # Write charging mode
        await ModbusWriter.write_to_register(
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=power_mode_register,
            data_type=power_mode_data_type,
            value=1  # enable charge
        )

        # Write setpoint
        await ModbusWriter.write_to_register(
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=charge_register,
            data_type=charge_data_type,
            value=-charge_setpoint_kw
        )

        # Log to DB
        sp_rec = SetpointWrite(
            strategy_id=extra_info.get("strategy_id"),
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            register_mode=1,
            setpoint_value=-charge_setpoint_kw,
            unit="kW",
            device_id=device_id,
            success=True,
            response_text="Charging setpoint updated (from schedules)"
        )

        pool = await get_async_connection()
        async with pool.acquire() as conn:
            await write_setpoint_record(conn, sp_rec)

        # Poll faster or slower as required
        await asyncio.sleep(1)


async def flat_tou_charging(
    resolver,
    controller_static,
    soc,
    power,
    current_tariff,
    channel, 
    server_ip, 
    server_port, 
    slave_id,
    device_id,
    battery_capacity,
):
    """
    Smart ToU Charging Trigger

    Triggers charge setpoint if SoC < 30%, power ≥ 0, and tariff is below calculated threshold.

    Returns:
        avg power
    """
    logger.info(f"{LogColor.BLUE} [TouCharging] Checking charge condition with SoC={soc}%, Power={power}kW, Tariff={current_tariff} {LogColor.RESET}")

    battery_capacity = controller_static.get("battery_capacity")

    # Battery Temperature
    data = resolver.get_latest_device_data(device_id)
    battery_temp = data.get("Battery_Temperature", {}).get("value", 25)  # default to 25°C if not available

    # call bess settings
    bess_params = load_bess_params_setting()

    min_soc = bess_params["min_soc"]
    max_soc = bess_params["max_soc"]
    max_charge_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (max_soc - min_soc) / 100)

    # threshold_tariff = min_discharge_tariff - (min_discharge_tariff * loss_percent) - diff_tariff
    # logger.info(f"[TouCharging] Calculated Tariff Threshold: {threshold_tariff:.2f} (Current Tariff: {current_tariff})")


    # logger.info("[TouCharging] Charging condition met. Writing charge setpoint...")
    charge_setpoint_kw = min(battery_capacity * 0.8, max_charge_kw) # 0.8 -> 80% of battery capacity
    # Get the register address and data type for PowerMode
    power_mode_register = data["PowerMode"]["register"]
    power_mode_data_type = data["PowerMode"]["dataType"]

    charge_register = data["ChargeAH"]["register"]
    charge_data_type = data["ChargeAH"]["dataType"]

    # active mode 1 to write
    await ModbusWriter.write_to_register(
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        register=power_mode_register,
        data_type=power_mode_data_type,
        value=1
    )

    # charge setpoint in kw
    await ModbusWriter.write_to_register(
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        register=charge_register,
        data_type=charge_data_type,
        value=-charge_setpoint_kw
    )

    logger.info(f"{LogColor.BLUE} [TouCharging] Charging setpoint of {charge_setpoint_kw} kW written. {LogColor.RESET}")
    return charge_setpoint_kw

async def flat_tou_schedule_charging(
    resolver,
    controller_static,
    soc,
    power,
    current_tariff,
    channel, 
    server_ip, 
    server_port, 
    slave_id,
    device_id,
    battery_capacity,
    extra_info
):
    """
    Flat ToU Charging Trigger

    Triggers charge setpoint if SoC < 30%, power ≥ 0, and tariff is below calculated threshold.

    Returns:
        avg power
    """
    logger.info(f"{LogColor.CYAN}[TouCharging] Checking charge condition with SoC={soc}%, Power={power}kW, Tariff={current_tariff} {LogColor.RESET}")

    # live device data
    battery_capacity = controller_static.get("battery_capacity")

    # Battery Temperature
    device_data = resolver.get_latest_device_data(device_id)
    battery_temp = device_data.get("Battery_Temperature", {}).get("value", 25)  # default to 25°C if not available

    # call bess settings
    bess_params = load_bess_params_setting()

    min_soc = bess_params["min_soc"]
    max_soc = bess_params["max_soc"]
    max_charge_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (max_soc - min_soc) / 100)

    # Get the register address and data type for PowerMode
    charge_register = device_data["ChargeAH"]["register"]
    charge_data_type = device_data["ChargeAH"]["dataType"]
    
    power_mode_register = device_data["PowerMode"]["register"]
    power_mode_data_type = device_data["PowerMode"]["dataType"]
    
    initial_power = device_data["ac_active_power"]["value"]

    # Loop until target SoC reached
    while True:

        device_data = resolver.get_latest_device_data(device_id)

        soc = device_data["SoC"]["value"]
        active_power = device_data["ac_active_power"]["value"]

        logger.info(f"{LogColor.CYAN} [TouCharging] Current SoC={soc}%, Target={max_soc}%, Power={active_power} kW {LogColor.RESET}")

        # Check proirity from Authority Manager
        proceed = AuthorityManager.can_proceed(extra_info.get("authority", "internal"))
        if not proceed:
            logger.info(f"{LogColor.RED} [AUTHORITY] Execution paused due to Priority. {LogColor.RESET}")
            return

        # Stop if target reached
        if soc >= max_soc or active_power >= (initial_power + power):
            logger.info(f"[TouCharging] Target SoC reached ({soc}%). Stopping charging...")

            # Write stop: PowerMode=0 & Charge=0
            await ModbusWriter.write_to_register(
                channel_id=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                register=power_mode_register,
                data_type=power_mode_data_type,
                value=0
            )
            await ModbusWriter.write_to_register(
                channel_id=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                register=charge_register,
                data_type=charge_data_type,
                value=0
            )

            logger.info(f"[TouCharging] Charging setpoint of {charge_setpoint_kw} kW written.")

            # Save DB record
            sp_rec = SetpointWrite(
                strategy_id=extra_info.get("strategy_id"),
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                register_mode=1,
                setpoint_value=0,
                unit="kW",
                device_id=device_id,
                success=True,
                response_text="Charging stopped (target SoC reached from schedules)"
            )

            pool = await get_async_connection()
            async with pool.acquire() as conn:
                await write_setpoint_record(conn, sp_rec)

            return {
                    "status": "charging stopped",
                    "final_soc": soc,
                }  # Final SoC
        
        # Compute charge setpoint
        charge_setpoint_kw = min(
            battery_capacity * 0.8,
            max_charge_kw,
            power if power is not None else max_charge_kw
        )

        logger.info(f"{LogColor.CYAN}[TouCharging] Writing charge {charge_setpoint_kw} kW {LogColor.RESET}")

        # Write charging mode
        await ModbusWriter.write_to_register(
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=power_mode_register,
            data_type=power_mode_data_type,
            value=1  # enable charge
        )

        # Write setpoint
        await ModbusWriter.write_to_register(
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=charge_register,
            data_type=charge_data_type,
            value=-charge_setpoint_kw
        )

        # Log to DB
        sp_rec = SetpointWrite(
            strategy_id=extra_info.get("strategy_id"),
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            register_mode=1,
            setpoint_value=-charge_setpoint_kw,
            unit="kW",
            device_id=device_id,
            success=True,
            response_text="Charging setpoint updated (from schedules)"
        )

        pool = await get_async_connection()
        async with pool.acquire() as conn:
            await write_setpoint_record(conn, sp_rec)

        # Poll faster or slower as required
        await asyncio.sleep(1)


async def PeakShavingChargingStrategy(
    resolver,
    controller_static,
    device_id: str,
    channel: str,
    server_ip: str,
    server_port: int,
    slave_id: int,
    available_kw: float,
    max_soc : float,
    extra_info: dict = None
) -> List[Dict]:

    extra_info = extra_info or {}

    logger.info(f"[PeakShavingCharging] Starting Peak Shaving charging loop.")

    # DB storing result
    results = []

    # Static params
    battery_capacity = controller_static.get("battery_capacity")
    efficiency = controller_static.get("efficiency")

    # Battery Temperature
    data = resolver.get_latest_device_data(device_id)
    battery_temp = data.get("Battery_Temperature", {}).get("value", 25)  # default to 25°C if not available
    current_soc = data.get(Tags.BATTERY_SOC).get("value")

    # call bess settings
    bess_params = load_bess_params_setting()

    min_soc = bess_params["min_soc"]
    max_soc = bess_params["max_soc"]
    max_charge_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (max_soc - min_soc) / 100)

    # ========= COMPUTE NEW CHARGE SETPOINT ========= #

    # (kWh battery space available)
    available_capacity_kwh = (
        battery_capacity * ((max_soc - current_soc) / 100) * efficiency
    )

    # charging limited by battery physical rating + energy still possible
    max_charge = min(max_charge_kw, available_capacity_kwh)

    # final, real charging power
    charge_setpoint_kw = int(
        min(available_kw, max_charge)
    )

    logger.info(
        f"{LogColor.MAGENTA}[PeakShavingCharging] Writing charge setpoint = {charge_setpoint_kw} kW {LogColor.RESET}"
    )

    # ========= WRITE TO DEVICE ========= #

    proceed = AuthorityManager.can_proceed(extra_info.get("authority", "internal"))
    if not proceed:
        logger.info(f"{LogColor.RED}[AUTHORITY] Priority blocked, aborting.{LogColor.RESET}")
        return results.append({
            "status": "paused_due_authority",
            "soc": current_soc
        })

    await _execute_slot_async(
        resolver=resolver,
        setpoint_kw=int(charge_setpoint_kw),
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        device_id=device_id,
        slot_number=1,
        extra_info={"strategy_id": 3},
    )

    results.append({
        "soc": current_soc,
        "charge_kw": charge_setpoint_kw,
        "timestamp": datetime.now().isoformat()
    })

    last_setpoint = charge_setpoint_kw
    slot_number = 1

    while True:

        await asyncio.sleep(1)   # polling interval

        poi_meter_id = "plant_1_poi_meter_1"
        try:
            config = await bess_decision(device_id, poi_meter_id)
            (
                prevent_charging,
                cloud_details,
                pv_power_kw,
                bess_power_kw,
                current_soc,
                current_load_kw,
                max_soc,
                max_charge_kw,
                efficiency,
            ) = (
                config["prevent_charging"],
                config["cloud_details"],
                config["pv_power_kw"],
                config["bess_power_kw"],
                config["soc"],
                config["current_load_kw"],
                config["max_soc"],
                config["max_charge_kw"],
                config["efficiency"],
            )
        except Exception as e:
            logger.info(f"{LogColor.RED} Error loading live params: {e} {LogColor.RESET}")
            return

        # ========= LIVE DATA ========= #

        logger.info(
            f"[PeakShavingCharging] SoC={current_soc}%, "
            f"PV Power={available_kw:.2f} kW, "
            f"Battery Power={bess_power_kw:.2f} kW"
        )

        # ========= AUTHORITY CHECK ========= #
        proceed = AuthorityManager.can_proceed(extra_info.get("authority", "internal"))
        if not proceed:
            logger.info("[AUTHORITY] Priority lock, pausing peak shaving charging.")
            results.append({
                "status": "paused_due_authority",
                "soc": current_soc
            })
            break

        # ========= STOP CONDITION ========= #
        if current_soc >= max_soc:
            logger.info(
                f"[PeakShavingCharging] Target SoC reached ({current_soc}%). Stopping charging..."
            )
            break

        if prevent_charging:
            logger.warning(f"{LogColor.RED} Charging blocked by live config! {LogColor.RESET}")
            break

        if cloud_details.get("is_cloud_detected") and not extra_info.get("cloud_status", False):
            logger.warning(f"{LogColor.RED} Charging blocked due to cloud detection! {LogColor.RESET}")
            # logger.warning(f"Cloud details: {cloud_details}")
            break

        # ------------------------------------
        # Determine available charging power
        # ------------------------------------

        pv_surplus_power_kw = max(0, pv_power_kw - current_load_kw)

        if pv_surplus_power_kw > 0:
            available_kw = pv_surplus_power_kw

        else:
            logger.info("No charging source available.")
            break

        # ------------------------------------
        # Calculate new charge setpoint
        # ------------------------------------

        available_capacity_kwh = (
            battery_capacity
            * ((max_soc - current_soc) / 100)
            * efficiency
        )

        max_charge = min(max_charge_kw, available_capacity_kwh)

        new_setpoint = int(
            min(
                available_kw,
                max_charge
            )
        )

        if new_setpoint <= 0:
            logger.info("Calculated setpoint is zero.")
            break

        # ------------------------------------
        # Update only if changed
        # ------------------------------------

        if new_setpoint != last_setpoint:

            slot_number = 1 + 1

            logger.info(
                f"{LogColor.MAGENTA} Updating charge setpoint {LogColor.RESET}"
                f"{LogColor.MAGENTA} {last_setpoint} -> {new_setpoint} kW {LogColor.RESET}"
            )

            await _execute_slot_async(
                resolver=resolver,
                setpoint_kw=int(new_setpoint),
                channel_id=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
                slot_number=slot_number,
                extra_info={"strategy_id": 3},
            )

            last_setpoint = new_setpoint

        logger.info(
            f"SoC={current_soc}% "
            f"PV={pv_surplus_power_kw:.2f}kW "
            f"Charge={last_setpoint}kW"
        )

    # ---------------------------
    # FINAL STOP
    # STOP all charging – write 0 to mode & setpoint
    # ---------------------------
    logger.info("STOPPING PEAK SHAVING CHARGE")

    await _execute_slot_async(
        resolver=resolver,
        setpoint_kw=0,
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        device_id=device_id,
        slot_number=200,
        extra_info={"strategy_id": 3},
    )

    return results        


async def PeakShavingChargingStrategyType7(
    resolver,
    controller_static,
    device_id: str,
    channel: str,
    server_ip: str,
    server_port: int,
    slave_id: int,
    available_kw: float,
    max_soc : float,
    extra_info: dict = None
) -> List[Dict]:

    extra_info = extra_info or {}

    logger.info(f"[PeakShavingCharging] Starting Peak Shaving charging loop.")

    # DB storing result
    results = []

    # Static params
    battery_capacity = controller_static.get("battery_capacity")
    efficiency = controller_static.get("efficiency")

    # Battery Temperature
    data = resolver.get_latest_device_data(device_id)
    battery_temp = data.get("Battery_Temperature", {}).get("value", 20)  # default to 25°C if not available
    current_soc = data.get(Tags.BATTERY_SOC).get("value")

    # call bess settings
    bess_params = load_bess_params_setting()

    min_soc = bess_params["min_soc"]
    max_soc = bess_params["max_soc"]
    max_charge_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (max_soc - min_soc) / 100)

    # Function that check every bank's live temp, based on that enable/disable PCS to participate via PPC

    # ========= COMPUTE NEW CHARGE SETPOINT ========= #

    # (kWh battery space available)
    available_capacity_kwh = (
        battery_capacity * ((max_soc - current_soc) / 100) * efficiency
    )

    # charging limited by battery physical rating + energy still possible
    max_charge = min(max_charge_kw, available_capacity_kwh)

    # final, real charging power
    charge_setpoint_kw = int(
        min(available_kw, max_charge)
    )

    logger.info(
        f"{LogColor.MAGENTA}[PeakShavingCharging] Writing charge setpoint = {charge_setpoint_kw} kW {LogColor.RESET}"
    )

    # ========= WRITE TO DEVICE ========= #

    proceed = AuthorityManager.can_proceed(extra_info.get("authority", "internal"))
    if not proceed:
        logger.info(f"{LogColor.RED}[AUTHORITY] Priority blocked, aborting.{LogColor.RESET}")
        return results.append({
            "status": "paused_due_authority",
            "soc": current_soc
        })

    await _execute_slot_async(
        resolver=resolver,
        setpoint_kw=int(charge_setpoint_kw),
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        device_id=device_id,
        slot_number=1,
        extra_info={"strategy_id": 3},
    )

    results.append({
        "soc": current_soc,
        "charge_kw": charge_setpoint_kw,
        "timestamp": datetime.now().isoformat()
    })

    last_setpoint = charge_setpoint_kw
    slot_number = 1

    while True:

        await asyncio.sleep(1)   # polling interval

        poi_meter_id = "plant_1_poi_meter_1"
        try:
            config = await bess_decisionType7(device_id, poi_meter_id)
            (
                prevent_charging,
                cloud_details,
                pv_power_kw,
                bess_power_kw,
                dg_power_kw,
                threshold_kw,
                current_soc,
                current_load_kw,
                max_soc,
                max_charge_kw,
                efficiency,
                optimal_diesel_output,
                dg_load_percent,
                allow_charging_during_off_peak,
                current_window,
                battery_temperature,
            ) = (
                config["prevent_charging"],
                config["cloud_details"],
                config["pv_power_kw"],
                config["bess_power_kw"],
                config["dg_power_kw"],
                config["threshold_kw"],
                config["soc"],
                config["current_load_kw"],
                config["max_soc"],
                config["max_charge_kw"],
                config["efficiency"],
                config["optimal_diesel_output"],
                config["dg_load_percent"],
                config["allow_charging_during_off_peak"],
                config["current_window"],
                config["battery_temperature"]
            )
        except Exception as e:
            logger.info(f"{LogColor.RED} Error loading live params: {e} {LogColor.RESET}")
            return

        # ========= LIVE DATA ========= #

        logger.info(
            f"[PeakShavingCharging] SoC={current_soc}%, "
            f"PV Power={pv_power_kw:.2f} kW, "
            f"Battery Power={bess_power_kw:.2f} kW"
        )

        # ========= AUTHORITY CHECK ========= #
        proceed = AuthorityManager.can_proceed(extra_info.get("authority", "internal"))
        if not proceed:
            logger.info("[AUTHORITY] Priority lock, pausing peak shaving charging.")
            results.append({
                "status": "paused_due_authority",
                "soc": current_soc
            })
            break

        # ========= STOP CONDITION ========= #
        if current_soc >= max_soc:
            logger.info(
                f"[PeakShavingCharging] Target SoC reached ({current_soc}%). Stopping charging..."
            )
            break

        if prevent_charging:
            logger.warning(f"{LogColor.RED} Charging blocked by live config! {LogColor.RESET}")
            break

        if cloud_details.get("is_cloud_detected") and not extra_info.get("cloud_status", False):
            logger.warning(f"{LogColor.RED} Charging blocked due to cloud detection! {LogColor.RESET}")
            # logger.warning(f"Cloud details: {cloud_details}")
            break

        # ------------------------------------
        # Determine available charging power
        # ------------------------------------

        max_charge_kw = get_c_rate(battery_temperature, bess_params) * (battery_capacity * (max_soc - min_soc) / 100)

        pv_surplus_power_kw = max(0, pv_power_kw - current_load_kw)

        if pv_surplus_power_kw > 0:
            available_kw = pv_surplus_power_kw

        elif dg_load_percent < 0.3:
            available_kw = max(
                0,
                optimal_diesel_output - current_load_kw
            )
        elif current_window is not None and current_window["state"] == "off-peak" and allow_charging_during_off_peak:
            available_kw = max(
                0,
                threshold_kw + pv_power_kw - current_load_kw
            )

        else:
            logger.info("No charging source available.")
            break

        # ------------------------------------
        # Calculate new charge setpoint
        # ------------------------------------

        available_capacity_kwh = (
            battery_capacity
            * ((max_soc - current_soc) / 100)
            * efficiency
        )

        max_charge = min(max_charge_kw, available_capacity_kwh)

        new_setpoint = int(
            min(
                available_kw,
                max_charge
            )
        )

        if new_setpoint <= 0:
            logger.info("Calculated setpoint is zero.")
            break

        # ------------------------------------
        # Update only if changed
        # ------------------------------------

        if new_setpoint != last_setpoint:

            slot_number = 1 + 1

            logger.info(
                f"{LogColor.MAGENTA} Updating charge setpoint {LogColor.RESET}"
                f"{LogColor.MAGENTA} {last_setpoint} -> {new_setpoint} kW {LogColor.RESET}"
            )

            await _execute_slot_async(
                resolver=resolver,
                setpoint_kw=int(new_setpoint),
                channel_id=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
                slot_number=slot_number,
                extra_info={"strategy_id": 3},
            )

            last_setpoint = new_setpoint

        logger.info(
            f"SoC={current_soc}% "
            f"Charge={last_setpoint}kW"
        )

    # ---------------------------
    # FINAL STOP
    # STOP all charging – write 0 to mode & setpoint
    # ---------------------------
    logger.info("STOPPING PEAK SHAVING CHARGE")

    await _execute_slot_async(
        resolver=resolver,
        setpoint_kw=0,
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        device_id=device_id,
        slot_number=200,
        extra_info={"strategy_id": 3},
    )

    return results        


async def ScheduleRunnerCharging(
    resolver,
    controller_static,
    power_kw,
    startTs,
    endTs,
    sch_min_soc,
    sch_max_soc,
    device_id,
    channel,
    server_ip,
    server_port,
    slave_id,
    extra_info=None,
    # authority="external",
):
    """
    Schedule charging strategy - POWER based (Option B).
    Writes a positive charge setpoint and loops until actual charging power reaches
    the requested setpoint within tolerance. Stops also on authority takeover or safety.
    """
    logger.info(f"{LogColor.MAGENTA} [Schedule] CHARGE (POWER-BASED) STARTED {LogColor.RESET}")

    extra_info = extra_info or {}

    # DB storing result
    results = []

    # Static params
    battery_capacity = controller_static.get("battery_capacity")
    efficiency = controller_static.get("efficiency")

    # Battery Temperature
    data = resolver.get_latest_device_data(device_id)
    battery_temp = data.get("Battery_Temperature", {}).get("value", 25)  # default to 25°C if not available
    avg_cell_voltage = data.get("AVG_Cell_Voltage", {}).get("value", 4.2)
    current_soc = data.get(Tags.BATTERY_SOC).get("value")

    # call bess settings
    bess_params = load_bess_params_setting()

    min_soc = bess_params["min_soc"]
    max_soc = bess_params["max_soc"]
    max_charge_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (max_soc - min_soc) / 100)

    # ========= COMPUTE NEW CHARGE SETPOINT ========= #

    # (kWh battery space available)
    available_capacity_kwh = (
        battery_capacity * ((max_soc - current_soc) / 100) * efficiency
    )

    # charging limited by battery physical rating + energy still possible
    max_charge = min(max_charge_kw, available_capacity_kwh)

    # final, real charging power
    charge_setpoint_kw = int(
        min(power_kw, max_charge)
    )

    logger.info(
        f"{LogColor.MAGENTA}[Schedule Charge] Writing charge setpoint = {charge_setpoint_kw} kW {LogColor.RESET}"
    )

    # ========= WRITE TO DEVICE ========= #

    proceed = AuthorityManager.can_proceed(extra_info.get("authority", "internal"))
    if not proceed:
        logger.info(f"{LogColor.RED}[AUTHORITY] Priority blocked, aborting.{LogColor.RESET}")
        return results.append({
            "status": "paused_due_authority",
            "soc": current_soc
        })

    await _execute_slot_async(
        resolver=resolver,
        setpoint_kw=int(charge_setpoint_kw),
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        device_id=device_id,
        slot_number=1,
        extra_info={"strategy_id": StrategyID.MANUAL_SCHEDULE.value},
    )

    results.append({
        "soc": current_soc,
        "charge_kw": charge_setpoint_kw,
        "timestamp": datetime.now().isoformat()
    })

    last_setpoint = charge_setpoint_kw
    slot_number = 1

    # Parse end time once
    end_dt = None

    if endTs:
        try:
            end_dt = datetime.strptime(
                endTs,
                "%Y-%m-%d %H:%M:%S"
            )
            logger.info(
                f"[SCHEDULE] Charging end time set to {end_dt}"
            )
        except ValueError as e:
            logger.error(
                f"[SCHEDULE] Invalid endTs '{endTs}': {e}"
            )
            end_dt = None

    while True:

        await asyncio.sleep(1)   # polling interval

        # ========= END TIME CHECK ========= #
        if end_dt is not None:
            current_time = datetime.now()

            if current_time >= end_dt:
                logger.info(
                    f"{LogColor.YELLOW}"
                    f"[SCHEDULE] End time reached "
                    f"(current={current_time}, end={end_dt}). "
                    f"Stopping charging..."
                    f"{LogColor.RESET}"
                )
                break

        poi_meter_id = DeviceID.POI_ID
        try:
            config = await bess_decision_schedule(device_id)
            (
                prevent_charging,
                cloud_details,
                bess_power_kw,
                current_soc,
                max_soc,
                max_charge_kw,
                efficiency,
                battery_temperature
            ) = (
                config["prevent_charging"],
                config["cloud_details"],
                config["bess_power_kw"],
                config["soc"],
                config["max_soc"],
                config["max_charge_kw"],
                config["efficiency"],
                config["battery_temperature"]
            )
        except Exception as e:
            logger.info(f"{LogColor.RED} Error loading live params: {e} {LogColor.RESET}")
            return

        # ========= LIVE DATA ========= #

        logger.info(
            f"[SCHEDULE] SoC={current_soc}%, "
            f"Battery Power={bess_power_kw:.2f} kW"
        )

        # ========= AUTHORITY CHECK ========= #
        proceed = AuthorityManager.can_proceed(extra_info.get("authority", "internal"))
        if not proceed:
            logger.info("[AUTHORITY] Priority lock, pausing peak shaving charging.")
            results.append({
                "status": "paused_due_authority",
                "soc": current_soc
            })
            break

        # Stop when schedule max SOC is reached.
        if sch_max_soc is not None and current_soc >= sch_max_soc:
            logger.info(
                f"[SCHEDULE] Schedule Max SoC reached "
                f"({current_soc}% >= {sch_max_soc}%). "
                f"Stopping charging..."
            )
            break

        # ========= STOP CONDITION ========= #
        if sch_max_soc is None and current_soc >= max_soc:
            logger.info(
                f"[SCHEDULE] Target SoC reached ({current_soc}%). Stopping charging..."
            )
            break

        if prevent_charging:
            logger.warning(f"{LogColor.RED} Charging blocked by live config! {LogColor.RESET}")
            break

        if cloud_details.get("is_cloud_detected") and not extra_info.get("cloud_status", False):
            logger.warning(f"{LogColor.RED} Charging blocked due to cloud detection! {LogColor.RESET}")
            # logger.warning(f"Cloud details: {cloud_details}")
            break

        # ------------------------------------
        # Determine available charging power
        # ------------------------------------
        max_charge_kw = get_c_rate(battery_temperature, bess_params) * (battery_capacity * (max_soc - min_soc) / 100)
        # ------------------------------------
        # Calculate new charge setpoint
        # ------------------------------------

        available_capacity_kwh = (
            battery_capacity
            * ((max_soc - current_soc) / 100)
            * efficiency
        )

        max_charge = min(max_charge_kw, available_capacity_kwh)

        new_setpoint = int(
            min(
                power_kw,
                max_charge
            )
        )

        if new_setpoint <= 0:
            logger.info("Calculated setpoint is zero.")
            break

        # ------------------------------------
        # Update only if changed
        # ------------------------------------

        if new_setpoint != last_setpoint:

            slot_number = 1 + 1

            logger.info(
                f"{LogColor.MAGENTA} Updating charge setpoint {LogColor.RESET}"
                f"{LogColor.MAGENTA} {last_setpoint} -> {new_setpoint} kW {LogColor.RESET}"
            )

            await _execute_slot_async(
                resolver=resolver,
                setpoint_kw=int(new_setpoint),
                channel_id=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
                slot_number=slot_number,
                extra_info={"strategy_id": StrategyID.MANUAL_SCHEDULE.value},
            )

            last_setpoint = new_setpoint

        logger.info(
            f"SoC={current_soc}% "
            f"Charge={last_setpoint}kW"
        )

    # ---------------------------
    # FINAL STOP
    # STOP all charging – write 0 to mode & setpoint
    # ---------------------------
    logger.info("STOPPING CHARGE")

    await _execute_slot_async(
        resolver=resolver,
        setpoint_kw=0,
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        device_id=device_id,
        slot_number=200,
        extra_info={"strategy_id": StrategyID.MANUAL_SCHEDULE.value},
    )

    return results        


# For scheduling setpoints
def run_coroutine_slot(**kwargs):
    """Wrapper to run async execution inside thread-safe scheduler."""
    asyncio.run_coroutine_threadsafe(
        _execute_slot_async(**kwargs),
        main_loop
    )


# for executing of writing slots
async def _execute_slot_async(
    resolver,
    setpoint_kw: float,
    channel_id: str,
    server_ip: str,
    server_port: int,
    slave_id: int,
    device_id: str,
    slot_number=None,
    extra_info=None
):
    try:
        now = datetime.now(timezone.utc)

        color = LogColor.for_slot(int(slot_number)) if isinstance(slot_number, int) else LogColor.RED

        logger.info(f"{color} {now} [Slot {slot_number}] Starting execution for {device_id} {LogColor.RESET}")

        data = resolver.get_latest_device_data(device_id)

        logger.debug(f"{color}[Slot {slot_number}] Resolved device: {data}{LogColor.RESET}")

        # Resolve registers
        power_mode_register = data[Tags.EMS_START]["register"]
        power_mode_data_type = data[Tags.EMS_START]["dataType"]

        charge_register = data[Tags.ACTIVE_POWER_COMMAND]["register"]
        charge_data_type = data[Tags.ACTIVE_POWER_COMMAND]["dataType"]


        logger.info(f"{color}[Slot {slot_number}] Writing PowerMode=1 to reg {power_mode_register}{LogColor.RESET}")
        # Step 1: set external control mode
        await ModbusWriter.write_to_register(
            channel_id=channel_id,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=power_mode_register,
            data_type=power_mode_data_type,
            value=1
        )

        logger.info(f"{color}[Slot {slot_number}] Writing Setpoint={setpoint_kw} to reg {charge_register}{LogColor.RESET}")
        # Step 2: set charge setpoint
        await ModbusWriter.write_to_register(
            channel_id=channel_id,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=charge_register,
            data_type=charge_data_type,
            value=(setpoint_kw) if (setpoint_kw) < 0 else (-1 * setpoint_kw)
        )

        logger.info(f"{color}[Slot {slot_number}] Setpoint {setpoint_kw} kW written to register {charge_register}{LogColor.RESET}")
        sp_rec = SetpointWrite(strategy_id=extra_info.get("strategy_id", 0), timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), register_mode=1, setpoint_value=setpoint_kw, unit="kW", device_id=device_id, success=True, response_text="Setpoint for Charge has been written")
        pool = await get_async_connection()
        async with pool.acquire() as conn:
            await write_setpoint_record(conn, sp_rec)
        await pool.close()

        if slot_number == 200:
            logger.info(f"{color}[Slot {slot_number}] Writing PowerMode=0 to reg {power_mode_register}{LogColor.RESET}")
            # Step 3: set external control mode
            await ModbusWriter.write_to_register(
                channel_id=channel_id,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                register=power_mode_register,
                data_type=power_mode_data_type,
                value=0
            )

            sp_rec = SetpointWrite(strategy_id=extra_info.get("strategy_id", 0), timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), register_mode=0, setpoint_value=setpoint_kw, unit="kW", device_id=device_id, success=True, response_text="Power mode is closed!")
            pool = await get_async_connection()
            async with pool.acquire() as conn:
                await write_setpoint_record(conn, sp_rec)
            await pool.close()

    except Exception as e:
        logger.error(f"{color}[Slot {slot_number}] Failed to write setpoint: {e}{LogColor.RESET}")
