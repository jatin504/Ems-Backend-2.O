import logging
import asyncio
from typing import List, Union, Dict
from datetime import datetime, timedelta, timezone
from controller_layer.strategies.classes.common.PeakShavingStrategy import bess_decision
from controller_layer.strategies.classes.common.ScheduleRunner import bess_decision as bess_decision_schedule
from controller_layer.strategies.classes.common.PeakShavingStrategyType7 import bess_decision as bess_decisionType7
from controller_layer.strategies.constants import DeviceID, StrategyID
from controller_layer.strategies.tags import Tags
from controller_layer.strategies.utils.power_balancing import discharge_power_balancing_strategy
from device_config_helper import DeviceConfigHelper
from utils import ModbusWriter
from apscheduler.schedulers.background import BackgroundScheduler
# from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from functools import partial
import pytz
from utils.loop_manager import get_main_loop
from LogColor import LogColor

from controller_layer.strategies.load_bess_params_setting import load_bess_params_setting, get_c_rate
from controller_layer.authority.authority_manager import AuthorityManager

# db
from model import get_async_connection

# Model
from model import SetpointWrite

# Repo'
from repository import write_setpoint_record

# Get logger
logger = logging.getLogger(__name__)

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


# Device helper
device_helper = DeviceConfigHelper()


# --------------------------------------------------------------------------------
# **************** Discharging means export to somewhere means +ive ***************
# --------------------------------------------------------------------------------

async def DischargeBasicStrategy(
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
    authority="internal"
):
    """
    Basic discharge strategy (POWER based)
    → Writes power setpoint (negative for discharge)
    → Runs UNTIL actual power reaches the setpoint (within tolerance)
    → No energy check, no startTs/endTs dependency
    """

    logger.info(f"{LogColor.MAGENTA} BASIC POWER DISCHARGE STARTED {LogColor.RESET}")

    # Load device state
    device_data = resolver.get_latest_device_data(device_id)
    if not device_data:
        logger.error("Missing device data")
        return {"error": "Missing data"}

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
    max_discharge_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (max_soc - min_soc) / 100)

    # Usable Power
    max_usable_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (current_soc - min_soc) / 100) * efficiency

    # clamp requested power
    requested_kw = -min(abs(power_kw), max_discharge_kw, max_usable_kw)
    tolerance_kw = 5  # allowed deviation

    logger.info(
        f"{LogColor.GREEN} TARGET POWER = {requested_kw:.2f} kW (tolerance = ±{tolerance_kw} kW){LogColor.RESET}"
    )

    # -------------------------
    # FIRST WRITE
    # -------------------------
    if not AuthorityManager.can_proceed(authority):
        logger.info(f"{LogColor.RED} [AUTHORITY] Blocked — cannot start. {LogColor.RESET}")
        return

    await _execute_slot_async(
        resolver=resolver,
        setpoint_kw=requested_kw,
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        device_id=device_id,
        slot_number=1,
        extra_info={"strategy_id": 99},
    )

    logger.info(
        f"{LogColor.GREEN} INITIAL SETPOINT WRITTEN: {requested_kw:.2f} kW {LogColor.RESET}"
    )

    # -------------------------
    # LOOP UNTIL POWER ACHIEVED
    # -------------------------
    results = []

    while True:

        # 1. Priority check
        if not AuthorityManager.can_proceed(authority):
            logger.info(f"{LogColor.RED} [AUTHORITY] Execution stopped. {LogColor.RESET}")
            break

        # 2. Refresh device data
        device_data = resolver.get_latest_device_data(device_id)
        soc = device_data["SoC"]["value"]
        actual_power = device_data["bess_power_kw"]["value"]  # adjust tag name
        logger.info(
            f"{LogColor.CYAN} Actual = {actual_power:.2f} kW | Target = {requested_kw:.2f} kW | SoC = {soc}% {LogColor.RESET}"
        )

        # 3. Safety rule
        if soc <= min_soc:
            logger.warning("MIN SOC reached — stopping discharge")
            break

        # 4. Check if power is achieved
        if abs(actual_power - requested_kw) <= tolerance_kw:
            logger.info(
                f"{LogColor.GREEN} POWER ACHIEVED → {actual_power:.2f} kW {LogColor.RESET}"
            )
            break

        await asyncio.sleep(1)

    # -------------------------
    # FINAL STOP
    # -------------------------
    await _execute_slot_async(
        resolver=resolver,
        setpoint_kw=0,
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        device_id=device_id,
        slot_number=200,
        extra_info={"strategy_id": 99},
    )

    logger.info(f"{LogColor.RED} Discharge STOPPED. {LogColor.RESET}")

    results.append({
        "final_actual_kw": actual_power,
        "timestamp": datetime.now().isoformat(),
        "status": "completed"
    })

    return results


async def TouDischargingSmartStrategy(
    resolver,
    controller_static,
    tariffs: Union[float, List[float]],
    durations_minutes: Union[int, List[int]],
    startTs: str,
    endTs: str,
    device_id: str,
    channel,
    server_ip,
    server_port,
    slave_id,
    extra_info
    ) -> List[Dict]:
    logger.info("IN MAIN FUNCTION of smart discharge strategy")

    device_data = resolver.get_latest_device_data(device_id)

    efficiency = controller_static.get("efficiency")
    power_rating_kw = controller_static.get("power_rating_kw")
    battery_capacity = controller_static.get("battery_capacity")


    device_data = resolver.get_latest_device_data(device_id)
    current_soc = device_data["SoC"]["value"]   #70  # Ideally from 

    # Battery Temperature
    battery_temp = device_data.get("Battery_Temperature", {}).get("value", 25)  # default to 25°C if not available

    try :

        # call bess settings
        bess_params = load_bess_params_setting()

        min_soc = bess_params["min_soc"]
        max_soc = bess_params["max_soc"]
        max_discharge_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (max_soc - min_soc) / 100)

    except Exception as e:
        logger.info(f"{LogColor.RED} Error While loading BESS params {e} {LogColor.RESET}")


    tariffs = [tariffs] if isinstance(tariffs, (float, int)) else tariffs
    durations_minutes = [durations_minutes] if isinstance(durations_minutes, (float, int)) else durations_minutes

    if len(tariffs) != len(durations_minutes):
        raise ValueError("tariffs and durations_minutes length mismatch.")

    # if current_soc < min_soc:
    #     logger.warning(f"{LogColor.RED} Battery SOC ({current_soc}) too low for discharge. {LogColor.RESET}")
    #     return []

    total_dischargeable_energy = ((current_soc - min_soc) / 100) * (efficiency / 100) * battery_capacity
    logger.info(f" {LogColor.CYAN} TOTAL DISCHARGING ENERGY: {total_dischargeable_energy} current SoC: {current_soc} {LogColor.RESET}")

    total_tariff = sum(tariffs)
    slot_energies = [(t / total_tariff) * total_dischargeable_energy for t in tariffs]
    logger.info(f" {LogColor.CYAN} SLOTS of Energies: {slot_energies} {LogColor.RESET}")

    slot_powers = []
    for i, energy_kwh in enumerate(slot_energies):
        duration_hr = durations_minutes[i] / 60
        avg_power_kw = min(energy_kwh / duration_hr, power_rating_kw, max_discharge_kw)
        slot_powers.append(avg_power_kw)
    logger.info(f" {LogColor.CYAN} SLOTS of Powers: {slot_powers} {LogColor.RESET}")


    results = []
    last_setpoint = 0
    initial_energy = device_data["active_energy"]["value"]
    current_slot = 0

    # Immediately write first slot's setpoint
    last_setpoint = -slot_powers[current_slot]
    # asyncio.run_coroutine_threadsafe(

    # Check proirity from Authority Manager
    proceed = AuthorityManager.can_proceed(extra_info.get("authority", "internal"))
    if not proceed:
        logger.info(f"{LogColor.RED} [AUTHORITY] Execution paused due to Priority. {LogColor.RESET}")
        return

    await _execute_slot_async(
            resolver=resolver,
            setpoint_kw=last_setpoint,
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            device_id=device_id,
            slot_number=current_slot + 1,
            extra_info={"strategy_id" : 1}
        )
    #     main_loop
    # )
    logger.info(f" {LogColor.RED} START: Writing setpoint for Slot {current_slot+1}: {last_setpoint:.2f} kW {LogColor.RESET}")

    # Start monitoring discharged energy
    while current_slot < len(slot_energies) - 1:

        # Check proirity from Authority Manager
        proceed = AuthorityManager.can_proceed(extra_info.get("authority", "internal"))
        if not proceed:
            logger.info(f"{LogColor.RED} [AUTHORITY] Execution paused due to Priority. {LogColor.RESET}")
            return

        device_data = resolver.get_latest_device_data(device_id)
        final_energy = device_data["active_energy"]["value"]
        delta =  initial_energy - final_energy
        logger.info(f"{LogColor.CYAN} Initial Energy {initial_energy:.2f} kWh  Final Energy {final_energy:.2f} kWh delta {delta:.2f} kWh {LogColor.RESET}")
        if delta >= slot_energies[current_slot]:
            current_slot += 1
            initial_energy = final_energy
            last_setpoint = -slot_powers[current_slot]
            # asyncio.run_coroutine_threadsafe(
            await _execute_slot_async(
                    resolver=resolver,
                    setpoint_kw=last_setpoint,
                    channel_id=channel,
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    device_id=device_id,
                    slot_number=current_slot + 1,
                    extra_info={"strategy_id" : 1}
                )
            #     main_loop
            # )
            logger.info(f"TRANSITION: {delta:.2f} kWh discharged, writing Slot {current_slot+1} setpoint: {last_setpoint:.2f} kW")
            results.append({
                "slot": current_slot,
                "transition_kwh": delta,
                "setpoint_kw": last_setpoint,
                "timestamp": datetime.now().isoformat()
            })
        await asyncio.sleep(1)  # Polling interval

    # Final stop: when initial_energy >= total_dischargeable_energy or at endTs (whichever comes first)
    # stop_time = ...  # Calculate or wait until end time
    # asyncio.run_coroutine_threadsafe(
    await _execute_slot_async(
            resolver=resolver,
            setpoint_kw=0,
            channel_id=channel,
            server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        device_id=device_id,
        slot_number=200,
        extra_info={"strategy_id" : 1}
    )
    #     main_loop
    # )
    logger.info(f"STOP: Accumulated {initial_energy} kWh, stopping discharge.")
    results.append({
        "slot": "final",
        "transition_kwh": initial_energy,
        "setpoint_kw": 0,
        "timestamp": datetime.now().isoformat()
    })

    return results


async def TouDischargingFlatStrategy(
    resolver,
    controller_static,
    tariffs: Union[float, List[float]],
    durations_minutes: Union[int, List[int]],
    startTs: str,
    endTs: str,
    device_id: str,
    channel,
    server_ip,
    server_port,
    slave_id,
    extra_info
) -> List[Dict]:
    now = datetime.now(timezone.utc)
    logger.info(f"{now} IN FLAT STRATEGY FUNCTION")

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
    max_discharge_kw = get_c_rate(battery_temp, bess_params) * (battery_capacity * (max_soc - min_soc) / 100) # bess_params["max_discharge_kw"]

    # Normalize inputs
    tariffs = [tariffs] if isinstance(tariffs, (float, int)) else tariffs
    durations_minutes = [durations_minutes] if isinstance(durations_minutes, (float, int)) else durations_minutes

    if len(tariffs) != len(durations_minutes):
        raise ValueError("Tariff and duration lengths must match.")

    start_time = datetime.strptime(startTs, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    end_time = datetime.strptime(endTs, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    total_seconds = (end_time - start_time).total_seconds()

    if total_seconds <= 0:
        raise ValueError("endTs must be after startTs.")

    total_dischargeable_energy = ((current_soc - min_soc) / 100) * (efficiency / 100) * battery_capacity
    logger.info(f"TOTAL DISCHARGING ENERGY (Flat): {total_dischargeable_energy} Current SoC : {current_soc}")

    num_slots = len(durations_minutes)
    slot_energies = [total_dischargeable_energy / num_slots] * num_slots
    
    # Calculate slot powers exactly same as Smart TOU
    slot_powers = []
    for i, energy_kwh in enumerate(slot_energies):
        duration_hr = durations_minutes[i] / 60
        avg_power_kw = min(energy_kwh / duration_hr, max_discharge_kw, power_rating_kw)
        slot_powers.append(avg_power_kw)

    logger.info(f"SLOT POWERS = {slot_powers}")

    results = []

    # Start with slot 0
    current_slot = 0
    initial_energy = device_data["active_energy"]["value"]
    logger.info(f"Initial Energy: {initial_energy} kWh")

    # Write first slot immediately
    first_setpoint = -slot_powers[current_slot]
    logger.info(f"START: Writing Slot 1 setpoint = {first_setpoint:.2f} kW")

    # Check proirity from Authority Manager
    proceed = AuthorityManager.can_proceed(extra_info.get("authority", "internal"))
    if not proceed:
        logger.info(f"{LogColor.RED} [AUTHORITY] Execution paused due to Priority. {LogColor.RESET}")
        return

    await _execute_slot_async(
        resolver=resolver,
        setpoint_kw=first_setpoint,
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        device_id=device_id,
        slot_number=current_slot + 1,
        extra_info={"strategy_id" : 2}
    )

    # call power balancing strategy to get lower cap for SoC for each PCS based on health score
    pcs_device_ids = device_helper.get_device_ids_by_type_pattern("pcs_pcs_")

    pcs_wise_caps = discharge_power_balancing_strategy(
        resolver=resolver,
        pcs_device_ids=pcs_device_ids,
        min_soc=min_soc
    )

    # Now monitor in loop (same as Smart TOU)
    while current_slot < num_slots - 1:

        # Check proirity from Authority Manager
        proceed = AuthorityManager.can_proceed(extra_info.get("authority", "internal"))
        if not proceed:
            logger.info(f"{LogColor.RED} [AUTHORITY] Execution paused due to Priority. {LogColor.RESET}")
            return

        # Stop if time window is over
        if datetime.now(timezone.utc) >= end_time:
            logger.warning("EndTs reached! Stopping discharge.")
            break

        device_data = resolver.get_latest_device_data(device_id)
        final_energy = device_data["active_energy"]["value"]
        delta = initial_energy - final_energy

        logger.info(f"Slot {current_slot+1} | Needed: {slot_energies[current_slot]:.2f} kWh | Discharged: {delta:.2f} kWh")

        for id in pcs_device_ids:
            device_data = resolver.get_latest_device_data(id)

        # Check if slot target reached
        if delta >= slot_energies[current_slot]:
            current_slot += 1
            initial_energy = final_energy  # reset base reading

            new_setpoint = -slot_powers[current_slot]

            logger.info(f"TRANSITION: Slot {current_slot} → Slot {current_slot+1} | Setpoint = {new_setpoint:.2f} kW")

            await _execute_slot_async(
                resolver=resolver,
                setpoint_kw=new_setpoint,
                channel_id=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
                slot_number=current_slot + 1,
                extra_info={"strategy_id" : 2}
            )

            results.append({
                "slot": current_slot,
                "transition_kwh": delta,
                "setpoint_kw": new_setpoint,
                "timestamp": datetime.now().isoformat()
            })

        await asyncio.sleep(1)  # polling interval

    # Final stop setpoint
    logger.info("STOPPING DISCHARGE (Flat TOU reached final slot or endTs).")

    await _execute_slot_async(
        resolver=resolver,
        setpoint_kw=0,
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        device_id=device_id,
        slot_number=200,
        extra_info={"strategy_id" : 2}
    )

    results.append({
        "slot": "final",
        "setpoint_kw": 0,
        "timestamp": datetime.now().isoformat()
    })

    return results


async def PeakShavingDischargeStrategy(
    resolver,
    controller_static,
    device_id: str,
    channel,
    server_ip,
    server_port,
    slave_id,
    excess_load: float,
    poi_meter_id: str,
    target_limit: float,
    authority: str = "internal",
    extra_info: dict = None,
):
    """
    Loop-based peak shaving discharge strategy.
    Discharge continuously until target load is shaved.

    excess_load = load - threshold (positive = need discharge)
    """

    try:

        logger.info(f"{LogColor.MAGENTA} Peak Shaving Discharge STARTED {LogColor.RESET}")

        results = []

        setpoint_kw = excess_load

        # ---------------------------
        # FIRST WRITE
        # ---------------------------
        proceed = AuthorityManager.can_proceed(authority)
        if not proceed:
            logger.info(f"{LogColor.RED}[AUTHORITY] Priority blocked, aborting.{LogColor.RESET}")
            return

        logger.info(
            f"{LogColor.MAGENTA}WRITE FIRST SETPOINT = {setpoint_kw:.2f} kW{LogColor.RESET}"
        )

        await _execute_slot_async(
            resolver=resolver,
            setpoint_kw=int(setpoint_kw),
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            device_id=device_id,
            slot_number=1,
            extra_info={"strategy_id": 3},
        )

        # ---------------------------
        # LOOP UNTIL TARGET SHAVED
        # ---------------------------
        logger.info(" ENTERING DISCHARGE LOOP ...")

        slot_number = 1

        while True:
            await asyncio.sleep(1)  # polling interval

            # authority check
            proceed = AuthorityManager.can_proceed(authority)
            if not proceed:
                logger.info(f"{LogColor.RED}[AUTHORITY] Pause exit.{LogColor.RESET}")
                break

            poi_meter_id = "plant_1_poi_meter_1"
            try:
                config = await bess_decision(device_id, poi_meter_id)
                (
                    block_discharging,
                    cloud_details,
                    pv_power_kw,
                    bess_power_kw,
                    soc,
                    current_load_kw,
                    min_soc,
                    max_discharge_kw,
                    max_usable_kw,
                    efficiency
                ) = (
                    config["block_discharging"],
                    config["cloud_details"],
                    config["pv_power_kw"],
                    config["bess_power_kw"],
                    config["soc"],
                    config["current_load_kw"],
                    config["min_soc"],
                    config["max_discharge_kw"],
                    config["max_usable_kw"],
                    config["efficiency"],
                )
            except Exception as e:
                logger.info(f"{LogColor.RED} Error loading live params: {e} {LogColor.RESET}")
                return
            
            if block_discharging:
                logger.warning(f"{LogColor.RED} Discharging blocked by live config! {LogColor.RESET}")
                break

            if cloud_details.get("is_cloud_detected") and not extra_info.get("cloud_status", False):
                logger.warning(f"{LogColor.RED} Discharging blocked due to cloud detection! {LogColor.RESET}")
                # logger.warning(f"Cloud details: {cloud_details}")
                break

            grid_power = current_load_kw - pv_power_kw - bess_power_kw

            remaining_excess = grid_power - target_limit

            # remaining_excess = excess_load - battery_power #current_load - # Condition will varify
            logger.info(
                f"SOC={soc:.1f}% | Load={current_load_kw:.2f} | BESS Power={int(bess_power_kw)} | Setpoint={int(setpoint_kw)} | Grid Power={grid_power:.2f} | Remaining Excess={remaining_excess:.2f}"
            )

            # safety exit conditions
            if soc <= min_soc:
                logger.warning("MIN SOC reached, stopping discharge.")
                break

            if remaining_excess < 0 and (grid_power + int(setpoint_kw)) < target_limit:
                logger.info("Target achieved — load shaved.")
                break

            # compute next setpoint
            if abs(int(bess_power_kw)) == abs(int(setpoint_kw)) and remaining_excess != 0:

                nwe_required_kw = (abs(setpoint_kw) + remaining_excess)

                next_kw = min(nwe_required_kw, max_discharge_kw, max_usable_kw)

                # limit
                if abs(next_kw) > max_usable_kw:
                    logger.info(f"New Required Power {nwe_required_kw:.2f} kW is more than Usable Power {max_usable_kw:.2f} kW")
                    # break   # Depends on Strategies, in this DG is present so other decision maybe much usefull.

                if abs(next_kw) == 0:
                    break

                slot_number = 1 + 1

                if int(next_kw) != int(setpoint_kw):

                    logger.info(f"WRITE NEW SETPOINT = {next_kw:.2f} kW")

                    await _execute_slot_async(
                        resolver=resolver,
                        setpoint_kw=int(-next_kw),
                        channel_id=channel,
                        server_ip=server_ip,
                        server_port=server_port,
                        slave_id=slave_id,
                        device_id=device_id,
                        slot_number=slot_number,
                        extra_info={"strategy_id": 3},
                    )

                    setpoint_kw = next_kw

                    results.append(
                        {
                            "setpoint": int(next_kw),
                            "soc": soc,
                            "load": current_load_kw,
                            "timestamp": datetime.now().isoformat(),
                        }
                    )

        # ---------------------------
        # FINAL STOP
        # ---------------------------
        logger.info("STOPPING PEAK SHAVING DISCHARGE")

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

        results.append(
            {
                "setpoint": 0,
                "soc": soc,
                "load": current_load_kw,
                "timestamp": datetime.now().isoformat(),
                "final": True,
            }
        )

        return results

    except Exception as e:
        logger.info(f"{LogColor.RED} Error While Executing [PeakShavingDischargeStrategy]. {e} {LogColor.RESET}")
        return {"error": f"Error While Executing [PeakShavingDischargeStrategy]. {e}"}

async def PeakShavingDischargeStrategyType7(
    resolver,
    controller_static,
    device_id: str,
    channel,
    server_ip,
    server_port,
    slave_id,
    excess_load: float,
    poi_meter_id: str,
    target_limit: float,
    authority: str = "internal",
    extra_info: dict = None,
):
    """
    Loop-based peak shaving discharge strategy.
    Discharge continuously until target load is shaved.

    excess_load = load - threshold (positive = need discharge)
    """

    try:

        logger.info(f"{LogColor.MAGENTA} Peak Shaving Discharge STARTED {LogColor.RESET}")

        results = []

        setpoint_kw = excess_load

        # ---------------------------
        # FIRST WRITE
        # ---------------------------
        proceed = AuthorityManager.can_proceed(authority)
        if not proceed:
            logger.info(f"{LogColor.RED}[AUTHORITY] Priority blocked, aborting.{LogColor.RESET}")
            return

        logger.info(
            f"{LogColor.MAGENTA}WRITE FIRST SETPOINT = {setpoint_kw:.2f} kW{LogColor.RESET}"
        )

        await _execute_slot_async(
            resolver=resolver,
            setpoint_kw=int(setpoint_kw),
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            device_id=device_id,
            slot_number=1,
            extra_info={"strategy_id": 3},
        )

        # ---------------------------
        # LOOP UNTIL TARGET SHAVED
        # ---------------------------
        logger.info(" ENTERING DISCHARGE LOOP ...")

        while True:
            await asyncio.sleep(1)  # polling interval

            # authority check
            proceed = AuthorityManager.can_proceed(authority)
            if not proceed:
                logger.info(f"{LogColor.RED}[AUTHORITY] Pause exit.{LogColor.RESET}")
                break

            poi_meter_id = "plant_1_poi_meter_1"
            try:
                config = await bess_decisionType7(device_id, poi_meter_id)
                (
                    block_discharging,
                    cloud_details,
                    pv_power_kw,
                    bess_power_kw,
                    dg_power_kw,
                    soc,
                    current_load_kw,
                    min_soc,
                    max_discharge_kw,
                    max_usable_kw,
                    efficiency
                ) = (
                    config["block_discharging"],
                    config["cloud_details"],
                    config["pv_power_kw"],
                    config["bess_power_kw"],
                    config["dg_power_kw"],
                    config["soc"],
                    config["current_load_kw"],
                    config["min_soc"],
                    config["max_discharge_kw"],
                    config["max_usable_kw"],
                    config["efficiency"],
                )
            except Exception as e:
                logger.info(f"{LogColor.RED} Error loading live params: {e} {LogColor.RESET}")
                return
            
            if block_discharging:
                logger.warning(f"{LogColor.RED} Discharging blocked by live config! {LogColor.RESET}")
                break

            if cloud_details.get("is_cloud_detected") and not extra_info.get("cloud_status", False):
                logger.warning(f"{LogColor.RED} Discharging blocked due to cloud detection! {LogColor.RESET}")
                # logger.warning(f"Cloud details: {cloud_details}")
                break

            grid_power = current_load_kw - pv_power_kw - bess_power_kw - dg_power_kw

            remaining_excess = grid_power - target_limit

            # remaining_excess = excess_load - battery_power #current_load - # Condition will varify
            logger.info(
                f"SOC={soc:.1f}% | Load={current_load_kw:.2f} | BESS Power={int(bess_power_kw)} | Setpoint={int(setpoint_kw)} | Grid Power={grid_power:.2f} | Remaining Excess={remaining_excess:.2f}"
            )

            # safety exit conditions
            if soc <= min_soc:
                logger.warning("MIN SOC reached, stopping discharge.")
                break

            if remaining_excess < 0 and (grid_power + int(setpoint_kw)) < target_limit:
                logger.info("Target achieved — load shaved.")
                break

            # compute next setpoint
            if abs(int(bess_power_kw)) == abs(int(setpoint_kw)) and remaining_excess != 0:

                nwe_required_kw = (abs(setpoint_kw) + remaining_excess)

                next_kw = min(nwe_required_kw, max_discharge_kw, max_usable_kw)

                # limit
                if abs(next_kw) > max_usable_kw:
                    logger.info(f"New Required Power {nwe_required_kw:.2f} kW is more than Usable Power {max_usable_kw:.2f} kW")
                    break   # Depends on Strategies, in this DG is present so other decision maybe much usefull.

                if abs(next_kw) == 0 and abs(setpoint_kw) == 0:
                    break

                if int(next_kw) != int(setpoint_kw):

                    logger.info(f"WRITE NEW SETPOINT = {next_kw:.2f} kW")

                    await _execute_slot_async(
                        resolver=resolver,
                        setpoint_kw=int(-next_kw),
                        channel_id=channel,
                        server_ip=server_ip,
                        server_port=server_port,
                        slave_id=slave_id,
                        device_id=device_id,
                        slot_number=1,
                        extra_info={"strategy_id": 3},
                    )

                    setpoint_kw = next_kw

                    results.append(
                        {
                            "setpoint": int(next_kw),
                            "soc": soc,
                            "load": current_load_kw,
                            "timestamp": datetime.now().isoformat(),
                        }
                    )

        # ---------------------------
        # FINAL STOP
        # ---------------------------
        logger.info("STOPPING PEAK SHAVING DISCHARGE")

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

        results.append(
            {
                "setpoint": 0,
                "soc": soc,
                "load": current_load_kw,
                "timestamp": datetime.now().isoformat(),
                "final": True,
            }
        )

        return results

    except Exception as e:
        logger.info(f"{LogColor.RED} Error While Executing [PeakShavingDischargeStrategy]. {e} {LogColor.RESET}")
        return {"error": f"Error While Executing [PeakShavingDischargeStrategy]. {e}"}


async def ScheduleRunnerDischarging(
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
    Schedule dicharging strategy - POWER based (Option B).
    Writes a positive charge setpoint and loops until actual dicharging power reaches
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
    battery_temperature = data.get("Battery_Temperature", {}).get("value", 25)  # default to 25°C if not available
    avg_cell_voltage = data.get("AVG_Cell_Voltage", {}).get("value", 4.2)
    current_soc = data.get(Tags.BATTERY_SOC).get("value")

    # call bess settings
    bess_params = load_bess_params_setting()

    min_soc = bess_params["min_soc"]
    max_soc = bess_params["max_soc"]
    max_discharge_kw = get_c_rate(battery_temperature, bess_params) * (battery_capacity * (current_soc - min_soc) / 100) * efficiency

    # ========= COMPUTE NEW CHARGE SETPOINT ========= #

    # (kWh battery space available)
    available_capacity_kwh = (
        battery_capacity * ((current_soc - min_soc) / 100) * efficiency
    )

    # dicharging limited by battery physical rating + energy still possible
    max_discharge = min(max_discharge_kw, available_capacity_kwh)

    # final, real dicharging power
    discharge_setpoint_kw = int(
        min(power_kw, max_discharge)
    )

    logger.info(
        f"{LogColor.MAGENTA}[Schedule Discharge] Writing discharge setpoint = {discharge_setpoint_kw} kW {LogColor.RESET}"
    )

    # ========= WRITE TO DEVICE ========= #

    proceed = AuthorityManager.can_proceed(extra_info.get("authority", "internal"))
    if not proceed:
        logger.info(f"{LogColor.RED}[AUTHORITY] Priority blocked, aborting.{LogColor.RESET}")
        return results.append({
            "status": "paused_due_authority",
            "soc": current_soc
        })
    
    # waiting for close other execution if running
    asyncio.sleep(1)

    await _execute_slot_async(
        resolver=resolver,
        setpoint_kw=int(discharge_setpoint_kw),
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
        "charge_kw": discharge_setpoint_kw,
        "timestamp": datetime.now().isoformat()
    })

    last_setpoint = discharge_setpoint_kw
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
                f"[SCHEDULE] diCharging end time set to {end_dt}"
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
                    f"Stopping dicharging..."
                    f"{LogColor.RESET}"
                )
                break

        poi_meter_id = DeviceID.POI_ID
        try:
            config = await bess_decision_schedule(device_id)
            (
                block_discharging,
                cloud_details,
                bess_power_kw,
                current_soc,
                min_soc,
                max_discharge_kw,
                efficiency,
                battery_temperature
            ) = (
                config["block_discharging"],
                config["cloud_details"],
                config["bess_power_kw"],
                config["soc"],
                config["min_soc"],
                config["max_discharge_kw"],
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
            logger.info("[AUTHORITY] Priority lock, pausing peak shaving dicharging.")
            results.append({
                "status": "paused_due_authority",
                "soc": current_soc
            })
            break

        # Stop when schedule max SOC is reached.
        if sch_min_soc is not None and current_soc <= sch_min_soc:
            logger.info(
                f"[SCHEDULE] Schedule Max SoC reached "
                f"({current_soc}% <= {sch_min_soc}%). "
                f"Stopping dicharging..."
            )
            break

        # ========= STOP CONDITION ========= #
        if sch_min_soc is None and current_soc <= min_soc:
            logger.info(
                f"[SCHEDULE] Target SoC reached ({current_soc}%). Stopping discharging..."
            )
            break

        if block_discharging:
            logger.warning(f"{LogColor.RED} Discharging blocked by live config! {LogColor.RESET}")
            break

        if cloud_details.get("is_cloud_detected") and not extra_info.get("cloud_status", False):
            logger.warning(f"{LogColor.RED} Discharging blocked due to cloud detection! {LogColor.RESET}")
            # logger.warning(f"Cloud details: {cloud_details}")
            break

        # ------------------------------------
        # Determine available discharging power
        # ------------------------------------
        max_discharge_kw = get_c_rate(battery_temperature, bess_params) * (battery_capacity * (current_soc - min_soc) / 100) * efficiency
        # ------------------------------------
        # Calculate new charge setpoint
        # ------------------------------------

        available_capacity_kwh = (
            battery_capacity
            * ((current_soc - min_soc) / 100)
            * efficiency
        )

        max_discharge = min(max_discharge_kw, available_capacity_kwh)

        new_setpoint = int(
            min(
                power_kw,
                max_discharge
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
            f"Discharge={last_setpoint}kW"
        )

    # ---------------------------
    # FINAL STOP
    # STOP all charging – write 0 to mode & setpoint
    # ---------------------------
    logger.info("STOPPING DISCHARGE")

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
        # Step 2: set discharge setpoint
        await ModbusWriter.write_to_register(
            channel_id=channel_id,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=charge_register,
            data_type=charge_data_type,
            value=(setpoint_kw) if (setpoint_kw) >= 0 else (-1 * setpoint_kw)
        )

        logger.info(f"{color}[Slot {slot_number}] Setpoint {setpoint_kw} kW written to register {charge_register}{LogColor.RESET}")
        sp_rec = SetpointWrite(strategy_id=extra_info.get("strategy_id", 0), timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), register_mode=1, setpoint_value=setpoint_kw, unit="kW", device_id=device_id, success=True, response_text="Setpoint for discharge has been written")
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

