from model.db import get_async_dict_cursor
from .load_cycle_config import load_cycle_config
from datetime import datetime, timedelta
import logging

from ..load_bess_params_setting import load_bess_params_setting
from ...writers.ems_control_state_manager import EMSControlStateManager

logger = logging.getLogger(__name__)

def safe_num(x):
    try:
        return float(x)
    except:
        return 0.0

async def fetch_today_controller_data(table_name: str):
    """
    Returns all rows from today's controller table.
    """
    pool = await get_async_dict_cursor()  # from your db.py

    now = datetime.now()
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE time_stamp >= $1
        ORDER BY time_stamp ASC
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, start_of_day)

    await pool.close()
    return rows

async def get_today_cycles(table_name="plant_1_pcs_controller_1"):
    """
    Computes today's cycle count using 3 different methods:
        METHOD 0 → Throughput (Ah)
        METHOD 1 → Full Cycle Detection
        METHOD 2 → DOD Weighted Counting
    """

    logger.info(f"[CycleCalc] Fetching today's controller data for table: {table_name}")

    config = load_cycle_config()
    method = config["cycle_counting_method"]
    max_cycles_per_day = config["max_cycles_per_day"]

    logger.info(f"Cycle method = {method}, Max cycles per day = {max_cycles_per_day}")

    rows = await fetch_today_controller_data(table_name)

    if not rows:
        logger.warning("No controller rows for today → cycles = 0")
        return 0
    
    bess = load_bess_params_setting()
    min_soc = bess["min_soc"]
    max_soc = bess["max_soc"]
    battery_capacity = 22000

    total_discharge_Ah = 0
    total_charge_Ah = 0
    dod_threshold = 100 - 0 #min_soc
    cycles = 0.0
    partial_dod = 0.0
    last_soc = None
    cycle_active = False

    logger.info(f"Total rows fetched for today: {len(rows)}")

    for row in rows:
        soc = safe_num(row["soc"])
        charge_ah = safe_num(row["chargeah"])
        discharge_ah = abs(safe_num(row["dischargeah"]))  # ensure +ve

        # ----------------------------------------------------
        # METHOD 0 → THROUGHPUT
        # ----------------------------------------------------
        if method == "0":
            total_charge_Ah += charge_ah
            total_discharge_Ah += discharge_ah

        # ----------------------------------------------------
        # METHOD 1 → Full Cycle Counting
        # ----------------------------------------------------
        elif method == "1":
            if last_soc is not None:

                # Start of discharge
                if soc < last_soc and not cycle_active:
                    cycle_active = True
                    min_soc_reached = soc
                    # logger.info(f"Discharge started at SOC={last_soc}% → min={min_soc_reached}%")

                # Track new minimum SOC
                if cycle_active and soc < min_soc_reached:
                    min_soc_reached = soc

                # Detect when cycle closes (charge started)
                if cycle_active and soc > last_soc:
                    dod = 100 - min_soc_reached
                    # logger.info(f"Cycle closed → DOD={dod:.2f}%")

                    if dod >= dod_threshold:
                        cycles += 1
                        # logger.info(f"Full cycle counted → Total cycles={cycles}")

                    cycle_active = False

        # ----------------------------------------------------
        # METHOD 2 → DOD Weighted Counting
        # ----------------------------------------------------
        elif method == "2":
            if last_soc is not None:
                # During discharge
                if soc < last_soc:
                    dod_delta = last_soc - soc
                    partial_dod += dod_delta / 100
                    logger.info(f"Partial DOD added: {dod_delta:.2f}% → total={partial_dod:.3f}")

                # When charging resumes, close partial cycle
                if soc > last_soc and partial_dod > 0:
                    cycles += partial_dod
                    logger.info(f"Charging → Closing partial cycle {partial_dod:.3f}")
                    partial_dod = 0

        last_soc = soc

    # --------------------------------------------------------
    # FINALIZE throughput method
    # --------------------------------------------------------
    if method == "0":
        NOMINAL_AH = battery_capacity  # change to actual value
        cycles = (total_charge_Ah + total_discharge_Ah) / (2 * NOMINAL_AH)

        logger.info(f"Throughput ChargeAh={total_charge_Ah}, DischargeAh={total_discharge_Ah}")
        logger.info(f"Throughput cycles (raw) = {cycles}")

    # --------------------------------------------------------
    # Limit cycles per day
    # --------------------------------------------------------
    if cycles > max_cycles_per_day:
        logger.warning(f"⚠ Cycle count {cycles:.3f} > max limit {max_cycles_per_day} → clamped")
        await EMSControlStateManager.raise_flag("max_cycle_life_reached", 1)
    cycles = cycles #min(cycles, max_cycles_per_day)

    logger.info(f"Final cycle count for today = {cycles:.3f}")

    return round(cycles, 3)

