import asyncio
import time
import logging

from config.device_config_resolver import DeviceConfigResolver
from controller_layer.writers.notification_writer import create_bess_notification
from utils.modbus_writer import ModbusWriter

logger = logging.getLogger(__name__)

SYNC_SUSTAIN_SECONDS = 120

async def gfl_gfm_mode_selection(
        resolver,
        mode: str,
        device_type: str,   # "BESS" or "DG"
):
    """
    This function sets GFL/GFM mode for a device and automatically
    sets the opposite mode for the other device.

    Rules:
        If BESS -> GFM, DG -> GFL
        If DG   -> GFM, BESS -> GFL
        If BESS -> GFL, DG -> GFM
        If DG   -> GFL, BESS -> GFM
    """

    device_ids = {
        "BESS": "plant_1_pcs_controller_1",
        "DG": "plant_1_dg_controller_1"
    }

    # -------------------------
    # Validate Inputs
    # -------------------------

    mode = mode.upper()
    device_type = device_type.upper()

    if device_type not in device_ids:
        raise ValueError("Invalid device_type. Must be BESS or DG")

    if mode not in ["GFL", "GFM"]:
        raise ValueError("Invalid mode. Must be GFL or GFM")

    # -------------------------
    # Determine Opposite Mode
    # -------------------------

    opposite_mode = "GFL" if mode == "GFM" else "GFM"

    # Mode values (update if different in your system)

    MODE_VALUES = {
        "GFL": 1,
        "GFM": 2
    }

    # Devices

    primary_device_id = device_ids[device_type]

    other_device_type = (
        "DG" if device_type == "BESS"
        else "BESS"
    )

    secondary_device_id = device_ids[other_device_type]

    device_resolver = DeviceConfigResolver()

    # -------------------------
    # Helper Function
    # -------------------------

    async def set_mode(device_id: str, mode_name: str):

        device_info = device_resolver.get_device_info_by_id(device_id)

        channel = device_info["channel"]
        server_ip = device_info["server_ip"]
        server_port = device_info["server_port"]
        slave_id = device_info["slave_id"]

        device_data = resolver.get_latest_device_data(device_id)

        power_mode_register = device_data["PowerMode"]["register"]
        power_mode_data_type = device_data["PowerMode"]["dataType"]

        value = MODE_VALUES[mode_name]

        await ModbusWriter.write_to_register(
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=power_mode_register,
            data_type=power_mode_data_type,
            value=value
        )

    # -------------------------
    # Execute
    # -------------------------

    is_synced = is_sync_conditions_met(resolver)

    if is_synced:

        # Set requested device mode
        await set_mode(primary_device_id, mode)

        # Set other device opposite mode
        await set_mode(secondary_device_id, opposite_mode)

        # notify user about mode change
        await create_bess_notification(
            source="Control_System",
            event_type="GFL/GFM Mode Transition",
            severity="intermediate",
            title=f"{device_type} switched to {mode}",
            message=f"Grid phase synchronized - {device_type} switched to {mode}",
            entity_type="plant",
            entity_id="PLANT_01",
            metadata={}
        )

        return
    
    else:

        sync_start_time = time.time()

        # notify user about mode change
        await create_bess_notification(
            source="Control_System",
            event_type="GFL/GFM Mode Transition",
            severity="intermediate",
            title=f"Waiting for phase synchronization",
            message=f"Waiting for phase synchronization - {device_type} switched to {mode}",
            entity_type="plant",
            entity_id="PLANT_01",
            metadata={}
        )

        while True:

            elapsed = time.time() - sync_start_time

            is_synced = is_sync_conditions_met(resolver)
            
            if is_synced:

                # Set requested device mode
                await set_mode(primary_device_id, mode)

                # Set other device opposite mode
                await set_mode(secondary_device_id, opposite_mode)

                # notify user about mode change
                await create_bess_notification(
                    source="Control_System",
                    event_type="GFL/GFM Mode Transition",
                    severity="intermediate",
                    title=f"{device_type} switched to {mode}",
                    message=f"Grid phase synchronized - {device_type} switched to {mode}",
                    entity_type="plant",
                    entity_id="PLANT_01",
                    metadata={}
                )

                return

            if elapsed >= 120:

                logger.info(
                    "Synchronization lost — Cancel switching mode"
                )

                # notify user about mode change
                await create_bess_notification(
                    source="Control_System",
                    event_type="GFL/GFM Mode Transition",
                    severity="intermediate",
                    title=f"Cancel switching mode",
                    message=f"Cancel switching mode - {device_type} switched to {mode} is cancelled due to synchronization loss.",
                    entity_type="plant",
                    entity_id="PLANT_01",
                    metadata={}
                )

                return

            await asyncio.sleep(1)


def is_sync_conditions_met(resolver):
    telemetry = resolver.get_latest_device_data("plant_1_poi_meter_1")

    grid_voltage = telemetry["grid_voltage"]
    dg_voltage = telemetry["dg_voltage"]
    grid_frequency = telemetry["grid_frequency"]
    dg_frequency = telemetry["dg_frequency"]
    grid_phase = telemetry["grid_phase"]
    dg_phase = telemetry["dg_phase"]

    if None in (grid_voltage, dg_voltage, grid_frequency, dg_frequency, grid_phase, dg_phase):
        return False
    
    if grid_voltage == 0:
        return False

    voltage_diff = abs(grid_voltage - dg_voltage) / grid_voltage * 100
    freq_diff = abs(grid_frequency - dg_frequency)
    # phase_diff = abs(grid_phase - dg_phase)

    return (
        voltage_diff <= 5
        and freq_diff <= 0.2
        # and phase_diff <= 10
    )

LAST_GRID_STATE = None
LAST_MODE = None  # Track BESS mode


async def smart_mode_controller(resolver):

    global LAST_GRID_STATE, LAST_MODE

    data = {
        "v_pcc_pu": 0.98,
        "f_hz": 49.92,
        "rocof_hz_per_s": -0.25,
        "kvq": 0.015,
        "z_th_ohm": 0.23,
        "v_variance": 0.004,
        "events_last_5min": 2,
    }  # resolver.get_ppc_data()

    # --- Step 1: Compute WGI ---
    wgi = calculate_wgi(data)

    # --- Step 2: Classification ---
    grid_state = classify_wgi(wgi)

    # --- Init state ---
    if LAST_GRID_STATE is None:
        LAST_GRID_STATE = grid_state

    # --- Step 3: Apply hysteresis ---
    stable_state = apply_hysteresis(LAST_GRID_STATE, wgi)

    # --- Step 4: Detect fast disturbance ---
    fast_event = detect_fast_disturbance(data)

    # --- Step 5: Decide mode ---
    if fast_event:
        target_mode = "GFM"
        reason = "FAST_DISTURBANCE"

    elif stable_state == "WEAK":
        target_mode = "GFM"
        reason = "WEAK_GRID"

    else:
        target_mode = "GFL"
        reason = "GRID_STABLE"

    # --- Step 6: Check if change is required ---
    if LAST_MODE == target_mode:
        LAST_GRID_STATE = stable_state
        return stable_state

    # --- Step 7: Apply hold time ONLY if switching ---
    if not can_switch_mode():
        return LAST_GRID_STATE

    # --- Step 8: Execute mode change ---
    await gfl_gfm_mode_selection(
        resolver=resolver,
        mode=target_mode,
        device_type="BESS"
    )

    # --- Update states ---
    LAST_MODE = target_mode
    LAST_GRID_STATE = stable_state
    update_mode_timestamp()

    # --- Logging ---
    logger.info(
        f"[WGI] {wgi} | Grid: {grid_state} | Stable: {stable_state} "
        f"| Mode: {target_mode} | Reason: {reason}"
    )

    return stable_state


def normalize(value, min_val, max_val):
    if value is None:
        return 0
    if value <= min_val:
        return 0
    if value >= max_val:
        return 1
    return (value - min_val) / (max_val - min_val)


def calculate_wgi(data: dict) -> float:
    """
    Calculate Weak Grid Index (0 → strong, 1 → weak)
    """

    # --- Individual Scores ---
    S_RoCoF = normalize(abs(data.get("rocof_hz_per_s", 0)), 0.05, 0.5)
    S_Vvar  = normalize(data.get("v_variance", 0), 0.001, 0.01)
    S_QV    = normalize(data.get("kvq", 0), 0.005, 0.05)
    S_event = normalize(data.get("events_last_5min", 0), 0, 5)
    S_Zth   = normalize(data.get("z_th_ohm", 0), 0.05, 0.5)

    # --- Weighted Index ---
    WGI = (
        0.30 * S_RoCoF +
        0.25 * S_Vvar +
        0.20 * S_QV +
        0.15 * S_event +
        0.10 * S_Zth
    )

    return round(WGI, 3) 

def classify_wgi(wgi):
    if wgi < 0.3:
        return "STRONG"
    elif wgi < 0.6:
        return "MODERATE"
    else:
        return "WEAK"

def detect_fast_disturbance(data: dict) -> bool:
    """
    Detect fast instability (RoCoF spike)
    """
    rocof = abs(data.get("rocof_hz_per_s", 0))

    # threshold depends on grid code, adjust if needed
    return rocof >= 0.3

def apply_hysteresis(current_state: str, wgi: float) -> str:
    """
    Prevent oscillation between modes
    """

    if current_state == "WEAK":
        if wgi < 0.5:
            return "MODERATE"  # relax slowly
        return "WEAK"

    if current_state == "STRONG":
        if wgi > 0.4:
            return "MODERATE"
        return "STRONG"

    return "MODERATE"

import time

LAST_MODE_CHANGE_TS = 0
MIN_HOLD_TIME = 120  # seconds


def can_switch_mode() -> bool:
    global LAST_MODE_CHANGE_TS
    return (time.time() - LAST_MODE_CHANGE_TS) > MIN_HOLD_TIME


def update_mode_timestamp():
    global LAST_MODE_CHANGE_TS
    LAST_MODE_CHANGE_TS = time.time()
