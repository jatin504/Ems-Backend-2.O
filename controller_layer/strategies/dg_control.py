import asyncio
import logging
from LogColor import LogColor
from config.device_config_resolver import DeviceConfigResolver
from controller_layer.strategies.classes.common.PeakShavingStrategyType7 import bess_decision
from device_config_helper import DeviceConfigHelper
from ..authority.authority_manager import AuthorityManager
from utils import ModbusWriter
from typing import Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)

async def DGStartControl(
        resolver, 
        controller_static,
        status, 
        load, 
        device_id,
        channel,
        server_ip,
        server_port,
        slave_id,
        authority="internal"
        ):
    """
        status => 0/1
        Based on the load it will control the DG.
    """

    logger.info(f"{LogColor.MAGENTA} DG Control STARTED {LogColor.RESET}")
    results = []

    device_config_resolver = DeviceConfigHelper()
    dg_device_ids = device_config_resolver.get_device_ids_by_device_type("dg")

    device_resolver = DeviceConfigResolver()
    devices = [ device_resolver.get_device_info_by_id(d) for d in dg_device_ids ]

    devices = [d for d in devices if d is not None]

    selected_dg = select_best_dg(devices, load)

    if not selected_dg:
        logger.warning("No suitable DG found")
        return []
    
    dg_id = selected_dg["device_id"]

    # get device snapshot
    device_data = resolver.get_latest_device_data(device_id)    #selected_dg["device_id"]
    if not device_data:
        logger.error("Missing device data")
        return {"error": "Missing device data"}
    
    
    # Registers ------ later will be for PV
    power_mode_register = device_data["PowerMode"]["register"]
    power_mode_data_type = device_data["PowerMode"]["dataType"]

    # Pre-flight authority check
    if not AuthorityManager.can_proceed(authority):
        logger.info(f"{LogColor.RED}[AUTHORITY] Blocked — cannot start/stop DG - {status}{LogColor.RESET}")
        return {"status": "blocked", "reason": "authority"}
    
    # Write Mode=1 then setpoint
    await ModbusWriter.write_to_register(
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        register=power_mode_register,
        data_type=power_mode_data_type,
        value=status
    )

    dg_running = True

    results.append(
        {
            "status": int(status),
            "required_load_power": load,
            "timestamp": datetime.now().isoformat(),
            "text": "DG is on"
        }
    )

    # control loop
    while dg_running:


        try:
            await asyncio.sleep(5)
        
            poi_meter_id = "plant_1_poi_meter_1"
            config = await bess_decision(device_id, poi_meter_id)

            (
                block_discharging,
                hard_limit_kw,
                soft_limit_kw,
                threshold_kw,
                pv_power_kw,
                bess_power_kw,
                dg_power_kw,
                current_load_kw,
                
            ) = (
                config["block_discharging"],
                config["hard_limit_kw"],
                config["soft_limit_kw"],
                config["threshold_kw"],
                config["pv_power_kw"],
                config["bess_power_kw"],
                config["dg_power_kw"],
                config["current_load_kw"],
            )

        except Exception as e:
            logger.info(f"{LogColor.RED} Error loading live params: {e} {LogColor.RESET}")
            return 
        
        grid_power = current_load_kw - pv_power_kw - bess_power_kw - dg_power_kw

        if grid_power > threshold_kw and not block_discharging:
            # Write Mode=0 then DG OFF
            await ModbusWriter.write_to_register(
                channel_id=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                register=power_mode_register,
                data_type=power_mode_data_type,
                value=0
            )

            results.append(
                {
                    "status": 0,
                    "required_load_power": load,
                    "timestamp": datetime.now().isoformat(),
                    "text": "DG is off"
                }
            )

            dg_running = False

    return results

async def DGStopControl(
        resolver, 
        controller_static,
        status, 
        load, 
        device_id,
        channel,
        server_ip,
        server_port,
        slave_id,
        authority="internal"
        ):
    """
        status => 0/1
        Based on the load it will control the DG.
    """

    logger.info(f"{LogColor.MAGENTA} DG Control STARTED {LogColor.RESET}")
    results = []

    # device_config_resolver = DeviceConfigHelper()
    # dg_device_ids = device_config_resolver.get_device_ids_by_device_type("dg")

    # device_resolver = DeviceConfigResolver()
    # devices = [ device_resolver.get_device_info_by_id(d) for d in dg_device_ids ]

    # devices = [d for d in devices if d is not None]

    # get device snapshot
    device_data = resolver.get_latest_device_data(device_id)    #selected_dg["device_id"]
    if not device_data:
        logger.error("Missing device data")
        return {"error": "Missing device data"}
    
    
    # Registers ------ later will be for DG
    power_mode_register = device_data["PowerMode"]["register"]
    power_mode_data_type = device_data["PowerMode"]["dataType"]

    # Pre-flight authority check
    if not AuthorityManager.can_proceed(authority):
        logger.info(f"{LogColor.RED}[AUTHORITY] Blocked — cannot start/stop DG - {status}{LogColor.RESET}")
        return {"status": "blocked", "reason": "authority"}
    
    # Write Mode=0 then Stop DG =======> OFF For ALL
    await ModbusWriter.write_to_register(
        channel_id=channel,
        server_ip=server_ip,
        server_port=server_port,
        slave_id=slave_id,
        register=power_mode_register,
        data_type=power_mode_data_type,
        value=status
    )

    results.append(
        {
            "status": int(status),
            "required_load_power": load,
            "timestamp": datetime.now().isoformat(),
            "text": "DG is off"
        }
    )

    return results
    

def select_best_dg(devices, load, target_loading=0.65, tolerance=0.1):
    """
    Select DG such that it runs around 60–70% loading.

    target_loading = 0.65 (ideal)
    tolerance = ±10% (acceptable range 55%–75%)
    """

    best_dg = None
    best_score = float("inf")

    for dg in devices:
        capacity = dg.get("dc_capacity", 0)

        if capacity <= 0:
            continue

        # Calculate actual loading if this DG is used
        loading = load / capacity

        # Check if within acceptable band
        if (target_loading - tolerance) <= loading <= (target_loading + tolerance):
            score = abs(loading - target_loading)

            if score < best_score:
                best_score = score
                best_dg = dg

    # Fallback: if no DG in ideal range → pick closest
    if not best_dg:
        for dg in devices:
            capacity = dg.get("dc_capacity", 0)
            if capacity <= 0:
                continue

            loading = load / capacity
            score = abs(loading - target_loading)

            if score < best_score:
                best_score = score
                best_dg = dg

    return best_dg