from datetime import datetime, timezone, timedelta
import logging
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

class AdvancedDrStrategy:
    """
    Advanced Demand Response (DR) BESS Control Strategy.
    Uses the current day’s TDV schedule to make dynamic time-of-use priorities.
    Activates on days with a peak TDV greater than 10 TDV/kBTU.
    """
    def __init__(self, resolver):
        self.resolver = resolver  # Used to resolve device and meter data

    async def execute(self, channel, server_ip, server_port, slave_id, bess_device_id, meter_device_id, max_discharge_rate=100, efficiency=0.9):
        """
        Execute the Advanced DR BESS control strategy.
        
        :param channel: The Modbus channel ID.
        :param server_ip: The IP address of the Modbus server.
        :param server_port: The port of the Modbus server.
        :param slave_id: The Modbus slave ID.
        :param bess_device_id: The ID of the BESS device.
        :param meter_device_id: The ID of the meter device.
        :param max_discharge_rate: Maximum discharge rate of the BESS in kW (default: 1000 kW).
        :param efficiency: Discharge efficiency of the BESS (default: 90% or 0.9).
        :return: Updated BESS data with charge/discharge request.
        """

        # Current Time
        current_time = datetime.now()

        # Simulate execution with a TDV schedule
        tdv_schedule = {
            0: 5.0, 1: 6.0, 2: 7.0, 3: 8.0, 4: 9.0, 5: 10.0,
            6: 11.0, 7: 12.0, 8: 13.0, 9: 14.0, 10: 15.0,
            11: 14.0, 12: 13.0, 13: 12.0, 14: 11.0, 15: 10.0,
            16: 9.0, 17: 8.0, 18: 7.0, 19: 6.0, 20: 5.0,
            21: 4.0, 22: 3.0, 23: 2.0
        }

        try:
            # Fetch BESS data
            bess_data = await self.resolver.resolve_block_device(bess_device_id)
            soc = bess_data.get("SoC", {}).get("value")
            soc_min_threshold = bess_data.get("soc_min_threshold", {}).get("value")
            soc_max_threshold = bess_data.get("soc_max_threshold", {}).get("value")

            if None in (soc, soc_min_threshold, soc_max_threshold):
                raise ValueError(f"Missing SoC values: soc={soc}, min={soc_min_threshold}, max={soc_max_threshold}")

            # Fetch meter data
            meter_data = await self.resolver.resolve_block_device(meter_device_id)
            load_power = meter_data.get("Power_kW", {}).get("value", 0)
            solar_power = meter_data.get("Total_PV_Power", {}).get("value", 0)

            # Calculate excess power
            excess_power = solar_power - load_power

            # Determine if it's a peak day
            day_tdv_peak = max(tdv_schedule.values())
            is_peak_day = day_tdv_peak > 10

            # Initialize charge_request
            charge_request = 0

            if is_peak_day:
                logger.info(f"Peak day detected with TDV peak: {day_tdv_peak} TDV/kBTU")
                
                # Rank hours by TDV value
                ranked_hours = sorted(tdv_schedule.items(), key=lambda x: x[1], reverse=True)
                top_hours = [hour for hour, _ in ranked_hours[:3]]  # Top 3 hours by TDV
                
                # Current hour
                current_hour = current_time.hour
                
                # Discharge logic for peak day
                if current_hour in top_hours[:2]:  # Top two TDV hours
                    charge_request = -max_discharge_rate  # Full discharge
                    logger.info(f"BESS Discharging at Full Power during Top TDV Hour: {current_hour}")
                
                elif current_hour == top_hours[2]:  # Third-highest TDV hour
                    # Use remaining capacity after top two hours
                    used_capacity = 2 * (max_discharge_rate / efficiency)
                    remaining_capacity = max_discharge_rate - used_capacity
                    charge_request = remaining_capacity
                    logger.info(f"BESS Discharging Remaining Capacity during Third TDV Hour: {remaining_capacity} kW")
                
                else:
                    # Charge from PV without subtracting load
                    charge_request = solar_power
                    logger.info(f"BESS Charging from PV Production: {solar_power} kW")
            
            else:
                # Non-peak day: Revert to basic strategy
                if excess_power > 0 and soc < soc_max_threshold:
                    charge_request = min(excess_power, soc_max_threshold - soc)
                    logger.info(f"BESS Charging: Excess Power={excess_power} kW, SoC={soc}% changeReq = {charge_request}")
                
                elif excess_power < 0 and soc > soc_min_threshold:
                    charge_request = max(-excess_power, -(soc - soc_min_threshold))
                    logger.info(f"BESS Discharging: Deficit Power={-excess_power} kW, SoC={soc}% changeReq = {charge_request}")

            # Get the register address and data type for SoC_Write
            register = bess_data["SoC_Write"]["register"]
            data_type = bess_data["SoC_Write"]["dataType"]

            # Write the charge_request value to the register using ModbusWriter
            await ModbusWriter.write_to_register(
                channel_id=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                register=register,
                data_type=data_type,
                value=charge_request
            )

        except Exception as e:
            logger.error(f"Error executing Advanced DR BESS strategy: {e}")
            raise
    
