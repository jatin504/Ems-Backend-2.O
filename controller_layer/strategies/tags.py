"""
Engineering Tag Definitions

This module contains every engineering variable
name used inside the simulator.

The simulator MUST never use raw strings.

Always use Tags.
"""

from __future__ import annotations
from sre_constants import CH_LOCALE


class Tags:

    # Battery

    BATTERY_SOC = "Battery_SOC"

    BATTERY_POWER = "Battery_Power"

    BATTERY_ENERGY = "Battery_Energy"

    # Grid

    GRID_VOLTAGE = "Grid_Voltage"

    GRID_FREQUENCY = "Grid_Frequency"

    # POI

    Load = "Load_kW"

    PV_POWER = "PV_Power_kW"

    POA = "POA"

    # EMS

    ACTIVE_POWER_COMMAND = "EMS_Active_Power_Command"

    EMS_START = "EMS_Start"

    EMS_STOP = "EMS_Stop"

    # BMS

    CHARGE_SIGNAL = "BMS_Charge_Signal"
    
    DISCHARGE_SIGNAL = "BMS_Discharge_Signal"

    # Fault

    Fault_Code = "Fault_Code"



from enum import IntEnum

class FaultCode(IntEnum):

    NONE = 0

    GRID_LOSS = 1

    OVER_VOLTAGE = 2

    UNDER_VOLTAGE = 3

    OVER_FREQUENCY = 4

    UNDER_FREQUENCY = 5

    BMS_FAULT = 6

    EMERGENCY_STOP = 7

    OVER_TEMPERATURE = 8

    TRANSFORMER_OVERLOADING = 9

    TRANSFORMER_UNDERLOADING = 10

    