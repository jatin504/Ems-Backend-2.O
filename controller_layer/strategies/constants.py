"""
This module contains constants used in the controller layer strategies.
"""

from __future__ import annotations
from enum import IntEnum

class DeviceID:

    CONTROLLER_ID = "plant_1_pcs_controller_1"
    POI_ID = "plant_1_poi_meter_1"

class StrategyID(IntEnum):

    BASIC = 4
    PEAK_SHAVING = 3
    MANUAL_SCHEDULE = 5
    EMS_SCHEDULE = 6

class StrategyName:

    BASIC = "Basic"
    PEAK_SHAVING = "Peak_Shaving"
    MANUAL_SCHEDULE = "Manual_Schedule"
    EMS_SCHEDULE = "EMS_Schedule"