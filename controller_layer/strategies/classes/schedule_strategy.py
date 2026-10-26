import asyncio
from pathlib import Path
from typing import List, Dict
from datetime import datetime, timezone, timedelta
import logging
from controller_layer.authority.authority_manager import AuthorityManager
# from controller_layer.strategies.classes.common.ScheduleStrategy import bess_decision as bess_decision7
from controller_layer.cloud_detection.detect import CloudDetection
from controller_layer.scheduler.bess_scheduler import BESScheduler
from controller_layer.strategies.grif_forming_following_control import gfl_gfm_mode_selection, smart_mode_controller
from controller_layer.strategies.tags import FaultCode, Tags
from model.models import EnergyStrategy, NotificationEvent, SetpointWrite
from repository.strategy_repo import write_energy_strategy_record, write_notification_event, write_setpoint_record
from statics.controller_resolver import ControllerStaticResolver
from latest_telemetry_resolver import LatestTelemetryResolver
from utils.modbus_writer import ModbusWriter
from ...writers import EMSControlStateManager
from ..cycle_management import get_today_cycles
from apscheduler.schedulers.background import BackgroundScheduler
from LogColor import LogColor
import pytz
import json
import time

from utils.loop_manager import get_main_loop

from model.db import get_async_connection

from ..load_bess_params_setting import get_c_rate, load_bess_params_setting
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

class ScheduleStrategy:
    def __init__(self, resolver, sample_data=None):
        self.resolver = LatestTelemetryResolver()
        self.controller_static = ControllerStaticResolver()
        self.ess_control_state_manager = EMSControlStateManager()
        self.strategy_flags_controller = StrategyFlagsController(
            ess_control_state_manager = self.ess_control_state_manager,
        )
        # self.metadata_resolver = MetadataResolver()
        self.sample_window_1min = [
            {'load_kw': 2200.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:00:00'},
            {'load_kw': 2108.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:01:00'},
            {'load_kw': 2007.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:02:00'},
            {'load_kw': 2396.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:03:00'},
            {'load_kw': 2094.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:04:00'},
            {'load_kw': 2293.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:05:00'},
            {'load_kw': 2492.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:06:00'},
            {'load_kw': 2590.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:07:00'},
            {'load_kw': 2689.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:08:00'},
            {'load_kw': 2488.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:09:00'},
            {'load_kw': 2286.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:10:00'},
            {'load_kw': 2185.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:11:00'},
            {'load_kw': 2104.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:12:00'},
            {'load_kw': 2002.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:13:00'},
            {'load_kw': 2181.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:14:00'},
            {'load_kw': 1980.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:15:00'},
            {'load_kw': 1878.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:16:00'},
            {'load_kw': 2076.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:17:00'},
            {'load_kw': 2274.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:18:00'},
            {'load_kw': 2452.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:19:00'},
            {'load_kw': 2670.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:20:00'},
            {'load_kw': 2668.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:21:00'},
            {'load_kw': 2666.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:22:00'},
            {'load_kw': 2564.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:23:00'},
            {'load_kw': 2462.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:24:00'},
            {'load_kw': 2360.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:25:00'},
            {'load_kw': 2558.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:26:00'},
            {'load_kw': 2656.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:27:00'},
            {'load_kw': 2454.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:28:00'},
            {'load_kw': 2352.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:29:00'},
            {'load_kw': 2150.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:30:00'},
            {'load_kw': 2148.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:31:00'},
            {'load_kw': 2146.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:32:00'},
            {'load_kw': 2144.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:33:00'},
            {'load_kw': 2142.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:34:00'},
            {'load_kw': 2140.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:35:00'},
            {'load_kw': 2138.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:36:00'},
            {'load_kw': 2136.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:37:00'},
            {'load_kw': 2134.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:38:00'},
            {'load_kw': 2132.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:39:00'},
            {'load_kw': 2130.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:40:00'},
            {'load_kw': 2128.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:41:00'},
            {'load_kw': 2126.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:42:00'},
            {'load_kw': 2124.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:43:00'},
            {'load_kw': 2122.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:44:00'},
            {'load_kw': 2120.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:45:00'},
            {'load_kw': 2118.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:46:00'},
            {'load_kw': 2117.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:47:00'},
            {'load_kw': 2116.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:48:00'},
            {'load_kw': 2114.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:49:00'},
            {'load_kw': 2113.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:50:00'},
            {'load_kw': 2112.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:51:00'},
            {'load_kw': 2110.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:52:00'},
            {'load_kw': 2109.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:53:00'},
            {'load_kw': 2108.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:54:00'},
            {'load_kw': 2106.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:55:00'},
            {'load_kw': 2105.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:56:00'},
            {'load_kw': 2104.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:57:00'},
            {'load_kw': 2102.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:58:00'},
            {'load_kw': 2101.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 00:59:00'},
            {'load_kw': 2100.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:00:00'},
            {'load_kw': 2098.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:01:00'},
            {'load_kw': 2097.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:02:00'},
            {'load_kw': 2096.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:03:00'},
            {'load_kw': 2094.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:04:00'},
            {'load_kw': 2093.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:05:00'},
            {'load_kw': 2092.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:06:00'},
            {'load_kw': 2090.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:07:00'},
            {'load_kw': 2089.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:08:00'},
            {'load_kw': 2088.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:09:00'},
            {'load_kw': 2086.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:10:00'},
            {'load_kw': 2085.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:11:00'},
            {'load_kw': 2084.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:12:00'},
            {'load_kw': 2082.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:13:00'},
            {'load_kw': 2081.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:14:00'},
            {'load_kw': 2080.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:15:00'},
            {'load_kw': 2078.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:16:00'},
            {'load_kw': 2077.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:17:00'},
            {'load_kw': 2076.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:18:00'},
            {'load_kw': 2074.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:19:00'},
            {'load_kw': 2073.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:20:00'},
            {'load_kw': 2072.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:21:00'},
            {'load_kw': 2070.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:22:00'},
            {'load_kw': 2069.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:23:00'},
            {'load_kw': 2068.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:24:00'},
            {'load_kw': 2066.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:25:00'},
            {'load_kw': 2065.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:26:00'},
            {'load_kw': 2064.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:27:00'},
            {'load_kw': 2062.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:28:00'},
            {'load_kw': 2061.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:29:00'},
            {'load_kw': 2060.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:30:00'},
            {'load_kw': 2059.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:31:00'},
            {'load_kw': 2058.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:32:00'},
            {'load_kw': 2058.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:33:00'},
            {'load_kw': 2057.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:34:00'},
            {'load_kw': 2056.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:35:00'},
            {'load_kw': 2056.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:36:00'},
            {'load_kw': 2055.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:37:00'},
            {'load_kw': 2054.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:38:00'},
            {'load_kw': 2054.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:39:00'},
            {'load_kw': 2053.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:40:00'},
            {'load_kw': 2052.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:41:00'},
            {'load_kw': 2052.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:42:00'},
            {'load_kw': 2051.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:43:00'},
            {'load_kw': 2050.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:44:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:45:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:46:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:47:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:48:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:49:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:50:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:51:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:52:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:53:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:54:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:55:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:56:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:57:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:58:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 01:59:00'},
            {'load_kw': 2050.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:00:00'},
            {'load_kw': 2051.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:01:00'},
            {'load_kw': 2052.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:02:00'},
            {'load_kw': 2054.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:03:00'},
            {'load_kw': 2055.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:04:00'},
            {'load_kw': 2056.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:05:00'},
            {'load_kw': 2058.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:06:00'},
            {'load_kw': 2059.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:07:00'},
            {'load_kw': 2060.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:08:00'},
            {'load_kw': 2062.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:09:00'},
            {'load_kw': 2063.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:10:00'},
            {'load_kw': 2064.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:11:00'},
            {'load_kw': 2066.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:12:00'},
            {'load_kw': 2067.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:13:00'},
            {'load_kw': 2068.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:14:00'},
            {'load_kw': 2070.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:15:00'},
            {'load_kw': 2072.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:16:00'},
            {'load_kw': 2074.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:17:00'},
            {'load_kw': 2076.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:18:00'},
            {'load_kw': 2078.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:19:00'},
            {'load_kw': 2080.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:20:00'},
            {'load_kw': 2082.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:21:00'},
            {'load_kw': 2084.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:22:00'},
            {'load_kw': 2086.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:23:00'},
            {'load_kw': 2088.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:24:00'},
            {'load_kw': 2090.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:25:00'},
            {'load_kw': 2092.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:26:00'},
            {'load_kw': 2094.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:27:00'},
            {'load_kw': 2096.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:28:00'},
            {'load_kw': 2098.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:29:00'},
            {'load_kw': 2100.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:30:00'},
            {'load_kw': 2101.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:31:00'},
            {'load_kw': 2102.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:32:00'},
            {'load_kw': 2104.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:33:00'},
            {'load_kw': 2105.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:34:00'},
            {'load_kw': 2106.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:35:00'},
            {'load_kw': 2108.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:36:00'},
            {'load_kw': 2109.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:37:00'},
            {'load_kw': 2110.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:38:00'},
            {'load_kw': 2112.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:39:00'},
            {'load_kw': 2113.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:40:00'},
            {'load_kw': 2114.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:41:00'},
            {'load_kw': 2116.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:42:00'},
            {'load_kw': 2117.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:43:00'},
            {'load_kw': 2118.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:44:00'},
            {'load_kw': 2120.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:45:00'},
            {'load_kw': 2122.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:46:00'},
            {'load_kw': 2124.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:47:00'},
            {'load_kw': 2126.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:48:00'},
            {'load_kw': 2128.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:49:00'},
            {'load_kw': 2130.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:50:00'},
            {'load_kw': 2132.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:51:00'},
            {'load_kw': 2134.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:52:00'},
            {'load_kw': 2136.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:53:00'},
            {'load_kw': 2138.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:54:00'},
            {'load_kw': 2140.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:55:00'},
            {'load_kw': 2142.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:56:00'},
            {'load_kw': 2144.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:57:00'},
            {'load_kw': 2146.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:58:00'},
            {'load_kw': 2148.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 02:59:00'},
            {'load_kw': 2150.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:00:00'},
            {'load_kw': 2152.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:01:00'},
            {'load_kw': 2154.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:02:00'},
            {'load_kw': 2156.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:03:00'},
            {'load_kw': 2158.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:04:00'},
            {'load_kw': 2160.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:05:00'},
            {'load_kw': 2162.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:06:00'},
            {'load_kw': 2164.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:07:00'},
            {'load_kw': 2166.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:08:00'},
            {'load_kw': 2168.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:09:00'},
            {'load_kw': 2170.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:10:00'},
            {'load_kw': 2172.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:11:00'},
            {'load_kw': 2174.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:12:00'},
            {'load_kw': 2176.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:13:00'},
            {'load_kw': 2178.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:14:00'},
            {'load_kw': 2180.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:15:00'},
            {'load_kw': 2181.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:16:00'},
            {'load_kw': 2182.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:17:00'},
            {'load_kw': 2184.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:18:00'},
            {'load_kw': 2185.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:19:00'},
            {'load_kw': 2186.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:20:00'},
            {'load_kw': 2188.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:21:00'},
            {'load_kw': 2189.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:22:00'},
            {'load_kw': 2190.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:23:00'},
            {'load_kw': 2192.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:24:00'},
            {'load_kw': 2193.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:25:00'},
            {'load_kw': 2194.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:26:00'},
            {'load_kw': 2196.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:27:00'},
            {'load_kw': 2197.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:28:00'},
            {'load_kw': 2198.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:29:00'},
            {'load_kw': 2200.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:30:00'},
            {'load_kw': 2203.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:31:00'},
            {'load_kw': 2206.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:32:00'},
            {'load_kw': 2210.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:33:00'},
            {'load_kw': 2213.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:34:00'},
            {'load_kw': 2216.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:35:00'},
            {'load_kw': 2220.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:36:00'},
            {'load_kw': 2223.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:37:00'},
            {'load_kw': 2226.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:38:00'},
            {'load_kw': 2230.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:39:00'},
            {'load_kw': 2233.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:40:00'},
            {'load_kw': 2236.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:41:00'},
            {'load_kw': 2240.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:42:00'},
            {'load_kw': 2243.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:43:00'},
            {'load_kw': 2246.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:44:00'},
            {'load_kw': 2250.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:45:00'},
            {'load_kw': 2253.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:46:00'},
            {'load_kw': 2256.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:47:00'},
            {'load_kw': 2260.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:48:00'},
            {'load_kw': 2263.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:49:00'},
            {'load_kw': 2266.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:50:00'},
            {'load_kw': 2270.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:51:00'},
            {'load_kw': 2273.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:52:00'},
            {'load_kw': 2276.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:53:00'},
            {'load_kw': 2280.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:54:00'},
            {'load_kw': 2283.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:55:00'},
            {'load_kw': 2286.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:56:00'},
            {'load_kw': 2290.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:57:00'},
            {'load_kw': 2293.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:58:00'},
            {'load_kw': 2296.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 03:59:00'},
            {'load_kw': 2300.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:00:00'},
            {'load_kw': 2303.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:01:00'},
            {'load_kw': 2306.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:02:00'},
            {'load_kw': 2310.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:03:00'},
            {'load_kw': 2313.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:04:00'},
            {'load_kw': 2316.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:05:00'},
            {'load_kw': 2320.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:06:00'},
            {'load_kw': 2323.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:07:00'},
            {'load_kw': 2326.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:08:00'},
            {'load_kw': 2330.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:09:00'},
            {'load_kw': 2333.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:10:00'},
            {'load_kw': 2336.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:11:00'},
            {'load_kw': 2340.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:12:00'},
            {'load_kw': 2343.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:13:00'},
            {'load_kw': 2346.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:14:00'},
            {'load_kw': 2350.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:15:00'},
            {'load_kw': 2353.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:16:00'},
            {'load_kw': 2356.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:17:00'},
            {'load_kw': 2360.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:18:00'},
            {'load_kw': 2363.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:19:00'},
            {'load_kw': 2366.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:20:00'},
            {'load_kw': 2370.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:21:00'},
            {'load_kw': 2373.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:22:00'},
            {'load_kw': 2376.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:23:00'},
            {'load_kw': 2380.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:24:00'},
            {'load_kw': 2383.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:25:00'},
            {'load_kw': 2386.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:26:00'},
            {'load_kw': 2390.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:27:00'},
            {'load_kw': 2393.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:28:00'},
            {'load_kw': 2396.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:29:00'},
            {'load_kw': 2400.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:30:00'},
            {'load_kw': 2403.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:31:00'},
            {'load_kw': 2406.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:32:00'},
            {'load_kw': 2410.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:33:00'},
            {'load_kw': 2413.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:34:00'},
            {'load_kw': 2416.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:35:00'},
            {'load_kw': 2420.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:36:00'},
            {'load_kw': 2423.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:37:00'},
            {'load_kw': 2426.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:38:00'},
            {'load_kw': 2430.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:39:00'},
            {'load_kw': 2433.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:40:00'},
            {'load_kw': 2436.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:41:00'},
            {'load_kw': 2440.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:42:00'},
            {'load_kw': 2443.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:43:00'},
            {'load_kw': 2446.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:44:00'},
            {'load_kw': 2450.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:45:00'},
            {'load_kw': 2453.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:46:00'},
            {'load_kw': 2456.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:47:00'},
            {'load_kw': 2460.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:48:00'},
            {'load_kw': 2463.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:49:00'},
            {'load_kw': 2466.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:50:00'},
            {'load_kw': 2470.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:51:00'},
            {'load_kw': 2473.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:52:00'},
            {'load_kw': 2476.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:53:00'},
            {'load_kw': 2480.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:54:00'},
            {'load_kw': 2483.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:55:00'},
            {'load_kw': 2486.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:56:00'},
            {'load_kw': 2490.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:57:00'},
            {'load_kw': 2493.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:58:00'},
            {'load_kw': 2496.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 04:59:00'},
            {'load_kw': 2500.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:00:00'},
            {'load_kw': 2503.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:01:00'},
            {'load_kw': 2506.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:02:00'},
            {'load_kw': 2510.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:03:00'},
            {'load_kw': 2513.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:04:00'},
            {'load_kw': 2516.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:05:00'},
            {'load_kw': 2520.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:06:00'},
            {'load_kw': 2523.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:07:00'},
            {'load_kw': 2526.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:08:00'},
            {'load_kw': 2530.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:09:00'},
            {'load_kw': 2533.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:10:00'},
            {'load_kw': 2536.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:11:00'},
            {'load_kw': 2540.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:12:00'},
            {'load_kw': 2543.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:13:00'},
            {'load_kw': 2546.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:14:00'},
            {'load_kw': 2550.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:15:00'},
            {'load_kw': 2553.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:16:00'},
            {'load_kw': 2556.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:17:00'},
            {'load_kw': 2560.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:18:00'},
            {'load_kw': 2563.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:19:00'},
            {'load_kw': 2566.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:20:00'},
            {'load_kw': 2570.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:21:00'},
            {'load_kw': 2573.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:22:00'},
            {'load_kw': 2576.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:23:00'},
            {'load_kw': 2580.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:24:00'},
            {'load_kw': 2583.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:25:00'},
            {'load_kw': 2586.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:26:00'},
            {'load_kw': 2590.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:27:00'},
            {'load_kw': 2593.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:28:00'},
            {'load_kw': 2596.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:29:00'},
            {'load_kw': 2600.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:30:00'},
            {'load_kw': 2603.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:31:00'},
            {'load_kw': 2606.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:32:00'},
            {'load_kw': 2610.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:33:00'},
            {'load_kw': 2613.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:34:00'},
            {'load_kw': 2616.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:35:00'},
            {'load_kw': 2620.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:36:00'},
            {'load_kw': 2623.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:37:00'},
            {'load_kw': 2626.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:38:00'},
            {'load_kw': 2630.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:39:00'},
            {'load_kw': 2633.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:40:00'},
            {'load_kw': 2636.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:41:00'},
            {'load_kw': 2640.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:42:00'},
            {'load_kw': 2643.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:43:00'},
            {'load_kw': 2646.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:44:00'},
            {'load_kw': 2650.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:45:00'},
            {'load_kw': 2653.33, 'pv_kw': 6.67, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:46:00'},
            {'load_kw': 2656.67, 'pv_kw': 13.33, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:47:00'},
            {'load_kw': 2660.0, 'pv_kw': 20.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:48:00'},
            {'load_kw': 2663.33, 'pv_kw': 26.67, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:49:00'},
            {'load_kw': 2666.67, 'pv_kw': 33.33, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:50:00'},
            {'load_kw': 2670.0, 'pv_kw': 40.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:51:00'},
            {'load_kw': 2673.33, 'pv_kw': 46.67, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:52:00'},
            {'load_kw': 2676.67, 'pv_kw': 53.33, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:53:00'},
            {'load_kw': 2680.0, 'pv_kw': 60.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:54:00'},
            {'load_kw': 2683.33, 'pv_kw': 66.67, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:55:00'},
            {'load_kw': 2686.67, 'pv_kw': 73.33, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:56:00'},
            {'load_kw': 2690.0, 'pv_kw': 80.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:57:00'},
            {'load_kw': 2693.33, 'pv_kw': 86.67, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:58:00'},
            {'load_kw': 2696.67, 'pv_kw': 93.33, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 05:59:00'},
            {'load_kw': 2700.0, 'pv_kw': 100.0, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:00:00'},
            {'load_kw': 2703.33, 'pv_kw': 105.33, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:01:00'},
            {'load_kw': 2706.67, 'pv_kw': 110.67, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:02:00'},
            {'load_kw': 2710.0, 'pv_kw': 116.0, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:03:00'},
            {'load_kw': 2713.33, 'pv_kw': 121.33, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:04:00'},
            {'load_kw': 2716.67, 'pv_kw': 126.67, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:05:00'},
            {'load_kw': 2720.0, 'pv_kw': 132.0, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:06:00'},
            {'load_kw': 2723.33, 'pv_kw': 137.33, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:07:00'},
            {'load_kw': 2726.67, 'pv_kw': 142.67, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:08:00'},
            {'load_kw': 2730.0, 'pv_kw': 148.0, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:09:00'},
            {'load_kw': 2733.33, 'pv_kw': 153.33, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:10:00'},
            {'load_kw': 2736.67, 'pv_kw': 158.67, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:11:00'},
            {'load_kw': 2740.0, 'pv_kw': 164.0, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:12:00'},
            {'load_kw': 2743.33, 'pv_kw': 169.33, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:13:00'},
            {'load_kw': 2746.67, 'pv_kw': 174.67, 'co_relation_percent': 16, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:14:00'},
            {'load_kw': 2750.0, 'pv_kw': 180.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:15:00'},
            {'load_kw': 2753.33, 'pv_kw': 186.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:16:00'},
            {'load_kw': 2756.67, 'pv_kw': 193.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:17:00'},
            {'load_kw': 2760.0, 'pv_kw': 200.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:18:00'},
            {'load_kw': 2763.33, 'pv_kw': 206.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:19:00'},
            {'load_kw': 2766.67, 'pv_kw': 213.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:20:00'},
            {'load_kw': 2770.0, 'pv_kw': 220.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:21:00'},
            {'load_kw': 2773.33, 'pv_kw': 226.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:22:00'},
            {'load_kw': 2776.67, 'pv_kw': 233.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:23:00'},
            {'load_kw': 2780.0, 'pv_kw': 240.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:24:00'},
            {'load_kw': 2783.33, 'pv_kw': 246.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:25:00'},
            {'load_kw': 2786.67, 'pv_kw': 253.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:26:00'},
            {'load_kw': 2790.0, 'pv_kw': 260.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:27:00'},
            {'load_kw': 2793.33, 'pv_kw': 266.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:28:00'},
            {'load_kw': 2796.67, 'pv_kw': 273.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:29:00'},
            {'load_kw': 2800.0, 'pv_kw': 280.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:30:00'},
            {'load_kw': 2803.33, 'pv_kw': 288.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:31:00'},
            {'load_kw': 2806.67, 'pv_kw': 296.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:32:00'},
            {'load_kw': 2810.0, 'pv_kw': 304.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:33:00'},
            {'load_kw': 2813.33, 'pv_kw': 312.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:34:00'},
            {'load_kw': 2816.67, 'pv_kw': 320.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:35:00'},
            {'load_kw': 2820.0, 'pv_kw': 328.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:36:00'},
            {'load_kw': 2823.33, 'pv_kw': 336.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:37:00'},
            {'load_kw': 2826.67, 'pv_kw': 344.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:38:00'},
            {'load_kw': 2830.0, 'pv_kw': 352.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:39:00'},
            {'load_kw': 2833.33, 'pv_kw': 360.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:40:00'},
            {'load_kw': 2836.67, 'pv_kw': 368.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:41:00'},
            {'load_kw': 2840.0, 'pv_kw': 376.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:42:00'},
            {'load_kw': 2843.33, 'pv_kw': 384.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:43:00'},
            {'load_kw': 2846.67, 'pv_kw': 392.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:44:00'},
            {'load_kw': 2850.0, 'pv_kw': 400.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:45:00'},
            {'load_kw': 2853.33, 'pv_kw': 410.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:46:00'},
            {'load_kw': 2856.67, 'pv_kw': 420.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:47:00'},
            {'load_kw': 2860.0, 'pv_kw': 430.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:48:00'},
            {'load_kw': 2863.33, 'pv_kw': 440.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:49:00'},
            {'load_kw': 2866.67, 'pv_kw': 450.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:50:00'},
            {'load_kw': 2870.0, 'pv_kw': 460.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:51:00'},
            {'load_kw': 2873.33, 'pv_kw': 470.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:52:00'},
            {'load_kw': 2876.67, 'pv_kw': 480.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:53:00'},
            {'load_kw': 2880.0, 'pv_kw': 490.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:54:00'},
            {'load_kw': 2883.33, 'pv_kw': 500.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:55:00'},
            {'load_kw': 2886.67, 'pv_kw': 510.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:56:00'},
            {'load_kw': 2890.0, 'pv_kw': 520.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:57:00'},
            {'load_kw': 2893.33, 'pv_kw': 530.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:58:00'},
            {'load_kw': 2896.67, 'pv_kw': 540.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 06:59:00'},
            {'load_kw': 2900.0, 'pv_kw': 550.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:00:00'},
            {'load_kw': 2903.33, 'pv_kw': 560.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:01:00'},
            {'load_kw': 2906.67, 'pv_kw': 570.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:02:00'},
            {'load_kw': 2910.0, 'pv_kw': 580.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:03:00'},
            {'load_kw': 2913.33, 'pv_kw': 590.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:04:00'},
            {'load_kw': 2916.67, 'pv_kw': 600.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:05:00'},
            {'load_kw': 2920.0, 'pv_kw': 610.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:06:00'},
            {'load_kw': 2923.33, 'pv_kw': 620.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:07:00'},
            {'load_kw': 2926.67, 'pv_kw': 630.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:08:00'},
            {'load_kw': 2930.0, 'pv_kw': 640.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:09:00'},
            {'load_kw': 2933.33, 'pv_kw': 650.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:10:00'},
            {'load_kw': 2936.67, 'pv_kw': 660.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:11:00'},
            {'load_kw': 2940.0, 'pv_kw': 670.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:12:00'},
            {'load_kw': 2943.33, 'pv_kw': 680.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:13:00'},
            {'load_kw': 2946.67, 'pv_kw': 690.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:14:00'},
            {'load_kw': 2950.0, 'pv_kw': 700.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:15:00'},
            {'load_kw': 2953.33, 'pv_kw': 710.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:16:00'},
            {'load_kw': 2956.67, 'pv_kw': 720.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:17:00'},
            {'load_kw': 2960.0, 'pv_kw': 730.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:18:00'},
            {'load_kw': 2963.33, 'pv_kw': 740.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:19:00'},
            {'load_kw': 2966.67, 'pv_kw': 750.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:20:00'},
            {'load_kw': 2970.0, 'pv_kw': 760.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:21:00'},
            {'load_kw': 2973.33, 'pv_kw': 770.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:22:00'},
            {'load_kw': 2976.67, 'pv_kw': 780.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:23:00'},
            {'load_kw': 2980.0, 'pv_kw': 790.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:24:00'},
            {'load_kw': 2983.33, 'pv_kw': 800.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:25:00'},
            {'load_kw': 2986.67, 'pv_kw': 810.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:26:00'},
            {'load_kw': 2990.0, 'pv_kw': 820.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:27:00'},
            {'load_kw': 2993.33, 'pv_kw': 830.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:28:00'},
            {'load_kw': 2996.67, 'pv_kw': 840.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:29:00'},
            {'load_kw': 3000.0, 'pv_kw': 850.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:30:00'},
            {'load_kw': 3003.33, 'pv_kw': 860.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:31:00'},
            {'load_kw': 3006.67, 'pv_kw': 870.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:32:00'},
            {'load_kw': 3010.0, 'pv_kw': 880.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:33:00'},
            {'load_kw': 3013.33, 'pv_kw': 890.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:34:00'},
            {'load_kw': 3016.67, 'pv_kw': 900.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:35:00'},
            {'load_kw': 3020.0, 'pv_kw': 910.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:36:00'},
            {'load_kw': 3023.33, 'pv_kw': 920.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:37:00'},
            {'load_kw': 3026.67, 'pv_kw': 930.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:38:00'},
            {'load_kw': 3030.0, 'pv_kw': 940.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:39:00'},
            {'load_kw': 3033.33, 'pv_kw': 950.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:40:00'},
            {'load_kw': 3036.67, 'pv_kw': 960.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:41:00'},
            {'load_kw': 3040.0, 'pv_kw': 970.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:42:00'},
            {'load_kw': 3043.33, 'pv_kw': 980.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:43:00'},
            {'load_kw': 3046.67, 'pv_kw': 990.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:44:00'},
            {'load_kw': 3050.0, 'pv_kw': 1000.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:45:00'},
            {'load_kw': 3053.33, 'pv_kw': 1010.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:46:00'},
            {'load_kw': 3056.67, 'pv_kw': 1020.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:47:00'},
            {'load_kw': 3060.0, 'pv_kw': 1030.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:48:00'},
            {'load_kw': 3063.33, 'pv_kw': 1040.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:49:00'},
            {'load_kw': 3066.67, 'pv_kw': 1050.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:50:00'},
            {'load_kw': 3070.0, 'pv_kw': 1060.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:51:00'},
            {'load_kw': 3073.33, 'pv_kw': 1070.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:52:00'},
            {'load_kw': 3076.67, 'pv_kw': 1080.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:53:00'},
            {'load_kw': 3080.0, 'pv_kw': 1090.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:54:00'},
            {'load_kw': 3083.33, 'pv_kw': 1100.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:55:00'},
            {'load_kw': 3086.67, 'pv_kw': 1110.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:56:00'},
            {'load_kw': 3090.0, 'pv_kw': 1120.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:57:00'},
            {'load_kw': 3093.33, 'pv_kw': 1130.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:58:00'},
            {'load_kw': 3096.67, 'pv_kw': 1140.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 07:59:00'},
            {'load_kw': 3100.0, 'pv_kw': 1150.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:00:00'},
            {'load_kw': 3103.33, 'pv_kw': 1160.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:01:00'},
            {'load_kw': 3106.67, 'pv_kw': 1170.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:02:00'},
            {'load_kw': 3110.0, 'pv_kw': 1180.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:03:00'},
            {'load_kw': 3113.33, 'pv_kw': 1190.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:04:00'},
            {'load_kw': 3116.67, 'pv_kw': 1200.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:05:00'},
            {'load_kw': 3120.0, 'pv_kw': 1210.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:06:00'},
            {'load_kw': 3123.33, 'pv_kw': 1220.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:07:00'},
            {'load_kw': 3126.67, 'pv_kw': 1230.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:08:00'},
            {'load_kw': 3130.0, 'pv_kw': 1240.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:09:00'},
            {'load_kw': 3133.33, 'pv_kw': 1250.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:10:00'},
            {'load_kw': 3136.67, 'pv_kw': 1260.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:11:00'},
            {'load_kw': 3140.0, 'pv_kw': 1270.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:12:00'},
            {'load_kw': 3143.33, 'pv_kw': 1280.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:13:00'},
            {'load_kw': 3146.67, 'pv_kw': 1290.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:14:00'},
            {'load_kw': 3150.0, 'pv_kw': 1300.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:15:00'},
            {'load_kw': 3153.33, 'pv_kw': 1310.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:16:00'},
            {'load_kw': 3156.67, 'pv_kw': 1320.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:17:00'},
            {'load_kw': 3160.0, 'pv_kw': 1330.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:18:00'},
            {'load_kw': 3163.33, 'pv_kw': 1340.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:19:00'},
            {'load_kw': 3166.67, 'pv_kw': 1350.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:20:00'},
            {'load_kw': 3170.0, 'pv_kw': 1360.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:21:00'},
            {'load_kw': 3173.33, 'pv_kw': 1370.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:22:00'},
            {'load_kw': 3176.67, 'pv_kw': 1380.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:23:00'},
            {'load_kw': 3180.0, 'pv_kw': 1390.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:24:00'},
            {'load_kw': 3183.33, 'pv_kw': 1400.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:25:00'},
            {'load_kw': 3186.67, 'pv_kw': 1410.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:26:00'},
            {'load_kw': 3190.0, 'pv_kw': 1420.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:27:00'},
            {'load_kw': 3193.33, 'pv_kw': 1430.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:28:00'},
            {'load_kw': 3196.67, 'pv_kw': 1440.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:29:00'},
            {'load_kw': 3200.0, 'pv_kw': 1450.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:30:00'},
            {'load_kw': 3203.33, 'pv_kw': 1460.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:31:00'},
            {'load_kw': 3206.67, 'pv_kw': 1470.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:32:00'},
            {'load_kw': 3210.0, 'pv_kw': 1480.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:33:00'},
            {'load_kw': 3213.33, 'pv_kw': 1490.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:34:00'},
            {'load_kw': 3216.67, 'pv_kw': 1500.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:35:00'},
            {'load_kw': 3220.0, 'pv_kw': 1510.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:36:00'},
            {'load_kw': 3223.33, 'pv_kw': 1520.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:37:00'},
            {'load_kw': 3226.67, 'pv_kw': 1530.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:38:00'},
            {'load_kw': 3230.0, 'pv_kw': 1540.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:39:00'},
            {'load_kw': 3233.33, 'pv_kw': 1550.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:40:00'},
            {'load_kw': 3236.67, 'pv_kw': 1560.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:41:00'},
            {'load_kw': 3240.0, 'pv_kw': 1570.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:42:00'},
            {'load_kw': 3243.33, 'pv_kw': 1580.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:43:00'},
            {'load_kw': 3246.67, 'pv_kw': 1590.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:44:00'},
            {'load_kw': 3250.0, 'pv_kw': 1600.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:45:00'},
            {'load_kw': 3253.33, 'pv_kw': 1610.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:46:00'},
            {'load_kw': 3256.67, 'pv_kw': 1620.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:47:00'},
            {'load_kw': 3260.0, 'pv_kw': 1630.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:48:00'},
            {'load_kw': 3263.33, 'pv_kw': 1640.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:49:00'},
            {'load_kw': 3266.67, 'pv_kw': 1650.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:50:00'},
            {'load_kw': 3270.0, 'pv_kw': 1660.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:51:00'},
            {'load_kw': 3273.33, 'pv_kw': 1670.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:52:00'},
            {'load_kw': 3276.67, 'pv_kw': 1680.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:53:00'},
            {'load_kw': 3280.0, 'pv_kw': 1690.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:54:00'},
            {'load_kw': 3283.33, 'pv_kw': 1700.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:55:00'},
            {'load_kw': 3286.67, 'pv_kw': 1710.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:56:00'},
            {'load_kw': 3290.0, 'pv_kw': 1720.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:57:00'},
            {'load_kw': 3293.33, 'pv_kw': 1730.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:58:00'},
            {'load_kw': 3296.67, 'pv_kw': 1740.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 08:59:00'},
            {'load_kw': 3300.0, 'pv_kw': 1750.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:00:00'},
            {'load_kw': 3303.33, 'pv_kw': 1760.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:01:00'},
            {'load_kw': 3306.67, 'pv_kw': 1770.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:02:00'},
            {'load_kw': 3310.0, 'pv_kw': 1780.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:03:00'},
            {'load_kw': 3313.33, 'pv_kw': 1790.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:04:00'},
            {'load_kw': 3316.67, 'pv_kw': 1800.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:05:00'},
            {'load_kw': 3320.0, 'pv_kw': 1810.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:06:00'},
            {'load_kw': 3323.33, 'pv_kw': 1820.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:07:00'},
            {'load_kw': 3326.67, 'pv_kw': 1830.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:08:00'},
            {'load_kw': 3330.0, 'pv_kw': 1840.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:09:00'},
            {'load_kw': 3333.33, 'pv_kw': 1850.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:10:00'},
            {'load_kw': 3336.67, 'pv_kw': 1860.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:11:00'},
            {'load_kw': 3340.0, 'pv_kw': 1870.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:12:00'},
            {'load_kw': 3343.33, 'pv_kw': 1880.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:13:00'},
            {'load_kw': 3346.67, 'pv_kw': 1890.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:14:00'},
            {'load_kw': 3350.0, 'pv_kw': 1900.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:15:00'},
            {'load_kw': 3353.33, 'pv_kw': 1910.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:16:00'},
            {'load_kw': 3356.67, 'pv_kw': 1920.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:17:00'},
            {'load_kw': 3360.0, 'pv_kw': 1930.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:18:00'},
            {'load_kw': 3363.33, 'pv_kw': 1940.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:19:00'},
            {'load_kw': 3366.67, 'pv_kw': 1950.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:20:00'},
            {'load_kw': 3370.0, 'pv_kw': 1960.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:21:00'},
            {'load_kw': 3373.33, 'pv_kw': 1970.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:22:00'},
            {'load_kw': 3376.67, 'pv_kw': 1980.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:23:00'},
            {'load_kw': 3380.0, 'pv_kw': 1990.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:24:00'},
            {'load_kw': 3383.33, 'pv_kw': 2000.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:25:00'},
            {'load_kw': 3386.67, 'pv_kw': 2010.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:26:00'},
            {'load_kw': 3390.0, 'pv_kw': 2020.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:27:00'},
            {'load_kw': 3393.33, 'pv_kw': 2030.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:28:00'},
            {'load_kw': 3396.67, 'pv_kw': 2040.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:29:00'},
            {'load_kw': 3400.0, 'pv_kw': 2050.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:30:00'},
            {'load_kw': 3403.33, 'pv_kw': 2060.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:31:00'},
            {'load_kw': 3406.67, 'pv_kw': 2070.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:32:00'},
            {'load_kw': 3410.0, 'pv_kw': 2080.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:33:00'},
            {'load_kw': 3413.33, 'pv_kw': 2090.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:34:00'},
            {'load_kw': 3416.67, 'pv_kw': 2100.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:35:00'},
            {'load_kw': 3420.0, 'pv_kw': 2110.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:36:00'},
            {'load_kw': 3423.33, 'pv_kw': 2120.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:37:00'},
            {'load_kw': 3426.67, 'pv_kw': 2130.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:38:00'},
            {'load_kw': 3430.0, 'pv_kw': 2140.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:39:00'},
            {'load_kw': 3433.33, 'pv_kw': 2150.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:40:00'},
            {'load_kw': 3436.67, 'pv_kw': 2160.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:41:00'},
            {'load_kw': 3440.0, 'pv_kw': 2170.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:42:00'},
            {'load_kw': 3443.33, 'pv_kw': 2180.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:43:00'},
            {'load_kw': 3446.67, 'pv_kw': 2190.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:44:00'},
            {'load_kw': 3450.0, 'pv_kw': 2200.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:45:00'},
            {'load_kw': 3453.33, 'pv_kw': 2206.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:46:00'},
            {'load_kw': 3456.67, 'pv_kw': 2213.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:47:00'},
            {'load_kw': 3460.0, 'pv_kw': 2220.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:48:00'},
            {'load_kw': 3463.33, 'pv_kw': 2226.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:49:00'},
            {'load_kw': 3466.67, 'pv_kw': 2233.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:50:00'},
            {'load_kw': 3470.0, 'pv_kw': 2240.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:51:00'},
            {'load_kw': 3473.33, 'pv_kw': 2246.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:52:00'},
            {'load_kw': 3476.67, 'pv_kw': 2253.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:53:00'},
            {'load_kw': 3480.0, 'pv_kw': 2260.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:54:00'},
            {'load_kw': 3483.33, 'pv_kw': 2266.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:55:00'},
            {'load_kw': 3486.67, 'pv_kw': 2273.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:56:00'},
            {'load_kw': 3490.0, 'pv_kw': 2280.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:57:00'},
            {'load_kw': 3493.33, 'pv_kw': 2286.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:58:00'},
            {'load_kw': 3496.67, 'pv_kw': 2293.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 09:59:00'},
            {'load_kw': 3500.0, 'pv_kw': 2300.0, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:00:00'},
            {'load_kw': 3501.33, 'pv_kw': 2305.33, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:01:00'},
            {'load_kw': 3502.67, 'pv_kw': 2310.67, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:02:00'},
            {'load_kw': 3504.0, 'pv_kw': 2316.0, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:03:00'},
            {'load_kw': 3505.33, 'pv_kw': 2321.33, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:04:00'},
            {'load_kw': 3506.67, 'pv_kw': 2326.67, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:05:00'},
            {'load_kw': 3508.0, 'pv_kw': 2332.0, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:06:00'},
            {'load_kw': 3509.33, 'pv_kw': 2337.33, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:07:00'},
            {'load_kw': 3510.67, 'pv_kw': 2342.67, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:08:00'},
            {'load_kw': 3512.0, 'pv_kw': 2348.0, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:09:00'},
            {'load_kw': 3513.33, 'pv_kw': 2353.33, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:10:00'},
            {'load_kw': 3514.67, 'pv_kw': 2358.67, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:11:00'},
            {'load_kw': 3516.0, 'pv_kw': 2364.0, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:12:00'},
            {'load_kw': 3517.33, 'pv_kw': 2369.33, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:13:00'},
            {'load_kw': 3518.67, 'pv_kw': 2374.67, 'co_relation_percent': 20, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:14:00'},
            {'load_kw': 3520.0, 'pv_kw': 2380.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:15:00'},
            {'load_kw': 3522.0, 'pv_kw': 2384.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:16:00'},
            {'load_kw': 3524.0, 'pv_kw': 2389.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:17:00'},
            {'load_kw': 3526.0, 'pv_kw': 2394.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:18:00'},
            {'load_kw': 3528.0, 'pv_kw': 2398.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:19:00'},
            {'load_kw': 3530.0, 'pv_kw': 2403.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:20:00'},
            {'load_kw': 3532.0, 'pv_kw': 2408.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:21:00'},
            {'load_kw': 3534.0, 'pv_kw': 2412.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:22:00'},
            {'load_kw': 3536.0, 'pv_kw': 2417.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:23:00'},
            {'load_kw': 3538.0, 'pv_kw': 2422.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:24:00'},
            {'load_kw': 3540.0, 'pv_kw': 2426.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:25:00'},
            {'load_kw': 3542.0, 'pv_kw': 2431.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:26:00'},
            {'load_kw': 3544.0, 'pv_kw': 2436.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:27:00'},
            {'load_kw': 3546.0, 'pv_kw': 2440.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:28:00'},
            {'load_kw': 3548.0, 'pv_kw': 2445.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:29:00'},
            {'load_kw': 3550.0, 'pv_kw': 2450.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:30:00'},
            {'load_kw': 3552.0, 'pv_kw': 2453.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:31:00'},
            {'load_kw': 3554.0, 'pv_kw': 2456.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:32:00'},
            {'load_kw': 3556.0, 'pv_kw': 2460.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:33:00'},
            {'load_kw': 3558.0, 'pv_kw': 2463.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:34:00'},
            {'load_kw': 3560.0, 'pv_kw': 2466.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:35:00'},
            {'load_kw': 3562.0, 'pv_kw': 2470.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:36:00'},
            {'load_kw': 3564.0, 'pv_kw': 2473.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:37:00'},
            {'load_kw': 3566.0, 'pv_kw': 2476.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:38:00'},
            {'load_kw': 3568.0, 'pv_kw': 2480.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:39:00'},
            {'load_kw': 3570.0, 'pv_kw': 2483.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:40:00'},
            {'load_kw': 3572.0, 'pv_kw': 2486.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:41:00'},
            {'load_kw': 3574.0, 'pv_kw': 2490.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:42:00'},
            {'load_kw': 3576.0, 'pv_kw': 2493.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:43:00'},
            {'load_kw': 3578.0, 'pv_kw': 2496.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:44:00'},
            {'load_kw': 3580.0, 'pv_kw': 2500.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:45:00'},
            {'load_kw': 3581.33, 'pv_kw': 2503.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:46:00'},
            {'load_kw': 3582.67, 'pv_kw': 2506.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:47:00'},
            {'load_kw': 3584.0, 'pv_kw': 2510.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:48:00'},
            {'load_kw': 3585.33, 'pv_kw': 2513.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:49:00'},
            {'load_kw': 3586.67, 'pv_kw': 2516.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:50:00'},
            {'load_kw': 3588.0, 'pv_kw': 2520.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:51:00'},
            {'load_kw': 3589.33, 'pv_kw': 2523.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:52:00'},
            {'load_kw': 3590.67, 'pv_kw': 2526.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:53:00'},
            {'load_kw': 3592.0, 'pv_kw': 2530.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:54:00'},
            {'load_kw': 3593.33, 'pv_kw': 2533.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:55:00'},
            {'load_kw': 3594.67, 'pv_kw': 2536.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:56:00'},
            {'load_kw': 3596.0, 'pv_kw': 2540.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:57:00'},
            {'load_kw': 3597.33, 'pv_kw': 2543.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:58:00'},
            {'load_kw': 3598.67, 'pv_kw': 2546.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 10:59:00'},
            {'load_kw': 3600.0, 'pv_kw': 2550.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:00:00'},
            {'load_kw': 3596.67, 'pv_kw': 2553.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:01:00'},
            {'load_kw': 3593.33, 'pv_kw': 2556.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:02:00'},
            {'load_kw': 3590.0, 'pv_kw': 2560.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:03:00'},
            {'load_kw': 3586.67, 'pv_kw': 2563.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:04:00'},
            {'load_kw': 3583.33, 'pv_kw': 2566.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:05:00'},
            {'load_kw': 3580.0, 'pv_kw': 2570.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:06:00'},
            {'load_kw': 3576.67, 'pv_kw': 2573.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:07:00'},
            {'load_kw': 3573.33, 'pv_kw': 2576.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:08:00'},
            {'load_kw': 3570.0, 'pv_kw': 2580.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:09:00'},
            {'load_kw': 3566.67, 'pv_kw': 2583.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:10:00'},
            {'load_kw': 3563.33, 'pv_kw': 2586.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:11:00'},
            {'load_kw': 3560.0, 'pv_kw': 2590.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:12:00'},
            {'load_kw': 3556.67, 'pv_kw': 2593.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:13:00'},
            {'load_kw': 3553.33, 'pv_kw': 2596.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:14:00'},
            {'load_kw': 3550.0, 'pv_kw': 2600.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:15:00'},
            {'load_kw': 3546.67, 'pv_kw': 2603.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:16:00'},
            {'load_kw': 3543.33, 'pv_kw': 2606.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:17:00'},
            {'load_kw': 3540.0, 'pv_kw': 2610.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:18:00'},
            {'load_kw': 3536.67, 'pv_kw': 2613.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:19:00'},
            {'load_kw': 3533.33, 'pv_kw': 2616.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:20:00'},
            {'load_kw': 3530.0, 'pv_kw': 2620.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:21:00'},
            {'load_kw': 3526.67, 'pv_kw': 2623.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:22:00'},
            {'load_kw': 3523.33, 'pv_kw': 2626.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:23:00'},
            {'load_kw': 3520.0, 'pv_kw': 2630.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:24:00'},
            {'load_kw': 3516.67, 'pv_kw': 2633.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:25:00'},
            {'load_kw': 3513.33, 'pv_kw': 2636.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:26:00'},
            {'load_kw': 3510.0, 'pv_kw': 2640.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:27:00'},
            {'load_kw': 3506.67, 'pv_kw': 2643.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:28:00'},
            {'load_kw': 3503.33, 'pv_kw': 2646.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:29:00'},
            {'load_kw': 3500.0, 'pv_kw': 2650.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:30:00'},
            {'load_kw': 3498.67, 'pv_kw': 2653.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:31:00'},
            {'load_kw': 3497.33, 'pv_kw': 2656.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:32:00'},
            {'load_kw': 3496.0, 'pv_kw': 2660.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:33:00'},
            {'load_kw': 3494.67, 'pv_kw': 2663.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:34:00'},
            {'load_kw': 3493.33, 'pv_kw': 2666.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:35:00'},
            {'load_kw': 3492.0, 'pv_kw': 2670.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:36:00'},
            {'load_kw': 3490.67, 'pv_kw': 2673.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:37:00'},
            {'load_kw': 3489.33, 'pv_kw': 2676.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:38:00'},
            {'load_kw': 3488.0, 'pv_kw': 2680.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:39:00'},
            {'load_kw': 3486.67, 'pv_kw': 2683.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:40:00'},
            {'load_kw': 3485.33, 'pv_kw': 2686.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:41:00'},
            {'load_kw': 3484.0, 'pv_kw': 2690.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:42:00'},
            {'load_kw': 3482.67, 'pv_kw': 2693.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:43:00'},
            {'load_kw': 3481.33, 'pv_kw': 2696.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:44:00'},
            {'load_kw': 3480.0, 'pv_kw': 2700.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:45:00'},
            {'load_kw': 3478.0, 'pv_kw': 2703.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:46:00'},
            {'load_kw': 3476.0, 'pv_kw': 2706.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:47:00'},
            {'load_kw': 3474.0, 'pv_kw': 2710.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:48:00'},
            {'load_kw': 3472.0, 'pv_kw': 2713.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:49:00'},
            {'load_kw': 3470.0, 'pv_kw': 2716.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:50:00'},
            {'load_kw': 3468.0, 'pv_kw': 2720.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:51:00'},
            {'load_kw': 3466.0, 'pv_kw': 2723.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:52:00'},
            {'load_kw': 3464.0, 'pv_kw': 2726.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:53:00'},
            {'load_kw': 3462.0, 'pv_kw': 2730.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:54:00'},
            {'load_kw': 3460.0, 'pv_kw': 2733.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:55:00'},
            {'load_kw': 3458.0, 'pv_kw': 2736.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:56:00'},
            {'load_kw': 3456.0, 'pv_kw': 2740.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:57:00'},
            {'load_kw': 3454.0, 'pv_kw': 2743.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:58:00'},
            {'load_kw': 3452.0, 'pv_kw': 2746.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 11:59:00'},
            {'load_kw': 3450.0, 'pv_kw': 2750.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:00:00'},
            {'load_kw': 3453.33, 'pv_kw': 2752.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:01:00'},
            {'load_kw': 3456.67, 'pv_kw': 2754.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:02:00'},
            {'load_kw': 3460.0, 'pv_kw': 2756.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:03:00'},
            {'load_kw': 3463.33, 'pv_kw': 2758.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:04:00'},
            {'load_kw': 3466.67, 'pv_kw': 2760.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:05:00'},
            {'load_kw': 3470.0, 'pv_kw': 2762.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:06:00'},
            {'load_kw': 3473.33, 'pv_kw': 2764.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:07:00'},
            {'load_kw': 3476.67, 'pv_kw': 2766.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:08:00'},
            {'load_kw': 3480.0, 'pv_kw': 2768.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:09:00'},
            {'load_kw': 3483.33, 'pv_kw': 2770.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:10:00'},
            {'load_kw': 3486.67, 'pv_kw': 2772.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:11:00'},
            {'load_kw': 3490.0, 'pv_kw': 2774.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:12:00'},
            {'load_kw': 3493.33, 'pv_kw': 2776.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:13:00'},
            {'load_kw': 3496.67, 'pv_kw': 2778.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:14:00'},
            {'load_kw': 3500.0, 'pv_kw': 2780.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:15:00'},
            {'load_kw': 3503.33, 'pv_kw': 2781.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:16:00'},
            {'load_kw': 3506.67, 'pv_kw': 2782.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:17:00'},
            {'load_kw': 3510.0, 'pv_kw': 2784.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:18:00'},
            {'load_kw': 3513.33, 'pv_kw': 2785.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:19:00'},
            {'load_kw': 3516.67, 'pv_kw': 2786.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:20:00'},
            {'load_kw': 3520.0, 'pv_kw': 2788.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:21:00'},
            {'load_kw': 3523.33, 'pv_kw': 2789.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:22:00'},
            {'load_kw': 3526.67, 'pv_kw': 2790.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:23:00'},
            {'load_kw': 3530.0, 'pv_kw': 2792.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:24:00'},
            {'load_kw': 3533.33, 'pv_kw': 2793.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:25:00'},
            {'load_kw': 3536.67, 'pv_kw': 2794.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:26:00'},
            {'load_kw': 3540.0, 'pv_kw': 2796.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:27:00'},
            {'load_kw': 3543.33, 'pv_kw': 2797.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:28:00'},
            {'load_kw': 3546.67, 'pv_kw': 2798.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:29:00'},
            {'load_kw': 3550.0, 'pv_kw': 2800.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:30:00'},
            {'load_kw': 3553.33, 'pv_kw': 2798.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:31:00'},
            {'load_kw': 3556.67, 'pv_kw': 2797.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:32:00'},
            {'load_kw': 3560.0, 'pv_kw': 2796.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:33:00'},
            {'load_kw': 3563.33, 'pv_kw': 2794.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:34:00'},
            {'load_kw': 3566.67, 'pv_kw': 2793.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:35:00'},
            {'load_kw': 3570.0, 'pv_kw': 2792.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:36:00'},
            {'load_kw': 3573.33, 'pv_kw': 2790.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:37:00'},
            {'load_kw': 3576.67, 'pv_kw': 2789.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:38:00'},
            {'load_kw': 3580.0, 'pv_kw': 2788.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:39:00'},
            {'load_kw': 3583.33, 'pv_kw': 2786.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:40:00'},
            {'load_kw': 3586.67, 'pv_kw': 2785.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:41:00'},
            {'load_kw': 3590.0, 'pv_kw': 2784.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:42:00'},
            {'load_kw': 3593.33, 'pv_kw': 2782.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:43:00'},
            {'load_kw': 3596.67, 'pv_kw': 2781.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:44:00'},
            {'load_kw': 3600.0, 'pv_kw': 2780.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:45:00'},
            {'load_kw': 3603.33, 'pv_kw': 2778.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:46:00'},
            {'load_kw': 3606.67, 'pv_kw': 2776.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:47:00'},
            {'load_kw': 3610.0, 'pv_kw': 2774.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:48:00'},
            {'load_kw': 3613.33, 'pv_kw': 2772.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:49:00'},
            {'load_kw': 3616.67, 'pv_kw': 2770.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:50:00'},
            {'load_kw': 3620.0, 'pv_kw': 2768.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:51:00'},
            {'load_kw': 3623.33, 'pv_kw': 2766.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:52:00'},
            {'load_kw': 3626.67, 'pv_kw': 2764.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:53:00'},
            {'load_kw': 3630.0, 'pv_kw': 2762.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:54:00'},
            {'load_kw': 3633.33, 'pv_kw': 2760.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:55:00'},
            {'load_kw': 3636.67, 'pv_kw': 2758.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:56:00'},
            {'load_kw': 3640.0, 'pv_kw': 2756.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:57:00'},
            {'load_kw': 3643.33, 'pv_kw': 2754.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:58:00'},
            {'load_kw': 3646.67, 'pv_kw': 2752.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 12:59:00'},
            {'load_kw': 3650.0, 'pv_kw': 2750.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:00:00'},
            {'load_kw': 3653.33, 'pv_kw': 2746.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:01:00'},
            {'load_kw': 3656.67, 'pv_kw': 2743.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:02:00'},
            {'load_kw': 3660.0, 'pv_kw': 2740.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:03:00'},
            {'load_kw': 3663.33, 'pv_kw': 2736.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:04:00'},
            {'load_kw': 3666.67, 'pv_kw': 2733.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:05:00'},
            {'load_kw': 3670.0, 'pv_kw': 2730.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:06:00'},
            {'load_kw': 3673.33, 'pv_kw': 2726.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:07:00'},
            {'load_kw': 3676.67, 'pv_kw': 2723.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:08:00'},
            {'load_kw': 3680.0, 'pv_kw': 2720.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:09:00'},
            {'load_kw': 3683.33, 'pv_kw': 2716.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:10:00'},
            {'load_kw': 3686.67, 'pv_kw': 2713.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:11:00'},
            {'load_kw': 3690.0, 'pv_kw': 2710.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:12:00'},
            {'load_kw': 3693.33, 'pv_kw': 2706.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:13:00'},
            {'load_kw': 3696.67, 'pv_kw': 2703.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:14:00'},
            {'load_kw': 3700.0, 'pv_kw': 2700.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:15:00'},
            {'load_kw': 3703.33, 'pv_kw': 2696.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:16:00'},
            {'load_kw': 3706.67, 'pv_kw': 2693.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:17:00'},
            {'load_kw': 3710.0, 'pv_kw': 2690.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:18:00'},
            {'load_kw': 3713.33, 'pv_kw': 2686.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:19:00'},
            {'load_kw': 3716.67, 'pv_kw': 2683.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:20:00'},
            {'load_kw': 3720.0, 'pv_kw': 2680.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:21:00'},
            {'load_kw': 3723.33, 'pv_kw': 2676.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:22:00'},
            {'load_kw': 3726.67, 'pv_kw': 2673.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:23:00'},
            {'load_kw': 3730.0, 'pv_kw': 2670.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:24:00'},
            {'load_kw': 3733.33, 'pv_kw': 2666.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:25:00'},
            {'load_kw': 3736.67, 'pv_kw': 2663.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:26:00'},
            {'load_kw': 3740.0, 'pv_kw': 2660.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:27:00'},
            {'load_kw': 3743.33, 'pv_kw': 2656.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:28:00'},
            {'load_kw': 3746.67, 'pv_kw': 2653.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:29:00'},
            {'load_kw': 3750.0, 'pv_kw': 2650.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:30:00'},
            {'load_kw': 3753.33, 'pv_kw': 2643.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:31:00'},
            {'load_kw': 3756.67, 'pv_kw': 2636.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:32:00'},
            {'load_kw': 3760.0, 'pv_kw': 2630.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:33:00'},
            {'load_kw': 3763.33, 'pv_kw': 2623.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:34:00'},
            {'load_kw': 3766.67, 'pv_kw': 2616.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:35:00'},
            {'load_kw': 3770.0, 'pv_kw': 2610.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:36:00'},
            {'load_kw': 3773.33, 'pv_kw': 2603.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:37:00'},
            {'load_kw': 3776.67, 'pv_kw': 2596.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:38:00'},
            {'load_kw': 3780.0, 'pv_kw': 2590.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:39:00'},
            {'load_kw': 3783.33, 'pv_kw': 2583.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:40:00'},
            {'load_kw': 3786.67, 'pv_kw': 2576.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:41:00'},
            {'load_kw': 3790.0, 'pv_kw': 2570.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:42:00'},
            {'load_kw': 3793.33, 'pv_kw': 2563.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:43:00'},
            {'load_kw': 3796.67, 'pv_kw': 2556.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:44:00'},
            {'load_kw': 3800.0, 'pv_kw': 2550.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:45:00'},
            {'load_kw': 3801.33, 'pv_kw': 2543.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:46:00'},
            {'load_kw': 3802.67, 'pv_kw': 2536.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:47:00'},
            {'load_kw': 3804.0, 'pv_kw': 2530.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:48:00'},
            {'load_kw': 3805.33, 'pv_kw': 2523.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:49:00'},
            {'load_kw': 3806.67, 'pv_kw': 2516.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:50:00'},
            {'load_kw': 3808.0, 'pv_kw': 2510.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:51:00'},
            {'load_kw': 3809.33, 'pv_kw': 2503.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:52:00'},
            {'load_kw': 3810.67, 'pv_kw': 2496.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:53:00'},
            {'load_kw': 3812.0, 'pv_kw': 2490.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:54:00'},
            {'load_kw': 3813.33, 'pv_kw': 2483.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:55:00'},
            {'load_kw': 3814.67, 'pv_kw': 2476.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:56:00'},
            {'load_kw': 3816.0, 'pv_kw': 2470.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:57:00'},
            {'load_kw': 3817.33, 'pv_kw': 2463.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:58:00'},
            {'load_kw': 3818.67, 'pv_kw': 2456.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 13:59:00'},
            {'load_kw': 3820.0, 'pv_kw': 2450.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:00:00'},
            {'load_kw': 3822.0, 'pv_kw': 2443.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:01:00'},
            {'load_kw': 3824.0, 'pv_kw': 2436.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:02:00'},
            {'load_kw': 3826.0, 'pv_kw': 2430.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:03:00'},
            {'load_kw': 3828.0, 'pv_kw': 2423.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:04:00'},
            {'load_kw': 3830.0, 'pv_kw': 2416.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:05:00'},
            {'load_kw': 3832.0, 'pv_kw': 2410.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:06:00'},
            {'load_kw': 3834.0, 'pv_kw': 2403.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:07:00'},
            {'load_kw': 3836.0, 'pv_kw': 2396.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:08:00'},
            {'load_kw': 3838.0, 'pv_kw': 2390.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:09:00'},
            {'load_kw': 3840.0, 'pv_kw': 2383.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:10:00'},
            {'load_kw': 3842.0, 'pv_kw': 2376.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:11:00'},
            {'load_kw': 3844.0, 'pv_kw': 2370.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:12:00'},
            {'load_kw': 3846.0, 'pv_kw': 2363.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:13:00'},
            {'load_kw': 3848.0, 'pv_kw': 2356.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:14:00'},
            {'load_kw': 3850.0, 'pv_kw': 2350.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:15:00'},
            {'load_kw': 3852.0, 'pv_kw': 2343.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:16:00'},
            {'load_kw': 3854.0, 'pv_kw': 2336.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:17:00'},
            {'load_kw': 3856.0, 'pv_kw': 2330.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:18:00'},
            {'load_kw': 3858.0, 'pv_kw': 2323.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:19:00'},
            {'load_kw': 3860.0, 'pv_kw': 2316.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:20:00'},
            {'load_kw': 3862.0, 'pv_kw': 2310.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:21:00'},
            {'load_kw': 3864.0, 'pv_kw': 2303.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:22:00'},
            {'load_kw': 3866.0, 'pv_kw': 2296.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:23:00'},
            {'load_kw': 3868.0, 'pv_kw': 2290.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:24:00'},
            {'load_kw': 3870.0, 'pv_kw': 2283.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:25:00'},
            {'load_kw': 3872.0, 'pv_kw': 2276.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:26:00'},
            {'load_kw': 3874.0, 'pv_kw': 2270.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:27:00'},
            {'load_kw': 3876.0, 'pv_kw': 2263.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:28:00'},
            {'load_kw': 3878.0, 'pv_kw': 2256.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:29:00'},
            {'load_kw': 3880.0, 'pv_kw': 2250.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:30:00'},
            {'load_kw': 3881.33, 'pv_kw': 2243.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:31:00'},
            {'load_kw': 3882.67, 'pv_kw': 2236.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:32:00'},
            {'load_kw': 3884.0, 'pv_kw': 2230.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:33:00'},
            {'load_kw': 3885.33, 'pv_kw': 2223.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:34:00'},
            {'load_kw': 3886.67, 'pv_kw': 2216.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:35:00'},
            {'load_kw': 3888.0, 'pv_kw': 2210.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:36:00'},
            {'load_kw': 3889.33, 'pv_kw': 2203.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:37:00'},
            {'load_kw': 3890.67, 'pv_kw': 2196.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:38:00'},
            {'load_kw': 3892.0, 'pv_kw': 2190.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:39:00'},
            {'load_kw': 3893.33, 'pv_kw': 2183.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:40:00'},
            {'load_kw': 3894.67, 'pv_kw': 2176.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:41:00'},
            {'load_kw': 3896.0, 'pv_kw': 2170.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:42:00'},
            {'load_kw': 3897.33, 'pv_kw': 2163.33, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:43:00'},
            {'load_kw': 3898.67, 'pv_kw': 2156.67, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:44:00'},
            {'load_kw': 3900.0, 'pv_kw': 2150.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:45:00'},
            {'load_kw': 3901.33, 'pv_kw': 2140.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:46:00'},
            {'load_kw': 3902.67, 'pv_kw': 2130.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:47:00'},
            {'load_kw': 3904.0, 'pv_kw': 2120.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:48:00'},
            {'load_kw': 3905.33, 'pv_kw': 2110.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:49:00'},
            {'load_kw': 3906.67, 'pv_kw': 2100.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:50:00'},
            {'load_kw': 3908.0, 'pv_kw': 2090.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:51:00'},
            {'load_kw': 3909.33, 'pv_kw': 2080.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:52:00'},
            {'load_kw': 3910.67, 'pv_kw': 2070.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:53:00'},
            {'load_kw': 3912.0, 'pv_kw': 2060.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:54:00'},
            {'load_kw': 3913.33, 'pv_kw': 2050.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:55:00'},
            {'load_kw': 3914.67, 'pv_kw': 2040.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:56:00'},
            {'load_kw': 3916.0, 'pv_kw': 2030.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:57:00'},
            {'load_kw': 3917.33, 'pv_kw': 2020.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:58:00'},
            {'load_kw': 3918.67, 'pv_kw': 2010.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 14:59:00'},
            {'load_kw': 3920.0, 'pv_kw': 2000.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:00:00'},
            {'load_kw': 3922.0, 'pv_kw': 1990.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:01:00'},
            {'load_kw': 3924.0, 'pv_kw': 1980.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:02:00'},
            {'load_kw': 3926.0, 'pv_kw': 1970.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:03:00'},
            {'load_kw': 3928.0, 'pv_kw': 1960.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:04:00'},
            {'load_kw': 3930.0, 'pv_kw': 1950.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:05:00'},
            {'load_kw': 3932.0, 'pv_kw': 1940.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:06:00'},
            {'load_kw': 3934.0, 'pv_kw': 1930.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:07:00'},
            {'load_kw': 3936.0, 'pv_kw': 1920.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:08:00'},
            {'load_kw': 3938.0, 'pv_kw': 1910.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:09:00'},
            {'load_kw': 3940.0, 'pv_kw': 1900.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:10:00'},
            {'load_kw': 3942.0, 'pv_kw': 1890.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:11:00'},
            {'load_kw': 3944.0, 'pv_kw': 1880.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:12:00'},
            {'load_kw': 3946.0, 'pv_kw': 1870.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:13:00'},
            {'load_kw': 3948.0, 'pv_kw': 1860.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:14:00'},
            {'load_kw': 3950.0, 'pv_kw': 1850.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:15:00'},
            {'load_kw': 3952.0, 'pv_kw': 1840.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:16:00'},
            {'load_kw': 3954.0, 'pv_kw': 1830.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:17:00'},
            {'load_kw': 3956.0, 'pv_kw': 1820.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:18:00'},
            {'load_kw': 3958.0, 'pv_kw': 1810.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:19:00'},
            {'load_kw': 3960.0, 'pv_kw': 1800.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:20:00'},
            {'load_kw': 3962.0, 'pv_kw': 1790.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:21:00'},
            {'load_kw': 3964.0, 'pv_kw': 1780.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:22:00'},
            {'load_kw': 3966.0, 'pv_kw': 1770.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:23:00'},
            {'load_kw': 3968.0, 'pv_kw': 1760.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:24:00'},
            {'load_kw': 3970.0, 'pv_kw': 1750.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:25:00'},
            {'load_kw': 3972.0, 'pv_kw': 1740.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:26:00'},
            {'load_kw': 3974.0, 'pv_kw': 1730.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:27:00'},
            {'load_kw': 3976.0, 'pv_kw': 1720.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:28:00'},
            {'load_kw': 3978.0, 'pv_kw': 1710.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:29:00'},
            {'load_kw': 3980.0, 'pv_kw': 1700.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:30:00'},
            {'load_kw': 3981.33, 'pv_kw': 1690.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:31:00'},
            {'load_kw': 3982.67, 'pv_kw': 1680.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:32:00'},
            {'load_kw': 3984.0, 'pv_kw': 1670.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:33:00'},
            {'load_kw': 3985.33, 'pv_kw': 1660.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:34:00'},
            {'load_kw': 3986.67, 'pv_kw': 1650.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:35:00'},
            {'load_kw': 3988.0, 'pv_kw': 1640.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:36:00'},
            {'load_kw': 3989.33, 'pv_kw': 1630.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:37:00'},
            {'load_kw': 3990.67, 'pv_kw': 1620.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:38:00'},
            {'load_kw': 3992.0, 'pv_kw': 1610.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:39:00'},
            {'load_kw': 3993.33, 'pv_kw': 1600.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:40:00'},
            {'load_kw': 3994.67, 'pv_kw': 1590.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:41:00'},
            {'load_kw': 3996.0, 'pv_kw': 1580.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:42:00'},
            {'load_kw': 3997.33, 'pv_kw': 1570.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:43:00'},
            {'load_kw': 3998.67, 'pv_kw': 1560.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:44:00'},
            {'load_kw': 4000.0, 'pv_kw': 1550.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:45:00'},
            {'load_kw': 4001.33, 'pv_kw': 1540.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:46:00'},
            {'load_kw': 4002.67, 'pv_kw': 1530.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:47:00'},
            {'load_kw': 4004.0, 'pv_kw': 1520.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:48:00'},
            {'load_kw': 4005.33, 'pv_kw': 1510.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:49:00'},
            {'load_kw': 4006.67, 'pv_kw': 1500.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:50:00'},
            {'load_kw': 4008.0, 'pv_kw': 1490.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:51:00'},
            {'load_kw': 4009.33, 'pv_kw': 1480.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:52:00'},
            {'load_kw': 4010.67, 'pv_kw': 1470.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:53:00'},
            {'load_kw': 4012.0, 'pv_kw': 1460.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:54:00'},
            {'load_kw': 4013.33, 'pv_kw': 1450.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:55:00'},
            {'load_kw': 4014.67, 'pv_kw': 1440.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:56:00'},
            {'load_kw': 4016.0, 'pv_kw': 1430.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:57:00'},
            {'load_kw': 4017.33, 'pv_kw': 1420.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:58:00'},
            {'load_kw': 4018.67, 'pv_kw': 1410.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 15:59:00'},
            {'load_kw': 4020.0, 'pv_kw': 1400.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:00:00'},
            {'load_kw': 4022.0, 'pv_kw': 1390.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:01:00'},
            {'load_kw': 4024.0, 'pv_kw': 1380.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:02:00'},
            {'load_kw': 4026.0, 'pv_kw': 1370.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:03:00'},
            {'load_kw': 4028.0, 'pv_kw': 1360.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:04:00'},
            {'load_kw': 4030.0, 'pv_kw': 1350.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:05:00'},
            {'load_kw': 4032.0, 'pv_kw': 1340.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:06:00'},
            {'load_kw': 4034.0, 'pv_kw': 1330.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:07:00'},
            {'load_kw': 4036.0, 'pv_kw': 1320.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:08:00'},
            {'load_kw': 4038.0, 'pv_kw': 1310.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:09:00'},
            {'load_kw': 4040.0, 'pv_kw': 1300.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:10:00'},
            {'load_kw': 4042.0, 'pv_kw': 1290.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:11:00'},
            {'load_kw': 4044.0, 'pv_kw': 1280.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:12:00'},
            {'load_kw': 4046.0, 'pv_kw': 1270.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:13:00'},
            {'load_kw': 4048.0, 'pv_kw': 1260.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:14:00'},
            {'load_kw': 4050.0, 'pv_kw': 1250.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:15:00'},
            {'load_kw': 4052.0, 'pv_kw': 1240.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:16:00'},
            {'load_kw': 4054.0, 'pv_kw': 1230.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:17:00'},
            {'load_kw': 4056.0, 'pv_kw': 1220.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:18:00'},
            {'load_kw': 4058.0, 'pv_kw': 1210.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:19:00'},
            {'load_kw': 4060.0, 'pv_kw': 1200.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:20:00'},
            {'load_kw': 4062.0, 'pv_kw': 1190.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:21:00'},
            {'load_kw': 4064.0, 'pv_kw': 1180.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:22:00'},
            {'load_kw': 4066.0, 'pv_kw': 1170.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:23:00'},
            {'load_kw': 4068.0, 'pv_kw': 1160.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:24:00'},
            {'load_kw': 4070.0, 'pv_kw': 1150.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:25:00'},
            {'load_kw': 4072.0, 'pv_kw': 1140.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:26:00'},
            {'load_kw': 4074.0, 'pv_kw': 1130.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:27:00'},
            {'load_kw': 4076.0, 'pv_kw': 1120.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:28:00'},
            {'load_kw': 4078.0, 'pv_kw': 1110.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:29:00'},
            {'load_kw': 4080.0, 'pv_kw': 1100.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:30:00'},
            {'load_kw': 4081.33, 'pv_kw': 1090.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:31:00'},
            {'load_kw': 4082.67, 'pv_kw': 1080.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:32:00'},
            {'load_kw': 4084.0, 'pv_kw': 1070.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:33:00'},
            {'load_kw': 4085.33, 'pv_kw': 1060.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:34:00'},
            {'load_kw': 4086.67, 'pv_kw': 1050.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:35:00'},
            {'load_kw': 4088.0, 'pv_kw': 1040.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:36:00'},
            {'load_kw': 4089.33, 'pv_kw': 1030.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:37:00'},
            {'load_kw': 4090.67, 'pv_kw': 1020.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:38:00'},
            {'load_kw': 4092.0, 'pv_kw': 1010.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:39:00'},
            {'load_kw': 4093.33, 'pv_kw': 1000.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:40:00'},
            {'load_kw': 4094.67, 'pv_kw': 990.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:41:00'},
            {'load_kw': 4096.0, 'pv_kw': 980.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:42:00'},
            {'load_kw': 4097.33, 'pv_kw': 970.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:43:00'},
            {'load_kw': 4098.67, 'pv_kw': 960.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:44:00'},
            {'load_kw': 4100.0, 'pv_kw': 950.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:45:00'},
            {'load_kw': 4343.33, 'pv_kw': 940.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:46:00'},
            {'load_kw': 4586.67, 'pv_kw': 930.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:47:00'},
            {'load_kw': 4830.0, 'pv_kw': 920.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:48:00'},
            {'load_kw': 5073.33, 'pv_kw': 910.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:49:00'},
            {'load_kw': 5316.67, 'pv_kw': 900.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:50:00'},
            {'load_kw': 5560.0, 'pv_kw': 890.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:51:00'},
            {'load_kw': 5803.33, 'pv_kw': 880.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:52:00'},
            {'load_kw': 6046.67, 'pv_kw': 870.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:53:00'},
            {'load_kw': 6290.0, 'pv_kw': 860.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:54:00'},
            {'load_kw': 6533.33, 'pv_kw': 850.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:55:00'},
            {'load_kw': 6776.67, 'pv_kw': 840.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:56:00'},
            {'load_kw': 7020.0, 'pv_kw': 830.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:57:00'},
            {'load_kw': 7263.33, 'pv_kw': 820.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:58:00'},
            {'load_kw': 7506.67, 'pv_kw': 810.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 16:59:00'},
            {'load_kw': 7750.0, 'pv_kw': 800.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:00:00'},
            {'load_kw': 7510.0, 'pv_kw': 790.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:01:00'},
            {'load_kw': 7270.0, 'pv_kw': 780.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:02:00'},
            {'load_kw': 7030.0, 'pv_kw': 770.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:03:00'},
            {'load_kw': 6790.0, 'pv_kw': 760.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:04:00'},
            {'load_kw': 6550.0, 'pv_kw': 750.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:05:00'},
            {'load_kw': 6310.0, 'pv_kw': 740.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:06:00'},
            {'load_kw': 6070.0, 'pv_kw': 730.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:07:00'},
            {'load_kw': 5830.0, 'pv_kw': 720.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:08:00'},
            {'load_kw': 5590.0, 'pv_kw': 710.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:09:00'},
            {'load_kw': 5350.0, 'pv_kw': 700.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:10:00'},
            {'load_kw': 5110.0, 'pv_kw': 690.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:11:00'},
            {'load_kw': 4870.0, 'pv_kw': 680.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:12:00'},
            {'load_kw': 4630.0, 'pv_kw': 670.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:13:00'},
            {'load_kw': 4390.0, 'pv_kw': 660.0, 'co_relation_percent': 22, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:14:00'},
            {'load_kw': 4150.0, 'pv_kw': 650.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:15:00'},
            {'load_kw': 4152.0, 'pv_kw': 640.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:16:00'},
            {'load_kw': 4154.0, 'pv_kw': 630.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:17:00'},
            {'load_kw': 4156.0, 'pv_kw': 620.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:18:00'},
            {'load_kw': 4158.0, 'pv_kw': 610.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:19:00'},
            {'load_kw': 4160.0, 'pv_kw': 600.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:20:00'},
            {'load_kw': 4162.0, 'pv_kw': 590.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:21:00'},
            {'load_kw': 4164.0, 'pv_kw': 580.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:22:00'},
            {'load_kw': 4166.0, 'pv_kw': 570.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:23:00'},
            {'load_kw': 4168.0, 'pv_kw': 560.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:24:00'},
            {'load_kw': 4170.0, 'pv_kw': 550.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:25:00'},
            {'load_kw': 4172.0, 'pv_kw': 540.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:26:00'},
            {'load_kw': 4174.0, 'pv_kw': 530.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:27:00'},
            {'load_kw': 4176.0, 'pv_kw': 520.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:28:00'},
            {'load_kw': 4178.0, 'pv_kw': 510.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:29:00'},
            {'load_kw': 4180.0, 'pv_kw': 500.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:30:00'},
            {'load_kw': 4181.33, 'pv_kw': 492.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:31:00'},
            {'load_kw': 4182.67, 'pv_kw': 484.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:32:00'},
            {'load_kw': 4184.0, 'pv_kw': 476.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:33:00'},
            {'load_kw': 4185.33, 'pv_kw': 468.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:34:00'},
            {'load_kw': 4186.67, 'pv_kw': 460.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:35:00'},
            {'load_kw': 4188.0, 'pv_kw': 452.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:36:00'},
            {'load_kw': 4189.33, 'pv_kw': 444.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:37:00'},
            {'load_kw': 4190.67, 'pv_kw': 436.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:38:00'},
            {'load_kw': 4192.0, 'pv_kw': 428.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:39:00'},
            {'load_kw': 4193.33, 'pv_kw': 420.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:40:00'},
            {'load_kw': 4194.67, 'pv_kw': 412.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:41:00'},
            {'load_kw': 4196.0, 'pv_kw': 404.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:42:00'},
            {'load_kw': 4197.33, 'pv_kw': 396.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:43:00'},
            {'load_kw': 4198.67, 'pv_kw': 388.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:44:00'},
            {'load_kw': 4200.0, 'pv_kw': 380.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:45:00'},
            {'load_kw': 4203.33, 'pv_kw': 372.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:46:00'},
            {'load_kw': 4206.67, 'pv_kw': 364.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:47:00'},
            {'load_kw': 4210.0, 'pv_kw': 356.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:48:00'},
            {'load_kw': 4213.33, 'pv_kw': 348.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:49:00'},
            {'load_kw': 4216.67, 'pv_kw': 340.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:50:00'},
            {'load_kw': 4220.0, 'pv_kw': 332.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:51:00'},
            {'load_kw': 4223.33, 'pv_kw': 324.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:52:00'},
            {'load_kw': 4226.67, 'pv_kw': 316.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:53:00'},
            {'load_kw': 4230.0, 'pv_kw': 308.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:54:00'},
            {'load_kw': 4233.33, 'pv_kw': 300.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:55:00'},
            {'load_kw': 4236.67, 'pv_kw': 292.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:56:00'},
            {'load_kw': 4240.0, 'pv_kw': 284.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:57:00'},
            {'load_kw': 4243.33, 'pv_kw': 276.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:58:00'},
            {'load_kw': 4246.67, 'pv_kw': 268.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 17:59:00'},
            {'load_kw': 4250.0, 'pv_kw': 260.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:00:00'},
            {'load_kw': 4253.33, 'pv_kw': 253.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:01:00'},
            {'load_kw': 4256.67, 'pv_kw': 246.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:02:00'},
            {'load_kw': 4260.0, 'pv_kw': 240.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:03:00'},
            {'load_kw': 4263.33, 'pv_kw': 233.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:04:00'},
            {'load_kw': 4266.67, 'pv_kw': 226.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:05:00'},
            {'load_kw': 4270.0, 'pv_kw': 220.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:06:00'},
            {'load_kw': 4273.33, 'pv_kw': 213.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:07:00'},
            {'load_kw': 4276.67, 'pv_kw': 206.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:08:00'},
            {'load_kw': 4280.0, 'pv_kw': 200.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:09:00'},
            {'load_kw': 4283.33, 'pv_kw': 193.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:10:00'},
            {'load_kw': 4286.67, 'pv_kw': 186.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:11:00'},
            {'load_kw': 4290.0, 'pv_kw': 180.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:12:00'},
            {'load_kw': 4293.33, 'pv_kw': 173.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:13:00'},
            {'load_kw': 4296.67, 'pv_kw': 166.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:14:00'},
            {'load_kw': 4300.0, 'pv_kw': 160.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:15:00'},
            {'load_kw': 4303.33, 'pv_kw': 154.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:16:00'},
            {'load_kw': 4306.67, 'pv_kw': 149.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:17:00'},
            {'load_kw': 4310.0, 'pv_kw': 144.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:18:00'},
            {'load_kw': 4313.33, 'pv_kw': 138.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:19:00'},
            {'load_kw': 4316.67, 'pv_kw': 133.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:20:00'},
            {'load_kw': 4320.0, 'pv_kw': 128.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:21:00'},
            {'load_kw': 4323.33, 'pv_kw': 122.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:22:00'},
            {'load_kw': 4326.67, 'pv_kw': 117.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:23:00'},
            {'load_kw': 4330.0, 'pv_kw': 112.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:24:00'},
            {'load_kw': 4333.33, 'pv_kw': 106.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:25:00'},
            {'load_kw': 4336.67, 'pv_kw': 101.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:26:00'},
            {'load_kw': 4340.0, 'pv_kw': 96.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:27:00'},
            {'load_kw': 4343.33, 'pv_kw': 90.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:28:00'},
            {'load_kw': 4346.67, 'pv_kw': 85.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:29:00'},
            {'load_kw': 4350.0, 'pv_kw': 80.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:30:00'},
            {'load_kw': 4353.33, 'pv_kw': 76.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:31:00'},
            {'load_kw': 4356.67, 'pv_kw': 73.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:32:00'},
            {'load_kw': 4360.0, 'pv_kw': 70.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:33:00'},
            {'load_kw': 4363.33, 'pv_kw': 66.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:34:00'},
            {'load_kw': 4366.67, 'pv_kw': 63.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:35:00'},
            {'load_kw': 4370.0, 'pv_kw': 60.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:36:00'},
            {'load_kw': 4373.33, 'pv_kw': 56.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:37:00'},
            {'load_kw': 4376.67, 'pv_kw': 53.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:38:00'},
            {'load_kw': 4380.0, 'pv_kw': 50.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:39:00'},
            {'load_kw': 4383.33, 'pv_kw': 46.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:40:00'},
            {'load_kw': 4386.67, 'pv_kw': 43.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:41:00'},
            {'load_kw': 4390.0, 'pv_kw': 40.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:42:00'},
            {'load_kw': 4393.33, 'pv_kw': 36.67, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:43:00'},
            {'load_kw': 4396.67, 'pv_kw': 33.33, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:44:00'},
            {'load_kw': 4400.0, 'pv_kw': 30.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:45:00'},
            {'load_kw': 4401.33, 'pv_kw': 28.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:46:00'},
            {'load_kw': 4402.67, 'pv_kw': 26.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:47:00'},
            {'load_kw': 4404.0, 'pv_kw': 24.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:48:00'},
            {'load_kw': 4405.33, 'pv_kw': 22.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:49:00'},
            {'load_kw': 4406.67, 'pv_kw': 20.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:50:00'},
            {'load_kw': 4408.0, 'pv_kw': 18.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:51:00'},
            {'load_kw': 4409.33, 'pv_kw': 16.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:52:00'},
            {'load_kw': 4410.67, 'pv_kw': 14.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:53:00'},
            {'load_kw': 4412.0, 'pv_kw': 12.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:54:00'},
            {'load_kw': 4413.33, 'pv_kw': 10.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:55:00'},
            {'load_kw': 4414.67, 'pv_kw': 8.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:56:00'},
            {'load_kw': 4416.0, 'pv_kw': 6.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:57:00'},
            {'load_kw': 4417.33, 'pv_kw': 4.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:58:00'},
            {'load_kw': 4418.67, 'pv_kw': 2.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 18:59:00'},
            {'load_kw': 4420.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:00:00'},
            {'load_kw': 4418.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:01:00'},
            {'load_kw': 4417.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:02:00'},
            {'load_kw': 4416.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:03:00'},
            {'load_kw': 4414.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:04:00'},
            {'load_kw': 4413.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:05:00'},
            {'load_kw': 4412.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:06:00'},
            {'load_kw': 4410.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:07:00'},
            {'load_kw': 4409.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:08:00'},
            {'load_kw': 4408.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:09:00'},
            {'load_kw': 4406.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:10:00'},
            {'load_kw': 4405.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:11:00'},
            {'load_kw': 4404.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:12:00'},
            {'load_kw': 4402.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:13:00'},
            {'load_kw': 4401.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:14:00'},
            {'load_kw': 4400.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:15:00'},
            {'load_kw': 4396.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:16:00'},
            {'load_kw': 4393.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:17:00'},
            {'load_kw': 4390.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:18:00'},
            {'load_kw': 4386.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:19:00'},
            {'load_kw': 4383.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:20:00'},
            {'load_kw': 4380.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:21:00'},
            {'load_kw': 4376.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:22:00'},
            {'load_kw': 4373.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:23:00'},
            {'load_kw': 4370.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:24:00'},
            {'load_kw': 4366.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:25:00'},
            {'load_kw': 4363.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:26:00'},
            {'load_kw': 4360.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:27:00'},
            {'load_kw': 4356.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:28:00'},
            {'load_kw': 4353.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:29:00'},
            {'load_kw': 4350.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:30:00'},
            {'load_kw': 4346.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:31:00'},
            {'load_kw': 4343.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:32:00'},
            {'load_kw': 4340.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:33:00'},
            {'load_kw': 4336.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:34:00'},
            {'load_kw': 4333.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:35:00'},
            {'load_kw': 4330.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:36:00'},
            {'load_kw': 4326.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:37:00'},
            {'load_kw': 4323.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:38:00'},
            {'load_kw': 4320.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:39:00'},
            {'load_kw': 4316.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:40:00'},
            {'load_kw': 4313.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:41:00'},
            {'load_kw': 4310.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:42:00'},
            {'load_kw': 4306.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:43:00'},
            {'load_kw': 4303.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:44:00'},
            {'load_kw': 4300.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:45:00'},
            {'load_kw': 4296.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:46:00'},
            {'load_kw': 4293.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:47:00'},
            {'load_kw': 4290.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:48:00'},
            {'load_kw': 4286.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:49:00'},
            {'load_kw': 4283.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:50:00'},
            {'load_kw': 4280.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:51:00'},
            {'load_kw': 4276.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:52:00'},
            {'load_kw': 4273.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:53:00'},
            {'load_kw': 4270.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:54:00'},
            {'load_kw': 4266.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:55:00'},
            {'load_kw': 4263.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:56:00'},
            {'load_kw': 4260.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:57:00'},
            {'load_kw': 4256.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:58:00'},
            {'load_kw': 4253.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 8.0, 'time_stamp': '2026-10-13 19:59:00'},
            {'load_kw': 4250.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:00:00'},
            {'load_kw': 4246.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:01:00'},
            {'load_kw': 4243.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:02:00'},
            {'load_kw': 4240.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:03:00'},
            {'load_kw': 4236.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:04:00'},
            {'load_kw': 4233.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:05:00'},
            {'load_kw': 4230.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:06:00'},
            {'load_kw': 4226.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:07:00'},
            {'load_kw': 4223.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:08:00'},
            {'load_kw': 4220.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:09:00'},
            {'load_kw': 4216.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:10:00'},
            {'load_kw': 4213.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:11:00'},
            {'load_kw': 4210.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:12:00'},
            {'load_kw': 4206.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:13:00'},
            {'load_kw': 4203.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:14:00'},
            {'load_kw': 4200.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:15:00'},
            {'load_kw': 4193.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:16:00'},
            {'load_kw': 4186.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:17:00'},
            {'load_kw': 4180.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:18:00'},
            {'load_kw': 4173.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:19:00'},
            {'load_kw': 4166.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:20:00'},
            {'load_kw': 4160.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:21:00'},
            {'load_kw': 4153.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:22:00'},
            {'load_kw': 4146.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:23:00'},
            {'load_kw': 4140.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:24:00'},
            {'load_kw': 4133.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:25:00'},
            {'load_kw': 4126.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:26:00'},
            {'load_kw': 4120.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:27:00'},
            {'load_kw': 4113.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:28:00'},
            {'load_kw': 4106.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:29:00'},
            {'load_kw': 4100.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:30:00'},
            {'load_kw': 4093.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:31:00'},
            {'load_kw': 4086.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:32:00'},
            {'load_kw': 4080.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:33:00'},
            {'load_kw': 4073.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:34:00'},
            {'load_kw': 4066.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:35:00'},
            {'load_kw': 4060.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:36:00'},
            {'load_kw': 4053.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:37:00'},
            {'load_kw': 4046.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:38:00'},
            {'load_kw': 4040.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:39:00'},
            {'load_kw': 4033.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:40:00'},
            {'load_kw': 4026.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:41:00'},
            {'load_kw': 4020.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:42:00'},
            {'load_kw': 4013.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:43:00'},
            {'load_kw': 4006.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:44:00'},
            {'load_kw': 4000.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:45:00'},
            {'load_kw': 3993.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:46:00'},
            {'load_kw': 3986.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:47:00'},
            {'load_kw': 3980.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:48:00'},
            {'load_kw': 3973.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:49:00'},
            {'load_kw': 3966.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:50:00'},
            {'load_kw': 3960.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:51:00'},
            {'load_kw': 3953.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:52:00'},
            {'load_kw': 3946.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:53:00'},
            {'load_kw': 3940.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:54:00'},
            {'load_kw': 3933.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:55:00'},
            {'load_kw': 3926.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:56:00'},
            {'load_kw': 3920.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:57:00'},
            {'load_kw': 3913.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:58:00'},
            {'load_kw': 3906.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 20:59:00'},
            {'load_kw': 3900.0, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:00:00'},
            {'load_kw': 3893.33, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:01:00'},
            {'load_kw': 3886.67, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:02:00'},
            {'load_kw': 3880.0, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:03:00'},
            {'load_kw': 3873.33, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:04:00'},
            {'load_kw': 3866.67, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:05:00'},
            {'load_kw': 3860.0, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:06:00'},
            {'load_kw': 3853.33, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:07:00'},
            {'load_kw': 3846.67, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:08:00'},
            {'load_kw': 3840.0, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:09:00'},
            {'load_kw': 3833.33, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:10:00'},
            {'load_kw': 3826.67, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:11:00'},
            {'load_kw': 3820.0, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:12:00'},
            {'load_kw': 3813.33, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:13:00'},
            {'load_kw': 3806.67, 'pv_kw': 0.0, 'co_relation_percent': 18, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:14:00'},
            {'load_kw': 3800.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:15:00'},
            {'load_kw': 3793.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:16:00'},
            {'load_kw': 3786.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:17:00'},
            {'load_kw': 3780.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:18:00'},
            {'load_kw': 3773.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:19:00'},
            {'load_kw': 3766.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:20:00'},
            {'load_kw': 3760.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:21:00'},
            {'load_kw': 3753.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:22:00'},
            {'load_kw': 3746.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:23:00'},
            {'load_kw': 3740.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:24:00'},
            {'load_kw': 3733.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:25:00'},
            {'load_kw': 3726.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:26:00'},
            {'load_kw': 3720.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:27:00'},
            {'load_kw': 3713.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:28:00'},
            {'load_kw': 3706.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:29:00'},
            {'load_kw': 3700.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:30:00'},
            {'load_kw': 3693.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:31:00'},
            {'load_kw': 3686.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:32:00'},
            {'load_kw': 3680.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:33:00'},
            {'load_kw': 3673.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:34:00'},
            {'load_kw': 3666.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:35:00'},
            {'load_kw': 3660.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:36:00'},
            {'load_kw': 3653.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:37:00'},
            {'load_kw': 3646.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:38:00'},
            {'load_kw': 3640.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:39:00'},
            {'load_kw': 3633.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:40:00'},
            {'load_kw': 3626.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:41:00'},
            {'load_kw': 3620.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:42:00'},
            {'load_kw': 3613.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:43:00'},
            {'load_kw': 3606.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:44:00'},
            {'load_kw': 3600.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:45:00'},
            {'load_kw': 3593.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:46:00'},
            {'load_kw': 3586.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:47:00'},
            {'load_kw': 3580.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:48:00'},
            {'load_kw': 3573.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:49:00'},
            {'load_kw': 3566.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:50:00'},
            {'load_kw': 3560.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:51:00'},
            {'load_kw': 3553.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:52:00'},
            {'load_kw': 3546.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:53:00'},
            {'load_kw': 3540.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:54:00'},
            {'load_kw': 3533.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:55:00'},
            {'load_kw': 3526.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:56:00'},
            {'load_kw': 3520.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:57:00'},
            {'load_kw': 3513.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:58:00'},
            {'load_kw': 3506.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 21:59:00'},
            {'load_kw': 3500.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:00:00'},
            {'load_kw': 3493.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:01:00'},
            {'load_kw': 3486.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:02:00'},
            {'load_kw': 3480.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:03:00'},
            {'load_kw': 3473.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:04:00'},
            {'load_kw': 3466.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:05:00'},
            {'load_kw': 3460.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:06:00'},
            {'load_kw': 3453.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:07:00'},
            {'load_kw': 3446.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:08:00'},
            {'load_kw': 3440.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:09:00'},
            {'load_kw': 3433.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:10:00'},
            {'load_kw': 3426.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:11:00'},
            {'load_kw': 3420.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:12:00'},
            {'load_kw': 3413.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:13:00'},
            {'load_kw': 3406.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:14:00'},
            {'load_kw': 3400.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:15:00'},
            {'load_kw': 3393.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:16:00'},
            {'load_kw': 3386.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:17:00'},
            {'load_kw': 3380.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:18:00'},
            {'load_kw': 3373.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:19:00'},
            {'load_kw': 3366.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:20:00'},
            {'load_kw': 3360.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:21:00'},
            {'load_kw': 3353.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:22:00'},
            {'load_kw': 3346.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:23:00'},
            {'load_kw': 3340.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:24:00'},
            {'load_kw': 3333.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:25:00'},
            {'load_kw': 3326.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:26:00'},
            {'load_kw': 3320.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:27:00'},
            {'load_kw': 3313.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:28:00'},
            {'load_kw': 3306.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:29:00'},
            {'load_kw': 3300.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:30:00'},
            {'load_kw': 3293.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:31:00'},
            {'load_kw': 3286.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:32:00'},
            {'load_kw': 3280.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:33:00'},
            {'load_kw': 3273.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:34:00'},
            {'load_kw': 3266.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:35:00'},
            {'load_kw': 3260.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:36:00'},
            {'load_kw': 3253.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:37:00'},
            {'load_kw': 3246.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:38:00'},
            {'load_kw': 3240.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:39:00'},
            {'load_kw': 3233.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:40:00'},
            {'load_kw': 3226.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:41:00'},
            {'load_kw': 3220.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:42:00'},
            {'load_kw': 3213.33, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:43:00'},
            {'load_kw': 3206.67, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:44:00'},
            {'load_kw': 3200.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:45:00'},
            {'load_kw': 3190.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:46:00'},
            {'load_kw': 3180.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:47:00'},
            {'load_kw': 3170.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:48:00'},
            {'load_kw': 3160.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:49:00'},
            {'load_kw': 3150.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:50:00'},
            {'load_kw': 3140.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:51:00'},
            {'load_kw': 3130.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:52:00'},
            {'load_kw': 3120.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:53:00'},
            {'load_kw': 3110.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:54:00'},
            {'load_kw': 3100.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:55:00'},
            {'load_kw': 3090.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:56:00'},
            {'load_kw': 3080.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:57:00'},
            {'load_kw': 3070.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:58:00'},
            {'load_kw': 3060.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'mid_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 4.0, 'time_stamp': '2026-10-13 22:59:00'},
            {'load_kw': 3050.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:00:00'},
            {'load_kw': 3040.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:01:00'},
            {'load_kw': 3030.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:02:00'},
            {'load_kw': 3020.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:03:00'},
            {'load_kw': 3010.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:04:00'},
            {'load_kw': 3000.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:05:00'},
            {'load_kw': 2990.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:06:00'},
            {'load_kw': 2980.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:07:00'},
            {'load_kw': 2970.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:08:00'},
            {'load_kw': 2960.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:09:00'},
            {'load_kw': 2950.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:10:00'},
            {'load_kw': 2940.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:11:00'},
            {'load_kw': 2930.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:12:00'},
            {'load_kw': 2920.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:13:00'},
            {'load_kw': 2910.0, 'pv_kw': 0.0, 'co_relation_percent': 16, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:14:00'},
            {'load_kw': 2900.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:15:00'},
            {'load_kw': 2890.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:16:00'},
            {'load_kw': 2880.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:17:00'},
            {'load_kw': 2870.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:18:00'},
            {'load_kw': 2860.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:19:00'},
            {'load_kw': 2850.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:20:00'},
            {'load_kw': 2840.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:21:00'},
            {'load_kw': 2830.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:22:00'},
            {'load_kw': 2820.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:23:00'},
            {'load_kw': 2810.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:24:00'},
            {'load_kw': 2800.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:25:00'},
            {'load_kw': 2790.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:26:00'},
            {'load_kw': 2780.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:27:00'},
            {'load_kw': 2770.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:28:00'},
            {'load_kw': 2760.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:29:00'},
            {'load_kw': 2750.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:30:00'},
            {'load_kw': 2740.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:31:00'},
            {'load_kw': 2730.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:32:00'},
            {'load_kw': 2720.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:33:00'},
            {'load_kw': 2710.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:34:00'},
            {'load_kw': 2700.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:35:00'},
            {'load_kw': 2690.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:36:00'},
            {'load_kw': 2680.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:37:00'},
            {'load_kw': 2670.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:38:00'},
            {'load_kw': 2660.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:39:00'},
            {'load_kw': 2650.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:40:00'},
            {'load_kw': 2640.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:41:00'},
            {'load_kw': 2630.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:42:00'},
            {'load_kw': 2620.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:43:00'},
            {'load_kw': 2610.0, 'pv_kw': 0.0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:44:00'},
            {'load_kw': 2600.0, 'pv_kw': 0, 'co_relation_percent': 0, 'load_state': 'off_peak', 'grid_import_limit_kw': 4000, 'tariff_per_kw': 2.0, 'time_stamp': '2026-10-13 23:45:00'},
        ]
        
        self.load_kw = 0.0
        self.pv_kw = 0.0
        self.load_state = "NONE"
        self.tariff_per_kw = 0.0
        self.time_stamp = "2026-10-13 00:00:00"
        self.fault = 0

        self.result_file = Path("D:/New folder/ems_backend_data/schedule_strategy/result.json")

        self.bess_scheduler = BESScheduler()
        self.sample_window = self.bess_scheduler.aggregate_to_15min(sample_data if sample_data else self.sample_window_1min)

        self.schedule = None
    
    async def prepare_plan_schedule(self):

        # -------------------------------------------------
        # 1. GENERATE SCHEDULE
        # -------------------------------------------------

        schedule = self.bess_scheduler.schedule(self.sample_window)

        logger.info("Schedule Strategy Execution...........")

        # -------------------------------------------------
        # 2. FIRST WRITE
        #    Always clean previous file and create fresh data
        # -------------------------------------------------

        initial_data = {
            "schedule": schedule,
            "results": []
        }

        with self.result_file.open("w", encoding="utf-8") as f:
            json.dump(initial_data, f, indent=4, default=str)

        self.schedule = schedule

        logger.info(
            f"[EVENT GENERATOR] Fresh schedule written to {self.result_file}"
        )

    async def prepare_ems_plan_schedule(self):

        # -------------------------------------------------
        # 1. GENERATE SCHEDULE
        # -------------------------------------------------

        schedule = self.bess_scheduler.schedule(self.sample_window)

        logger.info("Schedule Strategy Execution...........")

        # -------------------------------------------------
        # 2. FIRST WRITE
        #    Always clean previous file and create fresh data
        # -------------------------------------------------

        with self.result_file.open("r", encoding="utf-8") as f:
            data = json.load(f)

        data["schedule"] = schedule

        with self.result_file.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, default=str)


        logger.info(
            f"[EVENT GENERATOR] Fresh schedule written to {self.result_file}"
        )

    async def execute(
        self,
        channel,
        server_ip,
        server_port,
        slave_id,
        device_id,
        signal,
        event_number=1,
    ):
        """
        Full Schedule Strategy event simulator.
        """

        try:
            # time.sleep(5)
            interval_seconds = 5

            # JSON output path
            with self.result_file.open("r", encoding="utf-8") as f:
                data = json.load(f)

            schedule = data["schedule"]                

            results = data["results"]

            # Start from the requested event (1-based input).
            start_index = event_number - 1

            for index in range(start_index, len(schedule)):
                event = schedule[index]

                # await asyncio.sleep(5)

                self.time_stamp = event["time_stamp"]
                # self.load_kw = event["load_kw"]
                self.pv_kw = event["pv_kw"]
                self.load_state = event["load_state"]
                self.tariff_per_kw = event["tariff_per_kw"]
                self.fault = 0

                logger.info(
                    f"\n"
                    f"{'=' * 80}\n"
                    f"[SIMULATION EVENT {index + 1}/{len(schedule)}]\n"
                    f"Timestamp : {event['time_stamp']}\n"
                    f"Load      : {event['load_kw']} kW\n"
                    f"PV        : {event['pv_kw']} kW\n"
                    f"State     : {event['load_state']}\n"
                    f"Tariff    : {event['tariff_per_kw']}\n"
                    f"{'=' * 80}"
                )

                # -------------------------------------------------
                # 1. WRITE LOAD
                # -------------------------------------------------

                window_load = event.get("window_load", [])
                pv_window = event.get("pv_window", [])
                poa_window = event.get("poa_window", [])

                # -------------------------------------------------
                # 1. START 1-MINUTE LOAD PROFILE
                # -------------------------------------------------

                load_task = asyncio.create_task(
                    self._execute_window_load(
                        window_load=window_load,
                        channel=channel,
                        server_ip=server_ip,
                        server_port=server_port,
                        slave_id=slave_id,
                        device_id=device_id,
                    )
                )

                pv_task = asyncio.create_task(
                    self._execute_window_pv(
                        window_pv=pv_window,
                        window_poa=poa_window,
                        channel=channel,
                        server_ip=server_ip,
                        server_port=server_port,
                        slave_id=slave_id,
                        device_id=device_id,
                    )
                )

                # -------------------------------------------------
                # 2. EXECUTE YOUR STRATEGY
                # -------------------------------------------------
                # time.sleep(1)
                # 2. Wait for 5 seconds while background tasks run
                logger.info("Waiting 5 seconds before marking event as running...")
                await asyncio.sleep(5)
                logger.info("5-second wait completed.")

                # -------------------------------------------------
                # 0. MARK EVENT AS RUNNING
                # -------------------------------------------------

                with self.result_file.open("r", encoding="utf-8") as f:
                    data = json.load(f)

                data["running_event_id"] = event["event_id"]
                # data["running_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                with self.result_file.open("w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4, default=str)

                logger.info(
                    f"[EVENT GENERATOR] Event {event['event_id']} marked as running "
                    # f"at {data['running_timestamp']}."
                )

                result = await self.event_execute(
                    channel=channel,
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    device_id=device_id,
                    event_decision=event,
                    signal=None,
                )

                # -------------------------------------------------
                # 3. STORE EVENT RESULT
                # -------------------------------------------------
                
                if self.fault != 0:
                    logger.info(f"{LogColor.RED} Fault Code Detected: {self.fault}.{LogColor.RESET}")
                    event_result = {
                        "event_number": index + 1,
                        "bess_event_decision": event,
                        "strategy_result": result,
                        "is_pass": False,
                        "fault": FaultCode(self.fault).name,
                        "ems_recommendations": self.get_dynamic_recommendations(
                            fault_name=FaultCode(self.fault).name,
                            event=event
                        )
                    }

                    logger.info(f"{LogColor.RED} Fault Code Detected: {event_result}.{LogColor.RESET}")

                    notification = NotificationEvent(
                        source="EMS",
                        event_type=f"{event['decision'].upper()} Event cannot be Scheduled.",
                        severity="WARNING",
                        title=f"{event['decision'].upper()} Event cannot be Scheduled.",
                        message=f"Due to {FaultCode(self.fault).name} event of {event['description']} cannot be scheduled.",
                        entity_type="BESS_Schedule",
                        entity_id=event['event_id'],
                        status="on-hold",
                        metadata={
                            
                        },
                    )

                    pool = await get_async_connection()

                    try:
                        async with pool.acquire() as conn:
                            await write_notification_event(conn, notification)
                    finally:
                        await pool.close()
                else:
                    event_result = {
                        "event_number": index + 1,
                        "bess_event_decision": event,
                        "strategy_result": result,
                        "is_pass": True
                    }

                    logger.info("Creating Energy Strategy Record for the event...")

                    pool = await get_async_connection()
                    try:
                        strategy_rec = self.build_energy_strategy(event)

                        async with pool.acquire() as conn:
                            await write_energy_strategy_record(conn, strategy_rec)
                    except Exception as e:
                        logger.error(f"Error writing Energy Strategy Record: {e}")
                    finally:
                        await pool.close()

                    logger.info("Energy Strategy Record created successfully.")
                    logger.info("Creating Notification Event for the event...")

                    notification = NotificationEvent(
                        source="EMS",
                        event_type=f"{event['decision'].upper()} Event Scheduled.",
                        severity="INFO",
                        title=f"{event['decision'].upper()} Event Scheduled.",
                        message=f"{event['description']}.",
                        entity_type="BESS_Schedule",
                        entity_id=event['event_id'],
                        status="upcomming",
                        metadata={
                            
                        },
                    )

                    pool = await get_async_connection()

                    try:
                        async with pool.acquire() as conn:
                            await write_notification_event(conn, notification)
                    except Exception as e:
                        logger.error(f"Error writing Notification Event: {e}")
                    finally:
                        await pool.close()

                    logger.info("Notification Event created successfully.")


                results.append(event_result)

                # -------------------------------------------------
                # 4. SECOND WRITE / UPDATE
                #    Update the SAME JSON file
                # -------------------------------------------------

                with self.result_file.open("r", encoding="utf-8") as f:
                    data = json.load(f)

                data["results"] = results

                with self.result_file.open("w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4, default=str)

                logger.info(
                    f"[EVENT GENERATOR] Event {index + 1} result saved to {self.result_file}"
                )

                # -------------------------------------------------
                # 5. WAIT
                # -------------------------------------------------

                if index < len(schedule) - 1:
                    await asyncio.sleep(interval_seconds)

            logger.info(
                f"[EVENT GENERATOR] "
                f"Completed {len(schedule)} events. "
                f"Results saved to {self.result_file}"
            )

            return results
        
        except asyncio.CancelledError:
            logger.info("Schedule strategy stopping.")

            # Perform any necessary cleanup here.
            # For example, release resources or close connections.

            raise

    async def event_execute(
        self,
        channel,
        server_ip,
        server_port,
        slave_id,
        device_id,
        event_decision,
        signal,
    ):
        """
        Execute an already-calculated Schedule Strategy event.

        The event/decision engine is responsible for deciding WHAT should happen.
        This function is responsible for safely executing that decision on the BESS.

        Supported decisions:
            - grid_charge
            - charge
            - pv_charge
            - grid_discharge
            - discharge
            - pv_curtailment
            - idle
            - normal
            - no_action
        """

        logger.info(
            f"{LogColor.MAGENTA}"
            f"Starting [ScheduleStrategy] event execution..."
            f"{LogColor.RESET}"
        )

        strategy_id = 7
        strategy_name = "Schedule Strategy"

        poi_meter_id = "plant_1_poi_meter_1"

        # -------------------------------------------------------------------------
        # 1. NORMALIZE EVENT
        # -------------------------------------------------------------------------
        #
        # event_decision may be:
        #
        # {
        #     "decision": "grid_charge",
        #     "charge_kw": 0.0,
        #     "grid_charge_kw": 1800.0,
        #     "discharge_kw": 0.0,
        #     ...
        # }
        #
        # or, if your caller passes only the decision string:
        #
        # "grid_charge"
        #
        # -------------------------------------------------------------------------

        if isinstance(event_decision, dict):
            event = event_decision
        else:
            event = {
                "decision": event_decision
            }

        decision = str(
            event.get("decision", "idle")
        ).strip().lower()

        logger.info(
            f"{LogColor.CYAN}"
            f"Schedule event received | "
            f"Decision={decision} | "
            f"Event={event}"
            f"{LogColor.RESET}"
        )

        # -------------------------------------------------------------------------
        # 2. LOAD LIVE BESS CONFIGURATION
        # -------------------------------------------------------------------------

        try:

            config = await ScheduleStrategy.bess_decision(
                device_id,
                poi_meter_id
            )

            (
                prevent_charging,
                block_discharging,
                hard_limit_kw,
                soft_limit_kw,
                threshold_kw,
                max_discharge_kw,
                max_usable_kw,
                mode,
                forecast_source,
                current_load_kw,
                soc,
                max_soc,
                enforce_soft,
                force_hard,
                next_peak_window,
                pv_power_kw,
                bess_power_kw,
                bess_gfm_transition_threshold,
                bess_gfl_transition_threshold,
                allow_charging_during_off_peak,
                current_window,
            ) = (
                config["prevent_charging"],
                config["block_discharging"],
                config["hard_limit_kw"],
                config["soft_limit_kw"],
                config["threshold_kw"],
                config["max_discharge_kw"],
                config["max_usable_kw"],
                config["mode"],
                config["forecast_source"],
                config["current_load_kw"],
                config["soc"],
                config["max_soc"],
                config["enforce_soft"],
                config["force_hard"],
                config["next_peak_window"],
                config["pv_power_kw"],
                config["bess_power_kw"],
                config["bess_gfm_transition_threshold"],
                config["bess_gfl_transition_threshold"],
                config["allow_charging_during_off_peak"],
                config["current_window"],
            )

        except Exception as e:

            logger.error(
                f"{LogColor.RED}"
                f"Error loading live BESS parameters: {e}"
                f"{LogColor.RESET}"
            )

            return {
                "status": "error",
                "decision": decision,
                "error": str(e),
            }

        # -------------------------------------------------------------------------
        # 3. USE LIVE VALUES WHERE APPROPRIATE
        # -------------------------------------------------------------------------

        pv_power_kw = self.pv_kw
        # current_load_kw = self.load_kw
        current_tariff_per_kw = self.tariff_per_kw

        # Current grid import based on the actual BESS power.
        #
        # Positive bess_power_kw = BESS discharging
        # Negative bess_power_kw = BESS charging
        #
        # Keep this calculation for logging/diagnostics.
        grid_power = (
            current_load_kw
            - pv_power_kw
            - bess_power_kw
        )

        pv_surplus_power_kw = (
            pv_power_kw
            - current_load_kw
        )

        # -------------------------------------------------------------------------
        # 4. EVENT VALUES
        # -------------------------------------------------------------------------

        requested_charge_kw = max(
            0.0,
            float(event.get("charge_kw", 0.0) or 0.0)
        )

        requested_grid_charge_kw = max(
            0.0,
            float(event.get("grid_charge_kw", 0.0) or 0.0)
        )

        requested_discharge_kw = max(
            0.0,
            float(event.get("discharge_kw", 0.0) or 0.0)
        )

        grid_import_limit_kw = float(
            event.get(
                "grid_import_limit_kw",
                hard_limit_kw
            ) or hard_limit_kw
        )

        event_soc = event.get("soc_percent")
        event_battery_energy_mwh = event.get("battery_energy_mwh")

        # -------------------------------------------------------------------------
        # 5. LOG CURRENT STATE
        # -------------------------------------------------------------------------

        logger.info(
            f"{LogColor.CYAN}"
            f"Schedule Event State | "
            f"Decision={decision} | "
            f"Load={current_load_kw:.2f} kW | "
            f"PV={pv_power_kw:.2f} kW | "
            f"Grid={grid_power:.2f} kW | "
            f"SOC={soc:.2f}% | "
            f"Tariff={current_tariff_per_kw:.2f} | "
            f"Event SOC={event_soc} | "
            f"Event Battery={event_battery_energy_mwh} MWh"
            f"{LogColor.RESET}"
        )

        result = None

        # =========================================================================
        # 6. GRID CHARGE
        # =========================================================================
        #
        # Example:
        #
        # load = 2200 kW
        # grid limit = 4000 kW
        # grid charge = 1800 kW
        #
        # Result:
        #
        # 2200 + 1800 = 4000 kW grid import
        #
        # =========================================================================

        if decision in (
            "grid_charge",
            "utility_charge",
            "off_peak_charge",
        ):

            logger.info(
                f"{LogColor.MAGENTA}"
                f"Executing GRID CHARGE | "
                f"Requested={requested_grid_charge_kw:.2f} kW"
                f"{LogColor.RESET}"
            )

            # ---------------------------------------------------------------------
            # Charging protection
            # ---------------------------------------------------------------------

            if prevent_charging:

                logger.warning(
                    f"{LogColor.YELLOW}"
                    f"Grid charging requested by event, "
                    f"but charging is currently blocked."
                    f"{LogColor.RESET}"
                )

                return {
                    "status": "blocked",
                    "decision": decision,
                    "requested_grid_charge_kw": requested_grid_charge_kw,
                    "executed_grid_charge_kw": 0.0,
                    "reason": "prevent_charging",
                }

            if soc >= max_soc:

                logger.warning(
                    f"{LogColor.YELLOW}"
                    f"Grid charging requested but BESS is at/above max SOC. "
                    f"SOC={soc:.2f}%, MaxSOC={max_soc:.2f}%"
                    f"{LogColor.RESET}"
                )

                return {
                    "status": "blocked",
                    "decision": decision,
                    "requested_grid_charge_kw": requested_grid_charge_kw,
                    "executed_grid_charge_kw": 0.0,
                    "reason": "battery_full",
                }

            # ---------------------------------------------------------------------
            # Respect grid import limit
            # ---------------------------------------------------------------------

            available_grid_capacity_kw = max(
                0.0,
                grid_import_limit_kw
                - current_load_kw
                + pv_power_kw
            )

            executable_charge_kw = min(
                requested_grid_charge_kw,
                available_grid_capacity_kw
            )

            if executable_charge_kw <= 0:

                logger.warning(
                    f"{LogColor.YELLOW}"
                    f"No grid capacity available for charging. "
                    f"Load={current_load_kw:.2f}, "
                    f"PV={pv_power_kw:.2f}, "
                    f"GridLimit={grid_import_limit_kw:.2f}"
                    f"{LogColor.RESET}"
                )

                return {
                    "status": "blocked",
                    "decision": decision,
                    "requested_grid_charge_kw": requested_grid_charge_kw,
                    "executed_grid_charge_kw": 0.0,
                    "reason": "grid_import_limit",
                }

            logger.info(
                f"{LogColor.GREEN}"
                f"GRID CHARGE EXECUTION | "
                f"Requested={requested_grid_charge_kw:.2f} kW | "
                f"AvailableGridCapacity={available_grid_capacity_kw:.2f} kW | "
                f"Executing={executable_charge_kw:.2f} kW"
                f"{LogColor.RESET}"
            )

            result = await self.PeakShavingChargingStrategyType7(
                resolver=self.resolver,
                controller_static=self.controller_static,
                device_id=device_id,
                channel=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                available_kw=executable_charge_kw,
                max_soc=max_soc,
                event=event,
                extra_info={
                    "strategy_id": 7,
                },
            )

            return result

        # =========================================================================
        # 7. PV CHARGE
        # =========================================================================

        elif decision in (
            "charge",
            "pv_charge",
        ):

            logger.info(
                f"{LogColor.MAGENTA}"
                f"Executing PV CHARGE | "
                f"Requested={requested_charge_kw:.2f} kW"
                f"{LogColor.RESET}"
            )

            if prevent_charging:

                logger.warning(
                    f"{LogColor.YELLOW}"
                    f"PV charging requested but charging is blocked."
                    f"{LogColor.RESET}"
                )

                return {
                    "status": "blocked",
                    "decision": decision,
                    "executed_charge_kw": 0.0,
                    "reason": "prevent_charging",
                }

            if soc >= max_soc:

                logger.warning(
                    f"{LogColor.YELLOW}"
                    f"PV charging requested but battery is full."
                    f"{LogColor.RESET}"
                )

                return {
                    "status": "blocked",
                    "decision": decision,
                    "executed_charge_kw": 0.0,
                    "reason": "battery_full",
                }

            # Prefer the event's charge_kw.
            # If it is zero, calculate from PV surplus.

            if requested_charge_kw > 0:

                executable_charge_kw = min(
                    requested_charge_kw,
                    max(0.0, pv_surplus_power_kw)
                )

            else:

                executable_charge_kw = max(
                    0.0,
                    pv_surplus_power_kw
                )

            if executable_charge_kw <= 0:

                logger.info(
                    "PV charge requested but no PV surplus is available."
                )

                return {
                    "status": "no_action",
                    "decision": decision,
                    "executed_charge_kw": 0.0,
                    "reason": "no_pv_surplus",
                }

            logger.info(
                f"{LogColor.GREEN}"
                f"PV CHARGE EXECUTION | "
                f"PVSurplus={pv_surplus_power_kw:.2f} kW | "
                f"Executing={executable_charge_kw:.2f} kW"
                f"{LogColor.RESET}"
            )

            result = await self.PeakShavingChargingStrategyType7(
                resolver=self.resolver,
                controller_static=self.controller_static,
                device_id=device_id,
                channel=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                available_kw=executable_charge_kw,
                max_soc=max_soc,
                event=event,
                extra_info={
                    "strategy_id": 7,
                },
            )

            return result

        # =========================================================================
        # 8. DISCHARGE / PEAK SHAVING
        # =========================================================================

        elif decision in (
            "grid_discharge",
            "discharge",
            "peak_shaving",
        ):

            logger.info(
                f"{LogColor.MAGENTA}"
                f"Executing BESS DISCHARGE | "
                f"Requested={requested_discharge_kw:.2f} kW"
                f"{LogColor.RESET}"
            )

            # ---------------------------------------------------------------------
            # Discharge protection
            # ---------------------------------------------------------------------

            if block_discharging:

                logger.warning(
                    f"{LogColor.YELLOW}"
                    f"Discharge requested by event but BESS discharge is blocked."
                    f"{LogColor.RESET}"
                )

                return {
                    "status": "blocked",
                    "decision": decision,
                    "requested_discharge_kw": requested_discharge_kw,
                    "executed_discharge_kw": 0.0,
                    "reason": "block_discharging",
                }

            # ---------------------------------------------------------------------
            # Calculate safe executable discharge
            # ---------------------------------------------------------------------

            executable_discharge_kw = min(
                requested_discharge_kw,
                max_usable_kw,
                max_discharge_kw
            )

            if executable_discharge_kw <= 0:

                logger.warning(
                    f"{LogColor.YELLOW}"
                    f"No BESS discharge capability available. "
                    f"Requested={requested_discharge_kw:.2f} | "
                    f"MaxUsable={max_usable_kw:.2f} | "
                    f"MaxDischarge={max_discharge_kw:.2f}"
                    f"{LogColor.RESET}"
                )

                return {
                    "status": "blocked",
                    "decision": decision,
                    "requested_discharge_kw": requested_discharge_kw,
                    "executed_discharge_kw": 0.0,
                    "reason": "no_discharge_capacity",
                }

            # ---------------------------------------------------------------------
            # Determine target grid limit.
            #
            # Prefer event target if it exists.
            # Otherwise use the configured hard limit.
            # ---------------------------------------------------------------------

            target_limit = event.get("target_limit_kw")

            if target_limit is None:
                target_limit = event.get("grid_target_kw")

            if target_limit is None:
                target_limit = event.get("grid_import_limit_kw")

            if target_limit is None:
                target_limit = hard_limit_kw

            target_limit = float(target_limit)

            logger.info(
                f"{LogColor.GREEN}"
                f"BESS DISCHARGE EXECUTION | "
                f"Requested={requested_discharge_kw:.2f} kW | "
                f"Executing={executable_discharge_kw:.2f} kW | "
                f"TargetGrid={target_limit:.2f} kW"
                f"{LogColor.RESET}"
            )

            result = await self.PeakShavingDischargeStrategyType7(
                resolver=self.resolver,
                controller_static=self.controller_static,
                server_ip=server_ip,
                server_port=server_port,
                channel=channel,
                slave_id=slave_id,
                device_id=device_id,
                excess_load=executable_discharge_kw,
                target_limit=target_limit,
                poi_meter_id=poi_meter_id,
                event=event,
                extra_info={
                    "strategy_id": 7,
                },
                # extra_info={
                #     "force_hard": force_hard,
                #     "event_decision": decision,
                #     "event": event,
                # }
            )

            return result

        # =========================================================================
        # 9. PV CURTAILMENT
        # =========================================================================

        elif decision in (
            "pv_curtailment",
            "curtail_pv",
            "curtailment",
        ):

            logger.info(
                f"{LogColor.MAGENTA}"
                f"Executing PV CURTAILMENT"
                f"{LogColor.RESET}"
            )

            if pv_surplus_power_kw <= 0:

                logger.info(
                    "PV curtailment requested but no PV surplus exists."
                )

                return {
                    "status": "no_action",
                    "decision": decision,
                    "reason": "no_pv_surplus",
                }

            result = await ScheduleStrategy.pv_power_curtailment(
                pv_power_kw,
                pv_surplus_power_kw,
                grid_power,
                current_load_kw
            )

            return result

        # =========================================================================
        # 10. IDLE / NORMAL / NO ACTION
        # =========================================================================

        elif decision in (
            "idle",
            "normal",
            "no_action",
            "none",
            "hold",
        ):

            logger.info(
                f"{LogColor.CYAN}"
                f"Schedule event = {decision}. "
                f"No BESS action required."
                f"{LogColor.RESET}"
            )

            return {
                "status": "no_action",
                "decision": decision,
                "message": "No BESS action required.",
                "grid_power_kw": grid_power,
                "load_kw": current_load_kw,
                "pv_kw": pv_power_kw,
                "soc_percent": soc,
            }

        # =========================================================================
        # 11. UNKNOWN DECISION
        # =========================================================================

        else:

            logger.error(
                f"{LogColor.RED}"
                f"Unknown Schedule Strategy decision: {decision} | "
                f"Event={event}"
                f"{LogColor.RESET}"
            )

            return {
                "status": "error",
                "decision": decision,
                "error": "Unknown event decision",
                "event": event,
            }

        # -------------------------------------------------------------------------
        # 12. FALLBACK
        # -------------------------------------------------------------------------

        logger.info(
            f"{LogColor.MAGENTA}"
            f"[ScheduleStrategy] Event execution completed."
            f"{LogColor.RESET}"
        )

        return result


    async def p_event_execute(self, channel, server_ip, server_port, slave_id, device_id, event_decision, signal):
        """
        Peak Shaving strategy execution: calculates how much energy to charge / discharge per time slot based on Load.

        """

        logger.info(f"{LogColor.MAGENTA} Starting [ScheduleStrategy] executing... {LogColor.RESET}")

        strategy_id = 7
        strategy_name = "Schedule Strategy"

        # -------------------------------------------------
        # Raise EMS Status Flag fot Strategy
        # -------------------------------------------------
        # try:

        #     await self.strategy_flags_controller.raise_strategy_flags_once_per_day(
        #         strategy_name="flat_tou",
        #         flags={
        #             "enable_tou_tariff_strategy": 0,
        #             "enable_capped_dynamic_strategy": 0,
        #             "enable_peak_shaving_strategy": 1,
        #             "enable_basic_strategy": 0,
        #         }
        #     ) 

        # except Exception as e:
        #     logger.info(f"{LogColor.RED} Error whilte raising EMS STATUS FLAG {e} {LogColor.RESET}")

        # -------------------------------------------------
        # Load BESS Decision
        # -------------------------------------------------
        poi_meter_id = "plant_1_poi_meter_1"
        try:
            
            config = await ScheduleStrategy.bess_decision(device_id, poi_meter_id)

            (
                prevent_charging,
                block_discharging,
                hard_limit_kw,
                soft_limit_kw,
                threshold_kw,
                max_discharge_kw,
                max_usable_kw,
                mode,
                forecast_source,
                current_load_kw,
                soc,
                max_soc,
                enforce_soft,
                force_hard,
                next_peak_window,
                pv_power_kw,
                bess_power_kw,
                bess_gfm_transition_threshold,
                bess_gfl_transition_threshold,
                allow_charging_during_off_peak,
                current_window,
            ) = (
                config["prevent_charging"],
                config["block_discharging"],
                config["hard_limit_kw"],
                config["soft_limit_kw"],
                config["threshold_kw"],
                config["max_discharge_kw"],
                config["max_usable_kw"],
                config["mode"],
                config["forecast_source"],
                config["current_load_kw"],
                config["soc"],
                config["max_soc"],
                config["enforce_soft"],
                config["force_hard"],
                config["next_peak_window"],
                config["pv_power_kw"],
                config["bess_power_kw"],
                config["bess_gfm_transition_threshold"],
                config["bess_gfl_transition_threshold"],
                config["allow_charging_during_off_peak"],
                config["current_window"],
            )

        except Exception as e:
            logger.info(f"{LogColor.RED} Error loading live params: {e} {LogColor.RESET}")
            return

        pv_power_kw = self.pv_kw
        current_load_kw = self.load_kw
        current_tariff_per_kw = self.tariff_per_kw
        
        # ---------------------------------
        # Inserting Starting Execution Log
        # ---------------------------------
        # try :

        
        #     pool = await get_async_connection()
        #     async with pool.acquire() as conn:

        #         # create execution log
        #         exec_log = ExecutionLog(strategy_id=strategy_id, started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), input_snapshot={"live": {"soc" : soc, "current_load" : current_load_kw}, "config": {}, "flags": {}})
        #         exec_id = await create_execution_log(conn, exec_log)
        #     await pool.close()
        # except Exception as e:
        #     logger.info(f"{LogColor.RED} Error While Creating Log Exec. {e} {LogColor.RESET}")

        # -----------------------------------------------------------------------------
        # ***************************** EQUATIONS *************************************
        # -----------------------------------------------------------------------------

        grid_power = current_load_kw - pv_power_kw - bess_power_kw

        pv_surplus_power_kw = pv_power_kw - current_load_kw

        # ------------------------------------------------------------------------------
        # ***************************** SMART CONTROLLER MODE ************************
        # ------------------------------------------------------------------------------

        # try:
        #     await smart_mode_controller(self.resolver)

        # except Exception as e:
        #     logger.info(f"{LogColor.RED} Error While Execute [SmartModeController]: {e} {LogColor.RESET}")

        
        # ------------------------------------------------------------------------------
        # ***************************** CONTROLLED PEAK SHAVING ************************
        # ------------------------------------------------------------------------------

        # threshold_kw => Peak Shaving Threshold
        # Soft Limit => Safe Limit
        # Hard Limit => Contractual Limit

        logger.info(
            f"Grid Power: {grid_power} | "
            f"Current Load: {current_load_kw} | "
            f"PV Power: {pv_power_kw} | "
            f"BESS Power: {bess_power_kw} | "
            f"PV Surplus: {pv_surplus_power_kw} | "
            f"Threshold kW: {threshold_kw} | "
            f"Soft Limit kW: {soft_limit_kw} | "
            f"Block Discharge: {block_discharging} | "
            f"Prevent Charging: {prevent_charging} | "
            f"Schedule Decision: {event_decision} | "
        )

        result = None

        # Set Target Limit kW
        target_limit = None

        # Determine which limit is active
        if threshold_kw < grid_power < soft_limit_kw:
            target_limit = threshold_kw

        elif soft_limit_kw < grid_power < hard_limit_kw:
            if enforce_soft:
                target_limit = soft_limit_kw
            else:
                target_limit = hard_limit_kw

        elif grid_power > hard_limit_kw:
            target_limit = hard_limit_kw

        if target_limit is None:

            logger.info("target limit is none!!")

            if pv_surplus_power_kw > 0:
            
                if soc >= max_soc:
                    logger.info(
                        f"{LogColor.MAGENTA}PV surplus available but battery is full. "
                        f"Curtailing PV.{LogColor.RESET}"
                    )

                    result = await ScheduleStrategy.pv_power_curtailment(
                        pv_power_kw,
                        pv_surplus_power_kw,
                        grid_power,
                        current_load_kw
                    )

                elif not prevent_charging:

                    logger.info(
                        f"{LogColor.MAGENTA}Charging battery from PV surplus "
                        f"({pv_surplus_power_kw:.2f} kW).{LogColor.RESET}"
                    )

                    result = await ScheduleStrategy.PeakShavingChargingStrategyType7(
                        resolver=self.resolver,
                        controller_static=self.controller_static,
                        device_id=device_id,
                        channel=channel,
                        server_ip=server_ip,
                        server_port=server_port,
                        slave_id=slave_id,
                        available_kw=pv_surplus_power_kw,
                        max_soc=max_soc
                    )

            elif current_window is not None and current_window["state"] == "off-peak" and allow_charging_during_off_peak and not prevent_charging:
                    available_kw = threshold_kw + pv_power_kw - current_load_kw

                    if available_kw > 0:
                        logger.info(
                            f"{LogColor.MAGENTA}Charging battery from utility during off-peak "
                            f"({available_kw:.2f} kW).{LogColor.RESET}"
                        )

                        result = await ScheduleStrategy.PeakShavingChargingStrategyType7(
                            resolver=self.resolver,
                            controller_static=self.controller_static,
                            device_id=device_id,
                            channel=channel,
                            server_ip=server_ip,
                            server_port=server_port,
                            slave_id=slave_id,
                            available_kw=available_kw,
                            max_soc=max_soc
                        )

        else :

            required_kw = grid_power - target_limit
            discharge_kw = min(required_kw, max_usable_kw, max_discharge_kw)

            logger.info(
                f"(Required={required_kw:.2f}, Available={discharge_kw:.2f}) "
            )

            # Battery cannot support required load
            if discharge_kw < required_kw:
                block_discharging = True

            # GFL -> GFM Transition SoC threshold
            # if soc < bess_gfm_transition_threshold:
            #     block_discharging = False
                
            # main Execution
            if not block_discharging:

                logger.info(
                    f"Peak shaving using BESS | "
                    f"Grid={grid_power:.2f}, "
                    f"Target={target_limit:.2f}, "
                    f"Required={required_kw:.2f}, "
                    f"Discharge={discharge_kw:.2f}"
                )

                # await gfl_gfm_mode_selection(
                #     resolver=self.resolver,
                #     mode="GFM",
                #     device_type="BESS"
                # )

                result = await ScheduleStrategy.PeakShavingDischargeStrategyType7(
                    resolver=self.resolver,
                    controller_static=self.controller_static,
                    server_ip=server_ip,
                    server_port=server_port,
                    channel=channel,
                    slave_id=slave_id,
                    device_id=device_id,
                    excess_load=discharge_kw,
                    target_limit=target_limit,
                    poi_meter_id=poi_meter_id,
                    extra_info={"force_hard": force_hard}
                )

            else:

                logger.info(
                    f"BESS capacity insufficient "
                    f"(Required={required_kw:.2f}, Available={discharge_kw:.2f}) "
                    f"Starting DG."
                )


        # Store result into DB
        # pool = await get_async_connection()
        # async with pool.acquire() as conn:
        #     await finish_execution_log(
        #         conn,
        #         exec_id,
        #         finished_at=datetime.now(),
        #         decision_snapshot=result if result else {"text": "Peak Shaving is normal. No action. Strategy execution completed"},
        #         error_text=""
        #     )
        # await pool.close()

        logger.info("[ScheduleStrategy] Strategy execution completed.")
        return

    def set_event_running_time(self):
        with self.result_file.open("r", encoding="utf-8") as f:
            data = json.load(f)

        data["running_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with self.result_file.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, default=str)

        logger.info(
            f"[EVENT Running] at {data['running_timestamp']}."
        )

    def build_energy_strategy(self, event):
        start_ts = datetime.strptime(
            event["time_stamp"],
            "%Y-%m-%d %H:%M:%S",
        )

        end_ts = start_ts + timedelta(minutes=15)

        strategy_type = event["decision"].lower()

        if strategy_type == "discharge":
            power = event["discharge_kw"]
        elif strategy_type == "charge":
            power = -max(event["charge_kw"], event["grid_charge_kw"])
        else:
            power = 0.0

        remark = event.get("description", "")

        strategy_rec = EnergyStrategy(
            ts=datetime.now(),
            start_ts=start_ts,
            end_ts=end_ts,
            type=strategy_type,
            tariff=event.get("tariff_per_kw", 0.0),
            power=power,
            duration='15',
            min_soc=0.0,       # replace with your configured value
            max_soc=80.0,     # replace with your configured value
            priority="low",        # replace with your configured value
            remark=remark,
            schedule_from="INTERNAL",
        )

        return strategy_rec

    async def _execute_window_load(
        self,
        window_load,
        channel,
        server_ip,
        server_port,
        slave_id,
        device_id,
    ):
        """
        Write the load profile once every minute.
        window_load contains 15 values for a 15-minute BESS event.
        """

        for minute_index, load_kw in enumerate(window_load):

            logger.info(
                f"[LOAD PROFILE] "
                f"Minute {minute_index + 1}/{len(window_load)}: "
                f"{load_kw:.2f} kW"
            )

            self.load_kw = load_kw

            await _execute_load_value_writer(
                resolver=self.resolver,
                value=float(load_kw),
                channel_id=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
            )

            # Wait exactly 1 minute before the next load value
            if minute_index < len(window_load) - 1:
                await asyncio.sleep(60)

    async def _execute_window_pv(
        self,
        window_pv,
        window_poa,
        channel,
        server_ip,
        server_port,
        slave_id,
        device_id,
    ):
        """
        Write the load profile once every minute.
        window_load contains 15 values for a 15-minute BESS event.
        """

        for minute_index, pv_kw in enumerate(window_pv):
            poa = window_poa[minute_index]
            logger.info(
                f"[LOAD PROFILE] "
                f"Minute {minute_index + 1}/{len(window_pv)}: "
                f"{pv_kw:.2f} kW"
                f"{pv_kw:.2f} kW"
            )

            self.pv_kw = pv_kw

            await _execute_pv_value_writer(
                resolver=self.resolver,
                value=float(poa),
                channel_id=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
            )

            # Wait exactly 1 minute before the next load value
            if minute_index < len(window_pv) - 1:
                await asyncio.sleep(60)

    def get_dynamic_recommendations(self, fault_name: str, event: dict) -> list[str]:
        """
        Generates magnitude-based EMS recommendations based on the specific fault 
        and current system state (load, charge/discharge, grid limits).
        """
        recommendations = []
        
        # 1. Extract State Metrics
        load_kw = event.get("load_kw", 0.0)
        net_load_kw = event.get("net_load_kw", 0.0)
        charge_kw = event.get("charge_kw", 0.0)
        grid_charge_kw = event.get("grid_charge_kw", 0.0)
        discharge_kw = event.get("discharge_kw", 0.0)
        grid_import_kw = event.get("grid_import_kw", 0.0)
        grid_import_limit_kw = event.get("grid_import_limit_kw", 0.0)
        
        # 2. Calculate System Capacities & Margins
        # Sum of all transformer ratings (assuming kVA ≈ kW for magnitude estimation)
        total_transformer_capacity_kw = 4000
        
        # Define a 5% tolerance band as requested
        tolerance_kw = grid_import_limit_kw * 0.05
        
        import_headroom_kw = grid_import_limit_kw - grid_import_kw
        transformer_headroom_kw = total_transformer_capacity_kw - net_load_kw

        if fault_name == "GRID_LOSS":
            recommendations.append(f"DG online and increase ramp to {grid_import_kw/1000:.2f} MW.")
        
        # 3. Evaluate Faults & Generate Magnitude-Based Suggestions
        if fault_name == "TRANSFORMER_OVERLOADING" or import_headroom_kw < tolerance_kw:
            # Calculate how much we need to correct (the deficit)
            deficit_kw = max((import_headroom_kw - tolerance_kw), (transformer_headroom_kw - tolerance_kw), 0.0)
            
            # Suggestion A: Limit Charge (if currently charging)
            if charge_kw > 0 or grid_charge_kw > 0:
                # Suggest reducing charge by at least 500kW or 60% of the deficit
                charge = max(charge_kw, grid_charge_kw)
                reduce_charge_by_kw = min(charge, max(deficit_kw * 0.6, 500.0))
                recommendations.append(f"Stop or limit BESS charge by {reduce_charge_by_kw/1000:.2f} MW to relieve transformer/grid stress.")
            
            # Suggestion B: Increase Discharge (to offset the deficit)
            if deficit_kw > 0:
                # Suggest increasing discharge to cover a portion of the deficit (capped at 2MW for safety)
                increase_discharge_by_kw = min(deficit_kw * 0.8, 2000.0)
                recommendations.append(f"Increase BESS discharge by {increase_discharge_by_kw/1000:.2f} MW to offset the {deficit_kw/1000:.2f} MW overload.")
            
            # Suggestion C: Load Shifting / Shedding
            # Suggest shifting at least 500kW or 50% of the calculated deficit
            load_shift_kw = max(deficit_kw * 0.5, 500.0)
            recommendations.append(f"Shift or shed non-critical load by {load_shift_kw/1000:.2f} MW.")

        elif fault_name == "TRANSFORMER_UNDERLOADING":
            available_headroom_kw = min(import_headroom_kw, transformer_headroom_kw)
            if available_headroom_kw > 1000:  # If more than 1MW available
                charge_increase_kw = min(available_headroom_kw * 0.7, 2000.0)
                recommendations.append(f"Utilize available capacity: Increase BESS charge by {charge_increase_kw/1000:.2f} MW.")
                recommendations.append(f"Consider advancing deferrable loads by {min(available_headroom_kw * 0.3, 1000.0)/1000:.2f} MW.")

        elif fault_name == "UNDER_VOLTAGE":
            # Estimate reactive power support needed (typically 20-30% of active load)
            q_support_mvar = (net_load_kw * 0.25) / 1000.0
            recommendations.append(f"Inject approximately {q_support_mvar:.2f} MVAR of reactive power to support voltage levels.")
            recommendations.append("Verify transformer tap changer position; consider raising tap by 1-2 steps.")

        elif fault_name == "OVER_VOLTAGE":
            q_absorb_mvar = (net_load_kw * 0.25) / 1000.0
            recommendations.append(f"Absorb approximately {q_absorb_mvar:.2f} MVAR of reactive power to mitigate over-voltage.")
            recommendations.append("Verify transformer tap changer position; consider lowering tap by 1-2 steps.")
            
        else:
            recommendations.append(f"Investigate {fault_name} and review real-time telemetry for magnitude-based corrective actions.")

        return recommendations

    @staticmethod
    async def pv_power_curtailment(
        pv_power_kw: float,
        pv_surplus_power_kw: float,
        grid_power: float,
        current_load_kw: float
    ):
        """
            This Function is responsible fot PV Curtailment.
            Condition: Load Power is less then PV Power but battery is at MAX. Action Curtailment of PV
        """

        logger.info(f"{LogColor.MAGENTA} [PV Curtailment] in execution... {LogColor.RESET}")

        grid_threshold_kw = -150
        pv_setpoint = pv_power_kw
        if grid_power < grid_threshold_kw:
            pv_setpoint = current_load_kw + (grid_threshold_kw * -1)
        

        return {}

    @staticmethod
    def load_peak_shaving_settings(json_path="controller_layer/strategies/strategy_configuration/PeakShaving.json") -> dict:
        """
        Load peak shaving settings and return resolved values.
        
        Returns:
            dict containing:
                - peak_shaving_mode (string)
                - forcast_source (string)
                - threshold_kw
                - reserve_soc_percent
                - advanced_settings (dict)
        """
        path = DataPaths.resolve(json_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {json_path}")

        with open(path, "r") as f:
            data = json.load(f)

        # Resolve mode name
        mode_key = data.get("peak_shaving_mode")
        mode = data.get("peak_shaving_mode_option", {}).get(mode_key)

        # Resolve forecast source
        source_key = data.get("forcast_source")
        source = data.get("forcast_source_option", {}).get(source_key)

        # Resolve Load Windows
        windows = data.get("windows", {})
        current_window = ScheduleStrategy.get_current_load_window_state(windows)
        next_peak_window = ScheduleStrategy.get_next_peak_window_state(windows)
        next_load_window = ScheduleStrategy.get_next_load_window(windows)

        return {
            "peak_shaving_mode": mode,
            "peak_shaving_mode_key": mode_key,
            "forcast_source": source,
            "forcast_source_key": source_key,
            "threshold_kw": data.get("peak_shaving_threshold_kw"),
            "reserve_soc_percent": data.get("reserve_soc_percent"),
            "advanced_settings": data.get("advanced_settings", {}),
            "optimization_rules": data.get("optimization_rules", {}),
            "load_windows" : windows,
            "current_window" : current_window,
            "next_peak_window" : next_peak_window,
            "next_load_window" : next_load_window,
            "bess_gfl_transition_threshold": data.get("bess_gfl_transition_threshold"),
            "bess_gfm_transition_threshold": data.get("bess_gfm_transition_threshold")
        }

    @staticmethod
    def get_next_load_window(windows: dict):
        """
        Returns next upcoming load window (off_peak, mid_peak, peak).
        """

        now = datetime.now()
        today = now.date()
        next_window = None

        for state_name, ranges in windows.items():
            for r in ranges:
                start_time = datetime.strptime(r["start_time"], "%H:%M").time()
                end_time = datetime.strptime(r["end_time"], "%H:%M").time()

                start_dt = datetime.combine(today, start_time)
                end_dt = datetime.combine(today, end_time)

                # Handle midnight crossing
                if start_time > end_time:
                    end_dt += timedelta(days=1)

                # If already passed → move to tomorrow
                if start_dt <= now:
                    start_dt += timedelta(days=1)
                    end_dt += timedelta(days=1)

                if not next_window or start_dt < next_window["startTs"]:
                    next_window = {
                        "state": state_name,
                        "startTs": start_dt,
                        "endTs": end_dt,
                        "load": r["load"]
                    }

        return next_window
    
    @staticmethod
    def get_next_peak_window_state(windows: dict):
        """
        Find the next upcoming peak window.
        Returns state info with startTs, endTs, load.
        """

        now = datetime.now()
        today = now.date()
        next_peak = None

        peak_ranges = windows.get("peak", [])

        for r in peak_ranges:
            start_time = datetime.strptime(r["start_time"], "%H:%M").time()
            end_time = datetime.strptime(r["end_time"], "%H:%M").time()

            start_dt = datetime.combine(today, start_time)
            end_dt = datetime.combine(today, end_time)

            # Handle midnight crossing
            if start_time > end_time:
                end_dt += timedelta(days=1)

            # If already passed today → move to tomorrow
            if start_dt <= now:
                start_dt += timedelta(days=1)
                end_dt += timedelta(days=1)

            # Keep the earliest upcoming peak
            if not next_peak or start_dt < next_peak["startTs"]:
                next_peak = {
                    "state": "peak",
                    "startTs": start_dt,
                    "endTs": end_dt,
                    "load": r["load"]
                }

        return next_peak
    
    @staticmethod
    def get_current_load_window_state(windows: dict):
        """
        Determine current state (off_peak, mid_peak, peak) and return
        the state info with startTs, endTs, price, and optimization rules.
        """
        now = datetime.now()
        current_time = now.time()
        
        def time_in_range(start_str, end_str, check_time):
            start = datetime.strptime(start_str, "%H:%M").time()
            end = datetime.strptime(end_str, "%H:%M").time()

            # Handle ranges that cross midnight
            if start <= end:
                return start <= check_time <= end
            else:
                return check_time >= start or check_time <= end

        result = {}

        # Iterate each TOU type (off_peak, mid_peak, peak)
        for state_name, ranges in windows.items():
            for r in ranges:
                if time_in_range(r["start_time"], r["end_time"], current_time):
                    # Build today's datetime timestamps
                    start_dt = datetime.combine(now.date(), datetime.strptime(r["start_time"], "%H:%M").time())
                    end_dt = datetime.combine(now.date(), datetime.strptime(r["end_time"], "%H:%M").time())

                    result = {
                        "state": state_name,
                        "startTs": start_dt,
                        "endTs": end_dt,
                        "load": r["load"]
                    }
                    return result

        return None  # If nothing matches (should not normally happen)

    # @staticmethod
    # async def bess_decision(device_id: str, poi_meter_id: str):
    #     """
    #     CENTRALIZED DECISION ENGINE
    #     Reads configs + live data + cycle data and returns one decision dict.
    #     """

    #     result = await bess_decision7(device_id, poi_meter_id)

    #     return result
  
    @staticmethod
    async def bess_decision(device_id: str, poi_meter_id: str):
        """
        CENTRALIZED DECISION ENGINE
        Reads configs + live data + cycle data and returns one decision dict.
        """

        result = {}

        # ============================================================
        # 1. Load Peak Shaving Settings
        # ============================================================
        cfg = ScheduleStrategy.load_peak_shaving_settings()

        result["mode"]                  = cfg["peak_shaving_mode"]
        result["threshold_kw"]          = cfg["threshold_kw"]
        result["forecast_source"]       = cfg["forcast_source"]

        result["load_windows"]          = cfg["load_windows"]
        result["current_window"]        = cfg["current_window"]
        result["next_peak_window"]      = cfg["next_peak_window"]
        result["next_load_window"]      = cfg["next_load_window"]
        
        adv = cfg["advanced_settings"]

        result["soft_limit_kw"]         = adv["soft_limit_kw"]
        result["hard_limit_kw"]         = adv["hard_limit_kw"]
        result["enforce_soft"]          = adv["maintain_load_under_soft_limit_using_bess"]
        result["force_hard"]            = adv["force_control_to_stay_under_hard_limit"]

        result["bess_gfl_transition_threshold"] = cfg["bess_gfl_transition_threshold"]
        result["bess_gfm_transition_threshold"] = cfg["bess_gfm_transition_threshold"]

        opti = cfg["optimization_rules"]

        result["allow_charging_during_off_peak"] = opti["allow_charging_during_off_peak"]
        result["allow_charging_only_from_solar"] = opti["allow_charging_only_from_solar"]
        result["discharge_only_during_peak"] = opti["discharge_only_during_peak"]

        # ============================================================
        # 2. Read POI Load (Always needed)
        # ============================================================
        resolver = LatestTelemetryResolver()
        # poi_data = resolver.get_latest_device_data(poi_meter_id)
        # result["current_load_kw"] = float(poi_data[Tags.Load]["value"])
        # result["pv_power_kw"] = float(poi_data[Tags.PV_POWER]["value"]) # from solar meter
        

        # ============================================================
        # 3. Load BESS Config
        # ============================================================
        controller_static = ControllerStaticResolver()
        bess = load_bess_params_setting()
        result["min_soc"]      = bess["min_soc"]
        result["max_soc"]      = bess["max_soc"]
        result["backup_soc"]   = bess["backup_soc"]
        result["battery_capacity"] = controller_static.get("battery_capacity")
        result["load_rise_kw_per_sec"] = controller_static.get("load_rise_kw_per_sec")
        result["pv_drop_kw_sec"] = controller_static.get("pv_drop_kw_sec")
        result["ems_response_time_delay_sec"] = controller_static.get("ems_response_time_delay_sec")
        efficiency = controller_static.get("efficiency")
        result["efficiency"] = efficiency if efficiency <= 1 else efficiency * 0.01
        result["max_discharge_kw"] = bess.get("max_discharge_kw")
        result["max_charge_kw"] = bess.get("max_charge_kw")


        # ============================================================
        # 4. Live BESS Data
        # ============================================================
        device_data = resolver.get_latest_device_data(device_id)

        result["soc"] = float(device_data[Tags.BATTERY_SOC]["value"])
        result["bess_power_kw"] = float(device_data[Tags.BATTERY_POWER]["value"])
        result["charge_ah"] = float(device_data[Tags.BATTERY_ENERGY]["value"])
        result["battery_energy_kwh"] = float(device_data[Tags.BATTERY_ENERGY]["value"])
        result["battery_temperature"] = device_data.get("Battery_Temperature", {}).get("value", 25)

        result["fault_code"] = device_data[Tags.Fault_Code]["value"]

        # Data from Twin
        result["current_load_kw"] = float(device_data[Tags.Load]["value"])
        result["pv_power_kw"] = float(device_data[Tags.PV_POWER]["value"])

        # Usable Discharge Power of Battery
        result["max_usable_kw"] = get_c_rate(result["battery_temperature"], bess) * (result["battery_capacity"] * (result["soc"] - result["min_soc"]) / 100) * result["efficiency"]

        # ============================================================
        # 5. Cycle & Safety Management
        # ============================================================
        cycle_cfg            = load_cycle_config()

        result["cycle_used_today"]       = 4#await get_today_cycles()
        result["max_cycles_per_day"]     = cycle_cfg["max_cycles_per_day"]

        result["soc_safety"]             = cycle_cfg["soc_safety_management"]
        result["lockout"]                = cycle_cfg["lockout_behaviour"]
        result["price_cfg"]              = cycle_cfg["price_logic_overrides"]

        # Derived flags
        result["disable_low_price_charging"] = result["price_cfg"]["disable_low_price_charging"]
        result["disable_peak_shaving"]       = result["price_cfg"]["disable_peak_shaving"]

        # ============================================================
        # 6. Block / Safety Logic Precomputation
        # ============================================================

        # 6.1 Cycle Limit
        result["block_discharging"] = (
            result["cycle_used_today"] >= result["max_cycles_per_day"]
        )

        # 6.2 SOC Safe
        min_safe_soc = result["min_soc"]
        soc = result["soc"]
        result["aggressively_charge"] = False
        result["prevent_charging"] = False

        if soc < min_safe_soc :
            result["block_discharging"] = True
            result["aggressively_charge"] = True
            result["prevent_charging"] = False   # allow charging
            result["target_soc"] = result["backup_soc"]

        # RESERVE
        reserve = result["backup_soc"]
        if soc < reserve and result["lockout"]["allow_reserve_charging"]:
            result["prevent_charging"] = False

        # ============================================================
        # 7. Dynamic Peak Shaving Threshold Computation
        # ============================================================
        try:

            computation_result = ScheduleStrategy.calculate_dynamic_peak_shaving(
                windows=result["load_windows"],
                battery_capacity=result["battery_capacity"],
                current_soc=result["soc"],
                max_soc=result["max_soc"],
                efficiency=result["efficiency"],
                contractual_limit_kw=result["hard_limit_kw"],
                load_rise_kw_per_sec=result["load_rise_kw_per_sec"],
                pv_drop_kw_sec=result["pv_drop_kw_sec"],
                ems_response_time_delay_sec=result["ems_response_time_delay_sec"]
            )
        except Exception as e:
            logger.info(f"Error While Dynamic Writes: {e}")

        result["threshold_kw"] = computation_result.get("dynamic_ps_limit_kw", result["threshold_kw"])
        result["soft_limit_kw"] = computation_result.get("soft_limit_kw", result["soft_limit_kw"])

        return result
    
    @staticmethod
    async def fetch_yesterday_power_data(
        conn,
        load_table: str,
        pv_table: str,
        bess_table: str,
        load_col: str,
        pv_col: str,
        bess_col: str,
        ts_col: str = "timestamp"
    ):
        """
        Fetch yesterday's load, pv and bess power values.
        """

        yesterday_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        yesterday_end = yesterday_start + timedelta(days=1)

        query = f"""
        SELECT 
            l.{ts_col} as ts,
            l.{load_col} as load_power,
            p.{pv_col} as pv_power,
            b.{bess_col} as bess_power
        FROM {load_table} l
        LEFT JOIN {pv_table} p ON l.{ts_col} = p.{ts_col}
        LEFT JOIN {bess_table} b ON l.{ts_col} = b.{ts_col}
        WHERE l.{ts_col} >= $1
        AND l.{ts_col} < $2
        ORDER BY l.{ts_col}
        """

        rows = await conn.fetch(query, yesterday_start, yesterday_end)

        result = []

        for r in rows:
            result.append({
                "ts": r["ts"].isoformat(),
                "load_kw": r["load_power"] or 0,
                "pv_kw": r["pv_power"] or 0,
                "bess_kw": r["bess_power"] or 0
            })

        return result
     
    @staticmethod
    async def store_daily_snapshot(config):
        import os
        import json

        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        day_before_yesterday = yesterday - timedelta(days=1)

        cache_dir = "/data/cache"
        os.makedirs(cache_dir, exist_ok=True)

        day_before_yesterday_file = f"{cache_dir}/{day_before_yesterday}_snapshot.json"
        yesterday_file = f"{cache_dir}/{yesterday}_snapshot.json"

        # -------------------------------------------------
        # If today's snapshot already exists → SKIP
        # -------------------------------------------------
        if os.path.exists(yesterday_file):
            print(f"[SNAPSHOT] yesterday's snapshot already exists → Skipping")
            return yesterday_file

        # -------------------------------------------------
        # If day_before_yesterday file exists → DELETE
        # -------------------------------------------------
        if os.path.exists(day_before_yesterday_file):
            print(f"[SNAPSHOT] Removing old snapshot file")
            os.remove(day_before_yesterday_file)

        # -------------------------------------------------
        # Fetch yesterday data
        # -------------------------------------------------

        async with A_DB as conn:
            data = await ScheduleStrategy.fetch_yesterday_power_data(
                conn,
                load_table=config["load_table"],
                pv_table=config["pv_table"],
                bess_table=config["bess_table"],
                load_col=config["load_column"],
                pv_col=config["pv_column"],
                bess_col=config["bess_column"]
            )

        with open(yesterday_file, "w") as f:
            json.dump(data, f)

        return yesterday_file
    
    @staticmethod
    def calculate_dynamic_peak_shaving(
        windows: dict,
        battery_capacity: float,
        current_soc: float,
        max_soc: float,
        efficiency: float,
        contractual_limit_kw: float,
        load_rise_kw_per_sec,
        pv_drop_kw_sec,
        ems_response_time_delay_sec
    ):
        """
        Calculates dynamic peak shaving threshold and energy sufficiency.
        """

        now = datetime.now()

        # Get next peak window
        peak_window = ScheduleStrategy.get_next_peak_window_state(windows=windows)

        if not peak_window:
            return {"error": "No upcoming peak window found"}

        # Ensure we are subtracting datetime objects, not tuples
        start = peak_window["startTs"]
        end = peak_window["endTs"]

        # If they accidentally became tuples, grab the first element
        if isinstance(start, tuple): start = start[0]
        if isinstance(end, tuple): end = end[0]

        # Calculate duration in hours
        peak_hours = (end - start).total_seconds() / 3600
        # Usable energy (kWh)
        usable_energy_kwh = (
            battery_capacity * ((max_soc - current_soc) / 100) * efficiency
        )

        # p_max calculation
        p_max_kw = min(usable_energy_kwh / peak_hours, peak_window["load"] - contractual_limit_kw) if peak_hours > 0 else 1

        # Dynamic PS limit
        dynamic_threshold_limit_kw = contractual_limit_kw - p_max_kw


        # Worst case net ramp rate
        r_net = load_rise_kw_per_sec + pv_drop_kw_sec
        overshoot = r_net * ems_response_time_delay_sec

        # Soft Limit Calculation
        soft_limit_kw = contractual_limit_kw - overshoot  # If include safety factor then (1.5 * overshoot)

        # Writing Dynamic Thresholds KW
        ScheduleStrategy.write_dynamic_threshold_to_json(dynamic_threshold_limit_kw, soft_limit_kw)

        # Required energy to shave full peak
        required_energy_kwh = p_max_kw * peak_hours

        sufficient = usable_energy_kwh >= required_energy_kwh
        shortfall_kwh = 0 if sufficient else required_energy_kwh - usable_energy_kwh

        # Get next window
        next_window = ScheduleStrategy.get_next_load_window(windows)

        charging_possible = False
        charging_sufficient = None
        max_possible_charge_kwh = 0

        if next_window and next_window["state"] == "off_peak":
            # Ensure we are subtracting datetime objects, not tuples
            start = next_window["startTs"]
            end = next_window["endTs"]

            # If they accidentally became tuples, grab the first element
            if isinstance(start, tuple): start = start[0]
            if isinstance(end, tuple): end = end[0]

            # Calculate duration in hours
            charge_hours = (
               end  - start
            ).total_seconds() / 3600

            # Max possible charge during off-peak
            max_possible_charge_kwh = min(
                (battery_capacity * efficiency), contractual_limit_kw - next_window["load"]
            ) * (charge_hours)

            charging_possible = True
            charging_sufficient = max_possible_charge_kwh >= shortfall_kwh

        return {
            "next_peak_window": peak_window,
            "peak_duration_hours": peak_hours,
            "usable_energy_kwh": usable_energy_kwh,
            "p_max_kw": p_max_kw,
            "contractual_limit_kw": contractual_limit_kw,
            "dynamic_ps_limit_kw": dynamic_threshold_limit_kw,
            "soft_limit_kw": soft_limit_kw,
            "energy_sufficient": sufficient,
            "energy_shortfall_kwh": shortfall_kwh,
            "next_window": next_window,
            "charging_possible_in_next_window": charging_possible,
            "charging_sufficient_for_shortfall": charging_sufficient,
            "max_possible_charge_kwh_during_shorfall": max_possible_charge_kwh
        }
    
    @staticmethod
    def write_dynamic_threshold_to_json(
        dynamic_threshold_kw: float,
        soft_limit_kw: float
    ):
        """
        Writes dynamic peak shaving threshold into JSON file
        under key: 'peak_shaving_threshold_kw'
        """
        json_path="controller_layer/strategies/strategy_configuration/PeakShaving.json"

        # Resolve path
        path = DataPaths.resolve(json_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {json_path}")

        # Load existing JSON
        with open(path, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                raise ValueError(f"Invalid JSON format in {json_path}")

        # Update threshold
        data["peak_shaving_threshold_kw"] = round(dynamic_threshold_kw, 3)
        data["advanced_settings"]["soft_limit_kw"] = round(soft_limit_kw, 3)

        # Write back safely
        with open(path, "w") as f:
            json.dump(data, f, indent=4)

        return {
            "status": "success",
            "written_threshold_kw": dynamic_threshold_kw,
            "file": str(path)
        }
    
    @staticmethod
    def simulate_daily_optimization(controller_device_id: str):
        """
        Simulates EMS peak shaving + charging strategy
        for all remaining windows today.
        """

        # ============================================================
        # 1. Load Peak Shaving Settings
        # ============================================================
        cfg = ScheduleStrategy.load_peak_shaving_settings()

        windows = cfg["load_windows"]
        contractual_limit_kw = cfg["advanced_settings"]["hard_limit_kw"]

        # ============================================================
        # 2. Live BESS Data
        # ============================================================
        resolver = LatestTelemetryResolver()
        device_data = resolver.get_latest_device_data(controller_device_id)
        current_soc = float(device_data["SoC"]["value"])

        # ============================================================
        # 3. Load BESS Config
        # ============================================================
        controller_static = ControllerStaticResolver()
        bess = load_bess_params_setting()
        min_soc      = bess["min_soc"]
        max_soc      = bess["max_soc"]
        battery_capacity = controller_static.get("battery_capacity")
        load_rise_kw_per_sec = controller_static.get("load_rise_kw_per_sec")
        pv_drop_kw_sec = controller_static.get("pv_drop_kw_sec")
        ems_response_time_delay_sec = controller_static.get("ems_response_time_delay_sec")
        efficiency = controller_static.get("efficiency")
        efficiency = efficiency if efficiency <= 1 else efficiency * 0.01
        max_charge_power_kw = bess.get("max_charge_kw")
        max_discharge_power_kw = bess.get("max_discharge_kw")

        now = datetime.now()
        today = now.date()

        results = []

        # Flatten and sort all windows
        all_windows = []

        for state, ranges in windows.items():
            for r in ranges:
                start_time = datetime.strptime(r["start_time"], "%H:%M").time()
                end_time = datetime.strptime(r["end_time"], "%H:%M").time()

                start_dt = datetime.combine(today, start_time)
                end_dt = datetime.combine(today, end_time)

                if start_time > end_time:
                    end_dt = end_dt.replace(day=end_dt.day + 1)

                if end_dt > now:  # only future windows
                    all_windows.append({
                        "state": state,
                        "start": start_dt,
                        "end": end_dt,
                        "load": r["load"]
                    })

        all_windows.sort(key=lambda x: x["start"])

        soc = current_soc
        total_energy_discharged = 0

        for window in all_windows:

            duration_hours = (window["end"] - window["start"]).total_seconds() / 3600
            if duration_hours <= 0:
                continue

            soc_before = soc

            usable_energy_kwh = (
                battery_capacity * ((soc - min_soc) / 100) * efficiency
            )

            action = "idle"
            p_max_kw = 0
            dynamic_threshold = None
            soft_limit_kw = None
            shortfall_kwh = 0
            energy_used_kwh = 0
            energy_charged_kwh = 0

            # PEAK WINDOW → DISCHARGE
            if window["state"] == "peak":

                if usable_energy_kwh > 0:

                    p_max_kw = usable_energy_kwh / duration_hours
                    p_max_kw = min(p_max_kw, window["load"] - contractual_limit_kw, max_discharge_power_kw)

                    energy_used_kwh = p_max_kw * duration_hours
                    total_energy_discharged = total_energy_discharged + energy_charged_kwh

                    dynamic_threshold = contractual_limit_kw - p_max_kw

                    # Worst case net ramp rate
                    r_net = load_rise_kw_per_sec + pv_drop_kw_sec
                    overshoot = r_net * ems_response_time_delay_sec

                    # Soft Limit Calculation
                    soft_limit_kw = contractual_limit_kw - overshoot  # If include safety factor then (1.5 * overshoot)

                    soc_drop_percent = (energy_used_kwh / battery_capacity) * 100
                    soc -= soc_drop_percent

                    action = "discharge"

                else:
                    shortfall_kwh = battery_capacity * ((min_soc - soc) / 100)

            # OFF-PEAK WINDOW → CHARGE
            elif window["state"] == "off_peak":

                available_room_kwh = (
                    battery_capacity * ((max_soc - soc) / 100) * efficiency
                )

                if available_room_kwh > 0:

                    charge_power = min(max_charge_power_kw, available_room_kwh / duration_hours, contractual_limit_kw - window["load"])

                    energy_charged_kwh = charge_power * duration_hours * efficiency

                    energy_charged_kwh = min(energy_charged_kwh, available_room_kwh)

                    soc_rise_percent = (energy_charged_kwh / battery_capacity) * 100
                    soc += soc_rise_percent

                    action = "charge"

            # MID PEAK → Optional (you can customize)
            else:
                action = "idle"

            soc = max(min(soc, max_soc), min_soc)

            results.append({
                "window_state": window["state"],
                "start": window["start"],
                "end": window["end"],
                "load": window["load"],
                "duration_hours": duration_hours,
                "soc_before_percent": round(soc_before, 2),
                "soc_after_percent": round(soc, 2),
                "action": action,
                "p_max_kw": round(p_max_kw, 2),
                "dynamic_threshold_kw": round(dynamic_threshold, 2) if dynamic_threshold else None,
                "soft_limit_kw": round(soft_limit_kw, 2) if soft_limit_kw else None,
                "energy_used_kwh": round(energy_used_kwh, 2),
                "energy_charged_kwh": round(energy_charged_kwh, 2),
                "shortfall_kwh": round(shortfall_kwh, 2),
            })

        return {
            "simulation_start": now,
            "initial_soc": current_soc,
            "final_soc": round(soc, 2),
            "total_energy_discharged": total_energy_discharged,
            "windows_simulated": len(results),
            "timeline": results
        }


    async def PeakShavingChargingStrategyType7(
        self,
        resolver,
        controller_static,
        device_id: str,
        channel: str,
        server_ip: str,
        server_port: int,
        slave_id: int,
        available_kw: float,
        max_soc: float,
        extra_info: dict = None,
        event: dict = None,
    ) -> List[Dict]:

        """
        Execute charging according to the Schedule Strategy event.

        Important:
            The parent event is authoritative.

        Example:

            event = {
                "decision": "grid_charge",
                "grid_charge_kw": 1800.0,
                "soc_percent": 21.64,
                "battery_energy_mwh": 5.628,
                ...
            }

        For grid_charge:
            - The event decides the charging power.
            - The loop does NOT switch to PV charging automatically.
            - grid_charge_kw is revalidated against the live grid capacity
            on every polling cycle.

        Charging stops when:
            - Event target SOC is reached
            - Event target battery energy is reached
            - Maximum SOC is reached
            - Charging is blocked
            - Grid charging becomes unsafe/unavailable
            - No charging source is available
            - Parent event decision changes
        """

        extra_info = extra_info or {}
        event = event or {}

        results = []

        logger.info(
            f"{LogColor.MAGENTA}"
            f"[PeakShavingCharging] Starting charging execution."
            f"{LogColor.RESET}"
        )

        # =========================================================================
        # MAXIMUM EXECUTION TIME: 15 MINUTES
        # =========================================================================

        MAX_EXECUTION_SECONDS = 15 * 60  # 900 seconds

        start_time = time.monotonic()
        deadline = start_time + MAX_EXECUTION_SECONDS

        # Set Event running time in ETALab
        self.set_event_running_time()

        logger.info(
            "[PeakShavingCharging] Maximum execution time: 15 minutes."
        )

        # =========================================================================
        # 1. EVENT INFORMATION
        # =========================================================================

        event_decision = str(
            event.get("decision", "")
        ).strip().lower()

        event_target_soc = event.get("soc_percent")
        event_target_energy_mwh = event.get("battery_energy_mwh")

        if event_target_soc is not None:
            event_target_soc = float(event_target_soc)

        if event_target_energy_mwh is not None:
            event_target_energy_mwh = float(
                event_target_energy_mwh
            )

        event_grid_charge_kw = float(
            event.get("grid_charge_kw", 0.0) or 0.0
        )

        event_charge_kw = float(
            event.get("charge_kw", 0.0) or 0.0
        )

        event_discharge_kw = float(
            event.get("discharge_kw", 0.0) or 0.0
        )

        event_grid_import_limit_kw = event.get(
            "grid_import_limit_kw"
        )

        if event_grid_import_limit_kw is not None:
            event_grid_import_limit_kw = float(
                event_grid_import_limit_kw
            )

        logger.info(
            f"{LogColor.CYAN}"
            f"[PeakShavingCharging] Event | "
            f"Decision={event_decision} | "
            f"GridCharge={event_grid_charge_kw:.2f} kW | "
            f"Charge={event_charge_kw:.2f} kW | "
            f"TargetSOC={event_target_soc}% | "
            f"TargetEnergy={event_target_energy_mwh} MWh"
            f"{LogColor.RESET}"
        )

        # =========================================================================
        # 2. STATIC PARAMETERS
        # =========================================================================

        battery_capacity = controller_static.get(
            "battery_capacity"
        )

        efficiency = controller_static.get(
            "efficiency"
        )

        if efficiency is None:
            efficiency = 1.0

        if battery_capacity is None:
            logger.error(
                f"{LogColor.RED}"
                f"[PeakShavingCharging] Battery capacity unavailable."
                f"{LogColor.RESET}"
            )

            return [{
                "status": "error",
                "reason": "battery_capacity_unavailable",
                "timestamp": datetime.now().isoformat(),
            }]

        # =========================================================================
        # 3. BESS SETTINGS
        # =========================================================================

        bess_params = load_bess_params_setting()

        min_soc = float(
            bess_params["min_soc"]
        )

        configured_max_soc = float(
            bess_params["max_soc"]
        )

        # Runtime max SOC cannot exceed configured BESS max SOC.
        safe_max_soc = min(
            float(max_soc),
            configured_max_soc
        )

        # =========================================================================
        # 4. INITIAL LIVE DATA
        # =========================================================================

        data = resolver.get_latest_device_data(
            device_id
        )

        battery_temp = data.get(
            "Battery_Temperature",
            {}
        ).get(
            "value",
            20
        )

        current_soc = data.get(
            Tags.BATTERY_SOC,
            {}
        ).get("value")

        if current_soc is None:

            logger.error(
                f"{LogColor.RED}"
                f"[PeakShavingCharging] Current SOC unavailable."
                f"{LogColor.RESET}"
            )

            return [{
                "status": "error",
                "reason": "soc_unavailable",
                "timestamp": datetime.now().isoformat(),
            }]

        current_soc = float(
            current_soc
        )

        # =========================================================================
        # 5. BATTERY ENERGY
        # =========================================================================

        def get_battery_energy_mwh(
            live_data,
            soc
        ):
            """
            Get battery energy in MWh.

            First preference:
                battery_energy_kwh

            Fallback:
                battery_capacity * SOC / 100

            IMPORTANT:
                If battery_capacity is actually kWh in your controller_static,
                change the fallback accordingly.
            """

            energy_value = live_data.get(
                Tags.BATTERY_ENERGY,
                {}
            ).get("value")

            if energy_value is not None:

                try:
                    return float(
                        energy_value / 1000
                    )

                except (
                    TypeError,
                    ValueError
                ):
                    pass

            return (
                float(battery_capacity)
                * float(soc)
                / (100.0 * 1000.0)
            )

        current_energy_mwh = get_battery_energy_mwh(
            data,
            current_soc
        )

        # =========================================================================
        # 6. INITIAL TARGET CHECK
        # =========================================================================

        target_soc_reached = (
            event_target_soc is not None
            and current_soc >= event_target_soc
        )

        target_energy_reached = (
            event_target_energy_mwh is not None
            and current_energy_mwh >= event_target_energy_mwh
        )

        max_soc_reached = (
            current_soc >= safe_max_soc
        )

        # if (
        #     target_energy_reached
        #     or max_soc_reached
        # ):

        #     if target_soc_reached:
        #         reason = "event_target_soc_reached"

        #     elif target_energy_reached:
        #         reason = "event_target_energy_reached"

        #     else:
        #         reason = "max_soc_reached"

        #     logger.info(
        #         f"{LogColor.GREEN}"
        #         f"[PeakShavingCharging] Charging already complete. "
        #         f"Reason={reason} | "
        #         f"SOC={current_soc:.2f}% | "
        #         f"Energy={current_energy_mwh:.3f} MWh"
        #         f"{LogColor.RESET}"
        #     )

        #     await _execute_slot_async_charging(
        #         resolver=resolver,
        #         setpoint_kw=0,
        #         channel_id=channel,
        #         server_ip=server_ip,
        #         server_port=server_port,
        #         slave_id=slave_id,
        #         device_id=device_id,
        #         slot_number=200,
        #         extra_info={
        #             **extra_info,
        #             "strategy_id": 7,
        #             "stop_reason": reason,
        #         },
        #     )

        #     return [{
        #         "status": "completed",
        #         "reason": reason,
        #         "soc": current_soc,
        #         "battery_energy_mwh": current_energy_mwh,
        #         "charge_kw": 0,
        #         "timestamp": datetime.now().isoformat(),
        #     }]

        # =========================================================================
        # 7. INITIAL CHARGING POWER
        # =========================================================================

        max_charge_kw = (
            get_c_rate(
                battery_temp,
                bess_params
            )
            * (
                battery_capacity
                * (
                    safe_max_soc - min_soc
                )
                / 100
            )
        )

        available_capacity_kwh = (
            battery_capacity
            * (
                safe_max_soc - current_soc
            )
            / 100
            * efficiency
        )

        max_charge = min(
            max_charge_kw,
            available_capacity_kwh
        )

        # =========================================================================
        # 8. DETERMINE INITIAL SETPOINT BASED ON EVENT
        # =========================================================================

        if event_decision == "grid_charge":

            requested_charge_kw = event_grid_charge_kw

        elif event_decision in (
            "charge",
            "pv_charge",
        ):

            requested_charge_kw = (
                event_charge_kw
                if event_charge_kw > 0
                else available_kw
            )

        else:

            logger.warning(
                f"{LogColor.YELLOW}"
                f"[PeakShavingCharging] Invalid charging decision: "
                f"{event_decision}"
                f"{LogColor.RESET}"
            )

            return [{
                "status": "error",
                "reason": "invalid_charging_decision",
                "decision": event_decision,
                "timestamp": datetime.now().isoformat(),
            }]

        charge_setpoint_kw = int(
            max(
                0,
                min(
                    requested_charge_kw,
                    max_charge
                )
            )
        )

        if charge_setpoint_kw <= 0:

            logger.warning(
                f"{LogColor.YELLOW}"
                f"[PeakShavingCharging] Initial charge setpoint is zero."
                f"{LogColor.RESET}"
            )

            await _execute_slot_async_charging(
                resolver=resolver,
                setpoint_kw=0,
                channel_id=channel,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
                slot_number=200,
                extra_info={
                    **extra_info,
                    "strategy_id": 7,
                    "stop_reason": "zero_initial_setpoint",
                },
            )

            return [{
                "status": "no_action",
                "reason": "zero_initial_setpoint",
                "soc": current_soc,
                "battery_energy_mwh": current_energy_mwh,
                "charge_kw": 0,
                "timestamp": datetime.now().isoformat(),
            }]

        # =========================================================================
        # 9. WRITE INITIAL SETPOINT
        # =========================================================================

        logger.info(
            f"{LogColor.MAGENTA}"
            f"[PeakShavingCharging] Initial setpoint = "
            f"{charge_setpoint_kw} kW | "
            f"Decision={event_decision}"
            f"{LogColor.RESET}"
        )

        await _execute_slot_async_charging(
            resolver=resolver,
            setpoint_kw=charge_setpoint_kw,
            channel_id=channel,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            device_id=device_id,
            slot_number=1,
            extra_info={
                **extra_info,
                "strategy_id": 7,
                "event_decision": event_decision,
            },
        )

        results.append({
            "soc": current_soc,
            "battery_energy_mwh": current_energy_mwh,
            "charge_kw": charge_setpoint_kw,
            "decision": event_decision,
            "timestamp": datetime.now().isoformat(),
        })

        last_setpoint = charge_setpoint_kw
        slot_number = 1

        # =========================================================================
        # 10. CHARGING LOOP
        # =========================================================================

        await asyncio.sleep(5)

        try:

            while True:
                # =================================================================
                # 10.1 MAXIMUM EXECUTION TIME CHECK
                # =================================================================

                elapsed = time.monotonic() - start_time

                if elapsed >= MAX_EXECUTION_SECONDS:

                    logger.warning(
                        f"{LogColor.YELLOW}"
                        f"[PeakShavingCharging] 15-minute execution limit reached. "
                        f"Elapsed={elapsed:.1f} seconds. Stopping charging loop."
                        f"{LogColor.RESET}"
                    )

                    break

                await asyncio.sleep(1)

                # =================================================================
                # LOAD LIVE CONFIGURATION
                # =================================================================

                poi_meter_id = "plant_1_poi_meter_1"

                try:

                    config = await ScheduleStrategy.bess_decision(
                        device_id,
                        poi_meter_id
                    )

                    (
                        prevent_charging,
                        pv_power_kw,
                        bess_power_kw,
                        threshold_kw,
                        live_soc,
                        current_load_kw,
                        live_max_soc,
                        live_max_charge_kw,
                        live_efficiency,
                        allow_charging_during_off_peak,
                        current_window,
                        battery_energy_kwh,
                        battery_temperature,
                        fault_code,
                    ) = (
                        config["prevent_charging"],
                        config["pv_power_kw"],
                        config["bess_power_kw"],
                        config["threshold_kw"],
                        config["soc"],
                        config["current_load_kw"],
                        config["max_soc"],
                        config["max_charge_kw"],
                        config["efficiency"],
                        config["allow_charging_during_off_peak"],
                        config["current_window"],
                        config["battery_energy_kwh"],
                        config["battery_temperature"],
                        config["fault_code"]
                    )

                except Exception as e:

                    logger.error(
                        f"{LogColor.RED}"
                        f"[PeakShavingCharging] "
                        f"Error loading live params: {e}"
                        f"{LogColor.RESET}"
                    )

                    # break
                    await asyncio.sleep(1)  # Wait before retrying
                    continue

                # =================================================================
                # LIVE SOC
                # =================================================================

                current_soc = float(
                    live_soc
                )

                # Prefer live device energy if available.
                live_data = resolver.get_latest_device_data(
                    device_id
                )

                current_energy_mwh = (
                    get_battery_energy_mwh(
                        live_data,
                        current_soc
                    )
                )

                # =================================================================
                # LIVE LOAD / PV
                # =================================================================

                pv_power_kw = self.pv_kw
                # current_load_kw = self.load_kw
                if fault_code > 0:
                    self.fault = fault_code
                    logger.info(f"Fault Detected: {self.fault}")

                logger.info(
                    f"[PeakShavingCharging] "
                    f"SOC={current_soc:.2f}% | "
                    f"Energy={current_energy_mwh:.3f} MWh | "
                    f"PV={pv_power_kw:.2f} kW | "
                    f"Load={current_load_kw:.2f} kW | "
                    f"BESS={bess_power_kw:.2f} kW | "
                    f"Decision={event_decision} | "
                    f"Fault={self.fault}"
                )

                # =================================================================
                # 10.2 EVENT TARGET ENERGY
                # =================================================================

                if (
                    event_target_energy_mwh is not None
                    and current_energy_mwh >= event_target_energy_mwh
                ):

                    logger.info(
                        f"{LogColor.GREEN}"
                        f"[PeakShavingCharging] Event target energy reached. "
                        f"Current={current_energy_mwh:.3f} MWh | "
                        f"Target={event_target_energy_mwh:.3f} MWh"
                        f"{LogColor.RESET}"
                    )

                    break

                # =================================================================
                # 10.3 BESS MAX SOC SAFETY
                # =================================================================

                if current_soc >= safe_max_soc:

                    logger.info(
                        f"{LogColor.GREEN}"
                        f"[PeakShavingCharging] BESS maximum SOC reached. "
                        f"Current={current_soc:.2f}% | "
                        f"Max={safe_max_soc:.2f}%"
                        f"{LogColor.RESET}"
                    )

                    break

                # =================================================================
                # 10.4 LIVE CHARGING BLOCK
                # =================================================================

                if prevent_charging:

                    logger.warning(
                        f"{LogColor.RED}"
                        f"[PeakShavingCharging] Charging blocked "
                        f"by live configuration."
                        f"{LogColor.RESET}"
                    )

                    break

                # =================================================================
                # 10.5 IMPORTANT:
                #      EVENT DECISION REMAINS AUTHORITATIVE
                # =================================================================
                #
                # Do NOT do this:
                #
                #     PV surplus -> switch to PV
                #
                # when the event is grid_charge.
                #
                # The event says what source the scheduler selected.
                #
                # =================================================================

                if event_decision == "grid_charge":

                    # -------------------------------------------------------------
                    # GRID CHARGE
                    # -------------------------------------------------------------

                    requested_grid_charge_kw = (
                        event_grid_charge_kw
                    )

                    if requested_grid_charge_kw <= 0:

                        logger.info(
                            "[PeakShavingCharging] "
                            "Grid charge requested but event power is zero."
                        )

                        break

                    # -------------------------------------------------------------
                    # Current grid import
                    #
                    # During charging:
                    #
                    # grid import =
                    # load - PV + charge
                    #
                    # Therefore:
                    #
                    # max charge =
                    # grid limit - load + PV
                    # -------------------------------------------------------------

                    if event_grid_import_limit_kw is not None:

                        available_grid_capacity_kw = max(
                            0.0,
                            event_grid_import_limit_kw
                            - current_load_kw
                            + pv_power_kw
                        )

                    else:

                        # If event doesn't provide a grid limit,
                        # don't apply an additional grid constraint here.
                        available_grid_capacity_kw = (
                            requested_grid_charge_kw
                        )

                    # -------------------------------------------------------------
                    # Do NOT blindly use the old available_kw here.
                    #
                    # The parent event calculated:
                    #
                    #     grid_charge_kw
                    #
                    # We only clamp it against the live grid limit.
                    # -------------------------------------------------------------

                    source_available_kw = min(
                        requested_grid_charge_kw,
                        available_grid_capacity_kw
                    )

                    logger.info(
                        f"[PeakShavingCharging] "
                        f"GRID CHARGE | "
                        f"EventRequest={requested_grid_charge_kw:.2f} kW | "
                        f"GridCapacity={available_grid_capacity_kw:.2f} kW | "
                        f"Executable={source_available_kw:.2f} kW"
                    )

                elif event_decision in (
                    "charge",
                    "pv_charge",
                ):

                    # -------------------------------------------------------------
                    # PV CHARGE
                    # -------------------------------------------------------------

                    pv_surplus_power_kw = max(
                        0.0,
                        pv_power_kw - current_load_kw
                    )

                    source_available_kw = min(
                        event_charge_kw
                        if event_charge_kw > 0
                        else pv_surplus_power_kw,
                        pv_surplus_power_kw
                    )

                    logger.info(
                        f"[PeakShavingCharging] "
                        f"PV CHARGE | "
                        f"PVSurplus={pv_surplus_power_kw:.2f} kW | "
                        f"Executable={source_available_kw:.2f} kW"
                    )

                else:

                    logger.warning(
                        f"{LogColor.YELLOW}"
                        f"[PeakShavingCharging] "
                        f"Event decision changed/invalid during execution: "
                        f"{event_decision}"
                        f"{LogColor.RESET}"
                    )

                    break

                # =================================================================
                # 10.6 BATTERY CHARGE RATE LIMIT
                # =================================================================

                max_charge_kw = (
                    get_c_rate(
                        battery_temperature,
                        bess_params
                    )
                    * (
                        battery_capacity
                        * (
                            safe_max_soc - current_soc
                        )
                        / 100
                    )
                )

                # =================================================================
                # 10.7 AVAILABLE BATTERY CAPACITY
                # =================================================================

                available_capacity_kwh = (
                    battery_capacity
                    * (
                        safe_max_soc - current_soc
                    )
                    / 100
                    * efficiency
                )

                max_charge = min(
                    max_charge_kw,
                    available_capacity_kwh
                )

                # =================================================================
                # 10.10 FINAL SETPOINT
                # =================================================================

                new_setpoint = int(
                    max(
                        0.0,
                        min(
                            source_available_kw,
                            max_charge
                        )
                    )
                )

                logger.info(
                    f"[PeakShavingCharging] "
                    f"Requested={source_available_kw:.2f} kW | "
                    f"BatteryLimit={max_charge:.2f} kW | "
                    f"NewSetpoint={new_setpoint} kW"
                )

                # =================================================================
                # 10.11 ZERO SETPOINT
                # =================================================================

                if new_setpoint <= 0:

                    logger.info(
                        "[PeakShavingCharging] "
                        "Calculated setpoint is zero."
                    )

                    break

                # =================================================================
                # 10.12 UPDATE SETPOINT
                # =================================================================

                if new_setpoint != last_setpoint:

                    slot_number += 1

                    logger.info(
                        f"{LogColor.MAGENTA}"
                        f"[PeakShavingCharging] Updating setpoint "
                        f"{last_setpoint} -> {new_setpoint} kW | "
                        f"Slot={slot_number}"
                        f"{LogColor.RESET}"
                    )

                    await _execute_slot_async_charging(
                        resolver=resolver,
                        setpoint_kw=int(new_setpoint),
                        channel_id=channel,
                        server_ip=server_ip,
                        server_port=server_port,
                        slave_id=slave_id,
                        device_id=device_id,
                        slot_number=slot_number,
                        extra_info={
                            **extra_info,
                            "strategy_id": 7,
                            "event_decision": event_decision,
                            "event_grid_charge_kw": event_grid_charge_kw,
                            "event_target_soc": event_target_soc,
                            "event_target_energy_mwh": event_target_energy_mwh,
                        },
                    )

                    last_setpoint = new_setpoint

                # =================================================================
                # 10.13 STORE RESULT
                # =================================================================

                results.append({
                    "soc": current_soc,
                    "battery_energy_mwh": current_energy_mwh,
                    "charge_kw": last_setpoint,
                    "decision": event_decision,
                    "timestamp": datetime.now().isoformat(),
                })

        # =========================================================================
        # 11. ALWAYS STOP CHARGING
        # =========================================================================

        finally:

            logger.info(
                f"{LogColor.MAGENTA}"
                f"[PeakShavingCharging] STOPPING CHARGING."
                f"{LogColor.RESET}"
            )

            try:

                await _execute_slot_async_charging(
                    resolver=resolver,
                    setpoint_kw=0,
                    channel_id=channel,
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    device_id=device_id,
                    slot_number=200,
                    extra_info={
                        **extra_info,
                        "strategy_id": 7,
                        "event_decision": event_decision,
                        "final_soc": current_soc,
                        "final_battery_energy_mwh": current_energy_mwh,
                    },
                )

            except Exception as e:

                logger.error(
                    f"{LogColor.RED}"
                    f"[PeakShavingCharging] "
                    f"Failed to stop charging: {e}"
                    f"{LogColor.RESET}"
                )

        # =========================================================================
        # 12. FINAL RESULT
        # =========================================================================

        results.append({
            "status": "completed",
            "soc": current_soc,
            "battery_energy_mwh": current_energy_mwh,
            "charge_kw": 0,
            "decision": event_decision,
            "target_soc_percent": event_target_soc,
            "target_battery_energy_mwh": event_target_energy_mwh,
            "timestamp": datetime.now().isoformat(),
        })

        return results

    async def PeakShavingDischargeStrategyType7(
            self,
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
            event: dict = None,
        ):
            """
            Schedule Strategy Type 7 - Event Based Peak Shaving Discharge.

            The parent event is authoritative.

            Example event:

                {
                    "decision": "discharge",
                    "discharge_kw": 1500.0,
                    "grid_import_limit_kw": 4000.0,
                    "soc_percent": 21.64,
                    "battery_energy_mwh": 5.628,
                    ...
                }

            Behaviour:

                1. Event determines whether discharge is allowed.
                2. Event provides the requested discharge power.
                3. Live grid conditions are checked every polling cycle.
                4. Required discharge is recalculated from live grid power.
                5. Discharge is limited by:
                    - event discharge_kw
                    - live grid requirement
                    - max_discharge_kw
                    - max_usable_kw
                    - minimum SOC
                6. Event target SOC / energy stops the event.
                7. Always sends zero discharge in finally.
            """

            extra_info = extra_info or {}
            event = event or {}

            results = []

            current_soc = None
            current_energy_mwh = None
            last_setpoint = 0.0
            slot_number = 1

            try:

                logger.info(
                    f"{LogColor.MAGENTA}"
                    f"[PeakShavingDischarge] Starting event discharge."
                    f"{LogColor.RESET}"
                )

                # =========================================================================
                # MAXIMUM EXECUTION TIME: 15 MINUTES
                # =========================================================================
        
                MAX_EXECUTION_SECONDS = 15 * 60  # 900 seconds
        
                start_time = time.monotonic()
                deadline = start_time + MAX_EXECUTION_SECONDS

                # Set Event running time in ETALab
                self.set_event_running_time()
        
                logger.info(
                    "[PeakShavingCharging] Maximum execution time: 15 minutes."
                )

                # =====================================================================
                # 1. EVENT INFORMATION
                # =====================================================================

                event_decision = str(
                    event.get("decision", "")
                ).strip().lower()

                event_target_soc = event.get("soc_percent")

                if event_target_soc is not None:
                    event_target_soc = float(event_target_soc)

                event_target_energy_mwh = event.get(
                    "battery_energy_mwh"
                )

                if event_target_energy_mwh is not None:
                    event_target_energy_mwh = float(
                        event_target_energy_mwh
                    )

                event_discharge_kw = float(
                    event.get("discharge_kw", 0.0) or 0.0
                )

                event_grid_import_limit_kw = event.get(
                    "grid_import_limit_kw"
                )

                if event_grid_import_limit_kw is not None:
                    event_grid_import_limit_kw = float(
                        event_grid_import_limit_kw
                    )

                event_target_limit_kw = event.get(
                    "target_limit_kw"
                )

                if event_target_limit_kw is not None:
                    event_target_limit_kw = float(
                        event_target_limit_kw
                    )
                else:
                    event_target_limit_kw = float(
                        target_limit
                    )

                logger.info(
                    f"{LogColor.CYAN}"
                    f"[PeakShavingDischarge] Event | "
                    f"Decision={event_decision} | "
                    f"Discharge={event_discharge_kw:.2f} kW | "
                    f"GridLimit={event_grid_import_limit_kw} kW | "
                    f"TargetLimit={event_target_limit_kw:.2f} kW | "
                    f"TargetSOC={event_target_soc}% | "
                    f"TargetEnergy={event_target_energy_mwh} MWh"
                    f"{LogColor.RESET}"
                )

                # =====================================================================
                # 2. VALIDATE EVENT
                # =====================================================================

                if event_decision not in (
                    "discharge",
                    "peak_shave",
                    "peak_shaving",
                ):

                    logger.warning(
                        f"{LogColor.YELLOW}"
                        f"[PeakShavingDischarge] "
                        f"Invalid discharge event decision: "
                        f"{event_decision}"
                        f"{LogColor.RESET}"
                    )

                    return [{
                        "status": "no_action",
                        "reason": "invalid_discharge_decision",
                        "decision": event_decision,
                        "timestamp": datetime.now().isoformat(),
                    }]

                if event_discharge_kw <= 0:

                    logger.warning(
                        f"{LogColor.YELLOW}"
                        f"[PeakShavingDischarge] "
                        f"Event discharge power is zero."
                        f"{LogColor.RESET}"
                    )

                    return [{
                        "status": "no_action",
                        "reason": "zero_event_discharge",
                        "timestamp": datetime.now().isoformat(),
                    }]

                # =====================================================================
                # 3. STATIC PARAMETERS
                # =====================================================================

                battery_capacity = controller_static.get(
                    "battery_capacity"
                )

                if battery_capacity is None:
                    logger.error(
                        f"{LogColor.RED}"
                        f"[PeakShavingDischarge] "
                        f"Battery capacity unavailable."
                        f"{LogColor.RESET}"
                    )

                    return [{
                        "status": "error",
                        "reason": "battery_capacity_unavailable",
                        "timestamp": datetime.now().isoformat(),
                    }]

                battery_capacity = float(
                    battery_capacity
                )

                # =====================================================================
                # 4. INITIAL LIVE DATA
                # =====================================================================

                data = resolver.get_latest_device_data(
                    device_id
                )

                current_soc = data.get(
                    Tags.BATTERY_SOC,
                    {}
                ).get("value")

                if current_soc is None:

                    logger.error(
                        f"{LogColor.RED}"
                        f"[PeakShavingDischarge] "
                        f"Current SOC unavailable."
                        f"{LogColor.RESET}"
                    )

                    return [{
                        "status": "error",
                        "reason": "soc_unavailable",
                        "timestamp": datetime.now().isoformat(),
                    }]

                current_soc = float(
                    current_soc
                )

                # =====================================================================
                # 5. BATTERY ENERGY HELPER
                # =====================================================================

                def get_battery_energy_mwh(
                    live_data,
                    soc
                ):
                    """
                    Prefer live battery energy.

                    If battery energy tag is unavailable,
                    calculate it from battery capacity and SOC.
                    """

                    energy_value = live_data.get(
                        Tags.BATTERY_ENERGY,
                        {}
                    ).get("value")

                    if energy_value is not None:

                        try:
                            return float(
                                energy_value / 1000.0
                            )

                        except (
                            TypeError,
                            ValueError
                        ):
                            pass

                    return (
                        battery_capacity
                        * float(soc)
                        / 100.0
                        / 1000.0
                    )

                current_energy_mwh = get_battery_energy_mwh(
                    data,
                    current_soc
                )

                # =====================================================================
                # 6. INITIAL TARGET CHECK
                # =====================================================================

                # if (
                #     event_target_soc is not None
                #     and current_soc <= event_target_soc
                # ):

                #     logger.info(
                #         f"{LogColor.GREEN}"
                #         f"[PeakShavingDischarge] "
                #         f"Target SOC already reached."
                #         f" Current={current_soc:.2f}% "
                #         f"Target={event_target_soc:.2f}%"
                #         f"{LogColor.RESET}"
                #     )

                #     return [{
                #         "status": "completed",
                #         "reason": "event_target_soc_reached",
                #         "soc": current_soc,
                #         "battery_energy_mwh": current_energy_mwh,
                #         "discharge_kw": 0,
                #         "timestamp": datetime.now().isoformat(),
                #     }]

                # if (
                #     event_target_energy_mwh is not None
                #     and current_energy_mwh <= event_target_energy_mwh
                # ):

                #     logger.info(
                #         f"{LogColor.GREEN}"
                #         f"[PeakShavingDischarge] "
                #         f"Target battery energy already reached."
                #         f" Current={current_energy_mwh:.3f} MWh "
                #         f"Target={event_target_energy_mwh:.3f} MWh"
                #         f"{LogColor.RESET}"
                #     )

                #     return [{
                #         "status": "completed",
                #         "reason": "event_target_energy_reached",
                #         "soc": current_soc,
                #         "battery_energy_mwh": current_energy_mwh,
                #         "discharge_kw": 0,
                #         "timestamp": datetime.now().isoformat(),
                #     }]

                # =====================================================================
                # 7. LOAD INITIAL CONFIG
                # =====================================================================

                try:

                    config = await ScheduleStrategy.bess_decision(
                        device_id,
                        poi_meter_id
                    )

                    (
                        block_discharging,
                        pv_power_kw,
                        bess_power_kw,
                        live_soc,
                        current_load_kw,
                        min_soc,
                        max_discharge_kw,
                        max_usable_kw,
                        efficiency,
                    ) = (
                        config["block_discharging"],
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

                    logger.error(
                        f"{LogColor.RED}"
                        f"[PeakShavingDischarge] "
                        f"Error loading live parameters: {e}"
                        f"{LogColor.RESET}"
                    )

                    return [{
                        "status": "error",
                        "reason": "live_config_error",
                        "error": str(e),
                        "timestamp": datetime.now().isoformat(),
                    }]

                current_soc = float(
                    live_soc
                )

                min_soc = float(
                    min_soc
                )

                max_discharge_kw = float(
                    max_discharge_kw
                    or 0
                )

                max_usable_kw = float(
                    max_usable_kw
                    or 0
                )

                # =====================================================================
                # 8. DETERMINE INITIAL DISCHARGE
                # =====================================================================

                pv_power_kw = self.pv_kw
                # current_load_kw = self.load_kw

                bess_power_kw = float(
                    bess_power_kw or 0
                )

                # Grid import/load seen by POI.
                grid_power = (
                    current_load_kw
                    - pv_power_kw
                    - bess_power_kw
                )

                # How much additional BESS discharge is currently required.
                if event_grid_import_limit_kw is not None:

                    live_required_discharge_kw = max(
                        0.0,
                        grid_power
                        - event_grid_import_limit_kw
                    )

                else:

                    live_required_discharge_kw = max(
                        0.0,
                        grid_power
                        - event_target_limit_kw
                    )

                # Event is the upper authority.
                initial_setpoint_kw = min(
                    event_discharge_kw,
                    live_required_discharge_kw
                    if live_required_discharge_kw > 0
                    else event_discharge_kw,
                    max_discharge_kw,
                    max_usable_kw,
                )

                initial_setpoint_kw = max(
                    0.0,
                    initial_setpoint_kw
                )

                logger.info(
                    f"[PeakShavingDischarge] Initial calculation | "
                    f"Load={current_load_kw:.2f} kW | "
                    f"PV={pv_power_kw:.2f} kW | "
                    f"BESS={bess_power_kw:.2f} kW | "
                    f"Grid={grid_power:.2f} kW | "
                    f"LiveRequired={live_required_discharge_kw:.2f} kW | "
                    f"EventDischarge={event_discharge_kw:.2f} kW | "
                    f"Setpoint={initial_setpoint_kw:.2f} kW"
                )

                # =====================================================================
                # 9. INITIAL SAFETY CHECK
                # =====================================================================

                if block_discharging:

                    logger.warning(
                        f"{LogColor.RED}"
                        f"[PeakShavingDischarge] "
                        f"Discharging blocked by live configuration."
                        f"{LogColor.RESET}"
                    )

                    return [{
                        "status": "no_action",
                        "reason": "discharging_blocked",
                        "soc": current_soc,
                        "timestamp": datetime.now().isoformat(),
                    }]

                if current_soc <= min_soc:

                    logger.warning(
                        f"{LogColor.RED}"
                        f"[PeakShavingDischarge] "
                        f"Minimum SOC already reached."
                        f"{LogColor.RESET}"
                    )

                    return [{
                        "status": "completed",
                        "reason": "min_soc_reached",
                        "soc": current_soc,
                        "timestamp": datetime.now().isoformat(),
                    }]

                if initial_setpoint_kw <= 0:

                    logger.info(
                        "[PeakShavingDischarge] "
                        "No discharge currently required."
                    )

                    return [{
                        "status": "no_action",
                        "reason": "no_live_discharge_required",
                        "soc": current_soc,
                        "battery_energy_mwh": current_energy_mwh,
                        "discharge_kw": 0,
                        "timestamp": datetime.now().isoformat(),
                    }]

                # =====================================================================
                # 10. FIRST WRITE
                # =====================================================================

                last_setpoint = initial_setpoint_kw

                logger.info(
                    f"{LogColor.MAGENTA}"
                    f"[PeakShavingDischarge] "
                    f"WRITE FIRST SETPOINT = "
                    f"{initial_setpoint_kw:.2f} kW"
                    f"{LogColor.RESET}"
                )

                await _execute_slot_async_discharge(
                    resolver=resolver,
                    setpoint_kw=int(-initial_setpoint_kw),
                    channel_id=channel,
                    server_ip=server_ip,
                    server_port=server_port,
                    slave_id=slave_id,
                    device_id=device_id,
                    slot_number=1,
                    extra_info={
                        **extra_info,
                        "strategy_id": 7,
                        "event_decision": event_decision,
                        "event_discharge_kw": event_discharge_kw,
                        "event_target_soc": event_target_soc,
                        "event_target_energy_mwh": event_target_energy_mwh,
                    },
                )

                results.append({
                    "soc": current_soc,
                    "battery_energy_mwh": current_energy_mwh,
                    "discharge_kw": initial_setpoint_kw,
                    "decision": event_decision,
                    "timestamp": datetime.now().isoformat(),
                })

                # =====================================================================
                # 11. DISCHARGE LOOP
                # =====================================================================

                logger.info(
                    "[PeakShavingDischarge] Entering discharge loop."
                )

                await asyncio.sleep(5)

                while True:
                    # =================================================================
                    # 10.1 MAXIMUM EXECUTION TIME CHECK
                    # =================================================================

                    elapsed = time.monotonic() - start_time

                    if elapsed >= MAX_EXECUTION_SECONDS:

                        logger.warning(
                            f"{LogColor.YELLOW}"
                            f"[PeakShavingCharging] 15-minute execution limit reached. "
                            f"Elapsed={elapsed:.1f} seconds. Stopping charging loop."
                            f"{LogColor.RESET}"
                        )

                        break
                    await asyncio.sleep(1)

                    # =================================================================
                    # 11.1 LOAD LIVE CONFIG
                    # =================================================================

                    try:

                        config = await ScheduleStrategy.bess_decision(
                            device_id,
                            poi_meter_id
                        )

                        (
                            block_discharging,
                            pv_power_kw,
                            bess_power_kw,
                            live_soc,
                            current_load_kw,
                            min_soc,
                            max_discharge_kw,
                            max_usable_kw,
                            efficiency,
                            fault_code,
                        ) = (
                            config["block_discharging"],
                            config["pv_power_kw"],
                            config["bess_power_kw"],
                            config["soc"],
                            config["current_load_kw"],
                            config["min_soc"],
                            config["max_discharge_kw"],
                            config["max_usable_kw"],
                            config["efficiency"],
                            config["fault_code"]
                        )

                    except Exception as e:

                        logger.error(
                            f"{LogColor.RED}"
                            f"[PeakShavingDischarge] "
                            f"Error loading live parameters: {e}"
                            f"{LogColor.RESET}"
                        )

                        # break
                        await asyncio.sleep(1)  # Wait before retrying
                        continue

                    # =================================================================
                    # 11.2 LIVE VALUES
                    # =================================================================

                    if fault_code > 0:
                        self.fault = fault_code
                        # break

                    current_soc = float(
                        live_soc
                    )

                    min_soc = float(
                        min_soc
                    )

                    max_discharge_kw = float(
                        max_discharge_kw or 0
                    )

                    max_usable_kw = float(
                        max_usable_kw or 0
                    )

                    pv_power_kw = self.pv_kw
                    current_load_kw = self.load_kw

                    bess_power_kw = float(
                        bess_power_kw or 0
                    )

                    live_data = resolver.get_latest_device_data(
                        device_id
                    )

                    current_energy_mwh = get_battery_energy_mwh(
                        live_data,
                        current_soc
                    )

                    # =================================================================
                    # 11.3 LIVE GRID POWER
                    # =================================================================

                    grid_power = (
                        current_load_kw
                        - pv_power_kw
                        - bess_power_kw
                    )

                    # =================================================================
                    # 11.4 LOG
                    # =================================================================

                    logger.info(
                        f"[PeakShavingDischarge] "
                        f"SOC={current_soc:.2f}% | "
                        f"Energy={current_energy_mwh:.3f} MWh | "
                        f"Load={current_load_kw:.2f} kW | "
                        f"PV={pv_power_kw:.2f} kW | "
                        f"BESS={bess_power_kw:.2f} kW | "
                        f"Grid={grid_power:.2f} kW | "
                        f"Setpoint={last_setpoint:.2f} kW"
                    )

                    # =================================================================
                    # 11.5 TARGET SOC
                    # =================================================================

                    # if (
                    #     event_target_soc is not None
                    #     and current_soc <= event_target_soc
                    # ):

                    #     logger.info(
                    #         f"{LogColor.GREEN}"
                    #         f"[PeakShavingDischarge] "
                    #         f"Event target SOC reached. "
                    #         f"Current={current_soc:.2f}% | "
                    #         f"Target={event_target_soc:.2f}%"
                    #         f"{LogColor.RESET}"
                    #     )

                    #     break

                    # =================================================================
                    # 11.6 TARGET ENERGY
                    # =================================================================

                    if (
                        event_target_energy_mwh is not None
                        and current_energy_mwh <= event_target_energy_mwh
                    ):

                        logger.info(
                            f"{LogColor.GREEN}"
                            f"[PeakShavingDischarge] "
                            f"Event target energy reached. "
                            f"Current={current_energy_mwh:.3f} MWh | "
                            f"Target={event_target_energy_mwh:.3f} MWh"
                            f"{LogColor.RESET}"
                        )

                        break

                    # =================================================================
                    # 11.7 MINIMUM SOC
                    # =================================================================

                    if current_soc <= min_soc:

                        logger.warning(
                            f"{LogColor.RED}"
                            f"[PeakShavingDischarge] "
                            f"Minimum SOC reached. "
                            f"SOC={current_soc:.2f}% | "
                            f"MinSOC={min_soc:.2f}%"
                            f"{LogColor.RESET}"
                        )

                        break

                    # =================================================================
                    # 11.8 BLOCK DISCHARGING
                    # =================================================================

                    if block_discharging:

                        logger.warning(
                            f"{LogColor.RED}"
                            f"[PeakShavingDischarge] "
                            f"Discharging blocked by live configuration."
                            f"{LogColor.RESET}"
                        )

                        break

                    # =================================================================
                    # 11.9 LIVE REQUIRED DISCHARGE
                    # =================================================================
                    #
                    # Grid power is the import at POI.
                    #
                    # If:
                    #
                    #     Grid = 4500 kW
                    #     Limit = 4000 kW
                    #
                    # required discharge = 500 kW
                    #
                    # If Grid is already <= limit:
                    #
                    # required discharge = 0
                    #
                    # =================================================================

                    if event_grid_import_limit_kw is not None:

                        remaining_excess_kw = max(
                            0.0,
                            grid_power
                            - event_grid_import_limit_kw
                        )

                        grid_target_kw = (
                            event_grid_import_limit_kw
                        )

                    else:

                        remaining_excess_kw = max(
                            0.0,
                            grid_power
                            - event_target_limit_kw
                        )

                        grid_target_kw = (
                            event_target_limit_kw
                        )

                    logger.info(
                        f"[PeakShavingDischarge] "
                        f"Grid={grid_power:.2f} kW | "
                        f"Target={grid_target_kw:.2f} kW | "
                        f"RemainingExcess={remaining_excess_kw:.2f} kW"
                    )

                    # =================================================================
                    # 11.10 TARGET ALREADY ACHIEVED
                    # =================================================================

                    # if remaining_excess_kw <= 0:

                    #     logger.info(
                    #         f"{LogColor.GREEN}"
                    #         f"[PeakShavingDischarge] "
                    #         f"Grid target achieved. "
                    #         f"Grid={grid_power:.2f} kW <= "
                    #         f"Target={grid_target_kw:.2f} kW"
                    #         f"{LogColor.RESET}"
                    #     )

                    #     break

                    # =================================================================
                    # 11.11 DETERMINE NEXT DISCHARGE
                    # =================================================================
                    #
                    # IMPORTANT:
                    #
                    # Event is the upper authority.
                    #
                    # We cannot exceed event_discharge_kw.
                    #
                    # But if live grid requires less discharge, reduce it.
                    #
                    # =================================================================

                    requested_discharge_kw = max(      # max
                        # event_discharge_kw,
                        current_load_kw - pv_power_kw,
                        remaining_excess_kw,
                    )

                    # =================================================================
                    # 11.12 APPLY BATTERY LIMITS
                    # =================================================================

                    next_setpoint_kw = min(
                        requested_discharge_kw,
                        max_discharge_kw,
                        max_usable_kw,
                    )

                    next_setpoint_kw = max(
                        0.0,
                        next_setpoint_kw
                    )

                    logger.info(
                        f"[PeakShavingDischarge] "
                        f"EventLimit={event_discharge_kw:.2f} kW | "
                        f"GridRequired={remaining_excess_kw:.2f} kW | "
                        f"MaxDischarge={max_discharge_kw:.2f} kW | "
                        f"MaxUsable={max_usable_kw:.2f} kW | "
                        f"NextSetpoint={next_setpoint_kw:.2f} kW"
                    )

                    # =================================================================
                    # 11.13 NOTHING AVAILABLE
                    # =================================================================

                    if next_setpoint_kw <= 0:

                        logger.warning(
                            f"{LogColor.YELLOW}"
                            f"[PeakShavingDischarge] "
                            f"No usable discharge power available."
                            f"{LogColor.RESET}"
                        )

                        break

                    # =================================================================
                    # 11.14 UPDATE SETPOINT
                    # =================================================================

                    if int(next_setpoint_kw) != int(last_setpoint):

                        slot_number += 1

                        logger.info(
                            f"{LogColor.MAGENTA}"
                            f"[PeakShavingDischarge] "
                            f"Updating discharge setpoint "
                            f"{last_setpoint:.2f} -> "
                            f"{next_setpoint_kw:.2f} kW | "
                            f"Slot={slot_number}"
                            f"{LogColor.RESET}"
                        )

                        await _execute_slot_async_discharge(
                            resolver=resolver,
                            setpoint_kw=int(-next_setpoint_kw),
                            channel_id=channel,
                            server_ip=server_ip,
                            server_port=server_port,
                            slave_id=slave_id,
                            device_id=device_id,
                            slot_number=slot_number,
                            extra_info={
                                **extra_info,
                                "strategy_id": 7,
                                "event_decision": event_decision,
                                "event_discharge_kw": event_discharge_kw,
                                "event_target_soc": event_target_soc,
                                "event_target_energy_mwh": event_target_energy_mwh,
                            },
                        )

                        last_setpoint = next_setpoint_kw

                        results.append({
                            "soc": current_soc,
                            "battery_energy_mwh": current_energy_mwh,
                            "discharge_kw": next_setpoint_kw,
                            "grid_power_kw": grid_power,
                            "remaining_excess_kw": remaining_excess_kw,
                            "decision": event_decision,
                            "timestamp": datetime.now().isoformat(),
                        })

                # =====================================================================
                # 12. NORMAL EXIT
                # =====================================================================

                logger.info(
                    "[PeakShavingDischarge] "
                    "Discharge loop completed."
                )

            except Exception as e:

                logger.error(
                    f"{LogColor.RED}"
                    f"[PeakShavingDischarge] "
                    f"Error while executing discharge strategy: {e}"
                    f"{LogColor.RESET}"
                )

                results.append({
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                })

            finally:

                # =====================================================================
                # 13. ALWAYS STOP DISCHARGE
                # =====================================================================

                logger.info(
                    f"{LogColor.MAGENTA}"
                    f"[PeakShavingDischarge] "
                    f"STOPPING DISCHARGE."
                    f"{LogColor.RESET}"
                )

                try:

                    await _execute_slot_async_discharge(
                        resolver=resolver,
                        setpoint_kw=0,
                        channel_id=channel,
                        server_ip=server_ip,
                        server_port=server_port,
                        slave_id=slave_id,
                        device_id=device_id,
                        slot_number=200,
                        extra_info={
                            **extra_info,
                            "strategy_id": 7,
                            "event_decision": event_decision,
                            "final_soc": current_soc,
                            "final_battery_energy_mwh": current_energy_mwh,
                        },
                    )

                except Exception as e:

                    logger.error(
                        f"{LogColor.RED}"
                        f"[PeakShavingDischarge] "
                        f"Failed to stop discharge: {e}"
                        f"{LogColor.RESET}"
                    )

            # =====================================================================
            # 14. FINAL RESULT
            # =====================================================================

            results.append({
                "status": "completed",
                "soc": current_soc,
                "battery_energy_mwh": current_energy_mwh,
                "discharge_kw": 0,
                "decision": event_decision,
                "target_soc_percent": event_target_soc,
                "target_battery_energy_mwh": event_target_energy_mwh,
                "timestamp": datetime.now().isoformat(),
            })

            return results

    async def PP_PeakShavingDischargeStrategyType7(
        self,
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

            logger.info(
                f"{LogColor.MAGENTA}WRITE FIRST SETPOINT = {setpoint_kw:.2f} kW{LogColor.RESET}"
            )

            await _execute_slot_async_discharge(
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

                poi_meter_id = "plant_1_poi_meter_1"
                try:
                    config = await ScheduleStrategy.bess_decision(device_id, poi_meter_id)
                    (
                        block_discharging,
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

                        await _execute_slot_async_discharge(
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

            await _execute_slot_async_discharge(
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

    async def event_generator(
        self,
        sample_window,
        channel_id,
        server_ip,
        server_port,
        slave_id,
        device_id,
        interval_seconds=900,
    ):
        """
        Generate events from sample_window and write each event's
        load_kw value to the Load register.

        sample_window:
            List of 96 events, one event every 15 minutes.

        interval_seconds:
            Real execution interval.

            900 seconds = 15 minutes.

            For testing, you can use:
                1   -> one event every second
                5   -> one event every 5 seconds
                0   -> execute all events immediately
        """

        logger.info(
            f"[EVENT GENERATOR] Starting simulation "
            f"with {len(sample_window)} events"
        )

        for index, event in enumerate(sample_window):

            self.time_stamp = event["time_stamp"]
            self.load_kw = event["load_kw"]
            self.pv_kw = event["pv_kw"]
            self.load_state = event["load_state"]
            self.tariff_per_kw = event["tariff_per_kw"]

            logger.info(
                f"[EVENT {index + 1}/{len(sample_window)}] "
                f"Timestamp={self.time_stamp} | "
                f"Load={self.load_kw} kW"
            )

            # -------------------------------------------------
            # WRITE LOAD TO DEVICE
            # -------------------------------------------------

            await _execute_load_value_writer(
                resolver=self.resolver,
                value=self.load_kw,
                channel_id=channel_id,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
            )

            # -------------------------------------------------
            # WAIT FOR NEXT EVENT
            # -------------------------------------------------

            if index < len(sample_window) - 1:

                if interval_seconds > 0:
                    await asyncio.sleep(interval_seconds)

        logger.info(
            "[EVENT GENERATOR] Completed all sample events."
        )

    async def event_generator(
        self,
        sample_window,
        channel_id,
        server_ip,
        server_port,
        slave_id,
        device_id,
        interval_seconds=1,
    ):
        """
        Full Schedule Strategy event simulator.
        """

        results = []

        for index, event in enumerate(sample_window):

            logger.info(
                f"\n"
                f"{'=' * 80}\n"
                f"[SIMULATION EVENT {index + 1}/{len(sample_window)}]\n"
                f"Timestamp : {event['time_stamp']}\n"
                f"Load      : {event['load_kw']} kW\n"
                f"PV        : {event['pv_kw']} kW\n"
                f"State     : {event['load_state']}\n"
                f"Tariff    : {event['tariff_per_kw']}\n"
                f"{'=' * 80}"
            )

            # -------------------------------------------------
            # 1. WRITE LOAD
            # -------------------------------------------------

            await _execute_load_value_writer(
                resolver=self.resolver,
                value=event["load_kw"],
                channel_id=channel_id,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
            )

            # -------------------------------------------------
            # 2. EXECUTE YOUR STRATEGY
            # -------------------------------------------------

            result = await self.execute(
                channel=channel_id,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                device_id=device_id,
                signal=None,
            )

            # -------------------------------------------------
            # 3. STORE EVENT RESULT
            # -------------------------------------------------

            results.append({
                "event_number": index + 1,
                "time_stamp": event["time_stamp"],
                "load_kw": event["load_kw"],
                "pv_kw": event["pv_kw"],
                "load_state": event["load_state"],
                "tariff_per_kw": event["tariff_per_kw"],
                "strategy_result": result,
            })

            # -------------------------------------------------
            # 4. WAIT
            # -------------------------------------------------

            if index < len(sample_window) - 1:
                await asyncio.sleep(interval_seconds)

        logger.info(
            f"[EVENT GENERATOR] "
            f"Completed {len(sample_window)} events."
        )

        return results


async def _execute_slot_async_charging(
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
        # sp_rec = SetpointWrite(strategy_id=extra_info.get("strategy_id", 0), timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), register_mode=1, setpoint_value=setpoint_kw, unit="kW", device_id=device_id, success=True, response_text="Setpoint for Charge has been written")
        # pool = await get_async_connection()
        # async with pool.acquire() as conn:
        #     await write_setpoint_record(conn, sp_rec)
        # await pool.close()

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

            # sp_rec = SetpointWrite(strategy_id=extra_info.get("strategy_id", 0), timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), register_mode=0, setpoint_value=setpoint_kw, unit="kW", device_id=device_id, success=True, response_text="Power mode is closed!")
            # pool = await get_async_connection()
            # async with pool.acquire() as conn:
            #     await write_setpoint_record(conn, sp_rec)
            # await pool.close()

    except Exception as e:
        logger.error(f"{color}[Slot {slot_number}] Failed to write setpoint: {e}{LogColor.RESET}")


async def _execute_slot_async_discharge(
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
        # sp_rec = SetpointWrite(strategy_id=extra_info.get("strategy_id", 0), timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), register_mode=1, setpoint_value=setpoint_kw, unit="kW", device_id=device_id, success=True, response_text="Setpoint for discharge has been written")
        # pool = await get_async_connection()
        # async with pool.acquire() as conn:
        #     await write_setpoint_record(conn, sp_rec)
        # await pool.close()

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

            # sp_rec = SetpointWrite(strategy_id=extra_info.get("strategy_id", 0), timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), register_mode=0, setpoint_value=setpoint_kw, unit="kW", device_id=device_id, success=True, response_text="Power mode is closed!")
            # pool = await get_async_connection()
            # async with pool.acquire() as conn:
            #     await write_setpoint_record(conn, sp_rec)
            # await pool.close()

    except Exception as e:
        logger.error(f"{color}[Slot {slot_number}] Failed to write setpoint: {e}{LogColor.RESET}")


async def _execute_load_value_writer(
    resolver,
    value: float,
    channel_id: str,
    server_ip: str,
    server_port: int,
    slave_id: int,
    device_id: str,
):

    data = resolver.get_latest_device_data(device_id)

    # Resolve registers

    register = data[Tags.Load]["register"]
    data_type = data[Tags.Load]["dataType"]


    # logger.info(f"{color}[Slot {slot_number}] Writing PowerMode=1 to reg {power_mode_register}{LogColor.RESET}")
    try:
        await ModbusWriter.write_to_register(
            channel_id=channel_id,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=register,
            data_type=data_type,
            value=value
        )
    except Exception as e:
        logger.info(f"Error While Writing Value.. {e}")

async def _execute_pv_value_writer(
    resolver,
    value: float,
    channel_id: str,
    server_ip: str,
    server_port: int,
    slave_id: int,
    device_id: str,
):

    data = resolver.get_latest_device_data(device_id)

    # Resolve registers

    register = data[Tags.POA]["register"]
    data_type = data[Tags.POA]["dataType"]


    # logger.info(f"{color}[Slot {slot_number}] Writing PowerMode=1 to reg {power_mode_register}{LogColor.RESET}")
    try:
        await ModbusWriter.write_to_register(
            channel_id=channel_id,
            server_ip=server_ip,
            server_port=server_port,
            slave_id=slave_id,
            register=register,
            data_type=data_type,
            value=value
        )
    except Exception as e:
        logger.info(f"Error While Writing Value.. {e}")