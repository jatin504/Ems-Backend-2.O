import json
import logging
from pathlib import Path

from config.device_config_resolver import DeviceConfigResolver
from config.plant_config_resolver import PlantConfigResolver
from controller_layer.authority.authority_manager import AuthorityManager
from controller_layer.strategies.classes.schedule_strategy import ScheduleStrategy

from .strategies.bess_strategies import BessBasicStrategy, ToUSmartStrategy, ToUFlatStrategy, PeakShavingStrategy, PeakShavingStrategyType7
from .strategies.trading_strategy import TradingStrategy
from .manual_override.manual_override_controller import  ManualOverrideManager
from .writers.ems_control_state_manager import EMSControlStateManager

from enum import Enum
import asyncio
from LogColor import LogColor

from runtime_paths import DataPaths

logger = logging.getLogger(__name__)

class ControlAuthority(Enum):
    INTERNAL = 1
    EXTERNAL  = 2
    MANUAL    = 3

class ControllerManager:
    """
    Manages the execution of control strategies based on device type and current strategy.
    """
    def __init__(self, resolver=None):
        self.strategy_file = DataPaths.resolve("controller_layer/strategy_options.json")
        self.selected_strategy_file = DataPaths.resolve("controller_layer/current_strategy.json")
        self.manual_override_file = DataPaths.resolve("controller_layer/manual_override/manual_override_config.json")
        self.resolver = resolver  # Resolver instance for fetching device data
        self.current_strategy = self._load_current_strategy()
        self.plant_resolver = PlantConfigResolver()
        self.strategies = self._initialize_strategies()
        self.device_resolver = DeviceConfigResolver()

    def _initialize_strategies(self):
        """Initialize available strategies."""
        plant_type = self.plant_resolver.load_config()["plant_type"]

        # Choose peak shaving strategy
        if plant_type == 7:
            peak_shaving_strategy = PeakShavingStrategyType7(self.resolver)
        else:
            peak_shaving_strategy = PeakShavingStrategy(self.resolver)
            
        return {
            "basic": BessBasicStrategy(self.resolver),
            "SmartTou" : ToUSmartStrategy(self.resolver),
            "FlatTou" : ToUFlatStrategy(self.resolver),
            "PeakShaving" : peak_shaving_strategy,
            "Trading" : TradingStrategy(self.resolver),
            "Schedule": ScheduleStrategy(self.resolver),
        }

    def _load_current_strategy(self):
        """Load the currently selected strategy from the JSON file."""
        try:
            with open(self.strategy_file, 'r') as f:
                data = json.load(f)
            bess_strategy_options = data.get("bess_strategy_options", {})

            with open(self.selected_strategy_file, 'r') as c:
                selected = json.load(c)
                current_strategy_key = selected.get("current_bess_strategy", "0")
                strategy_name = bess_strategy_options.get(current_strategy_key, "basic")
                logger.info(f"Loaded current BESS strategy: {strategy_name}")
                return strategy_name
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in strategy file: {e}")
            raise

    def load_manual_override(self):
        """Load manual override config from JSON file."""
        try:
            with open(self.manual_override_file, 'r') as f:
                data = json.load(f)
            return data
        except:
            return {"enabled": False}
        
    async def decide_authority(self):
        """
            Decides authpority between Manual and Internal Control.
            Manual has higher priority.
            If Manual is enabled, then raise manual flag and return MANUAL.
            Else, return INTERNAL.
        """
        manual = self.load_manual_override()
        # logger.info(f"{LogColor.YELLOW} [Strategy Manager] Manual Override Config: {manual} {LogColor.RESET}")
        if manual.get("enabled", False):
            await EMSControlStateManager.raise_flag("manual_override_enabled", 1)
            return ControlAuthority.MANUAL

        await EMSControlStateManager.raise_flag("manual_override_enabled", 0)
        await EMSControlStateManager.raise_flag("internal_controlling_executing", 1)
        return ControlAuthority.INTERNAL



    async def process_device_data(self, channel, server_ip, server_port, slave_id, device_id, device_type, meter_device_id, signal=None):
        """
        Process data for a specific device and apply the appropriate strategy.
        :param device_id: The ID of the device (e.g., "block_2_bess_bess_1").
        :param meter_device_id: The ID of the meter device (e.g., "meter_meter_1").
        :return: Updated device data after applying the strategy.
        """
        try:
            # Checking Authority
            proceed = AuthorityManager.can_proceed("internal")
            if not proceed:
                logger.info(f"{LogColor.RED}[AUTHORITY] Priority blocked, aborting continuous loop.{LogColor.RESET}")
                
            # Reload strategy every time function runs
            self.current_strategy = self._load_current_strategy()
            self.strategies = self._initialize_strategies()

            # Handle BESS devices
            if device_id == "plant_1_pcs_controller_1":
                strategy = self.strategies.get(self.current_strategy)
                if strategy:
                    logger.info(f"Applying BESS strategy {self.current_strategy} to device {device_id}")
                    return await strategy.execute(
                        channel=channel,
                        server_ip=server_ip,
                        server_port=server_port,
                        slave_id=slave_id,
                        device_id=device_id,
                        signal = signal,
                        # meter_device_id=meter_device_id
                    )
                else:
                    # logger.warning(f"No strategy found for current_bess_strategy={self.current_strategy}")
                    return {"error": "No strategy found", "current_strategy": self.current_strategy}
            else:
                # logger.info(f"No strategy defined for device type: {device_type}")
                return {"error": "Unsupported device type", "device_type": device_type}

        except Exception as e:
            # logger.error(f"Error processing device data for {device_id}: {e}")
            return {"error": str(e), "device_id": device_id}


    async def start(self, device_id : str):
        """
        Always running control loop.
        """
        device_info = self.device_resolver.get_device_info_by_id(device_id)
        if not device_info:
            logger.error(f"[Strategy Manager] Device info not found for {device_id}")
            return
        
        # Extract Device Infos
        channel = device_info["channel"]
        server_ip = device_info["server_ip"]
        server_port = device_info["server_port"]
        slave_id = device_info["slave_id"]
        
        while True:
            try :

                authority = await self.decide_authority()

                if authority == ControlAuthority.MANUAL:
                    result = await ManualOverrideManager.run_manual_override(
                        channel, server_ip, server_port, slave_id, device_id
                    )

                else:
                    result = await self.process_device_data(
                        channel, server_ip, server_port, slave_id,
                        device_id=device_id,
                        device_type=None,
                        meter_device_id=None
                    )
            except Exception as e:
                logger.error(f"{LogColor.RED} [Controller Manager] Error in control loop for {device_id}: {e} {LogColor.RESET}")

            # short delay
            await asyncio.sleep(1)
