from controller_layer.strategies.bess_strategies import BessBasicStrategy
from statics.controller_resolver import ControllerStaticResolver
from metadata.metadata_resolver import MetadataResolver
from latest_telemetry_resolver import LatestTelemetryResolver

from LogColor import LogColor

from datetime import datetime
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)

from ..load_bess_params_setting import load_bess_params_setting
from ..cycle_management import load_cycle_config, get_today_cycles

from .forecastings import plant_forecast, tariff_forecast
from .bid_engine import RiskReserveBidEngine, AggressiveTradingBidEngine
from .trading_trigger_engine import TradingTriggerEngine

from .context_builder import TradingContextBuilder
from .trading_orchestration import TradingOrchestrator

class TradingStrategy:
    """
    BESS Trading Strategy.
    """
    def __init__(self, resolver):
        # self.resolver = resolver  # Used to resolve device and meter data
        self.resolver = LatestTelemetryResolver()
        self.controller_static = ControllerStaticResolver()
        self.metadata_resolver = MetadataResolver()

        # self.trigger_engine = TradingTriggerEngine()

        # Keeps memory of last trigger execution
        # key -> trigger_id, value -> timestamp
        self.last_trigger_state = {}

        # cooldown to avoid multiple bids in same window
        self.trigger_cooldown_sec = 60

        # DB Init
        

    async def execute(self, channel, server_ip, server_port, slave_id, device_id, signal):
        """
        Execute the BESS Trading strategy.
        """

        logger.info(f"{LogColor.YELLOW}Executing BESS Trading Strategy...{LogColor.RESET}")

        strategy_name = "TRADING"
        strategy_id = 5

        try:
            decision_config = await TradingStrategy.bess_decision(device_id=device_id)
            context = TradingContextBuilder.build(device_id, user_config=decision_config) #There will be trading config
            await TradingOrchestrator.process(context)
        except Exception as e:
            logger.info(f"{LogColor.RED} ERROR WHILE RUNNING TRADING PROCESS: {e} {LogColor.RESET}")

        try:

            meter_device_id = "plant_1_poi_meter_1"
            (
                prevent_charging,
                aggressively_charge,
                block_discharging,
                backup_soc,
                min_soc,
                max_soc,
                normal_recovery_soc,
                cycles_used_today,
                max_cycles_per_day,
                battery_capacity,
                max_discharge_kw,
                arbitrage_cfg,
                soc_cfg,
                bess_power,
                current_tariff,
                soc,
                disable_aggrassive_charging,
                disable_arbitrage,
                disable_low_price_charging
            ) = (
                decision_config["prevent_charging"],
                decision_config["aggressively_charge"],
                decision_config["block_discharging"],
                decision_config["backup_soc"],
                decision_config["min_soc"],
                decision_config["max_soc"],
                decision_config["normal_recovery_soc"],
                decision_config["cycles_used_today"],
                decision_config["max_cycles_per_day"],
                decision_config["battery_capacity"],
                decision_config["max_discharge_kw"],
                decision_config["arbitrage_cfg"],
                decision_config["soc_cfg"],
                decision_config["bess_power"],
                decision_config["current_tariff"],
                decision_config["soc"],
                decision_config["disable_aggrassive_charging"],
                decision_config["disable_arbitrage"],
                decision_config["disable_low_price_charging"]
            )

        except Exception as e:
            logger.info(f"{LogColor.RED} Error While Getting Constraints: {e} {LogColor.RESET}")

        # --------------------------------------
        # Forecastings
        # # --------------------------------------
        # try:
        #     tariffs = tariff_forecast
        #     predicted_plant_data = plant_forecast

        #     logger.info(f"{LogColor.YELLOW} Plant Forcast: {predicted_plant_data} Tariffs: {tariffs} {LogColor.RESET}")
        # except Exception as e:
        #     logger.info(f"{LogColor.RED} Error While Forecasting: {e} {LogColor.RESET}")

        # logger.info(f"{LogColor.YELLOW}Finished BESS Trading Strategy...{LogColor.RESET}")

        # -------------------------------------
        # Trading configs
        # -------------------------------------
        # try:

        #     cfg = TradingStrategy.load_trading_config()

        #     (mode, market_strategy) = (cfg["mode"], cfg["market_strategy"])

        #     logger.info(f"{LogColor.YELLOW}Selected mode: {mode} market_strategy: {market_strategy} {LogColor.YELLOW}")
        # except Exception as e:
        #     logger.info(f"Error While reading Trading Configs {e}")


        # -------------------------------------
        # Evaluate triggers
        # -------------------------------------
        # trigger_event = self.trigger_engine.evaluate(['RTM'])

        # -------------------------------------
        # No trigger → exit fast
        # -------------------------------------
        # if not trigger_event:
        #     return
        
        # -------------------------------------
        # Deduplication check
        # -------------------------------------
        # if self._already_triggered(trigger_event):
        #     logger.debug(
        #         f"Trigger {trigger_event['id']} already handled recently"
        #     )
        #     return


        # -------------------------------------
        # Preparing Bids
        # -------------------------------------
        # if mode == 'reserve_risk_mode':
        #     result = RiskReserveBidEngine()
        # else:
        #     result = AggressiveTradingBidEngine.execute()

        # -------------------------------------
        # Market Strategy Configs
        # -------------------------------------
        
        
    def _build_market_context(self, device_id):
        device_data = self.resolver.get_latest_device_data(device_id)

        return {
            "now": datetime.utcnow(),
            "soc": device_data["SoC"]["value"],
            "market_price": device_data.get("rtm_price"),
            "volume_imbalance": device_data.get("volume_imbalance"),
            "battery_capacity": self.controller_static.get("battery_capacity"),
            "price_threshold_high": 7.5,
            "price_threshold_low": 3.0,
            "imbalance_threshold": 5.0
        }
    
    def _already_triggered(self, trigger_event):
        trigger_id = trigger_event["id"]
        now = datetime.utcnow()

        last_time = self.last_trigger_state.get(trigger_id)

        if not last_time:
            return False

        return (now - last_time).total_seconds() < self.trigger_cooldown_sec

    async def _submit_bid(self, bid):
        """
        Replace with:
        - Exchange API
        - Kafka publish
        - DB insert
        """
        logger.info(f"Submitting bid → {bid}")

    
    @staticmethod
    def load_trading_config(CONFIG_JSON = "controller_layer/strategies/trading_strategy/config/trading_config.json"):
        """
        Loads trading configuration and resolves mode and trading type.

        Returns:
            {
                "mode": "<resolved_mode_name>",
                "market_strategy": "<resolved_market_strategy_name>"
            }
        """

        path = Path(CONFIG_JSON)

        if not path.exists():
            raise FileNotFoundError(f"{CONFIG_JSON} not found")

        with open(path, "r") as f:
            config = json.load(f)

        # --------------------------------------
        # mode
        # --------------------------------------
        mode_id = str(config.get("mode"))
        mode_options = config.get("mode_options", {})

        if mode_id not in mode_options:
            raise ValueError(f"Invalid mode '{mode_id}'. Allowed: {list(mode_options.keys())}")

        mode = mode_options[mode_id]

        # --------------------------------------
        # market_strategy
        # --------------------------------------
        market_strategy_id = str(config.get("market_strategy"))
        market_strategy_options = config.get("market_strategy_options", {})

        if market_strategy_id not in market_strategy_options:
            raise ValueError(
                f"Invalid market_strategy '{market_strategy_id}'. Allowed: {list(market_strategy_options.keys())}"
            )

        market_strategy = market_strategy_options[market_strategy_id]

        return {
            "mode": mode,
            "market_strategy": market_strategy
        }

    
    @staticmethod
    async def bess_decision(
        device_id: str
    ):
        """
        CENTRALIZED SAFETY + TARIF + CYCLE DECISION LOGIC FOR Trading Strategy

        :param device_id: The ID of the BESS device.
        :return:
        Returns a dict of decisions that any strategy can use.
        """
        
        resolver = LatestTelemetryResolver() 
        controller_static = ControllerStaticResolver() 
        # metadata_resolver = MetadataResolver() 
        # ess_control_state_manager = EMSControlStateManager()
        # smart_tou = ToUSmartStrategy.load_smart_tou_settings()

        # ----------------------------
        # LOAD CONFIGS
        # ----------------------------

        bess_cfg = load_bess_params_setting()
        cycle_cfg = load_cycle_config()

        min_soc = bess_cfg["min_soc"]
        max_soc = bess_cfg["max_soc"]
        backup_soc = bess_cfg["backup_soc"]
        battery_capacity = controller_static.get('battery_capacity')
        max_discharge_kw = bess_cfg.get("max_discharge_kw")

        arbitrage_cfg = cycle_cfg["price_logic_overrides"]
        soc_cfg = cycle_cfg["soc_safety_management"]
        lockout_cfg = cycle_cfg["lockout_behaviour"]
        price_config = cycle_cfg["price_logic_overrides"]
        notify_cfg = cycle_cfg["notifications"]

        max_cycles_per_day = cycle_cfg["max_cycles_per_day"]
        cycles_used_today = await get_today_cycles()

        # Derived values
        min_safe_soc = min_soc
        normal_recovery_soc = backup_soc

        disable_aggrassive_charging = price_config["disable_aggrassive_charging"]
        disable_arbitrage = price_config["disable_arbitrage"]
        disable_low_price_charging = price_config["disable_low_price_charging"]
        disable_peak_shaving = price_config["disable_peak_shaving"]

        # ---------------------------------------------------
        # Read latest device data
        # ---------------------------------------------------
        try:
            device_data = resolver.get_latest_device_data(device_id)

            soc = device_data["SoC"]["value"]
            bess_power = float(device_data["ac_active_power"]["value"])
            current_tariff = 3# float(self.metadata_resolver.get_latest_tariff())

            logger.info(f"{LogColor.BLUE} [BASIC Strategy] SoC: {soc}, Current Tariff: {current_tariff} {LogColor.RESET}")
        except Exception as e:
            logger.info(f"{LogColor.RED} Error loading live params: {e} {LogColor.RESET}")

        # ----------------------------
        # OUTPUT FLAGS
        # ----------------------------
        prevent_charging = False
        aggressively_charge = False
        block_discharging = False

        # ----------------------------
        # 2. MAX CYCLES ENFORCEMENT
        # ----------------------------
        if cycles_used_today >= max_cycles_per_day:
            logger.warning(f"[CYCLE] Max cycles reached → Blocking discharge")
            block_discharging = True

            if lockout_cfg["block_economic_charging"]:
                prevent_charging = True

        # ----------------------------
        # 3. SOC SAFETY MANAGEMENT
        # ----------------------------
        if soc < min_safe_soc:
            logger.warning(f"[SAFETY] SOC < minimum → forcing safety charging")

            if lockout_cfg["allow_safety_charging"]:
                prevent_charging = False
                aggressively_charge = True
                block_discharging = True

        # ----------------------------
        # 4. RESERVE BAND CHARGING
        # ----------------------------
        reserve_soc_threshold = backup_soc

        if soc < reserve_soc_threshold and lockout_cfg["allow_reserve_charging"]:
            prevent_charging = False  # allow charging anytime to reach reserve

        # ----------------------------
        # 5. OPTIONAL NOTIFICATIONS
        # ----------------------------
        if notify_cfg["notify_at_80_percent_usage"]:
            usage_percent = (cycles_used_today / max_cycles_per_day if max_cycles_per_day != 0 else 1) * 100
            if usage_percent >= 80:
                logger.info(f"[NOTICE] 80% cycles consumed today")

        return {
            "prevent_charging": prevent_charging,
            "aggressively_charge": aggressively_charge,
            "block_discharging": block_discharging,
            "backup_soc": backup_soc,
            "min_soc" : min_soc,
            "max_soc" : max_soc,
            "normal_recovery_soc": normal_recovery_soc,
            "cycles_used_today": cycles_used_today,
            "max_cycles_per_day": max_cycles_per_day,
            "battery_capacity" : battery_capacity,
            "max_discharge_kw" : max_discharge_kw,
            "arbitrage_cfg" : arbitrage_cfg,
            "soc_cfg" : soc_cfg,
            "bess_power" : bess_power,
            "current_tariff" : current_tariff,
            "soc" : soc,
            "disable_low_price_charging" : disable_low_price_charging,
            "disable_arbitrage" : disable_arbitrage,
            "disable_aggrassive_charging" : disable_aggrassive_charging
        }
