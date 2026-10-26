from .markets.dam.engine import DAMMarketEngine
from .markets.rtm.engine import RTMMarketEngine

from LogColor import LogColor

import logging

logger = logging.getLogger(__name__)

class TradingOrchestrator:

    async def process(context):

        logger.info(f"{LogColor.CYAN} In the Trading Orchestration {LogColor.RESET}")
        
        enabled_markets = ['DAM'] #context.user_config.enabled_markets

        if "DAM" in enabled_markets:
            await DAMMarketEngine.tick(context)

        if "RTM" in enabled_markets:
            await RTMMarketEngine.tick(context)
