from .phase_resolver import RTMPhaseResolver
from .planner import RTMBidPlanner
from .context_builder import RTMContextBuilder
from .time_block import TimeBlock
from .exchange_adapter import ExchangeAdapter
from .clearing_handler import RTMClearingHandler

from ...forecastings import plant_forecast, tariff_forecast
from ...bid_repository import BidRepository
from ...db import execute

from datetime import datetime

from LogColor import LogColor

import logging

logger = logging.getLogger(__name__)

class RTMMarketEngine:

    async def tick(context):

        now = datetime.now()

        logger.info(f"{LogColor.CYAN} In the RTM Engine {LogColor.RESET}")

        for block in TimeBlock.future_blocks(now):
            logger.info(f"{LogColor.CYAN} Future Block = {block} {LogColor.RESET}")
            phase = "PLAN" #RTMPhaseResolver.resolve(block, now)
            logger.info(f"{LogColor.CYAN} Phase = {phase} {LogColor.RESET}")
            if phase == "PLAN":
                try:

                    ctx = RTMContextBuilder().build(
                        context.device_id,
                        block,
                        plant_forecast,
                        tariff_forecast,
                        context.user_config
                    )
                    logger.info(f"CONTEXT = {ctx}")
                    bid = RTMBidPlanner().plan(ctx)
                    if bid:
                        logger.info(f"{LogColor.CYAN} BID: {bid} {LogColor.RESET}")
                        # await BidRepository.save(bid)
                except Exception as e:
                    logger.info(f"{LogColor.RED} ERROR While Building Context: {e} {LogColor.RESET}")



            elif phase == "FINALIZE":
                await execute("""
                    UPDATE bids SET status='FINALIZE'
                    WHERE start_time=$1 AND status='PLANNED'
                """, block.start)

            elif phase == "SUBMIT":
                bid = await BidRepository.get_frozen(block)
                if bid and bid["approved"]:
                    await ExchangeAdapter.submit_rtm_bid(bid)

            elif phase == "POST_CLEARING":
                await RTMClearingHandler().handle(block)

