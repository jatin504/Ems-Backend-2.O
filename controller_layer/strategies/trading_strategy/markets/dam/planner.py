from .data_collector import DAMDataCollector
from .block_builder import DAMBlockBuilder
from .constraint_engine import DAMConstraintEngine
from .bid_builder import DAMBidBuilder
from .bid_repo import DAMBidRepository
from .bid_planner import DAMBidPlanner
from .price_estimater import DAMPriceEstimator
from .direction_resolver import DAMDirectionResolver
from .bid_validator import DAMBidValidator

from ...approval_engine import ApprovalEngine
from ...bid_repository import BidRepository
from ...bid_locker import BidLocker

from LogColor import LogColor

import logging

logger = logging.getLogger(__name__)

async def run_dam_planning(device_id, bess_config, trade_date):

    raw = await DAMDataCollector.collect(device_id, bess_config)

    # raw contains:
    # - plant_forecast
    # - bess_limits
    # - historical_dam_prices (last N days, block-wise)

    blocks = DAMBlockBuilder.build(raw)
    # constraints = DAMConstraintEngine.evaluate(
    #     blocks,
    #     raw.bess_limits
    # )

    # actions = DAMBidPlanner.plan(
    #     blocks,
    #     constraints,
    #     raw.bess_limits
    # )

    # bids = DAMBidBuilder.build(
    #     actions=actions,
    #     blocks=blocks,
    #     trade_date=trade_date,
    #     asset_id=device_id
    # )

    all_bids = []

    # Iterate block-by-block (IMPORTANT CHANGE)
    for block in blocks:

        # Estimate price band from history
        price_band = DAMPriceEstimator.estimate(
            block_no=block.block_no,
            historical_prices=[2.5, 2.4, 2.7, 2.8, 2.9, 2.6] #raw.historical_dam_prices[block.block_no]
        )

        # Decide direction (SELL / BUY)
        direction = DAMDirectionResolver.resolve(
            block=block,
            bess_limits=raw.bess_limits
        )

        if direction == "IDLE":
            continue

        # Build multi-step bids
        raw_bids = DAMBidBuilder.build(
            block=block,
            price_band=price_band,
            bess_limits=raw.bess_limits,
            direction=direction,
            confidence=raw.market_confidence
        )

        # Enforce SOC & cycle safety
        valid_bids = DAMBidValidator.enforce_soc_limit(
            raw_bids,
            soc_available_kwh=raw.bess_limits["soc_available_kwh"]
        )

        all_bids.extend(valid_bids)


    # persist for approval
    await DAMBidRepository.save_for_approval(
        device_id=device_id,
        bids=all_bids,
        trade_date=trade_date
    )

class DAMPlanner:

    @staticmethod
    async def plan(context, trade_date):
        """
        Called multiple times during PLAN window.
        Must be idempotent.
        """

        await run_dam_planning(
            device_id=context.device_id,
            bess_config=context.user_config,
            trade_date=trade_date
        )

    @staticmethod
    async def finalize(context, trade_date):

        logger.info(f"{LogColor.CYAN} In the DAM Finalize {LogColor.RESET}")

        approved_bids = await ApprovalEngine.get_approved_bid(
            market="DAM",
            device_id=context.device_id,
            trade_date=trade_date
        )

        # locked_bids = [
        #     BidLocker.lock(bid)
        #     for bid in approved_bids
        # ]

        await BidRepository.bulk_update_status(
            bids=approved_bids,
            status="FINALIZED"
        )
    