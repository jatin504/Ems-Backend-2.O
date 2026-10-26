from .planner import DAMPlanner
from .phase_resolver import DAMPhaseResolver
from .submission import DAMSubmitter
from .settlement import DAMSettlement

from LogColor import LogColor

import logging

logger = logging.getLogger(__name__)

class DAMMarketEngine:

    async def tick(context):

        logger.info(f"{LogColor.CYAN} In the DAM Engine {LogColor.RESET}")
        
        phase = "PLAN" #DAMPhaseResolver.resolve(context.now)
        trade_date = '2025-12-24'
        logger.info(f"{LogColor.CYAN} CURRENT PHASE: {phase} {LogColor.RESET}")

        if phase == "PLAN":
            await DAMPlanner.plan(context, trade_date)

        elif phase == "FINALIZE":
            await DAMPlanner.finalize(context, trade_date)

        elif phase == "SUBMIT":
            await DAMSubmitter.submit(context)

        elif phase == "POST_CLEARING":
            await DAMSettlement.process(context)

