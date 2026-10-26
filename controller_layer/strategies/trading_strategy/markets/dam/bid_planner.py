from ...models.dam import DAMPlanningState, DAMPlannedAction
from LogColor import LogColor

import logging

logger = logging.getLogger(__name__)

class DAMBidPlanner:

    def plan(blocks, constraints, bess):

        logger.info(f"{LogColor.CYAN} In the DAM Bid Planner {LogColor.RESET}")

        state = DAMPlanningState(
            soc=bess["backup_soc"],
            cycles_used=bess["cycles_used_today"]
        )

        actions = []

        # Sort blocks by price
        sorted_blocks = sorted(
            blocks,
            key=lambda b: b.price_rs_per_kwh
        )

        # Price Threshold
        LOW_PRICE_THRESHOLD = 4
        HIGH_PRICE_THRESHOLD = 5

        for block in sorted_blocks:

            c = constraints[block.block_no]

            if block.price_rs_per_kwh < LOW_PRICE_THRESHOLD and c.can_charge:
                power = min(
                    c.max_charge_kw,
                    bess["battery_capacity"] * 0.25 #0.25 for 15min
                )

                state.soc += power #power_to_soc(power)
                actions.append(
                    DAMPlannedAction(block.block_no, "CHARGE", power)
                )

            elif block.price_rs_per_kwh > HIGH_PRICE_THRESHOLD and c.can_discharge:
                power = min(
                    c.max_discharge_kw,
                    bess["battery_capacity"] * 0.25 #0.25 for 15min
                )

                state.soc -= power #power_to_soc(power)
                actions.append(
                    DAMPlannedAction(block.block_no, "DISCHARGE", power)
                )

            else:
                actions.append(
                    DAMPlannedAction(block.block_no, "IDLE", 0)
                )

        return actions
