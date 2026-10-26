from LogColor import LogColor

import logging

logger = logging.getLogger(__name__)

class RTMBidPlanner:

    def plan(self, ctx):
        logger.info(f"{LogColor.CYAN} In the RTM PLAN {LogColor.RESET}")
        bess = ctx["bess"]
        available_kw = min(
            bess["max_discharge_kw"],
            bess["battery_capacity"] * (bess["max_soc"] - bess["backup_soc"])
        )

        surplus = ctx["forecast_power_kw"]

        if surplus <= 0:
            return None  # nothing to sell

        bid_kw = min(surplus, available_kw)

        return {
            "market": "RTM",
            "start_time": ctx["block"].start,
            "end_time": ctx["block"].end,
            "price": ctx["price"],
            "quantity_kw": bid_kw,
            "status": "PLANNED"
        }
