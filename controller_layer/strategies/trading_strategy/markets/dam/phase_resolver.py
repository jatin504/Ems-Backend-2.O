from datetime import datetime, timedelta

from ...market_clock import MarketClock

from ...config.dam_timing import DAM_TIMING

class DAMPhaseResolver:

    @staticmethod
    def resolve(now: datetime):

        gate_closure = MarketClock.dam_gate_closure()

        delta = gate_closure - now

        if (
            DAM_TIMING["planning_start_offset"]
            >= delta
            > DAM_TIMING["planning_end_offset"]
        ):
            return "PLAN"

        if (
            DAM_TIMING["finalize_start_offset"]
            >= delta
            > DAM_TIMING["finalize_end_offset"]
        ):
            return "FINALIZE"

        if delta <= DAM_TIMING["finalize_end_offset"] and delta > timedelta(0):
            return "SUBMIT"

        if delta <= timedelta(0):
            return "POST_CLEARING"

        return "IDLE"
