from datetime import datetime, timedelta
from ...config.rtm_timing import RTM_TIMING

class RTMPhaseResolver:

    @staticmethod
    def resolve(block, now: datetime):
        delta = block.start - now

        if RTM_TIMING["plan_start"] >= delta > RTM_TIMING["plan_end"]:
            return "PLAN"

        if RTM_TIMING["plan_end"] >= delta > RTM_TIMING["submit_at"]:
            return "FINALIZE"

        if RTM_TIMING["submit_at"] >= delta > timedelta(0):
            return "SUBMIT"

        if delta <= timedelta(0):
            return "POST_CLEARING"

        return "SKIP"
