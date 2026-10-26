from datetime import datetime

from LogColor import LogColor

import logging

logger = logging.getLogger(__name__)

class BidLocker:

    @staticmethod
    def lock(bid):

        try:

            if bid["status"] != "APPROVED":
                raise Exception("Only APPROVED bids can be locked")

            bid["locked_at"] = datetime.utcnow()
            bid["is_locked"] = True

            return bid

        except Exception as e:
            logger.info(f"{LogColor.RED} ERROR while Bid Locker: {e} {LogColor.RESET}")
