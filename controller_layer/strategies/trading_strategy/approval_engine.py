from .db import fetch_all

from .bid_repository import BidRepository

from datetime import date
from LogColor import LogColor

import logging

logger = logging.getLogger(__name__)

class ApprovalEngine:

    @staticmethod
    async def get_approved_bid(market: str, device_id: str, trade_date: date):
        """
        Returns operator-approved bids for a given trade date.
        """
        
        try:
            bids = await fetch_all(
                """
                SELECT *
                FROM market_bids
                WHERE market = $1
                AND asset_id = $2
                AND trade_date = $3
                AND status = 'APPROVED'
                ORDER BY block_no
                """,
                market,
                device_id,
                date.fromisoformat(trade_date)
            )

            if not bids:
                raise Exception("No approved bids found")

            return bids

        except Exception as e:
            logger.error(
                f"{LogColor.RED} ERROR while fetching approved bids: {e} {LogColor.RESET}"
            )
            raise

    async def is_approved(self, bid_id):

        bid = await BidRepository.get_by_bid_id(bid_id)

        return bid["approved"] is True
