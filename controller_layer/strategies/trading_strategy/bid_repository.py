from .db import execute, fetch_one

from LogColor import LogColor

import logging

logger = logging.getLogger(__name__)

class BidRepository:

    @staticmethod
    async def update(bid, status):

        try:
            if not BidRepository._is_valid_transition(
                bid["status"], status
            ):
                raise Exception(
                    f"Invalid bid state transition "
                    f"{bid['status']} → {status}"
                )

            await execute("""
                UPDATE market_bids
                SET status = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (status, bid["id"]))

        except Exception as e:
            logger.info(f"{LogColor.RED} ERROR while updating bid status Bid={bid} Error={e} {LogColor.RESET}")

    @staticmethod
    async def bulk_update_status(bids, status: str):

        try:

            query = """
            UPDATE market_bids
            SET status = $1,
                updated_at = NOW()
            WHERE bid_id = $2
            """

            for bid in bids:
                await execute(query, status, bid["bid_id"])

        except Exception as e:
            logger.info(f"{LogColor.RED} ERROR while bulk updating bid status Bids={bids} Error={e} {LogColor.RESET}")

    @staticmethod
    async def get(market, status):

        await execute("""
            SELECT * FROM market_bids
            WHERE status = %s,
            WHERE market = %s
        """, (status, market))

    @staticmethod
    async def get_by_bid_id(bid_id):

        await execute("""
            SELECT * FROM market_bids
            WHERE bid_id = %s,
        """, (bid_id))

    @staticmethod
    async def save(bid):
        return await execute("""
            INSERT INTO bids (
                market, start_time, end_time,
                price, quantity_kw, status
            )
            VALUES ($1,$2,$3,$4,$5,$6)
        """,
        bid["market"],
        bid["start_time"],
        bid["end_time"],
        bid["price"],
        bid["quantity_kw"],
        bid["status"]
        )

    @staticmethod
    async def get_frozen(block):
        return await fetch_one("""
            SELECT * FROM bids
            WHERE start_time=$1 AND status='FROZEN'
        """, block.start)

    @staticmethod
    def _is_valid_transition(old, new):

        allowed = {
            "PLANNED": ["APPROVED"],
            "APPROVED": ["FINALIZED"],
            "FINALIZED": ["SUBMITTED"],
            "SUBMITTED": ["CLEARED"]
        }

        return new in allowed.get(old, [])

