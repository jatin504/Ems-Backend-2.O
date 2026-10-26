from ...bid_repository import BidRepository
from .exchange_adapter import ExchangeAdapter

class DAMSubmitter:

    async def submit(context):

        bid = await BidRepository.get(
            market="DAM",
            status="FINALIZED"
        )

        #ExchangeAdapter.submit(bid)
        await ExchangeAdapter.submit_dam_bid(bid)

        await BidRepository.update(bid, status="SUBMITTED")
