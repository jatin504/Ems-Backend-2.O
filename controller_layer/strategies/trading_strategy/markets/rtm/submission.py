class RTMBidSubmitter:

    async def submit(self, bid):

        response = await ExchangeAdapter.submit_rtm_bid(bid)

        if response.success:
            await BidRepository.update_status(
                bid_id=bid["id"],
                status="SUBMITTED"
            )
        else:
            await BidRepository.update_status(
                bid_id=bid["id"],
                status="FAILED"
            )
