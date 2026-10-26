class RTMBidFreezer:

    async def freeze(self, planned_bid):

        frozen_bid = planned_bid.copy()
        frozen_bid["status"] = "FROZEN"

        await BidRepository.save(
            bid=frozen_bid,
            market="RTM",
            approval_required=True
        )

        return frozen_bid
