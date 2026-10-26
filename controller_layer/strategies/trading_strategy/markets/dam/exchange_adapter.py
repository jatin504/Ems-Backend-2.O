class ExchangeAdapter:

    @staticmethod
    async def submit_dam_bid(bid):
        """
        Called ONLY at gate closure.
        """

        payload = {
            "market": "DAM",
            "delivery_date": bid["delivery_date"],
            "blocks": bid["blocks"],   # price-quantity curve
            "bid_id": bid["id"]
        }

        # Placeholder for IEX / exchange API
        response = await IEXClient.submit(payload)

        return response
    
    @staticmethod
    async def submit_rtm_bid(bid):

        payload = {
            "market": "RTM",
            "time_block": bid["time_block"],
            "price": bid["price"],
            "quantity": bid["quantity"]
        }

        return await IEXClient.submit(payload)


    @staticmethod
    async def fetch_dam_results(delivery_date):

        response = await IEXClient.fetch_results(
            market="DAM",
            date=delivery_date
        )

        return response
