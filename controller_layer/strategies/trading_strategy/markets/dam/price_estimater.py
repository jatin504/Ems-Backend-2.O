from ...models.dam import DAMPriceBand

class DAMPriceEstimator:

    @staticmethod
    def estimate(block_no: int, historical_prices: list[float]) -> DAMPriceBand:
        """
        historical_prices = DAM MCPs for same block over last N days
        """

        prices = sorted(historical_prices)

        return DAMPriceBand(
            low=prices[int(len(prices) * 0.3)],
            mid=prices[int(len(prices) * 0.5)],
            high=prices[int(len(prices) * 0.8)]
        )
