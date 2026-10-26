from .models.trading_context import TradingContext
from .market_clock import MarketClock

class TradingContextBuilder:

    @staticmethod
    def build(device_id: str, user_config: dict) -> TradingContext:
        return TradingContext(
            device_id=device_id,
            now=MarketClock.now(),
            user_config=user_config,
            market_clock=MarketClock
        )
