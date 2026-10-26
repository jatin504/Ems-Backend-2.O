from dataclasses import dataclass
from datetime import datetime

@dataclass
class TradingContext:
    device_id: str
    now: datetime
    user_config: dict
    market_clock: "MarketClock"
