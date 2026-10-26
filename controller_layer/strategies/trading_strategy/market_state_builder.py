from .market_clock import MarketClock
from .markets.dam.phase_resolver import DAMPhaseResolver

from .models.market_state import MarketState

class MarketStateBuilder:

    @staticmethod
    def build(market: str):

        now = MarketClock.now()
        phase = DAMPhaseResolver.resolve(now)

        return MarketState(
            now=now,
            market=market,
            phase=phase,
            gate_open=phase in ["PLAN", "FINALIZE"],
            clearing_done=phase == "POST_CLEARING"
        )
