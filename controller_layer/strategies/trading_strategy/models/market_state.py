from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class MarketState:
    now: datetime
    market: str           # DAM / RTM
    phase: str            # PLAN / FINALIZE / SUBMIT / POST_CLEARING
    gate_open: bool
    clearing_done: bool
