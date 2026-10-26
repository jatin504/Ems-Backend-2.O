from .exchange_adapter import ExchangeAdapter
from ...schedule_repository import ScheduleRepository
from ...market_state_builder import MarketState

class DAMSettlement:

    async def process(context):

        result = ExchangeAdapter.fetch_clearing("DAM")

        ScheduleRepository.create_dispatch_schedule(result)

        MarketState.lock_capacity(result)
