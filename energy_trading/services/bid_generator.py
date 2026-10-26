import uuid
from models.bid import Bid
from ..configs.strategy_params import BID_ENTITY, MARKET, ASSET_ID

def generate_bid(row, decision, date):
    bid_id = "RTM-" + uuid.uuid4().hex[:8]
    
    return Bid(
        bid_id         = bid_id,
        market         = MARKET,
        entity         = BID_ENTITY,
        date           = date,
        block_no       = row.block_no,
        start_time     = row.start_time,
        end_time       = row.end_time,
        quantity_mw    = decision["quantity"],
        price_rs_per_kwh = decision["price"],
        direction      = decision["direction"],
        asset_id       = ASSET_ID,
        remarks        = decision["direction"] + " decision"
    )
