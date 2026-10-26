from dataclasses import dataclass

@dataclass
class Bid:
    bid_id: str
    market: str
    entity: str
    date: str
    block_no: int
    start_time: str
    end_time: str
    quantity_mw: float
    price_rs_per_kwh: float
    direction: str
    asset_id: str
    remarks: str
