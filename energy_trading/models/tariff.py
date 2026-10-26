from dataclasses import dataclass

@dataclass
class TariffBlock:
    block_no: int
    start_time: str
    end_time: str
    price_rs_per_kwh: float
