from dataclasses import dataclass
from .forecast import PlantForecast
from .tariff import TariffBlock

from datetime import datetime

@dataclass
class DAMRawData:
    plant_forecast: list[PlantForecast]
    tariff_forecast: list[TariffBlock]
    bess_limits: dict
    market_confidence: str

@dataclass
class DAMBlock:
    block_no: int
    start_time: str
    end_time: str
    plant_power_kw: float


@dataclass
class DAMBlockConstraint:
    can_charge: bool
    can_discharge: bool
    max_charge_kw: float
    max_discharge_kw: float


@dataclass
class DAMPlanningState:
    soc: float
    cycles_used: int

@dataclass
class DAMPlannedAction:
    block_no: int
    action: str  # CHARGE / DISCHARGE / IDLE
    power_kw: float

@dataclass
class DAMBid:
    block_no: int
    direction: str  # BUY / SELL
    price_rs_per_kwh: float
    quantity_kw: float
    step: int       # 1, 2, 3 (important for audit/debug)
    start_time: str
    end_time: str

@dataclass
class DAMPriceBand:
    low: float
    mid: float
    high: float

