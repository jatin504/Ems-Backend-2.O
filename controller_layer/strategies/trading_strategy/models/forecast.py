from dataclasses import dataclass

@dataclass
class PlantForecast:
    block_no: int
    start_time: str
    end_time: str
    plant_power_kw: float
