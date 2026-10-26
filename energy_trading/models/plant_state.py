from dataclasses import dataclass

@dataclass
class PlantState:
    soc: float
    available_capacity_mw: float
