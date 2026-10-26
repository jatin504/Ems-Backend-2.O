from .models.tariff import TariffBlock
from .models.forecast import PlantForecast

plant_forecast = [
    PlantForecast(45, "11:15", "11:30", 1350.8),
    PlantForecast(46, "11:30", "11:40", 1400.0),
    PlantForecast(90, "22:15", "22:30", 0.0),
    PlantForecast(91, "22:30", "22:45", 0.0)
]

tariff_forecast = [
    TariffBlock(45, "11:15", "11:30", 3.25),
    TariffBlock(46, "11:30", "11:45", 3.50),
    TariffBlock(90, "22:15", "22:30", 6.00),
    TariffBlock(91, "22:30", "22:45", 6.00)
]