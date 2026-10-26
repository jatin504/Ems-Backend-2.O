from ...forecastings import plant_forecast, tariff_forecast
from ...models.dam import DAMRawData

class DAMDataCollector:

    async def collect(device_id, bess_config):

        # 1️⃣ Forecasts (already available)
        #plant_forecast = ForecastService.get_plant_forecast(device_id)
        #tariff_forecast = #ForecastService.get_tariff_forecast("DAM")

        # 2️⃣ BESS decision inputs
        # bess_limits = await TradingStrategy.bess_decision(
        #     device_id=device_id
        # )
        bess_config["soc_available_kwh"] = 5000.0
        return DAMRawData(
            plant_forecast=plant_forecast,
            tariff_forecast=tariff_forecast,
            bess_limits=bess_config,
            market_confidence="HIGH"
        )
