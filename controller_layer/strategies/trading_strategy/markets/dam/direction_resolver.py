class DAMDirectionResolver:

    @staticmethod
    def resolve(block, bess_limits):

        if block.plant_power_kw > 0 and bess_limits["soc_available_kwh"] > 0:
            return "SELL"

        if bess_limits["soc"] < bess_limits["max_soc"]: #bess_limits["soc_available_kwh"] < bess_limits["max_soc_kwh"]:
            return "BUY"

        return "IDLE"
