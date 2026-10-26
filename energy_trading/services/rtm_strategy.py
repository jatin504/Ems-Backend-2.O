from datetime import datetime
from ..configs.market_strategy.rtm_config import RTM_CONFIG

def compute_direction(price, avg_price, soc):
    if price > avg_price * RTM_CONFIG["price_spike_trigger"] and soc > RTM_CONFIG["min_soc_limit"]:
        return "Sell"
    if soc < RTM_CONFIG["max_soc_limit"]:
        return "Buy"
    return "None"

def decide_quantity(direction, soc):
    if direction == "Sell":
        return RTM_CONFIG["max_discharge_rate_mw"]
    if direction == "Buy":
        return RTM_CONFIG["max_charge_rate_mw"]
    return 0.0

def generate_rtm_decision(block, plant_state, avg_price):
    price = block.price_rs_per_kwh
    direction = compute_direction(price, avg_price, plant_state.soc)
    qty = decide_quantity(direction, plant_state.soc)

    return {
        "direction": direction,
        "quantity": qty,
        "price": price
    }
