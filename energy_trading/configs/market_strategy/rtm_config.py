RTM_CONFIG = {
    "enable_session_bidding": True,
    "final_gate_closure_bid": "14:45",
    "price_spike_trigger": 1.30,         # 30% above average price
    "volume_imbalance_trigger": 0.85,    # sell volume > 85% threshold
    "renewable_forecast_adjustment": True,
    "default_bid_price": 3.25,           # Rs/kWh
    "min_soc_limit": 20,
    "max_soc_limit": 90,
    "max_charge_rate_mw": 5.0,
    "max_discharge_rate_mw": 5.0,
}
