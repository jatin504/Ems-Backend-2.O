def compute_profit(price: float, quantity: float) -> float:
    """
    Basic profit model: revenue = price * energy
    quantity is MW for 15 min => MWh = MW * 0.25
    """
    mwh = quantity * 0.25
    return round(price * mwh, 3)
