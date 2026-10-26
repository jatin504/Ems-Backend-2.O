class TradingTriggerEngine:
    """
    Evaluates all enabled triggers for selected market strategies.
    """

    def __init__(self, enabled_markets):
        self.triggers = []

        if "RTM" in enabled_markets:
            self.triggers.extend([
                RTMSessionTrigger(),
                RTMPriceTrigger(),
                RTMVolumeImbalanceTrigger()
            ])

        if "DAM" in enabled_markets:
            self.triggers.extend([
                # DAMSessionTrigger(),
                # DAMPriceTrigger()
            ])

    def evaluate(self, context):
        """
        Returns list of triggered events (can be empty).
        """
        events = []

        for trigger in self.triggers:
            event = trigger.check(context)
            if event:
                events.append(event)

        return events


class RTMSessionTrigger:

    def check(self, context):
        now = context["now"]

        # Example: every 15 min
        if now.minute % 15 == 0 and now.second < 5:
            return {
                "id": "SESSION_15MIN",
                "type": "SESSION",
                "timestamp": now,
                "market": "RTM"
            }

        return None

class RTMPriceTrigger:

    def check(self, context):
        price = context["market_price"]
        high_price = context["price_threshold_high"]
        low_price = context["price_threshold_low"]

        if price >= high_price:
            return {
                "id": "PRICE_HIGH",
                "type": "PRICE",
                "direction": "SELL",
                "timestamp": context["now"]
            }

        if price <= low_price:
            return {
                "id": "PRICE_LOW",
                "type": "PRICE",
                "direction": "BUY",
                "timestamp": context["now"]
            }

        return None


class RTMVolumeImbalanceTrigger:

    def check(self, context):
        imbalance = context["volume_imbalance"]

        if abs(imbalance) > context["imbalance_threshold"]:
            return {
                "id": "VOLUME_IMBALANCE",
                "type": "VOLUME",
                "value": imbalance,
                "timestamp": context["now"]
            }

        return None
