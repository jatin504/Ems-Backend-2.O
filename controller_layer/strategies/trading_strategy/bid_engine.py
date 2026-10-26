from .models.bid import Bid

class RiskReserveBidEngine:
    """
    Docstring for RiskReserveBidEngine
    """

class AggressiveTradingBidEngine:
    """
    Docstring for AggressiveTradingBidEngine
    """

    @staticmethod
    def execute():
        """
        Docstring for execute
        """
        return { 
            "participant_id": "CGS_1021",
            "market": "RTM",
            "bids": [ 
                { 
                    "bid_id": "ETAFX-RTM-001",
                    "asset_id": "BESS-01",
                    "direction": "SELL",
                    "delivery_date": "2025-10-30",
                    "start_block": 45,
                    "end_block": 45,
                    "quantity_mw": 2.5,
                    "price_per_kwh": 3.25 
                },
                { 
                    "bid_id": "ETAFX-RTM-002",
                    "asset_id": "Solar-02",
                    "direction": "SELL",
                    "delivery_date": "2025-10-30",
                    "start_block": 46,
                    "end_block": 46,
                    "quantity_mw": 2.0,
                    "price_per_kwh": 3.50 
                } 
            ] 
        }



class TradingBidEngine:

    def prepare_bid(self, trigger_event, market_context):
        """
        Converts trigger → bid
        """

        soc = market_context["soc"]
        capacity = market_context["battery_capacity"]

        if trigger_event["type"] == "PRICE":
            qty = self._compute_quantity(soc, capacity)

            return {
                "market": trigger_event.get("market", "RTM"),
                "direction": trigger_event["direction"],
                "price": market_context["market_price"],
                "quantity_kw": qty,
                "timestamp": market_context["now"]
            }

        # Extend for DAM, GTAM etc
