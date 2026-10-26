from datetime import datetime, timedelta, time

class MarketClock:

    @staticmethod
    def dam_gate_closure(date=None):
        # IEX DAM gate closure ~10:00 AM D-1
        date = date or datetime.now().date()
        return datetime.combine(date, time(10, 0))

    @staticmethod
    def now():
        return datetime.now()

