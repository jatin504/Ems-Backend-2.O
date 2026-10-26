from .db import execute, fetch_all


class ScheduleRepository:

    @staticmethod
    async def store_schedule(
        market: str,
        device_id: str,
        schedules: list,
        bid_id: int
    ):
        """
        schedules = [
            {start_time, end_time, power_kw}
        ]
        """

        for slot in schedules:
            await execute("""
                INSERT INTO market_schedules
                (market, device_id, start_time, end_time, power_kw, source_bid_id)
                VALUES ($1, $2, $3, $4, $5, $6)
            """,
            market,
            device_id,
            slot["start_time"],
            slot["end_time"],
            slot["power_kw"],
            bid_id
            )
