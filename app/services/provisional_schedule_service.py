from datetime import datetime, timedelta

from app.services.load_forecast_services import ensure_load_forecast_table, save_forecast_to_db
from app.utils.db_utils import DB


async def save_provisional_schedule(forecast, samples, schedule, model_name):
    """Pair each minute input with the planned setpoints in result.json."""
    slots = {datetime.fromisoformat(slot["time_stamp"]): slot for slot in schedule}
    rows = {}
    for sample in samples:
        minute = datetime.fromisoformat(sample["time_stamp"]).replace(second=0, microsecond=0)
        slot = slots[minute.replace(minute=minute.minute // 15 * 15)]
        charge = slot["charge_kw"]
        grid_charge = slot["grid_charge_kw"]
        discharge = slot["discharge_kw"]
        rows[minute] = (
            minute, model_name, sample["poa"], sample["pv_kw"], sample["load_kw"],
            charge, grid_charge, discharge, slot["decision"],
            round(discharge - charge - grid_charge, 2), slot["grid_import_kw"],
        )

    if not rows:
        return

    async with DB() as db:
        await ensure_load_forecast_table(db)
        async with db.connection.transaction():
            await db.execute("SELECT pg_advisory_xact_lock(hashtext('provisional_schedule'))")
            await save_forecast_to_db(forecast, model_name, db=db)
            await db.connection.executemany("""
                INSERT INTO provisional_schedule (
                    target_ts, model_name, poa, pv_kw, forecast_load_kw, charge_kw,
                    grid_charge_kw, discharge_kw, bess_mode, bess_power_kw, grid_import_kw
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (target_ts, model_name) DO UPDATE SET
                    poa = EXCLUDED.poa,
                    pv_kw = EXCLUDED.pv_kw,
                    forecast_load_kw = EXCLUDED.forecast_load_kw,
                    charge_kw = EXCLUDED.charge_kw,
                    grid_charge_kw = EXCLUDED.grid_charge_kw,
                    discharge_kw = EXCLUDED.discharge_kw,
                    bess_mode = EXCLUDED.bess_mode,
                    bess_power_kw = EXCLUDED.bess_power_kw,
                    grid_import_kw = EXCLUDED.grid_import_kw
            """, list(rows.values()))


async def get_provisional_schedule(first_day, last_day, model_name):
    async with DB() as db:
        rows = await db.fetch("""
            SELECT * FROM provisional_schedule
            WHERE target_ts >= $1::date AND target_ts < $2::date AND model_name = $3
            ORDER BY target_ts
        """, first_day, last_day + timedelta(days=1), model_name)
    return [
        {**dict(row), "target_ts": row["target_ts"].isoformat()}
        for row in rows
    ]


async def delete_forecast_and_provisional_schedule(target_date):
    async with DB() as db:
        await ensure_load_forecast_table(db)
        async with db.connection.transaction():
            await db.execute("SELECT pg_advisory_xact_lock(hashtext('provisional_schedule'))")
            forecasts = await db.execute(
                "DELETE FROM load_forecast WHERE forecast_date = $1", target_date
            )
            schedules = await db.execute("""
                DELETE FROM provisional_schedule
                WHERE target_ts >= $1::date AND target_ts < $2::date
            """, target_date, target_date + timedelta(days=1))
    return int(forecasts.split()[-1]), int(schedules.split()[-1])
