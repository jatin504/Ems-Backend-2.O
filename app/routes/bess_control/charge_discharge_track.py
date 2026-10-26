from collections import defaultdict
from datetime import datetime
from quart import Blueprint, jsonify
from app.utils.db_utils import DB

import email.utils
import time

from .init_bp import bess_control_bp

@bess_control_bp.route("/charge_discharge_track", methods=["GET"])
async def charge_discharge_track():

    # There will be dynamic way to fetch pcs tables.

    query = """
        SELECT 
            time_stamp,
            charge_energy_kwh,
            discharge_energy_kwh
        FROM plant_1_pcs_pcs_1
        WHERE time_stamp >= NOW() - INTERVAL '7 days'
        ORDER BY time_stamp
    """

    async with DB() as db:
        rows = await db.fetch(query)

    daily = defaultdict(list)

    for r in rows:
        day = r["time_stamp"].date()
        daily[day].append(dict(r))

    charge_bar = []
    discharge_bar = []
    charge_track = []
    discharge_track = []

    for day in sorted(daily.keys()):
        rows = daily[day]

        # ---- Total kWh ----
        charge_kwh = sum(
            (r["charge_energy_kwh"] or 0)
            for r in rows
            if (r["charge_energy_kwh"] or 0) > 0
        )

        discharge_kwh = sum(
            (r["discharge_energy_kwh"] or 0)
            for r in rows
            if (r["discharge_energy_kwh"] or 0) > 0
        )
        
        # ---- Timestamp for bar (day-wise) ----
        day_ts = datetime.combine(day, datetime.min.time())

        if rows[0]["time_stamp"].tzinfo:
            day_ts = day_ts.replace(tzinfo=rows[0]["time_stamp"].tzinfo)

        ms_timestamp = int(day_ts.timestamp() * 1000)

        charge_bar.append([ms_timestamp, round(charge_kwh, 2)])
        discharge_bar.append([ms_timestamp, round(discharge_kwh, 2)])

        # ---- Tracking ----
        charge_periods = group_periods(rows, "charge_energy_kwh")
        discharge_periods = group_periods(rows, "discharge_energy_kwh")

        charge_track.append({
            "X": ms_timestamp,
            "y": round(charge_kwh, 2),
            "meta": [
                f"{s.strftime('%H:%M')}–{e.strftime('%H:%M')}: {kwh}"
                for s, e, kwh in charge_periods
            ]
        })

        discharge_track.append({
            "x" : ms_timestamp,
            "y": round(discharge_kwh, 2),
            "meta": [
                f"{s.strftime('%H:%M')}–{e.strftime('%H:%M')}: {kwh}"
                for s, e, kwh in discharge_periods
            ]
        })

    return jsonify([
        {
            "name": "Charge kWh",
            "type": "column",
            "data": charge_bar,
            "color": "#3B82F6",
        },
        {
            "name": "Discharge kWh",
            "type": "column",
            "data": discharge_bar,
            "color": "#F59E0B",
        },
        {
            "name": "Charge Track",
            "type": "areaspline",
            "color": "#A78BFA",
            "opacity": 0.5,
            "data": charge_track,
        },
        {
            "name": "Discharge Track",
            "type": "areaspline",
            "color": "#F97316",
            "opacity": 0.5,
            "data": discharge_track,
        }
    ])

from datetime import timedelta

def group_periods(rows, key, interval_minutes=15):
    """
    Groups continuous charge/discharge windows.
    """
    periods = []
    start = None
    energy = 0

    for r in rows:
        value = r[key] or 0
        if value > 0:
            if start is None:
                start = r["time_stamp"]
                energy = 0
            energy += value * (interval_minutes / 60)
        else:
            if start:
                periods.append((start, r["time_stamp"], round(energy, 2)))
                start = None

    if start:
        periods.append((start, rows[-1]["time_stamp"], round(energy, 2)))

    return periods
