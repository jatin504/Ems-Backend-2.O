from datetime import datetime
from quart import request, jsonify
from app.utils.db_utils import DB

from .init_bp import dsm_summary_bp


@dsm_summary_bp.route("/block_summary", methods=["GET"])
async def block_summary():

    plant_id = request.args.get("plant_id")
    date = request.args.get("date")  # YYYY-MM-DD
    limit = request.args.get("limit", default=96, type=int)

    if not plant_id:
        return jsonify({"error": "plant_id is required"}), 400

    query = """
        SELECT
            plant_id,
            block_time,
            block_no,
            scheduled_power_kw,
            scheduled_energy_kwh,
            actual_export_kwh,
            actual_import_kwh,
            net_export_kwh,
            deemed_energy_kwh,
            delivered_energy_kwh,
            deviation_kwh,
            frequency,
            dsm_rate,
            dsm_charge
        FROM dsm_block_summary
        WHERE plant_id = $1
    """

    params = [plant_id]

    if date:
        query += " AND DATE(block_time) = $2 "
        params.append(datetime.strptime(date, '%Y-%m-%d').date())

    query += """
        ORDER BY block_time DESC
        LIMIT $""" + str(len(params) + 1)

    params.append(limit)

    async with DB() as db:
        rows = await db.fetch(query, *params)

    result = [dict(row) for row in rows]

    return jsonify({
        "count": len(result),
        "data": result
    })