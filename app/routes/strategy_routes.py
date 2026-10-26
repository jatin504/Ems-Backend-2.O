from quart import Blueprint, request, jsonify
from app.utils.db_utils import DB 
import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)

strategy_bp = Blueprint("strategy", __name__, url_prefix="/strategy")


@strategy_bp.route("/add", methods=["POST"])
async def add_strategy():
    logger.info("Received /strategy/add request")

    data = await request.get_json()

    try:
        start_ts_raw = data.get("start_ts")

        if not start_ts_raw:
            return jsonify({"error": "Start time is required"}), 400

        start_ts = datetime.fromisoformat(start_ts_raw)

        end_ts_raw = data.get("end_ts")
        if end_ts_raw and str(end_ts_raw).strip():
            end_ts = datetime.fromisoformat(str(end_ts_raw).strip())
        else:
            end_ts = None

        if start_ts.tzinfo is not None:
            start_ts = start_ts.replace(tzinfo=None)
        if end_ts is not None and end_ts.tzinfo is not None:
            end_ts = end_ts.replace(tzinfo=None)

        if end_ts is not None:
            min_soc = None
            max_soc = None
        else:
            min_soc = (
                int(data["min_soc"])
                if data.get("min_soc") not in (None, "")
                else None
            )
            max_soc = (
                int(data["max_soc"])
                if data.get("max_soc") not in (None, "")
                else None
            )

    except (ValueError, TypeError) as e:
        logger.error("Invalid datetime/value format: %s", e)
        return jsonify({"error": f"Invalid datetime format: {e}"}), 400

    query = """
        INSERT INTO energy_strategy (
            ts, start_ts, end_ts, type, u_tariff, power,
            duration, min_soc, max_soc, priority, remark, schedule_from
        )
        VALUES (
            NOW(), $1, $2, $3, $4, $5,
            $6, $7, $8, $9, $10, $11
        )
        RETURNING id
    """

    params = (
        start_ts,
        end_ts,
        data["type"],
        data.get("tariff") if data.get("tariff") else None,
        float(data["power"]) if data.get("power") not in (None, "") else None,
        data.get("duration") if data.get("duration") else None,
        min_soc,
        max_soc,
        data.get("priority") if data.get("priority") else None,
        data.get("remark", ""),
        data.get("schedule_from", "EXTERNAL")
    )

    async with DB() as db:
        try:
            row = await db.fetch(query, *params)
            return jsonify({"message": "Inserted successfully", "id": row[0]["id"]}), 201
        except Exception as e:
            logger.error("DB insert failed:" ,e)
            return jsonify({"error": str(e)}), 500


@strategy_bp.route("/list", methods=["GET"])
async def list_schedules():
    filter_type = request.args.get("filter", "all")

    if filter_type == "today":
        today_start = datetime.combine(
            date.today(),
            datetime.min.time()
        )
        today_end = datetime.combine(
            date.today(),
            datetime.max.time()
        )

        query = """
            SELECT *
            FROM energy_strategy
            WHERE start_ts >= $1
              AND (
                  end_ts <= $2
                  OR end_ts IS NULL
              )
            ORDER BY start_ts
        """
        params = (
            today_start,
            today_end
        )
    else:
        query = """
            SELECT *
            FROM energy_strategy
            WHERE start_ts IS NOT NULL
            ORDER BY start_ts
        """
        params = ()

    async with DB() as db:
        try:
            rows = await db.fetch(query, *params)
            result = [
                {
                    "id": r["id"],
                    "start_ts": r["start_ts"].isoformat() if r["start_ts"] else None,
                    "end_ts": r["end_ts"].isoformat() if r["end_ts"] else None,
                    "type": r["type"],
                    "tariff": r["tariff"],
                    "power": r["power"],
                    "duration": r["duration"],
                    "min_soc": r["min_soc"],
                    "max_soc": r["max_soc"],
                    "priority": r["priority"],
                    "remark": r["remark"],
                }
                for r in rows
            ]
            return jsonify(result)
        except Exception as e:
            logger.error("Failed to fetch strategy list: %s", e)
            return jsonify({"error": str(e)}), 500
      

@strategy_bp.route("/internal_list", methods=["GET"])
async def internal_list_schedules():
    filter_type = request.args.get("filter", "today")

    if filter_type == "today":
        today_start = datetime.combine(date.today(), datetime.min.time())
        today_end = datetime.combine(date.today(), datetime.max.time())

        query = """
            SELECT *
            FROM energy_strategy
            WHERE (ts BETWEEN $1 AND $2 OR start_ts BETWEEN $1 AND $2)
            ORDER BY ts
        """
        params = (today_start, today_end)
    else:
        query = """
            SELECT *
            FROM energy_strategy
            ORDER BY ts
        """
        params = ()

    async with DB() as db:
        try:
            rows = await db.fetch(query, *params)
            result = []
            for r in rows:
                result.append({
                    "id": r["id"],
                    "ts": r["ts"].isoformat() if r["ts"] else None,
                    "start_ts": r["start_ts"].isoformat() if r["start_ts"] else None,
                    "end_ts": r["end_ts"].isoformat() if r["end_ts"] else None,
                    "type": r["type"],
                    "tariff": r["tariff"],
                    "power": r["power"],
                    "u_tariff": r["u_tariff"],
                    "duration": r["duration"],
                    "priority": r["priority"],
                    "remark": r["remark"],
                })
            return jsonify(result)
        except Exception as e:
            logger.error("Failed to fetch strategy list: %s", e)
            return jsonify({"error": str(e)}), 500
