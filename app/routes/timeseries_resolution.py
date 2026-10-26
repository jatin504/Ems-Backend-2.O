import logging
from quart import Blueprint, request, jsonify
from app.utils.db_utils import DB
from device_config_helper import DeviceConfigHelper

device_helper = DeviceConfigHelper()
logger = logging.getLogger(__name__)

resolution_bp = Blueprint('resolution_timeseries', __name__)


@resolution_bp.route('/api/timeseries-resolution-data', methods=['POST'])
async def get_resolution_timeseries():

    try:
        body = await request.get_json()

        resolutions = body.get("resolutions", [])
        keys = body.get("keys", [])
        devices = body.get("devices", {})
        patterns = devices.get("patterns", [])

        if not resolutions:
            return jsonify({"error": "resolutions required"}), 400

        if not keys:
            return jsonify({"error": "keys required"}), 400

        suffix_map = {
            "Week": "_week",
            "Month": "_month",
            "Year": "_year",
            "Total": "_total"
        }

        # resolve device ids
        resolved_ids = set()

        for pattern in patterns:
            matched_ids = device_helper.get_device_ids_by_type_pattern(pattern)
            if matched_ids:
                resolved_ids.update(matched_ids)

        device_ids = sorted(resolved_ids)

        results = {}

        async with DB() as db:

            for resolution in resolutions:

                results[resolution] = {}

                for device_id in device_ids:

                    try:

                        column_string = ", ".join(keys)

                        # ------------------------
                        # YESTERDAY (RAW TABLE)
                        # ------------------------
                        if resolution == "Yesterday":

                            table_name = device_id

                            query = f"""
                            SELECT time_stamp, {column_string}
                            FROM {table_name}
                            WHERE time_stamp >= CURRENT_DATE - INTERVAL '1 day'
                            AND time_stamp < CURRENT_DATE
                            ORDER BY time_stamp DESC
                            LIMIT 1
                            """

                            rows = await db.fetch(query)

                            if rows:
                                results[resolution][device_id] = dict(rows[0])

                            continue


                        # ------------------------
                        # OTHER RESOLUTIONS
                        # ------------------------
                        suffix = suffix_map.get(resolution)

                        if not suffix:
                            continue

                        table_name = f"{device_id}{suffix}"

                        # check table exists
                        table_check = await db.fetchrow(
                            """
                            SELECT EXISTS (
                                SELECT 1
                                FROM information_schema.tables
                                WHERE table_name = $1
                            )
                            """,
                            table_name
                        )

                        if not table_check["exists"]:
                            continue

                        query = f"""
                        SELECT time_stamp, {column_string}
                        FROM {table_name}
                        ORDER BY time_stamp DESC
                        LIMIT 1
                        """

                        rows = await db.fetch(query)

                        if rows:
                            results[resolution][device_id] = dict(rows[0])

                    except Exception as e:
                        logger.error(f"{resolution} query failed for {device_id}: {e}")
                        continue

        return jsonify({
            "success": True,
            "data": results
        })

    except Exception as e:

        logger.error(f"Resolution API error: {e}")

        return jsonify({
            "success": False,
            "error": str(e)
        }), 500