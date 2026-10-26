from datetime import datetime, timedelta
from app.utils import constant
from app.utils.db_utils import DB
from config.device_config_resolver import DeviceConfigResolver


async def plant_card_service(request: dict):
    try:

        resolution = request.get("resolution", "all")

        ALL_RESOLUTIONS = {
            "today",
            "yesterday",
            "month",
            "year",
            "total"
        }

        # ---------------- RESOLUTION ---------------- #

        if resolution == "all":

            requested_resolutions = ALL_RESOLUTIONS

        elif isinstance(resolution, list):

            requested_resolutions = set(resolution) & ALL_RESOLUTIONS

        else:

            requested_resolutions = {resolution} & ALL_RESOLUTIONS

        async with DB() as session:

            yesterday_list = []
            month_list = []
            year_list = []
            today_list = []
            total_list = []

            # ---------------- TABLES ---------------- #

            base_table = request.get("table")

            today_table = base_table
            yesterday_table = base_table
            month_table = f"{base_table}_1_month"
            year_table = f"{base_table}_1_year"
            total_table = f"{base_table}_total"

            today = datetime.now().date()

            # ---------------- LOOP ---------------- #

            for req in request.get("info", []):

                key = req.get("key")
                alias = req.get("alias")
                agg = req.get("agg")
                unit = req.get("unit", "")

                # ---------------- INIT ---------------- #

                yest_data = None
                month_data = None
                year_data = None
                today_data = None
                total_data = None

                # ---------------- QUERY BUILDER ---------------- #

                def build_query(table):

                    if agg == "last":

                        return f"""
                            SELECT {key} AS t1
                            FROM {table}
                            ORDER BY time_stamp DESC
                            LIMIT 1
                        """

                    elif agg == "max":

                        return f"""
                            SELECT MAX({key}) AS t1
                            FROM {table}
                        """

                    elif agg == "min":

                        return f"""
                            SELECT MIN({key}) AS t1
                            FROM {table}
                        """

                    elif agg == "avg":

                        return f"""
                            SELECT AVG({key}) AS t1
                            FROM {table}
                        """

                    elif agg == "sum":

                        return f"""
                            SELECT SUM({key}) AS t1
                            FROM {table}
                        """

                    return None

                # ---------------- TODAY ---------------- #

                today_query = build_query(today_table)

                if "today" in requested_resolutions and today_query:

                    try:

                        today_data = await session.fetchrow(today_query)

                    except Exception as e:

                        print(f"TODAY QUERY ERROR ({key}):", e)

                        today_data = None

                # ---------------- YESTERDAY ---------------- #

                if "yesterday" in requested_resolutions:

                    if agg == "last":

                        yesterday_query = f"""
                            SELECT {key} AS t1
                            FROM {yesterday_table}
                            WHERE time_stamp >= $1
                            AND time_stamp < $2
                            ORDER BY time_stamp DESC
                            LIMIT 1
                        """

                    elif agg == "max":

                        yesterday_query = f"""
                            SELECT MAX({key}) AS t1
                            FROM {yesterday_table}
                            WHERE time_stamp >= $1
                            AND time_stamp < $2
                        """

                    elif agg == "min":

                        yesterday_query = f"""
                            SELECT MIN({key}) AS t1
                            FROM {yesterday_table}
                            WHERE time_stamp >= $1
                            AND time_stamp < $2
                        """

                    elif agg == "avg":

                        yesterday_query = f"""
                            SELECT AVG({key}) AS t1
                            FROM {yesterday_table}
                            WHERE time_stamp >= $1
                            AND time_stamp < $2
                        """

                    elif agg == "sum":

                        yesterday_query = f"""
                            SELECT SUM({key}) AS t1
                            FROM {yesterday_table}
                            WHERE time_stamp >= $1
                            AND time_stamp < $2
                        """

                    else:

                        yesterday_query = None

                    if yesterday_query:

                        try:

                            yest_data = await session.fetchrow(
                                yesterday_query,
                                today - timedelta(days=1),
                                today
                            )

                        except Exception as e:

                            print(f"YESTERDAY QUERY ERROR ({key}):", e)

                            yest_data = None

                # ---------------- MONTH ---------------- #

                if "month" in requested_resolutions:

                    try:

                        month_query = build_query(month_table)

                        if month_query:

                            month_data = await session.fetchrow(month_query)

                    except Exception as e:

                        print("MONTH TABLE ERROR:", e)

                # ---------------- YEAR ---------------- #

                if "year" in requested_resolutions:

                    try:

                        year_query = build_query(year_table)

                        if year_query:

                            year_data = await session.fetchrow(year_query)

                    except Exception as e:

                        print("YEAR TABLE ERROR:", e)

                # ---------------- TOTAL ---------------- #

                if "total" in requested_resolutions:

                    try:

                        total_query = build_query(total_table)

                        if total_query:

                            total_data = await session.fetchrow(total_query)

                    except Exception as e:

                        print("TOTAL TABLE ERROR:", e)

                # ---------------- FORMAT ---------------- #

                def format_value(row):

                    if not row:
                        return None

                    if row["t1"] is None:
                        return None

                    try:
                        return round(float(row["t1"]), 2)

                    except:
                        return row["t1"]

                # ---------------- APPEND ---------------- #

                if "today" in requested_resolutions:

                    today_list.append({
                        "key": alias,
                        "value": format_value(today_data),
                        "unit": unit
                    })

                if "yesterday" in requested_resolutions:

                    yesterday_list.append({
                        "key": alias,
                        "value": format_value(yest_data),
                        "unit": unit
                    })

                if "month" in requested_resolutions:

                    month_list.append({
                        "key": alias,
                        "value": format_value(month_data),
                        "unit": unit
                    })

                if "year" in requested_resolutions:

                    year_list.append({
                        "key": alias,
                        "value": format_value(year_data),
                        "unit": unit
                    })

                if "total" in requested_resolutions:

                    total_list.append({
                        "key": alias,
                        "value": format_value(total_data),
                        "unit": unit
                    })

            # ---------------- FINAL RESPONSE ---------------- #

            data = {}

            if "today" in requested_resolutions:
                data["today"] = today_list

            if "yesterday" in requested_resolutions:
                data["yesterday"] = yesterday_list

            if "month" in requested_resolutions:
                data["month"] = month_list

            if "year" in requested_resolutions:
                data["year"] = year_list

            if "total" in requested_resolutions:
                data["total"] = total_list

        return {
            "response": constant.SUCCECS_CODE,
            "data": data
        }

    except Exception as e:

        print(e)

        return {
            "response": constant.ERROR_CODE,
            "message": "Something Went Wrong"
        }