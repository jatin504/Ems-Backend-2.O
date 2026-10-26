from typing import List, Dict, Any
import logging
from config.device_config_resolver import DeviceConfigResolver
from app.utils.db_utils import DB
import json
from pathlib import Path
from runtime_paths import DataPaths

logger = logging.getLogger(__name__)


PlantArchitecturePath = Path(
    DataPaths.resolve("plant_architecture")
)


def _find_device_file(device_id: str) -> Path:
    matches = list(
        PlantArchitecturePath.rglob(f"{device_id}.json")
    )

    if not matches:
        raise FileNotFoundError(
            f"No device file found for device ID: {device_id}"
        )

    return matches[0]


class DeviceService:
    def __init__(self):
        self.device_config_obj = DeviceConfigResolver()

    def get_config(self):
        return self.device_config_obj.config

    async def get_device_data(
        self,
        device_ids: List[str],
        params: List[str],
        dateType: str,
        frequency: str,
        interval: str,
        startTs: str | None = None,
        endTs: str | None = None,
    ) -> List[Dict[str, Any]]:

        if not device_ids or not params:
            return []

        interval_seconds_map = {
            "1min": 60,
            "15min": 15 * 60,
            "1hr": 60 * 60,
            "1day": 24 * 60 * 60,
        }
        interval_seconds = interval_seconds_map.get(interval, 15 * 60)

        # ---------------- DB ANCHOR ----------------
        async with DB() as db:
            rows = await db.fetch(
                f"SELECT MAX(time_stamp) AS anchor_ts FROM {device_ids[0]}"
            )

        anchor_ts = rows[0]["anchor_ts"] if rows else None
        if not anchor_ts:
            return []

        # ---------------- HISTORY ----------------
        if dateType == "history":
            if not startTs or not endTs:
                raise ValueError("startTs and endTs required for history")

            ctes = []
            joins = []
            select_cols = ["base.timestamp AS timestamp"]
            
            for idx, device_id in enumerate(device_ids):
                cte_name = f"d{idx}"
                prefixed_params = [f"{p} AS {device_id}_{p}" for p in params]
                
                ctes.append(f"""
                    {cte_name} AS (
                        SELECT
                            time_stamp AS timestamp,
                            {", ".join(prefixed_params)}
                        FROM {device_id}
                        WHERE time_stamp BETWEEN '{startTs}'::timestamp AND '{endTs}'::timestamp
                    )
                """)
                
                if idx == 0:
                    joins.append(f"{cte_name} base")  # ✅ Fixed: removed invalid ON TRUE
                else:
                    joins.append(f"FULL OUTER JOIN {cte_name} ON base.timestamp = {cte_name}.timestamp")
                
                for p in params:
                    select_cols.append(f"{device_id}_{p}")

            query = f"WITH {', '.join(ctes)} SELECT {', '.join(select_cols)} FROM {' '.join(joins)} ORDER BY base.timestamp ASC"

        # ---------------- LIVE + RELATIVE LONG RANGE ----------------
        elif dateType in ("live", "relative") and frequency in (
            "current_day_so_far", "daily",
            "current_week_so_far", "weekly",
            "current_month_so_far", "monthly",
            "quarterly",
        ):
            now_expr = "NOW()"

            if frequency in ("current_day_so_far", "daily"):
                start_cond = f"date_trunc('day', {now_expr})"
                end_cond = now_expr
            elif frequency in ("current_week_so_far", "weekly"):
                start_cond = f"date_trunc('week', {now_expr})"
                end_cond = now_expr
            elif frequency in ("current_month_so_far", "monthly"):
                start_cond = f"date_trunc('month', {now_expr})"
                end_cond = now_expr
            elif frequency == "quarterly":
                start_cond = f"date_trunc('quarter', {now_expr})"
                end_cond = now_expr

            ctes = []
            joins = []
            select_cols = ["base.timestamp AS timestamp"]
            
            for idx, device_id in enumerate(device_ids):
                cte_name = f"d{idx}"
                prefixed_params = [f"{p} AS {device_id}_{p}" for p in params]
                
                ctes.append(f"""
                    {cte_name} AS (
                        SELECT
                            time_stamp AS timestamp,
                            {", ".join(prefixed_params)}
                        FROM {device_id}
                        WHERE time_stamp BETWEEN {start_cond} AND {end_cond}
                    )
                """)
                
                if idx == 0:
                    joins.append(f"{cte_name} base")  # ✅ Fixed
                else:
                    joins.append(f"FULL OUTER JOIN {cte_name} ON base.timestamp = {cte_name}.timestamp")
                
                for p in params:
                    select_cols.append(f"{device_id}_{p}")

            query = f"WITH {', '.join(ctes)} SELECT {', '.join(select_cols)} FROM {' '.join(joins)} ORDER BY base.timestamp ASC"

        # ---------------- LIVE SHORT RANGE (last_1_hour) ----------------
        elif dateType == "live" and frequency == "last_1_hour":

            series_start = "NOW() - interval '1 hour'"
            series_end = "NOW()"

            ctes = []
            joins = []
            select_cols = ["base.timestamp AS timestamp"]

            for idx, device_id in enumerate(device_ids):
                cte_name = f"d{idx}"

                prefixed_params = [
                    f"{p} AS {device_id}_{p}" for p in params
                ]

                ctes.append(f"""
                    {cte_name} AS (
                        SELECT
                            time_stamp AS timestamp,
                            {", ".join(prefixed_params)}
                        FROM {device_id}
                        WHERE time_stamp BETWEEN {series_start} AND {series_end}
                    )
                """)

                if idx == 0:
                    joins.append(f"{cte_name} base")
                else:
                    joins.append(
                        f"FULL OUTER JOIN {cte_name} "
                        f"ON base.timestamp = {cte_name}.timestamp"
                    )

                for p in params:
                    select_cols.append(f"{device_id}_{p}")

            query = f"""
                WITH {', '.join(ctes)}
                SELECT {', '.join(select_cols)}
                FROM {' '.join(joins)}
                ORDER BY base.timestamp ASC
            """

        else:
            raise ValueError(f"Unsupported combination: {dateType}, {frequency}")

        # ---------------- EXECUTE ----------------
        logger.info("Final Query:\n%s", query)

        async with DB() as db:
            rows = await db.fetch(query)

        formatted_rows = []

        for row in rows:
            row_dict = dict(row)

            formatted_row = {
                "timestamp": row_dict.get("timestamp")
            }

            for device_id in device_ids:

                # JSON load for units
                device_json = {}

                try:

                    # only for unit json lookup
                    lookup_device_id = (
                        device_id
                        .replace("_15_min", "")
                        .replace("_30_min", "")
                        .replace("_1_hour", "")
                        .replace("_1_day", "")
                        .replace("_1_week", "")
                        .replace("_1_month", "")
                        .replace("_1_year", "")
                    )

                    device_file = _find_device_file(lookup_device_id)

                    with device_file.open("r", encoding="utf-8") as f:
                        device_json = json.load(f)

                except Exception as e:
                    logger.warning(
                        f"Unit file load failed for {device_id}: {e}"
                    )

                for param in params:

                    db_key = f"{device_id}_{param}"

                    unit = None

                    # fetch unit from plant_architecture json using normalized keys
                    for json_key, json_value in device_json.items():

                        normalized_json_key = (
                            str(json_key)
                            .lower()
                            .replace(" ", "_")
                        )

                        normalized_param = (
                            str(param)
                            .lower()
                            .replace(" ", "_")
                        )

                        if (
                            normalized_json_key == normalized_param and
                            isinstance(json_value, dict)
                        ):
                            unit = json_value.get("unit")
                            break

                    formatted_row[db_key] = row_dict.get(db_key)

                    formatted_row[f"{db_key}_unit"] = unit

            formatted_rows.append(formatted_row)

        return formatted_rows

    async def get_device_columns(self, device_ids: List[str]) -> List[str]:
        columns = []
        for device_id in device_ids:
            query = (
                "SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{device_id}'"
            )
            async with DB() as db:
                result = await db.fetch(query)
                columns.extend([row["column_name"] for row in result])
        return list(set(columns))