import json
import logging
from app.utils.db_utils import DB
from app.services.table_record_transform.transforms import timeseries_records_to_nested_dict

logger = logging.getLogger(__name__)

async def execute_widget(widget_id: str, widget_config: dict):
    data_source = widget_config.get("dataSource", {})
    executor_type = data_source.get("executorType")
    function_name = data_source.get("execute")
    return_data_name = data_source.get("returnDataName")

    # Only support sql_function for now
    if executor_type != "sql_function" or not function_name:
        return {"returnDataName": return_data_name, "data": None, "error": "Unsupported executor"}

    # logger.info(function_name, return_data_name)
    # Build input for DB function (example structure)
    # db_args = build_function_parameters(widget_config)

    # Execute PostgreSQL function async
    async with DB() as db:
        try:
            # Call function: SELECT * FROM fn_get_timeseries_data($1)
            result = await db.fetch(f"SELECT * FROM {function_name}($1::jsonb)", json.dumps(widget_config))
            # logger.info(f"result: {result}")
            # Assume function returns a single row with JSON/JSONB column `data`
            # data = result[0]['data'] if result else []
            return {"returnDataName": return_data_name, "data": timeseries_records_to_nested_dict(result)}
        except Exception as e:
            logger.error(f"Widget {widget_id} failed: {e}")
            return {"returnDataName": return_data_name, "data": None, "error": str(e)}
        