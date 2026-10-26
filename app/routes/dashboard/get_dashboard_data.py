from quart import Blueprint, request, jsonify
import asyncio
import logging
from app.services.dashboard.execute_widget import execute_widget

logger = logging.getLogger(__name__)

get_dashboard_data_bp = Blueprint("get_dashboard_data_bp", __name__)


@get_dashboard_data_bp.route('', methods=['POST'])
async def get_dashboard_data():
    dashboard_config = await request.get_json()
    # logger.info(dashboard_config)
    widgets = dashboard_config.get("config", {}).get("widgets", {})

    # Execute all widgets concurrently
    tasks = [execute_widget(widget_id, config) for widget_id, config in widgets.items()]
    results = await asyncio.gather(*tasks)

    # Combine results using returnDataName as key
    response = {}
    for result in results:
        if result and "returnDataName" in result:
            response[result["returnDataName"]] = result["data"]

    return jsonify(response)