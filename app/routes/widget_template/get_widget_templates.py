from quart import Blueprint, request, jsonify
import json
from app.utils.db_utils import DB

widget_template_bp = Blueprint("widget_template_bp", __name__)


@widget_template_bp.route('/get', methods=['GET'])
async def get_widget_template():
    try:
        query = """
            SELECT * FROM widget_templates
        """
        async with DB() as db:
            result = await db.fetch(query)

        rows = [dict(record) for record in result]

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    return jsonify({"result": rows}), 200