from quart import request, jsonify, Blueprint
from app.utils import constant
 
from app.services.plant_view.fetch_battery_bank_data import (
    fetch_battery_bank_data,
    fetch_battery_bank_data_v2
)
 
from app.services.plant_view.fetch_inverter_block_data import (
    fetch_inverter_block_data
)
 
plant_view_bp = Blueprint(
    "plant_view_bp",
    __name__,
    url_prefix="/api/plantview"
)
 
# BATTERY BANK
 
@plant_view_bp.route("/battery_bank", methods=["POST"])
async def battery_bank_data():
 
    data = await request.get_json()
 
    res = await fetch_battery_bank_data(
        data.get('plant_id', 'plant_1')
    )
 
    return {
        'response': constant.SUCCECS_CODE,
        'data': res
    }
 
# BATTERY BANK V2
 
@plant_view_bp.route("/v2/battery_bank", methods=["POST"])
async def battery_bank_data_v2():
 
    data = await request.get_json()
 
    res = await fetch_battery_bank_data_v2(
        data.get('plant_id', 'plant_1')
    )
 
    return {
        'response': constant.SUCCECS_CODE,
        'data': res
    }
 
# INVERTER BLOCK
 
@plant_view_bp.route("/inverter_block", methods=["POST"])
async def inverter_block_data():
 
    data = await request.get_json()
 
    res = await fetch_inverter_block_data(
        data.get("plant_id", "plant_1")
    )
 
    return {
        "response": constant.SUCCECS_CODE,
        "data": res,
    }
 