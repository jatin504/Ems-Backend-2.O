from quart import request, jsonify, Blueprint
from app.utils import constant
from app.services.dsm.fetch_tariff_by_state_code import fetch_tariff_by_state_code

dsm_bp = Blueprint("dsm_bp", __name__, url_prefix="/api/dsm")


@dsm_bp.route("/get_tariff", methods=["POST"])
async def battery_bank_data():
        data = await request.get_json()
        res = await fetch_tariff_by_state_code(data.get('state_code','MH'))
        return {'response':constant.SUCCECS_CODE,'data':res}