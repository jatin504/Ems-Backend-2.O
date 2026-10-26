from quart import Blueprint
from quart import request
from app.services.plant_view.plant_card_service import plant_card_service

# Create a blueprint
plant_card_bp = Blueprint("plant_card_bp", __name__, url_prefix="/api/plant_card")
@plant_card_bp.route('/',methods=['POST'])
async def plant_card():
    data = await request.get_json()
    res = await plant_card_service(data)
    return res 