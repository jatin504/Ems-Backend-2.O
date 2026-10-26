from quart import Blueprint

bess_control_bp = Blueprint("bess_control_bp", __name__, url_prefix="/api/bess_control")