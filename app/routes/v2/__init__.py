"""Central registration for APIs served under /api/v2."""
from quart import Blueprint

from .multiple_latest_devices import multiple_latest_devices_v2_bp
from .stream_routes import stream_v2_bp

v2_bp = Blueprint("api_v2", __name__, url_prefix="/api/v2")
v2_bp.register_blueprint(multiple_latest_devices_v2_bp)
v2_bp.register_blueprint(stream_v2_bp)
