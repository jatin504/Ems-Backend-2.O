from quart import Blueprint

dsm_summary_bp = Blueprint("dsm_summary_bp", __name__, url_prefix="/api/dsm")