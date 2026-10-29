from quart import Quart
from dotenv import load_dotenv
from app.routes.stream_routes import stream_bp
from app.routes.latest_routes import latest_bp
from app.routes.history_routes import history_bp
from app.routes.audit_routes import audit_bp
from app.routes.strategy_routes import strategy_bp
from app.routes.get_timeseries_data import timeseries_bp
from app.routes.raw_data_routes import raw_data_bp
from app.routes.target_id_key_history_routes import target_id_key_history_bp
from app.routes.current_strategy_routes import current_strategy
from app.routes.strategy_configuration_api import strategy_configuration_bp
from app.routes.cycle_management_routes import cycle_management_bp
from app.routes.dashboard.save_dashboard import dashboard_bp
from app.routes.widget_template.get_widget_templates import widget_template_bp
from app.routes.dashboard.get_dashboard_data import get_dashboard_data_bp

from app.routes.backend_health_logs import backend_health_logs_bp

from app.routes.policy_routes import bp as policy_bp
from app.routes.permissions_routes import bp as permissions_bp
from app.routes.report_delivery_log_routes import report_delivery_log_bp
from app.routes.dashboard_logs_routes import dashboard_logs_bp

# health
from app.routes.health import health_bp

# alarms
from app.routes.alarms import alarms_bp
from app.routes.events_controller import events_bp

# Plant Config
from app.routes.plant_config_api import plant_config_bp
from app.routes.fault_routes import fault_bp

# Plant Card
from app.routes.plant_card.card_routes import plant_card_bp

# BESS Control
from app.routes.bess_control import bess_control_bp
from app.routes.bess_control import *

# DSM
from app.routes.dsm import dsm_summary_bp
from app.routes.dsm import *

# Load Forecast
from app.routes.load_forecast_routes import load_forecast_bp

# Latest Direct Data
from app.routes.lastest_direct_data.multiple_latest_devices import multiple_latest_devices_bp
from app.routes.lastest_direct_data.ems_status import ems_status_bp

# Devices Online
from app.routes.devices_online import online_devices_bp

from app.routes.application_actions import restart_bp
from app.utils.warming_up_db_connection import warm_up_db_connection
from app.routes.device import device
from app.routes.users_routes import users_bp
from app.routes.login_routes import login_bp
from app.routes.model_data import model_data_bp
from app.routes.report_routes import report_bp
from app.routes.device_configs.device_library_routes import device_library
from app.sse import broadcaster

from app.routes.plany_view_routes import plant_view_bp
from app.routes.dsm_routes import dsm_bp

from app.routes.tinydb_routes import tinydb_bp

from app.routes.ems_recommendation import ems_recommendation_bp
from app.routes.strategy_log_controller import strategy_logs_bp, setpoint_writes_bp
from app.routes.timeseries_resolution import resolution_bp
from app.routes.trend_routes import trend
from app.routes.market_config_routes import market_config_bp
from app.routes.invoice_api import invoice_bp

from app.routes.notification_routes import notification_bp

from app.routes.twin_schedule_execute import twin_schedule_exec_bp

from app.middleware.compress_response import compress_response
from app.middleware.api_scaling import configure_api_scaling
from app.middleware.cors_config import configure_cors
import logging
import os
import json
import sys

from quart import Quart
from quart import request
from app.utils import constant
from utils.db_config import db_config

load_dotenv()  # Loads .env into environment variables

# logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    stream=sys.stdout,
    force=True,
)

def create_app():
    app = Quart(__name__)
    configure_api_scaling(app)
    # Enable compression
    # compress_response(app)

    configure_cors(app)

    @app.route("/ping")
    async def health():
        return [200]


    app.db_config = db_config()

    app.broadcaster = broadcaster
    app.register_blueprint(stream_bp, url_prefix="/stream")
    app.register_blueprint(latest_bp, url_prefix="/latest")
    app.register_blueprint(history_bp, url_prefix="/history")
    app.register_blueprint(target_id_key_history_bp, url_prefix="/target_id_key_history")

    # Plant Config
    app.register_blueprint(plant_config_bp, url_prefix="/api")
    app.register_blueprint(fault_bp, url_prefix="/api/faults")
    app.register_blueprint(policy_bp)
    app.register_blueprint(permissions_bp)

    # Report Delivery Logs
    app.register_blueprint(report_delivery_log_bp, url_prefix="/api") 
    app.register_blueprint(audit_bp)

    app.register_blueprint(health_bp)

    # Alarms
    app.register_blueprint(alarms_bp, url_prefix="/api/alarms")
    app.register_blueprint(events_bp, url_prefix="/api/events")

    # Plant veiw Dashboard
    app.register_blueprint(plant_view_bp)

    # Plant Card
    app.register_blueprint(plant_card_bp)

    # BESS Control
    app.register_blueprint(bess_control_bp)

    # DSM
    app.register_blueprint(dsm_summary_bp)

    # Latest Direct Data
    app.register_blueprint(multiple_latest_devices_bp)
    app.register_blueprint(ems_status_bp)

    # Online Devices
    app.register_blueprint(online_devices_bp)

    # DSM Dashboard
    app.register_blueprint(dsm_bp)

    # Time series
    app.register_blueprint(timeseries_bp)
    # Raw history has its own endpoint
    app.register_blueprint(raw_data_bp)

    # Energy Strategy Routes
    app.register_blueprint(strategy_bp)

    app.register_blueprint(strategy_configuration_bp, url_prefix="/strategy")
    app.register_blueprint(cycle_management_bp, url_prefix="/cycle-management")


    # Current strategy Routes
    app.register_blueprint(current_strategy, url_prefix="/currentStrategy")

    # Config
    app.register_blueprint(device)
    app.register_blueprint(device_library)
    app.register_blueprint(model_data_bp)

    # Users
    app.register_blueprint(users_bp)
    app.register_blueprint(login_bp)

    # notification
    app.register_blueprint(notification_bp, url_prefix="/api/notification")

    # recommendation
    app.register_blueprint(ems_recommendation_bp)

    # load forecast
    app.register_blueprint(load_forecast_bp)

    # Dahsboard
    app.register_blueprint(dashboard_bp, url_prefix="/dashboard")
    app.register_blueprint(get_dashboard_data_bp, url_prefix="/get-dashboard-data")
    app.register_blueprint(dashboard_logs_bp)


    # Application Actions
    app.register_blueprint(restart_bp, url_prefix="/app")

    # Widget Templates
    app.register_blueprint(widget_template_bp, url_prefix="/widget-template")

    # Report 
    app.register_blueprint(report_bp, url_prefix="/api/reports") 

    # TinyDB
    app.register_blueprint(tinydb_bp, url_prefix="/tinydb")

    app.register_blueprint(strategy_logs_bp, url_prefix="/api/strategy")
    app.register_blueprint(setpoint_writes_bp)
    app.register_blueprint(resolution_bp)

    # Twin Schedule Execution
    app.register_blueprint(twin_schedule_exec_bp)

    # Trendz App
    app.register_blueprint(trend)
               
    # Invoice API
    app.register_blueprint(invoice_bp, url_prefix="/api/invoices")

    app.register_blueprint(market_config_bp, url_prefix="/api")

    app.register_blueprint(
        backend_health_logs_bp,
        url_prefix="/api/backend-health"
    )

    
    @app.before_serving
    async def startup():
        # await broadcaster.start()
        await broadcaster.start_cleanup_task()
        # Read workers must not write the shared status JSON during startup.
        await warm_up_db_connection(write_health_flag=app.config["EMS_API_ROLE"] == "primary")

    return app, broadcaster
