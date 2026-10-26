from quart import Blueprint, jsonify
import logging
from app.services.battery_bank_services import BatteryBankService

logger = logging.getLogger(__name__)

# Create a Blueprint for battery bank routes
battery_bank_bp = Blueprint("battery_bank_bp", __name__, url_prefix="/api/battery-banks")

# Initialize the service
battery_bank_service = BatteryBankService()

@battery_bank_bp.route("/<device_id>")
async def get_battery_details(device_id):
    """
    Endpoint to fetch data for all battery banks in the system.
    
    Returns:
        JSON response containing an array of battery bank data objects
    """
    try:
        logger.info("Fetching battery bank data...")
        battery_banks = await battery_bank_service.get_battery_details(device_id + str("_rack_rack_1"))
        
        response = {
            "success": True,
            "count": len(battery_banks),
            "data": battery_banks,
            "timestamp": "2025-07-23T11:05:00Z"  # This should be current timestamp in production
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error in get_battery_banks: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Failed to fetch battery bank data",
            "details": str(e)
        }), 500


@battery_bank_bp.route("/", methods=["GET"])
async def get_battery_banks():
    """
    Endpoint to fetch data for all battery banks in the system.
    
    Returns:
        JSON response containing an array of battery bank data objects
    """
    try:
        logger.info("Fetching battery bank data...")
        battery_banks = await battery_bank_service.get_battery_banks_data()
        
        response = {
            "success": True,
            "count": len(battery_banks),
            "data": battery_banks,
            "timestamp": "2025-07-23T11:05:00Z"  # This should be current timestamp in production
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error in get_battery_banks: {str(e)}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Failed to fetch battery bank data",
            "details": str(e)
        }), 500
        
        
