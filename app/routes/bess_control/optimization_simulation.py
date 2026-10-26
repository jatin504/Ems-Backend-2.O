from quart import Blueprint, jsonify, request
from app.utils.db_utils import DB

from controller_layer.strategies.bess_strategies import PeakShavingStrategy, ToUFlatStrategy, ToUSmartStrategy

from .init_bp import bess_control_bp

@bess_control_bp.route("/sim_optimize", methods=["POST"])
async def simulation_optimization():
    """
        In body pass the selected strategy
    """

    body = await request.get_json()

    try:
    
        if body.get("strategy") == 'peak_shaving':
            result = PeakShavingStrategy.simulate_daily_optimization('plant_1_pcs_controller_1')

        elif body.get("strategy") == 'tou_tariff':
            result = ToUFlatStrategy.simulate_tou_day('plant_1_pcs_controller_1')

        elif body.get("strategy") == 'cap_dynamic':
            result = await ToUSmartStrategy.simulate_dynamic_cap_day('plant_1_pcs_controller_1')

        return jsonify({"status": "success", "result": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


    