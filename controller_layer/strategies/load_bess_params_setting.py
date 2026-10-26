import json
from pathlib import Path

from runtime_paths import DataPaths

def load_bess_params_setting(json_path: str = "controller_layer/strategies/bess_params_setting.json") -> dict:
    """
    Load BESS parameter settings from a JSON file.

    Args:
        json_path (str): Path to the JSON file.

    Returns:
        dict: Dictionary containing BESS parameters.
    """
    file_path = DataPaths.resolve(json_path)

    if not file_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    with open(file_path, "r") as f:
        data = json.load(f)

    # Extract "bess_params" safely
    bess_params = data.get("bess_params")
    if bess_params is None:
        raise ValueError("Missing 'bess_params' field inside JSON file.")

    return bess_params

def get_c_rate(temp, data, voltage=None):
    try:
        temp_c_rate = None
        voltage_c_rate = None

        # Get C-rate based on temperature
        for entry in data["c_rate_by_temperature"]:
            if entry["min_temp"] <= temp < entry["max_temp"]:
                temp_c_rate = entry["c_rate"]
                break

        # Get C-rate based on cell voltage
        if voltage is not None:
            for entry in data["c_rate_by_cell_voltage"]:
                if entry["min_voltage"] <= voltage < entry["max_voltage"]:
                    voltage_c_rate = entry["c_rate"]
                    break

        # Collect available C-rates
        c_rates = []

        if temp_c_rate is not None:
            c_rates.append(temp_c_rate)

        if voltage_c_rate is not None:
            c_rates.append(voltage_c_rate)

        # Choose the minimum C-rate
        if c_rates:
            return min(c_rates)

        # Default C-rate if no matching range is found
        return 0.5

    except Exception as e:
        print(f"Error [Getting C-Rate]: {e}")
        return 0.5
