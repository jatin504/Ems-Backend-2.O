
from LogColor import LogColor
import logging

logger = logging.getLogger(__name__)


# def dicharge_power_balancing_startegy(
#         resolver,
#         pcs_device_ids,
#         min_soc
# ):
#     """
#     This Strategy is used to balance the soc among all the PCS in the plant. 
#     It will try to maintain the soc based on health score and respond with set Lower limit SoC for each PSC.

#     return => [id] : {health_score, soc, temperature, soh, soc_lower_cap}
#     """
#     logger.info(f"{LogColor.MAGENTA} SOC Balancing (POWER-BASED) STARTED {LogColor.RESET}")

#     # Get the list of all PCS in the plant
#     # pcs_device_ids = ["plant_1_pcs_2_bank_bank_1", "plant_1_pcs_2_bank_bank_2", "plant_1_pcs_2_bank_bank_3", "plant_1_pcs_2_bank_bank_4"]
#     # Participation Model
#     # Pi​=w1​Hi​+w2​Ti​+w3​Ei​
#     # w1​+w2​+w3​=1
#     soc_w=0.4
#     temp_w=0.2
#     soh_W=0.4

#     ideal_temp = 25  # PCS operates better at lower temperature

#     # Get Input Data for all pcs
#     pcs_data = {}

#     for id in pcs_device_ids:
#         device_data = resolver.get_latest_device_data(id)
#         if device_data is not None:
#                 # Get the values
#                 soc = device_data.get("soc").get("value", 70)
#                 temp = device_data.get("temperature").get("value", 30)
#                 soh = device_data.get("soh").get("value", 98)

#                 # Calculate Health Score
#                 health_score = soc_w * (soc / 100) + temp_w * (1 - ((temp - ideal_temp) / ideal_temp)) + soh_W * (soh / 100)

#                 # Calculate Lower cap Energy based on health score and capacity
#                 pcs_data[id] = {
#                     "health_score": health_score,
#                     "soc": soc,
#                     "temperature": temp,
#                     "soh": soh,
#                     "soc_lower_cap": min_soc + (1 - health_score) * soc   # SoC - ((health_score * SoC) - min_soc)
#                 }
#         else:
#             logger.warning(f"No data found for device {id}. Assigning default health score.")
#             pcs_data[id] = {
#                 "health_score": 0,
#                 "soc": 0,
#                 "temperature": 0,
#                 "soh": 0,
#                 "soc_lower_cap": min_soc
#             }

#     return pcs_data

def discharge_power_balancing_strategy(
        resolver,
        pcs_device_ids,
        min_soc,
        skew_limit=0.15
):
    """
    Discharge balancing strategy with participation skew limiting.

    skew_limit = 0.15 means:
    weakest PCS cannot be more than 15%
    below healthiest PCS.

    Returns:
    {
        pcs_id: {
            health_score,
            normalized_score,
            participation_percent,
            soc,
            temperature,
            soh,
            soc_lower_cap
        }
    }
    """

    logger.info(
        f"{LogColor.MAGENTA} "
        f"SOC Balancing (POWER-BASED) STARTED "
        f"{LogColor.RESET}"
    )

    # -----------------------------
    # CONFIG
    # -----------------------------

    soc_w = 0.4
    temp_w = 0.2
    soh_w = 0.4

    ideal_temp = 25
    temp_span = 40

    pcs_data = {}

    # -----------------------------
    # STEP-1 : Compute health score
    # -----------------------------

    max_health_score = 0

    for pcs_id in pcs_device_ids:

        device_data = resolver.get_latest_device_data(pcs_id)

        if device_data is None:

            logger.warning(f"No data found for device {pcs_id}")

            pcs_data[pcs_id] = {
                "health_score": 0,
                "soc": 0,
                "temperature": 0,
                "soh": 0
            }

            continue

        soc = device_data.get("soc", {}).get("value", 70)
        temp = device_data.get("temperature", {}).get("value", 30)
        soh = device_data.get("soh", {}).get("value", 98)

        # -----------------------------
        # Normalize factors
        # -----------------------------

        soc_factor = soc / 100

        thermal_factor = 1 - ((temp - ideal_temp) / temp_span)
        thermal_factor = max(0, min(thermal_factor, 1))

        soh_factor = soh / 100

        # -----------------------------
        # Health score
        # -----------------------------

        health_score = (
            soc_w * soc_factor +
            temp_w * thermal_factor +
            soh_w * soh_factor
        )

        max_health_score = max(
            max_health_score,
            health_score
        )

        pcs_data[pcs_id] = {
            "health_score": health_score,
            "soc": soc,
            "temperature": temp,
            "soh": soh
        }

    # -----------------------------
    # STEP-2 : Apply skew limiting
    # -----------------------------

    minimum_allowed_score = (
        max_health_score * (1 - skew_limit)
    )

    for pcs_id, data in pcs_data.items():

        original_score = data["health_score"]

        # Lift low-score PCS
        adjusted_score = max(
            original_score,
            minimum_allowed_score
        )

        data["adjusted_health_score"] = adjusted_score

    # -----------------------------
    # STEP-3 : Normalize participation
    # -----------------------------

    total_adjusted_score = sum(
        d["adjusted_health_score"]
        for d in pcs_data.values()
    )

    if total_adjusted_score == 0:
        total_adjusted_score = 1

    for pcs_id, data in pcs_data.items():

        normalized_score = (
            data["adjusted_health_score"]
            / total_adjusted_score
        )

        participation_percent = (
            normalized_score * 100
        )

        health_score = data["adjusted_health_score"]

        soc = data["soc"]

        # Lower SOC cap
        soc_lower_cap = (
            min_soc +
            (1 - health_score) * soc
        )

        data["normalized_score"] = round(
            normalized_score,
            4
        )

        data["participation_percent"] = round(
            participation_percent,
            2
        )

        data["soc_lower_cap"] = round(
            soc_lower_cap,
            2
        )

    return pcs_data

def charge_power_balancing_strategy(
        resolver,
        pcs_device_ids,
        max_soc
):
    """
    Charging participation balancing strategy.

    Returns:
    {
        pcs_id: {
            "soc": float,
            "temperature": float,
            "soh": float,
            "soc_deficit": float,
            "soc_need_factor": float,
            "thermal_factor": float,
            "soh_factor": float,
            "participation_weight": float,
            "allocated_power_kw": float
        }
    }
    """

    logger.info(
        f"{LogColor.MAGENTA} SOC Balancing (POWER-BASED) STARTED {LogColor.RESET}"
    )

    # -----------------------------
    # CONFIGURATION
    # -----------------------------

    required_power_kw = 16 * 1000  # 16 MW

    target_soc = 80

    # Participation weights
    soc_w = 0.5
    temp_w = 0.2
    soh_w = 0.2
    availability_w = 0.1

    ideal_temp = 25
    temp_span = 40

    availability = 1

    # -----------------------------
    # STEP-1 : Collect telemetry
    # -----------------------------

    pcs_data = {}

    total_soc_deficit = 0

    for pcs_id in pcs_device_ids:

        device_data = resolver.get_latest_device_data(pcs_id)

        if device_data is None:

            logger.warning(f"No data found for device {pcs_id}")

            pcs_data[pcs_id] = {
                "soc": 0,
                "temperature": 0,
                "soh": 0,
                "soc_deficit": 0
            }

            continue

        soc = device_data.get("soc", {}).get("value", 70)
        temp = device_data.get("temperature", {}).get("value", 30)
        soh = device_data.get("soh", {}).get("value", 98)

        # -----------------------------
        # SOC deficit
        # -----------------------------

        soc_deficit = max(target_soc - soc, 0)

        total_soc_deficit += soc_deficit

        pcs_data[pcs_id] = {
            "soc": soc,
            "temperature": temp,
            "soh": soh,
            "soc_deficit": soc_deficit
        }

    # Avoid divide-by-zero
    if total_soc_deficit == 0:
        total_soc_deficit = 1

    # -----------------------------
    # STEP-2 : Compute factors
    # -----------------------------

    total_weight = 0

    for pcs_id, data in pcs_data.items():

        soc = data["soc"]
        temp = data["temperature"]
        soh = data["soh"]
        soc_deficit = data["soc_deficit"]

        # -----------------------------
        # Normalize SOC need
        # -----------------------------

        soc_need_factor = soc_deficit / total_soc_deficit

        # -----------------------------
        # Thermal factor
        # -----------------------------

        thermal_factor = 1 - ((temp - ideal_temp) / temp_span)

        # Clamp between 0 and 1
        thermal_factor = max(0, min(thermal_factor, 1))

        # -----------------------------
        # SOH factor
        # -----------------------------

        soh_factor = soh / 100

        # Clamp
        soh_factor = max(0, min(soh_factor, 1))

        # -----------------------------
        # Participation weight
        # -----------------------------

        participation_weight = (
            soc_w * soc_need_factor +
            temp_w * thermal_factor +
            soh_w * soh_factor +
            availability_w * availability
        )

        data["soc_need_factor"] = round(soc_need_factor, 4)
        data["thermal_factor"] = round(thermal_factor, 4)
        data["soh_factor"] = round(soh_factor, 4)
        data["participation_weight"] = participation_weight

        total_weight += participation_weight

    # Avoid divide-by-zero
    if total_weight == 0:
        total_weight = 1

    # -----------------------------
    # STEP-3 : Normalize participation
    # -----------------------------

    for pcs_id, data in pcs_data.items():

        normalized_participation = (
            data["participation_weight"] / total_weight
        )

        allocated_power_kw = (
            normalized_participation * required_power_kw
        )

        data["participation_percent"] = round(
            normalized_participation * 100,
            2
        )

        data["allocated_power_kw"] = round(
            allocated_power_kw,
            2
        )

    return pcs_data