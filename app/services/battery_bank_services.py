from typing import List, Dict, Any
import logging
import re
from collections import defaultdict
from device_config_helper import DeviceConfigHelper
from app.utils.db_utils import DB

logger = logging.getLogger(__name__)

class BatteryBankService:
    def __init__(self):
        self.device_helper = DeviceConfigHelper()

    def extract_ess_group_key(self, device_id: str) -> str:
        match = re.search(r"(ess_\d)(?!\w)", device_id)
        return match.group(1) if match else None

    async def get_battery_banks_data(self) -> List[Dict[str, Any]]:
        try:
            ess_device_ids = self.device_helper.get_device_ids_by_type_pattern("ess")
            if not ess_device_ids:
                logger.info("No ESS devices found in config.")
                return []

            grouped_devices = defaultdict(list)
            for device_id in ess_device_ids:
                group_key = self.extract_ess_group_key(device_id)
                if group_key:
                    grouped_devices[group_key].append(device_id)
                else:
                    logger.warning(f"Device ID '{device_id}' does not match ESS group pattern.")

            logger.info(f"Grouped ESS devices: {dict(grouped_devices)}")

            battery_banks = []

            for idx, (group_key, device_ids) in enumerate(sorted(grouped_devices.items()), start=1):
                telemetry_values = {
                    "soc": [],
                    "voltage": [],
                    "amps": [],
                    "temperature": []
                }
                timestamps = []

                for device_id in device_ids:
                    query = f"""
                    SELECT soc, voltage, amps, temperature, time_stamp
                    FROM {device_id}
                    ORDER BY time_stamp DESC
                    LIMIT 1
                    """
                    async with DB() as db:
                        try:
                            rows = await db.fetch(query)
                        except Exception as db_err:
                            logger.warning(f"DB error for {device_id}: {db_err}")
                            continue

                    if rows:
                        row = rows[0]
                        for key in telemetry_values:
                            val = row.get(key)
                            if val is not None:
                                telemetry_values[key].append(float(val))
                        ts = row.get("time_stamp")
                        if ts:
                            timestamps.append(ts)

                def avg(values):
                    return round(sum(values) / len(values), 2) if values else None

                battery_banks.append({
                    "name": f"Battery Bank {idx}",
                    "soc": avg(telemetry_values["soc"]),
                    "voltage": avg(telemetry_values["voltage"]),
                    "amps": avg(telemetry_values["amps"]),
                    "temperature": avg(telemetry_values["temperature"]),
                    "device_time_stamps": [t.isoformat() for t in timestamps]
                })

            return battery_banks

        except Exception as e:
            logger.error(f"Error fetching battery bank data: {str(e)}", exc_info=True)
            raise

    async def get_battery_details(self, pattern) -> List[Dict[str, Any]]:
        try:
            ess_device_ids = self.device_helper.get_device_ids_by_type_pattern(pattern)
            if not ess_device_ids:
                logger.info("No ESS devices found in config.")
                return []
            battery_banks = []

            grouped_devices = defaultdict(list)
            
            for device_id in ess_device_ids:
                # import pdb; pdb.set_trace()
                
                telemetry_values = {
                    "soc": [],
                    "voltage": [],
                    "soh": [],
                    "dod": [],
                    "amps": [],
                    "temperature": []
                }
                timestamps = []

                query = f"""
                SELECT soc, soh, dod, voltage, amps, temperature, time_stamp
                FROM {device_id}
                ORDER BY time_stamp DESC
                LIMIT 1
                """
                async with DB() as db:
                    try:
                        rows = await db.fetch(query)
                    except Exception as db_err:
                        logger.warning(f"DB error for {device_id}: {db_err}")
                        continue

                if rows:
                    row = rows[0]
                    for key in telemetry_values:
                        val = row.get(key)
                        if val is not None:
                            telemetry_values[key].append(float(val))
                    ts = row.get("time_stamp")
                    if ts:
                        timestamps.append(ts)

                def avg(values):
                    return round(sum(values) / len(values), 2) if values else None

                battery_banks.append({
                    # "name": f"Battery Bank {idx}",
                    "soc": avg(telemetry_values["soc"]),
                    "dod": avg(telemetry_values["dod"]),
                    "voltage": avg(telemetry_values["voltage"]),
                    "soh": avg(telemetry_values["soh"]),
                    "amps": avg(telemetry_values["amps"]),
                    "temperature": avg(telemetry_values["temperature"]),
                    "device_time_stamps": [t.isoformat() for t in timestamps]
                })

            return battery_banks

        except Exception as e:
            logger.error(f"Error fetching battery bank data: {str(e)}", exc_info=True)
            raise
