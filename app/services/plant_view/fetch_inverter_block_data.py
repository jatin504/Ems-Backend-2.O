import re
from datetime import datetime, timedelta
 
from app.utils.db_utils import DB
from device_config_helper import DeviceConfigHelper
 
 
async def fetch_inverter_block_data(plant_id):
 
    helper = DeviceConfigHelper()
 
    # all inverter devices
    inverter_devices = helper.get_device_ids_by_type_pattern(
        "inverter_inverter"
    )
 
    if not inverter_devices:
        return []
 
    block_map = {}
 
    # =========================
    # GROUP DEVICES BY BLOCK
    # =========================
    for device in inverter_devices:
 
        # example:
        # plant_1_block_1_inverter_inverter_1
 
        match = re.search(r"(block_\d+)", device)
 
        if not match:
            continue
 
        block_key = match.group(1)
 
        if block_key not in block_map:
            block_map[block_key] = []
 
        block_map[block_key].append(device)
 
    final_data = []
 
    from_date = datetime.now().date()
    to_date = from_date + timedelta(days=1)
 
    async with DB() as session:
 
        for block_name, devices in block_map.items():
 
            connected = 0
 
            for device in devices:
 
                try:
 
                    row = await session.fetchrow(
                        f"""
                        SELECT time_stamp
                        FROM {device}
                        WHERE time_stamp >= $1
                        AND time_stamp < $2
                        LIMIT 1
                        """,
                        from_date,
                        to_date,
                    )
 
                    if row:
                        connected += 1
 
                except Exception:
                    pass
 
            final_data.append(
                {
                    "block_name": block_name.replace("_", " ").title(),
                    "connected": connected,
                    "total_device": len(devices),
                    "devices": devices,
                }
            )
 
    return final_data
 