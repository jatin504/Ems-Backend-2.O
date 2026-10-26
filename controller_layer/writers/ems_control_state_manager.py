from repository import set_ems_status
from .ems_modbus_server import EMSModbus
from .file_writer import FileService
from model import get_async_connection
from ..config.ems_flag_status_map import EMS_Flag_Register_Mapping

class EMSControlStateManager:

    @staticmethod
    async def raise_flag(flag_name, value):
        file = FileService()
        ems_server = EMSModbus.get_instance()

        # 1. DB
        # pool = await get_async_connection()
        # async with pool.acquire() as conn:

        await set_ems_status(flag_name, True if value == 1 else False)
            
        # await pool.close()

        # 2. JSON
        await file.update_flag(flag_name, value)

        # 3. Modbus
        await ems_server.write_flag(EMS_Flag_Register_Mapping[flag_name]["register"], EMS_Flag_Register_Mapping[flag_name]["bit_position"], value)

    # async def update_setpoint(self, strategy, setpoint_kw):
    #     # 1. DB
    #     await self.db.insert_setpoint(strategy, setpoint_kw)

    #     # 2. JSON
    #     await self.file.write_status({"last_setpoint": setpoint_kw})

    #     # 3. Modbus
    #     await self.modbus.write_setpoint(setpoint_kw)
