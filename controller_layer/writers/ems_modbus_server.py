# ems_modbus_server.py

import asyncio
from pymodbus.server import StartAsyncTcpServer
from pymodbus.datastore import (
    ModbusSlaveContext,
    ModbusSequentialDataBlock,
    ModbusServerContext
)

class EMSModbus:
    _instance = None

    @staticmethod
    def get_instance():
        if EMSModbus._instance is None:
            EMSModbus()
        return EMSModbus._instance

    def __init__(self):
        if EMSModbus._instance is not None:
            raise Exception("EMSModbus is a singleton! Use get_instance()")

        # Initialize datastores
        self.store = ModbusSlaveContext(
            di=ModbusSequentialDataBlock(0, [0] * 100),
            co=ModbusSequentialDataBlock(0, [0] * 100),
            hr=ModbusSequentialDataBlock(0, [0] * 2000),     # Holding regs
            ir=ModbusSequentialDataBlock(0, [0] * 100)
        )

        self.context = ModbusServerContext(slaves={1: self.store}, single=False)
        EMSModbus._instance = self

    async def start(self):
        print("[EMS MODBUS] Starting Modbus TCP server on port 5020...")
        await StartAsyncTcpServer(
            context=self.context,
            address=("0.0.0.0", 5021)
        )

    # -----------------------------------------
    # WRITE FUNCTIONS
    # -----------------------------------------

    async def write_flag(self, register, bit_position: int, value: int):
        reg = self.store.getValues(3, register, count=1)[0]

        if value == 1:
            reg |= (1 << bit_position)
        else:
            reg &= ~(1 << bit_position)

        self.store.setValues(3, register, [reg])
        print(f"[EMS MODBUS] Flag updated: bit={bit_position}, value={value}")

    async def write_setpoint(self, kw: int):
        self.store.setValues(3, 1100, [int(kw)])
        print(f"[EMS MODBUS] Setpoint updated: {kw} kW")
