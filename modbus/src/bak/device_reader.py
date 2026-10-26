# reading modbus

import logging
import asyncio

from processor.src.mappings.mapper import DeviceMappingInput

logger = logging.getLogger(__name__)


class deviceReader:
    def __init__(self, config, modbus_client_manager, mapping_queue):
        self.server_ip = config["server_ip"]
        self.server_port = config["server_port"]
        self.slave_id = config["slave_id"]
        self.channel_id = None
        self.block = config["block"]
        self.device_id = config["device_id"]
        self.device_type = config["deviceType"]
        self.modbus_client_manager = modbus_client_manager
        self.mapping_queue = mapping_queue

    # -------------------------------
    # MAIN READ METHOD
    # -------------------------------
    async def read_registers_with_retries(
        self,
        start_address=0,
        count=20,
        retries=1,
        functionCode=3,
        register_mapping=None
    ):
        for attempt in range(retries):
            try:
                client = await self.modbus_client_manager.get_client(
                    channel_id=self.channel_id,
                    server_ip=self.server_ip,
                    server_port=self.server_port,
                    retries=retries
                )

                function_code = int(functionCode)

                if function_code == 1:
                    result = await client.read_coils(
                        address=start_address,
                        count=count,
                        slave=self.slave_id
                    )

                elif function_code == 2:
                    result = await client.read_discrete_inputs(
                        address=start_address,
                        count=count,
                        slave=self.slave_id
                    )

                elif function_code == 3:
                    result = await client.read_holding_registers(
                        address=start_address,
                        count=count,
                        slave=self.slave_id
                    )

                elif function_code == 4:
                    result = await client.read_input_registers(
                        address=start_address,
                        count=count,
                        slave=self.slave_id
                    )
                else:
                    raise ValueError(f"Unsupported function code: {function_code}")

                if result.isError():
                    logger.warning(
                        f"[CH:{self.channel_id}] Modbus error device {self.device_id}: {result}"
                    )
                    return False

                # -------------------------------
                # EXTRACT DATA
                # -------------------------------
                data = result.bits if function_code in (1, 2) else result.registers
                data = list(data)  # force plain Python

                return {
                        "registers":data,
                        "device_id":self.device_id,
                        "device_type":self.device_type,
                        "block":self.block,
                        "register_mapper":register_mapping
                    }

            except Exception as e:
                logger.error(
                    f"[CH:{self.channel_id}] device {self.device_id} attempt {attempt+1} failed: {e}"
                )

            await asyncio.sleep(0)

        # -------------------------------
        # FAILURE CASE
        # -------------------------------
        logger.error(
            f"[CH:{self.channel_id}] device {self.device_id} failed after retries"
        )

        failed_values = [None] * count


        return {
            "registers":failed_values,
            "device_id":self.device_id,
            "device_type":self.device_type,
            "block":self.block,
            "register_mapper":register_mapping
        }