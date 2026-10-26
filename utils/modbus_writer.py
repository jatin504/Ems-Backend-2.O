import logging
import asyncio
from utils.loop_manager import get_main_loop

main_loop = get_main_loop()

def get_modbus_client():
    from modbus import ModbusClientManager
    ModbusClientManager.initialize(main_loop)
    return ModbusClientManager

logger = logging.getLogger(__name__)

class ModbusWriter:
    """
    A utility class to handle writing values to Modbus registers with proper data type handling and retry logic.
    """
    @staticmethod
    async def write_to_register(channel_id, server_ip, server_port, slave_id, register, data_type, value, fc=3):
        """
        Writes a value to a Modbus holding register with retries.
        """
        retries = 3
        delay = 0.1  # 100 ms

        for attempt in range(1, retries + 1):
            try:
                # Connect to the Modbus server
                modbus_manager_class = get_modbus_client()
                client = await modbus_manager_class.get_client(
                    channel_id="writing",
                    server_ip=server_ip,
                    server_port=server_port,
                    retries=1
                )

                if data_type == "int32":
                    value = int(value)
                    if value < 0:
                        value &= 0xFFFFFFFF  # 32-bit 2's complement
                    high_word = (value >> 16) & 0xFFFF
                    low_word = value & 0xFFFF

                    response_low = await client.write_register(address=register, value=low_word, slave=slave_id)
                    response_high = await client.write_register(address=register + 1, value=high_word, slave=slave_id)

                    if response_low.isError() or response_high.isError():
                        raise Exception(f"Write error: low={response_low}, high={response_high}")

                    logger.info(f"Successfully wrote int32 value {value} to registers {register} and {register + 1}")
                    return  # success

                elif data_type == "float":
                    import struct
                    packed_value = struct.pack(">f", value)
                    high_word, low_word = struct.unpack(">HH", packed_value)

                    response_high = await client.write_register(address=register, value=high_word, slave=slave_id)
                    response_low = await client.write_register(address=register + 1, value=low_word, slave=slave_id)

                    if response_high.isError() or response_low.isError():
                        raise Exception(f"Write error: high={response_high}, low={response_low}")

                    logger.info(f"Successfully wrote float value {value} to registers {register} and {register + 1}")
                    return  # success

                else:
                    # 16-bit
                    value = int(value) & 0xFFFF
                    if fc == 1:
                        response = await client.write_coil(address=register, value=value, slave=slave_id)
                    else:
                        response = await client.write_register(address=register, value=value, slave=slave_id)
                    if response.isError():
                        raise Exception(f"Write error: {response}")
                    logger.info(f"Successfully wrote value {value} to register {register}")
                    return  # success

            except Exception as e:
                logger.warning(f"Attempt {attempt}/3: Error writing to register {register} (type={data_type}): {e}")
                if attempt < retries:
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Failed to write after {retries} attempts to register {register}")
                    raise  # propagate final error
