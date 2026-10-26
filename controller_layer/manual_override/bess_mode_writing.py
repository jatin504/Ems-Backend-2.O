import asyncio
import logging
import json 

from datetime import datetime

from pathlib import Path

from app.utils.db_utils import DB
from config.device_config_resolver import DeviceConfigResolver

from latest_telemetry_resolver import LatestTelemetryResolver

from model.models import SetpointWrite
from repository.strategy_repo import write_setpoint_record
from runtime_paths import DataPaths

from utils import ModbusWriter

logger = logging.getLogger(__name__)

_MODE_REGISTER_MAP_CACHE = None
_DEVICE_CONTROL_MAP_CACHE = None

# Holds running watchdog tasks per device
P_WATCHDOG_TASKS: dict[str, asyncio.Task] = {}


class BESSModeWriting:
    """
    Handles writing of BESS control modes via Modbus.
    """
    def __init__(self):
        
        self.MODE_REGISTER_MAP_Path = DataPaths.resolve("controller_layer/manual_override/bess_mode_control_mappings.json")
        # Load Device Resolver
        self.resolver = LatestTelemetryResolver()

        # Load Device Infos
        self.device_resolver = DeviceConfigResolver()

        # Load Mode Register Map
        self.MODE_REGISTER_MAP = self.load_mode_register_map()

        self.MODES_JSON_PATH = DataPaths.resolve(
            "controller_layer/manual_override/bess_modes.json"
        )

        self.CONTROL_SETPOINTS_PATH = DataPaths.resolve(
            "controller_layer/manual_override/bess_control_setpoints.json"
        )

        self.DEVICE_CONTROL_PATH = DataPaths.resolve(
            "controller_layer/manual_override/device_control.json"
        )


        # Load mappings only after paths are initialized
        self.MODE_REGISTER_MAP = self.load_mode_register_map()
        self.DEVICE_CONTROL_MAP = self.load_device_control_map()

        # Check Interval for watchdogs
        self.CHECK_INTERVAL = 1

        self.MAX_SOC = 95
        self.MIN_SOC = 20


    def load_mode_register_map(self):
        global _MODE_REGISTER_MAP_CACHE

        if _MODE_REGISTER_MAP_CACHE is not None:
            return _MODE_REGISTER_MAP_CACHE

        try:
            path = Path(self.MODE_REGISTER_MAP_Path)

            if not path.exists():
                raise FileNotFoundError(f"Mode mapping file not found: {path}")

            with open(path, "r") as f:
                data = json.load(f)

            # Basic validation
            for mode, cfg in data.items():
                if "mode_key" not in cfg or "mode_active_value" not in cfg or "mode_deactive_value" not in cfg or "setpoint_key" not in cfg:
                    raise ValueError(
                        f"Invalid mapping for mode '{mode}'. "
                        f"Expected keys: ['mode_key', 'mode_active_value', 'mode_deactive_value', 'setpoint_key']"
                    )

            _MODE_REGISTER_MAP_CACHE = data
            logger.info("[Control Service (BESS)] Mode register mappings loaded successfully")

            return _MODE_REGISTER_MAP_CACHE

        except Exception as e:
            logger.exception("[Control Service (BESS)] Failed to load MODE_REGISTER_MAP")
            return {}

    def load_device_control_map(self):
        global _DEVICE_CONTROL_MAP_CACHE

        if _DEVICE_CONTROL_MAP_CACHE is not None:
            return _DEVICE_CONTROL_MAP_CACHE

        try:
            path = Path(self.DEVICE_CONTROL_PATH)

            if not path.exists():
                raise FileNotFoundError(
                    f"Device control mapping file not found: {path}"
                )

            with open(path, "r") as f:
                data = json.load(f)

            # Basic validation
            if not isinstance(data, dict):
                raise ValueError(
                    "Device control mapping must be a JSON object"
                )

            for device_name, device_cfg in data.items():

                if not isinstance(device_cfg, dict):
                    raise ValueError(
                        f"Invalid configuration for device '{device_name}'. "
                        f"Expected an object."
                    )

                for control_type, control_cfg in device_cfg.items():

                    if not isinstance(control_cfg, dict):
                        raise ValueError(
                            f"Invalid '{control_type}' configuration for "
                            f"device '{device_name}'. Expected an object."
                        )

                    required_keys = [
                        "device_id",
                        "key",
                        "active_value",
                        "is_bit",
                        "bit_index",
                    ]

                    missing_keys = [
                        key for key in required_keys
                        if key not in control_cfg
                    ]

                    if missing_keys:
                        raise ValueError(
                            f"Invalid mapping for device '{device_name}', "
                            f"control '{control_type}'. "
                            f"Missing keys: {missing_keys}. "
                            f"Expected keys: {required_keys}"
                        )

            _DEVICE_CONTROL_MAP_CACHE = data

            logger.info(
                "[Control Service (BESS)] "
                "Device control mappings loaded successfully"
            )

            return _DEVICE_CONTROL_MAP_CACHE

        except Exception:
            logger.exception(
                "[Control Service (BESS)] "
                "Failed to load DEVICE_CONTROL_MAP"
            )
            return {}


    async def apply_active_modes(
        self,
        active_modes: dict
    ):
        """
        Writes Modbus registers for all active modes and their associated PCS devices.
        """

        if not self.MODE_REGISTER_MAP:
            logger.error("[Control Service (BESS)] MODE_REGISTER_MAP is empty. Aborting.")
            return

        # Prepare Modbus write tasks
        tasks = []

        for mode_name, mode_info in active_modes.items():
            if not mode_info.get("active"):
                continue

            mode_mapping = self.MODE_REGISTER_MAP.get(mode_name)
            if not mode_mapping:
                logger.warning(f"No register mapping found for mode '{mode_name}'")
                continue

            register_key = mode_mapping["mode_key_name"]
            value_to_write = mode_mapping["mode_active_value"]

            for device_id in mode_info.get("pcs_device_id", []):
                try:
                    device_data = self.resolver.get_latest_device_data(device_id)

                    if register_key not in device_data:
                        logger.warning(
                            f"Register key '{register_key}' not found for device {device_id}"
                        )
                        continue

                    register_meta = device_data[register_key]

                    register = register_meta["register"]
                    data_type = register_meta["dataType"]

                    # Load Device Infos
                    device_info = self.device_resolver.get_device_info_by_id(device_id)

                    if not device_info:
                        logger.error(f"[Control Service (BESS)] Device info not found for {device_id}")
                        return
                    
                    # Extract Device Infos
                    channel_id = device_info["channel"]
                    server_ip = device_info["server_ip"]
                    server_port = device_info["server_port"]
                    slave_id = device_info["slave_id"]

                    tasks.append(
                        ModbusWriter.write_to_register(
                            channel_id=channel_id,
                            server_ip=server_ip,
                            server_port=server_port,
                            slave_id=slave_id,
                            register=register,
                            data_type=data_type,
                            value=value_to_write
                        )
                    )

                    sp_rec = SetpointWrite(strategy_id=0, 
                        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                        register_mode=1, 
                        setpoint_value=1, 
                        unit="", 
                        device_id=device_id, 
                        success=True, 
                        response_text=f"Activating Mode {mode_name} with value {value_to_write}"
                    )
                    
                    async with DB() as db:
                        await write_setpoint_record(db, sp_rec)
                    # await pool.close()

                except Exception as e:
                    logger.exception(
                        f"Failed preparing Modbus write for device {device_id}, mode {mode_name}: {e}"
                    )

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def apply_deactive_modes(
        self,
        deactive_modes: dict
    ):
        """
        Writes Modbus registers for all deactive modes and their associated PCS devices.
        """

        if not self.MODE_REGISTER_MAP:
            logger.error("[Control Service (BESS)] MODE_REGISTER_MAP is empty. Aborting.")
            return

        # Prepare Modbus write tasks
        tasks = []

        for mode_name, mode_info in deactive_modes.items():
            if mode_info.get("active"):
                continue

            mode_mapping = self.MODE_REGISTER_MAP.get(mode_name)
            if not mode_mapping:
                logger.warning(f"No register mapping found for mode '{mode_name}'")
                continue

            register_key = mode_mapping["mode_key_name"]
            value_to_write = mode_mapping["mode_deactive_value"]

            for device_id in mode_info.get("pcs_device_id", []):
                try:
                    device_data = self.resolver.get_latest_device_data(device_id)

                    if register_key not in device_data:
                        logger.warning(
                            f"Register key '{register_key}' not found for device {device_id}"
                        )
                        continue

                    register_meta = device_data[register_key]

                    register = register_meta["register"]
                    data_type = register_meta["dataType"]

                    # Load Device Infos
                    device_info = self.device_resolver.get_device_info_by_id(device_id)

                    if not device_info:
                        logger.error(f"[Control Service (BESS)] Device info not found for {device_id}")
                        return
                    
                    # Extract Device Infos
                    channel_id = device_info["channel"]
                    server_ip = device_info["server_ip"]
                    server_port = device_info["server_port"]
                    slave_id = device_info["slave_id"]

                    tasks.append(
                        ModbusWriter.write_to_register(
                            channel_id=channel_id,
                            server_ip=server_ip,
                            server_port=server_port,
                            slave_id=slave_id,
                            register=register,
                            data_type=data_type,
                            value=value_to_write
                        )
                    )

                    sp_rec = SetpointWrite(strategy_id=0, 
                        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                        register_mode=1, 
                        setpoint_value=1, 
                        unit="", 
                        device_id=device_id, 
                        success=True, 
                        response_text=f"Deactivating Mode {mode_name} with value {value_to_write}"
                    )

                    async with DB() as db:
                        await write_setpoint_record(db, sp_rec)

                except Exception as e:
                    logger.exception(
                        f"Failed preparing Modbus write for device {device_id}, mode {mode_name}: {e}"
                    )

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def apply_device_controls(
        self,
        device_controls: dict
    ):
        """
        Writes Modbus registers for device-level controls.

        Expected device_controls format:

        {
            "plant_1_pcs_pcs_1": {
                "ppc_control": {
                    "enable: true,
                    "status: "on"/off
                }
            }
        }
        """

        if not self.DEVICE_CONTROL_MAP:
            logger.error(
                "[Control Service (BESS)] "
                "DEVICE_CONTROL_MAP is empty. Aborting."
            )
            return

        # Prepare Modbus write tasks
        tasks = []

        for device_name, controls in device_controls.items():

            if not isinstance(controls, dict):
                logger.warning(
                    f"Invalid device controls for '{device_name}'"
                )
                continue

            device_mapping = self.DEVICE_CONTROL_MAP.get(device_name)

            if not device_mapping:
                logger.warning(
                    f"No device control mapping found for '{device_name}'"
                )
                continue

            for control_name, control_value in controls.items():

                # Skip inactive controls
                if not control_value.get("enable"):
                    continue

                control_mapping = device_mapping.get(control_name)

                if not control_mapping:
                    logger.warning(
                        f"No mapping found for control '{control_name}' "
                        f"on device '{device_name}'"
                    )
                    continue

                try:
                    device_id = control_mapping["device_id"]
                    register_key = control_mapping["key"]
                    active_value = control_mapping["active_value"] if control_value.get("status", "off") == "on" else control_mapping["active_value"]
                    is_bit = control_mapping["is_bit"]
                    bit_index = control_mapping["bit_index"]

                    print(f"key: {register_key} bid indx : {bit_index}")

                    # Get latest device data
                    device_data = self.resolver.get_latest_device_data(
                        device_id
                    )

                    print(f"Device Data: {device_data}")

                    if register_key not in device_data:
                        logger.warning(
                            f"Register key '{register_key}' not found "
                            f"for device {device_id}"
                        )
                        continue

                    register_meta = device_data[register_key]

                    register = register_meta["register"]
                    data_type = register_meta["dataType"]

                    # Load Device Infos
                    device_info = (
                        self.device_resolver.get_device_info_by_id(device_id)
                    )

                    if not device_info:
                        logger.error(
                            "[Control Service (BESS)] "
                            f"Device info not found for {device_id}"
                        )
                        continue

                    # Extract Device Infos
                    channel_id = device_info["channel"]
                    server_ip = device_info["server_ip"]
                    server_port = device_info["server_port"]
                    slave_id = device_info["slave_id"]

                    # Prepare Modbus write
                    if is_bit:
                        tasks.append(
                            ModbusWriter.write_to_register(
                                channel_id=channel_id,
                                server_ip=server_ip,
                                server_port=server_port,
                                slave_id=slave_id,
                                register=register,
                                data_type=data_type,
                                value=bool(active_value),
                                bit=bit_index
                            )
                        )
                    else:
                        tasks.append(
                            ModbusWriter.write_to_register(
                                channel_id=channel_id,
                                server_ip=server_ip,
                                server_port=server_port,
                                slave_id=slave_id,
                                register=register,
                                data_type=data_type,
                                value=active_value
                            )
                        )

                    # Save setpoint/control record
                    sp_rec = SetpointWrite(
                        strategy_id=0,
                        timestamp=datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        register_mode=1,
                        setpoint_value=active_value,
                        unit="",
                        device_id=device_id,
                        success=True,
                        response_text=(
                            f"Activating device control "
                            f"{control_name} with value {active_value}"
                        )
                    )

                    async with DB() as db:
                        await write_setpoint_record(db, sp_rec)

                except Exception as e:
                    logger.exception(
                        f"Failed preparing Modbus write for "
                        f"device {device_name}, "
                        f"control {control_name}: {e}"
                    )

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def write_device_new_value(
        self,
        device_control: dict
    ):
        """
        Writes a new value directly to a Modbus register.

        Expected format:

        {
            "block": 1,
            "device_type": "plc",
            "device_id": "plant_1_mcr_io_1",
            "alias": "spare_do_1",
            "name": "spare_do_1",
            "value": 0,
            "new_value": 1,
            "type": "analog",
            "unit": "",
            "dataType": "int16",
            "register": 300
        }
        """

        if not isinstance(device_control, dict):
            logger.warning(
                "[Control Service (BESS)] Invalid device control data."
            )
            return False

        print(f"Control Payload: {device_control}")

        try:
            # ---------------------------------------------------------
            # Extract values from input JSON
            # ---------------------------------------------------------
            device_id = device_control["device_id"]
            register = device_control["register"]
            data_type = device_control["dataType"]
            new_value = device_control["new_value"]

            control_name = device_control.get(
                "name",
                device_control.get("alias", "")
            )

            # ---------------------------------------------------------
            # Get device information
            # ---------------------------------------------------------
            device_info = (
                self.device_resolver.get_device_info_by_id(device_id)
            )

            if not device_info:
                logger.error(
                    "[Control Service (BESS)] "
                    f"Device info not found for {device_id}"
                )
                return False

            # ---------------------------------------------------------
            # Extract Modbus connection information
            # ---------------------------------------------------------
            channel_id = device_info["channel"]
            server_ip = device_info["server_ip"]
            server_port = device_info["server_port"]
            slave_id = device_info["slave_id"]

            # ---------------------------------------------------------
            # Log write operation
            # ---------------------------------------------------------
            logger.info(
                "[Control Service (BESS)] Writing new value: "
                f"device={device_id}, "
                f"control={control_name}, "
                f"register={register}, "
                f"data_type={data_type}, "
                f"value={new_value}"
            )

            # ---------------------------------------------------------
            # Prepare Modbus write
            # ---------------------------------------------------------
            await ModbusWriter.write_to_register(
                channel_id=channel_id,
                server_ip=server_ip,
                server_port=server_port,
                slave_id=slave_id,
                register=register,
                data_type=data_type,
                value=new_value,
                fc=1
            )

            # ---------------------------------------------------------
            # Execute Modbus write
            # ---------------------------------------------------------

            logger.info(
                "[Control Service (BESS)] "
                f"Successfully wrote {new_value} "
                f"to register {register} "
                f"for device {device_id}"
            )

            # ---------------------------------------------------------
            # Save setpoint/control record
            # ---------------------------------------------------------
            sp_rec = SetpointWrite(
                strategy_id=0,
                timestamp=datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                register_mode=1,
                setpoint_value=new_value,
                unit=device_control.get("unit", ""),
                device_id=device_id,
                success=True,
                response_text=(
                    f"Writing device control "
                    f"{control_name} with value {new_value}"
                )
            )

            async with DB() as db:
                await write_setpoint_record(db, sp_rec)

            return True

        except KeyError as e:
            logger.error(
                "[Control Service (BESS)] "
                f"Missing required field: {e}"
            )
            return False

        except Exception as e:
            logger.exception(
                "[Control Service (BESS)] "
                f"Failed writing value for device "
                f"{device_control.get('device_id')}: {e}"
            )
            return False


    async def write_modes_to_file(self, modes: dict):
        """
        Writes modes configuration to controller_layer/manual_override/modes.json
        """

        try:
            path = Path(self.MODES_JSON_PATH)
            path.parent.mkdir(parents=True, exist_ok=True)

            # Basic validation
            for mode_name, mode_cfg in modes.items():
                if not isinstance(mode_cfg, dict):
                    raise ValueError(f"Invalid config for mode '{mode_name}'")

                required_keys = {"name", "active", "pcs_device_id"}
                missing = required_keys - mode_cfg.keys()

                if missing:
                    raise ValueError(
                        f"Mode '{mode_name}' missing keys: {missing}"
                    )

            with open(path, "w", encoding="utf-8") as f:
                json.dump(modes, f, indent=4)


        except Exception as e:
            logger.exception(
                "[Control Service (BESS)] Failed to write modes.json"
            )
            raise


    async def set_bess_modes_and_apply(self, modes: dict):
        """
        1. Persist modes to modes.json
        2. Apply active modes by writing Modbus registers
        """

        try:
            # Step 1: Persist modes
            await self.write_modes_to_file(modes)
            logger.info("[Control Service (BESS)] Modes saved successfully")

            # Step 2: Apply active modes
            await self.apply_active_modes(modes)
            logger.info("[Control Service (BESS)] Active modes applied successfully")

            # Step 3: Apply deactive modes
            await self.apply_deactive_modes(modes)
            logger.info("[Control Service (BESS)] Deactive modes applied successfully")

        except Exception as e:
            logger.exception(
                "[Control Service (BESS)] Failed to set and apply modes"
            )
            sp_rec = SetpointWrite(strategy_id=0, 
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                register_mode=1, 
                setpoint_value=1, 
                unit="", 
                device_id="device_id", 
                success=True, 
                response_text=f"[Control Service (BESS)] Failed to set and apply modes"
            )

            async with DB() as db:
                await write_setpoint_record(db, sp_rec)
            raise


    async def apply_control_setpoints(self, control_setpoints: dict):
        """
        Writes control setpoints to Modbus registers after validating:
        - Mode is active
        - Mode control_state is local
        """

        # Load modes.json
        with open(self.MODES_JSON_PATH, "r", encoding="utf-8") as f:
            modes = json.load(f)

        tasks = [] # writing set point tasks

        watchdog_actions = [] # for "p" mode only

        for mode_name, setpoint_cfg in control_setpoints.items():
            try:
                # ---- Validate mode existence ----
                mode_cfg = modes.get(mode_name)
                if not mode_cfg:
                    logger.warning(f"Mode '{mode_name}' not found in modes.json")
                    continue

                # ---- Validate mode state ----
                if not mode_cfg.get("active"):
                    logger.info(f"Mode '{mode_name}' is not active. Skipping setpoint.")
                    continue

                if mode_cfg.get("control_state") != "local":
                    logger.info(
                        f"Mode '{mode_name}' is not in local control. Skipping setpoint."
                    )
                    continue

                # ---- Validate mapping ----

                mapping = self.MODE_REGISTER_MAP.get(mode_name)
                if not mapping or "setpoint_key" not in mapping:
                    logger.warning(
                        f"Setpoint mapping missing for mode '{mode_name}'"
                    )
                    continue

                setpoint_key = mapping["setpoint_key"]
                setpoint_value = setpoint_cfg.get("setpoint_value")

                if setpoint_value is None:
                    logger.warning(
                        f"Setpoint value missing for mode '{mode_name}'"
                    )
                    continue

                print(f"[Control Service (BESS)] Setpoint value for mode '{mode_name}': {setpoint_value}")

                # ---- Apply setpoint to all PCS devices ----
                for device_id in mode_cfg.get("pcs_device_id", []):
                    device_data = self.resolver.get_latest_device_data(device_id)

                    if setpoint_key not in device_data:
                        logger.warning(
                            f"Setpoint key '{setpoint_key}' not found for device {device_id}"
                        )
                        continue

                    register_meta = device_data[setpoint_key]
                    register = register_meta["register"]
                    data_type = register_meta["dataType"]

                    device_info = self.device_resolver.get_device_info_by_id(device_id)
                    if not device_info:
                        logger.error(f"Device info not found for {device_id}")
                        continue

                    print(f"[Control Service (BESS)] Writing setpoint for device {device_id}: Register={register}, Value={setpoint_value}")
                    tasks.append(
                        ModbusWriter.write_to_register(
                            channel_id=device_info["channel"],
                            server_ip=device_info["server_ip"],
                            server_port=device_info["server_port"],
                            slave_id=device_info["slave_id"],
                            register=register,
                            data_type=data_type,
                            value=setpoint_value
                        )
                    )

                    logger.info(
                        f"[Setpoint] Mode={mode_name}, Device={device_id}, "
                        f"Register={register}, Value={setpoint_value}"
                    )

                    sp_rec = SetpointWrite(strategy_id=0, 
                        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                        register_mode=1, 
                        setpoint_value=1, 
                        unit="", 
                        device_id=device_id, 
                        success=True, 
                        response_text=f"[Control Setpoint] Mode {mode_name} with value {setpoint_value}"
                    )

                    async with DB() as db:
                        await write_setpoint_record(db, sp_rec)

                    # if mode_name == "p":
                    #     watchdog_actions.append({
                    #         "device_id": device_id,
                    #         "setpoint_value": setpoint_value,
                    #         "register": register,
                    #         "data_type": data_type,
                    #         "device_info": device_info
                    #     })

            except Exception as e:
                logger.exception(
                    f"Failed processing control setpoint for mode '{mode_name}'"
                )

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)

        write_failed = any(isinstance(r, Exception) for r in results)
        if write_failed:
            logger.error("[Setpoint] One or more Modbus writes failed")

        if not write_failed:
            for action in watchdog_actions:
                device_id = action["device_id"]
                setpoint_value = action["setpoint_value"]

                if setpoint_value == 0:
                    self.stop_p_watchdog(device_id)
                else:
                    self.start_p_watchdog(
                        device_id=device_id,
                        target_power=setpoint_value,
                        register=action["register"],
                        data_type=action["data_type"],
                        device_info=action["device_info"]
                    )

    

    def write_control_setpoints_to_file(self, control_setpoints: dict):
        """
        Writes control setpoints to bess_control_setpoints.json
        """

        try:
            path = Path(self.CONTROL_SETPOINTS_PATH)
            path.parent.mkdir(parents=True, exist_ok=True)

            # Basic validation
            for mode, cfg in control_setpoints.items():
                if "setpoint_value" not in cfg:
                    raise ValueError(
                        f"Missing setpoint_value for mode '{mode}'"
                    )

            with open(path, "w", encoding="utf-8") as f:
                json.dump(control_setpoints, f, indent=4)

            logger.info(
                f"[Control Service (BESS)] Control setpoints written to {path}"
            )

        except Exception:
            logger.exception(
                "[Control Service (BESS)] Failed to write control setpoints"
            )
            raise

    async def set_control_setpoints_and_apply(self, control_setpoints: dict):
        """
        1. Persist control setpoints to bess_control_setpoints.json
        2. Apply control setpoints to PCS via Modbus
        """

        try:
            # Step 1: Save setpoints
            self.write_control_setpoints_to_file(control_setpoints)
            logger.info("[Control Service (BESS)] Control setpoints saved")

            # Step 2: Apply setpoints
            await self.apply_control_setpoints(control_setpoints)
            logger.info("[Control Service (BESS)] Control setpoints applied")

        except Exception:
            logger.exception(
                "[Control Service (BESS)] Failed to set and apply control setpoints"
            )
            sp_rec = SetpointWrite(strategy_id=0, 
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                register_mode=1, 
                setpoint_value=1, 
                unit="", 
                device_id="device_id", 
                success=True, 
                response_text=f"[Control Service (BESS)] Failed to set and apply control setpoints"
            )

            async with DB() as db:
                await write_setpoint_record(db, sp_rec)
            raise


    async def p_setpoint_watchdog(
        self, 
        device_id: str,
        target_power: float,
        register: int,
        data_type: str,
        device_info: dict
    ):
        """
        Monitors SoC and power for a PCS device.
        When stop condition is met → writes P setpoint = 0 and exits.
        """


        logger.info(
            f"[P-Watchdog] Started for {device_id} | Target Power={target_power}"
        )

        while True:
            try:
                telemetry = self.resolver.get_latest_device_data(device_id)

                soc = telemetry.get("SOC", {}).get("value")  # soc of device
                actual_power = telemetry.get("Active_power", {}).get("value") # actual power of a device

                logger.info(
                    f"[P-Watchdog] {device_id} | SoC={soc}%, Actual Power={actual_power}kW")

                if soc is None or actual_power is None:
                    logger.warning(
                        f"[P-Watchdog] Missing telemetry for {device_id}"
                    )
                    # await asyncio.sleep(self.CHECK_INTERVAL)
                    continue

                # -------- Stop Conditions --------
                stop = False

                # Charging
                if target_power > 0 and soc >= self.MAX_SOC:
                    logger.info(
                        f"[P-Watchdog] Max SoC reached ({soc}%) for {device_id}"
                    )
                    stop = True

                # Discharging
                if target_power < 0 and soc <= self.MIN_SOC:
                    logger.info(
                        f"[P-Watchdog] Min SoC reached ({soc}%) for {device_id}"
                    )
                    stop = True

                # Power achieved
                if abs(actual_power) >= abs(target_power):
                    logger.info(
                        f"[P-Watchdog] Target power reached "
                        f"(actual={actual_power}, target={target_power}) "
                        f"for {device_id}"
                    )
                    stop = True

                # -------- Stop → Idle --------
                if stop:
                    await ModbusWriter.write_to_register(
                        channel_id=device_info["channel"],
                        server_ip=device_info["server_ip"],
                        server_port=device_info["server_port"],
                        slave_id=device_info["slave_id"],
                        register=register,
                        data_type=data_type,
                        value=0
                    )

                    logger.info(
                        f"[P-Watchdog] Setpoint reset to 0 for {device_id}"
                    )
                    break

                # await asyncio.sleep(self.CHECK_INTERVAL)

            except Exception:
                logger.exception(
                    f"[P-Watchdog] Error for device {device_id}"
                )
                # await asyncio.sleep(self.CHECK_INTERVAL)
    
    def start_p_watchdog(
        self,
        device_id: str,
        target_power: float,
        register: int,
        data_type: str,
        device_info: dict
    ):
        """
        Cancels existing watchdog (if any) and starts a new one.
        """

        # Cancel existing watchdog
        existing_task = P_WATCHDOG_TASKS.get(device_id)
        if existing_task and not existing_task.done():
            logger.info(
                f"[P-Watchdog] Cancelling previous watchdog for {device_id}"
            )
            existing_task.cancel()

        # Start new watchdog
        task = asyncio.create_task(
            self.p_setpoint_watchdog(
                device_id=device_id,
                target_power=target_power,
                register=register,
                data_type=data_type,
                device_info=device_info
            )
        )

        P_WATCHDOG_TASKS[device_id] = task

    def stop_p_watchdog(self, device_id: str):
        task = P_WATCHDOG_TASKS.get(device_id)

        if task and not task.done():
            logger.info(
                f"[P-Watchdog] Stopping watchdog for {device_id}"
            )
            task.cancel()

        P_WATCHDOG_TASKS.pop(device_id, None)


