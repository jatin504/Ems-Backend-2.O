import time
import os
import json
import logging
from plant_architecture_resolver import PlantArchitectureResolver
from psycopg2 import sql
import psycopg2
from model.db import get_connection
from dotenv import load_dotenv
from device_config_helper import DeviceConfigHelper


import time
from datetime import datetime

import asyncio

# Load environment variables from .env file
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

class ComputationEngine:

    def __init__(self):
        """
        Initialize the computation engine.
        """
        self.device_resolver = PlantArchitectureResolver()
        self.device_config_helper = DeviceConfigHelper()

        # Max allowed data age (seconds)
        self.MAX_STALE_SECONDS = 120  # 2 minutes

    # Computation Insertion Script
    def computation_insertion(self, device_id: str, result: dict):
        """
        Insert computed tags into PostgreSQL.
        Dynamically creates columns if they do not exist in the table.
        Uses device_id as the table name.
        """
        try:

            # Prefer explicit timestamp from result; fallback to current minute
            time_stamp = None
            try:
                time_stamp = result.get("Time_Stamp", {}).get("value")
            except Exception:
                pass
            if not time_stamp:
                time_stamp = time.strftime('%Y-%m-%d %H:%M:00', time.localtime())

            # Build dynamic columns/values for whatever keys are present
            db_columns = []
            db_values = []
            for key, col in result.items():
                v = None
                try:
                    v = col.get("value")
                except Exception:
                    v = None
                if v is not None:
                    db_columns.append(key.lower())
                    db_values.append(v)

            if len(db_columns) <= 1:
                logger.warning("No computable values found to insert into DB; skipping.")
                return

            # Target table: dynamically named using device_id
            table_name = f"public.{device_id.lower()}"

            # Open database connection
            conn = cursor = None
            try:
                conn = get_connection()
                cursor = conn.cursor()

                # Prepare SQL for dynamic column creation and insertion
                # 1. Standard column and placeholder strings
                cols_sql = ", ".join(db_columns)
                placeholders = ", ".join(["%s"] * len(db_columns))

                # 2. Generate the "DO UPDATE SET" part dynamically
                # We skip 'time_stamp' because we don't want to update the ID/Key itself
                update_logic = ", ".join([f"{col} = EXCLUDED.{col}" for col in db_columns if col != "time_stamp"])

                # 3. Combine into the final UPSERT query
                insert_sql = f"""
                    INSERT INTO {table_name} ({cols_sql}) 
                    VALUES ({placeholders})
                    ON CONFLICT (time_stamp) 
                    DO UPDATE SET {update_logic};
                """

                # Combine column checks and insertion into a single transaction
                full_sql = insert_sql
                cursor.execute(full_sql, db_values)
                conn.commit()
                logger.info(f"DB insert OK for {device_id}: {dict(zip(db_columns, db_values))}")
            except Exception as db_err:
                logger.error(f"DB insert failed: {db_err}")
                if conn:
                    conn.rollback()
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        except Exception as e:
            logger.error(f"Error in computation_insertion: {e}")

    def execute_computation_functions(self):
        """
        Reads 'plant_1_plant_device.json', finds all computed fields with 'functionName',
        executes those functions dynamically, updates the values, and inserts computed tags into DB.
        """
        plant_device_device_id = "plant_1_plant_device"

        try:
            # Step 1: Load plant_1_plant_device.json
            plant_data = self.device_resolver.resolve_block_device_sync(plant_device_device_id)
            logger.info(f"Loaded {plant_device_device_id}.json successfully.")

            # updated_plant_data = plant_data.copy()

            # Step 2: Loop through keys in plant_data
            for key, field_info in plant_data.items():
                # Skip Time_Stamp or any non-computed fields
                if key == "Time_Stamp" or field_info.get("type") != "computed":
                    continue

                function_name = field_info.get("functionName")
                if not function_name:
                    logger.warning(f"No function defined for field '{key}'. Skipping.")
                    continue

                # Step 3: Dynamically get the function by name
                computation_function = getattr(self, function_name, None)
                if not computation_function or not callable(computation_function):
                    logger.error(f"Function '{function_name}' not found or not callable.")
                    continue

                # Step 4: Execute the function
                try:
                    logger.info(f"Executing function: {function_name}")
                    result = computation_function()
                except Exception as e:
                    logger.error(f"Error executing function '{function_name}': {e}")
                    continue


                logger.info(f"Entering values to the plant device: {result}")
                self.device_resolver.update_computed_device_sync(device_id=plant_device_device_id, updates=result)

                # Step 5: Insert computed tags into DB using the nested function
                try:
                    logger.info(f"Executing Insert computed tags into DB")
                    self.computation_insertion(device_id=plant_device_device_id, result=result)
                except Exception as e:
                    logger.error(f"Error while computation inserting : {e}")
                    continue

        except Exception as e:
            logger.error(f"Error during computation orchestration: {e}")
            

    def Plant_INV_Total_Power_Executer(self):
        """
        Computation layer to find all inverters, read their JSON data,
        extract Active_Power_kW.value, and compute the total.
        """

        try:
            # Step 1: First check is current device type is Inverter?
            accept_device = "inverter_inverter_"
            # if accept_device not in current_device_id:
            #     return logger.info(f"{current_device_id} is not match with {accept_device}")

            # Step 2: Find all inverter files
            inverter_files = self.device_resolver.find_devices_by_type("inverter")
            logger.info(f"Found {len(inverter_files)} inverter devices.")

            total_power = 0.0

            # Step 3: Loop through each file and resolve device data
            for file_path in inverter_files:
                device_id = file_path.stem  # filename without extension
                try:
                    device_data = self.device_resolver.resolve_block_device_sync(device_id)

                    # Step 4: Extract Active_Power_kW value
                    active_power_data = device_data.get("Active_Power", {})
                    power_value = active_power_data.get("value", 0)

                    total_power += power_value
                    logger.debug(f"{device_id} -> Active Power: {power_value} kW")

                except Exception as e:
                    logger.error(f"Error processing {device_id}: {e}")

            # Step 5: Log final total
            logger.info(f"Total Inverter Powerrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr: {total_power} kW")

            return {"INV_Total_Power" : {"value" : total_power}, "Time_Stamp": {"value" : time.strftime('%Y-%m-%d %H:%M:00', time.localtime(time.time()))}}

        except Exception as e:
            raise(f"Failed to execute Plant_INV_Total_Power_Executer: {e}")
        

    def Plant_INV_Total_Daily_AC_Energy_Executer(self):
        """
        Computation layer to find all inverters, read their JSON data,
        extract Daily_AC_Energy.value, and compute the total.
        """

        try:
            # Step 1: First check is current device type is Inverter?
            accept_device = "inverter_inverter_"
            # if accept_device not in current_device_id:
            #     return logger.info(f"{current_device_id} is not match with {accept_device}")

            # Step 2: Find all inverter files
            inverter_files = self.device_resolver.find_devices_by_type("inverter")
            logger.info(f"Found {len(inverter_files)} inverter devices.")

            total_energy = 0.0

            # Step 3: Loop through each file and resolve device data
            for file_path in inverter_files:
                device_id = file_path.stem  # filename without extension
                try:
                    device_data = self.device_resolver.resolve_block_device_sync(device_id)

                    # Step 4: Extract Active_energy_kW value
                    active_energy_data = device_data.get("Daily_AC_Energy", {})
                    energy_value = active_energy_data.get("value", 0)

                    total_energy += energy_value
                    logger.debug(f"{device_id} -> Daily AC energy: {energy_value} kWh")

                except Exception as e:
                    logger.error(f"Error processing {device_id}: {e}")

            # Step 5: Log final total
            logger.info(f"Total Inverter Energy: {total_energy} kW")

            return {"INV_Total_Daily_AC_Energy" : {"value" : total_energy}, "Time_Stamp": {"value" : time.strftime('%Y-%m-%d %H:%M:00', time.localtime(time.time()))}}

        except Exception as e:
            raise(f"Failed to execute Plant_INV_Total_Daily_AC_Energy_Executer: {e}")
        

    def Plant_ESS_Total_Power_Executer(self):
        """
        Computes total ESS active power considering only
        devices with fresh data.
        """

        try:
            accept_device = "bank"
            device_files = self.device_resolver.find_devices_by_type(accept_device)

            logger.info(f"Found {len(device_files)} {accept_device} devices.")

            total_power = 0.0
            now_ts = int(time.time())

            valid_devices = 0
            skipped_devices = 0

            for file_path in device_files:
                device_id = file_path.stem

                try:
                    device_data = self.device_resolver.resolve_block_device_sync(device_id)

                    # --- Step 1: Validate timestamp ---
                    ts_str = device_data.get("Time_Stamp", {}).get("value")
                    if not ts_str:
                        logger.warning(f"{device_id} has no timestamp. Skipping.")
                        skipped_devices += 1
                        continue

                    device_ts = int(datetime.strptime(
                        ts_str, "%Y-%m-%d %H:%M:%S"
                    ).timestamp())

                    age = now_ts - device_ts
                    if age > self.MAX_STALE_SECONDS:
                        logger.warning(
                            f"{device_id} stale data ({age}s old). Skipping."
                        )
                        skipped_devices += 1
                        continue

                    # --- Step 2: Extract power ---
                    active_power_data = device_data.get("Active_power", {})
                    power_value = float(active_power_data.get("value", 0))

                    total_power += power_value
                    valid_devices += 1

                    logger.debug(f"{device_id} -> Active Power: {power_value} kW")

                except Exception as e:
                    logger.error(f"Error processing {device_id}: {e}")

            logger.info(
                f"ESS Power Computed | valid={valid_devices}, skipped={skipped_devices}"
            )

            return {
                "ESS_Total_Power": {"value": round(total_power, 2)},
                "Time_Stamp": {
                    "value": time.strftime(
                        "%Y-%m-%d %H:%M:00",
                        time.localtime(now_ts)
                    )
                }
            }

        except Exception as e:
            raise (
                f"Failed to execute Plant_ESS_Total_Power_Executer: {e}"
            )
        

    def Plant_Common_Formula_INV_Executer(self):
        """
            This Computation funtion is for INV related formulas that run at once.
            Formulas that covered:
                1. INV_Total_Power
                2. Daily Energy
                3. CUF AC
                4. CUF DC
                5. Specific Yield
                6. Total Energy
                7. PR_Percent
        """

        try:
            accept_device = "bank"
            device_files = self.device_resolver.find_devices_by_type(accept_device)

            logger.info(f"Found {len(device_files)} {accept_device} devices.")

            # Statics Values
            ac_capacity = 660
            dc_capacity = 270

            total_power = 0.0
            energy_today = 0.0
            total_energy = 0.0

            now_ts = int(time.time())

            valid_devices = 0
            skipped_devices = 0

            for file_path in device_files:
                device_id = file_path.stem

                try:
                    device_data = self.device_resolver.resolve_block_device_sync(device_id)

                    # --- Step 1: Validate timestamp ---
                    ts_str = device_data.get("Time_Stamp", {}).get("value")
                    if not ts_str:
                        logger.warning(f"{device_id} has no timestamp. Skipping.")
                        skipped_devices += 1
                        continue

                    device_ts = int(datetime.strptime(
                        ts_str, "%Y-%m-%d %H:%M:%S"
                    ).timestamp())

                    age = now_ts - device_ts
                    if age > self.MAX_STALE_SECONDS:
                        logger.warning(
                            f"{device_id} stale data ({age}s old). Skipping."
                        )
                        skipped_devices += 1
                        continue

                    # --- Step 2: Extract power ---
                    active_power_data = device_data.get("Active_power", {})
                    power_value = float(active_power_data.get("value", 0))

                    total_power += power_value

                    energy_today_data = device_data.get("Energy_charged_today", {})
                    energy_value = float(energy_today_data.get("value", 0))

                    total_power += power_value
                    energy_today += energy_value

                    energy_total_data = device_data.get("Total_energy_charged", {})
                    total_energy_value = float(energy_total_data.get("value", 0))

                    total_power += power_value
                    total_energy += total_energy_value
                    
                    
                    valid_devices += 1

                    logger.debug(f"{device_id} -> Active Power: {power_value} kW")

                except Exception as e:
                    logger.error(f"Error processing {device_id}: {e}")

            logger.info(
                f"ESS Power Computed | valid={valid_devices}, skipped={skipped_devices}"
            )

            return {
                "INV_Total_Power": {"value": round(total_power, 2)},
                "INV_Daily_Energy": {"value": round(energy_today, 2)},
                "INV_Total_Energy": {"value": round(total_energy, 2)},
                "CUF_AC" : {"value": round((energy_today / (24 *ac_capacity) * 100), 2)},
                "CUF_DC" : {"value": round((energy_today / (24 *dc_capacity) * 100), 2)},
                "Specific_Yield": {"value": round((energy_today / dc_capacity), 2)},
                "Total_INV_PR": {"value": self.PR_Percent_Formula(dc_capacity, energy_today)},
                "Time_Stamp": {
                    "value": time.strftime(
                        "%Y-%m-%d %H:%M:00",
                        time.localtime(now_ts)
                    )
                }
            }

        except Exception as e:
            raise (
                f"Failed to execute Plant_ESS_Total_Power_Executer: {e}"
            )
        
    def PR_Percent_Formula(self, dc_capacity, energy_today):
        """
            Computes Performance Ratio (PR) percentage.
        
            :parms dc_capacity
            :params energy_today
        """
        
        total_cum_poa_kwhm2 = 3 # Can be Extract from a specific device or can be extract from formula

        pr = (energy_today * 100) / (dc_capacity * total_cum_poa_kwhm2)

        return round(pr, 2)


