import os
import json
from model.db import get_connection, get_connection_center, get_connection_redundant   # centralized DB access

from utils.data_type_mapping import MODBUS_TO_PG_TYPE

from runtime_paths import DataPaths

# def map_data_type(json_type):
#     return {
#         "boolean": "BOOLEAN",
#         "float": "FLOAT",
#         "timestamp": "TIMESTAMP"
#     }.get(json_type.lower(), "TEXT")

def map_data_type(json_type: str) -> str:
    if not json_type:
        return "TEXT"

    json_type = json_type.lower()

    return MODBUS_TO_PG_TYPE.get(json_type, "DOUBLE PRECISION")

def get_correct_connection():
    role = os.getenv("DB_ROLE", "primary")
    if role == "standby":
        return get_connection_redundant()
    return get_connection_redundant() if role == "standby" else get_connection()

def table_exists(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            );
        """, (table_name,))
        return cursor.fetchone()[0]

def get_existing_columns(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table_name,))
        return {row[0] for row in cursor.fetchall()}
    
def unique_constraint_exists(conn, table_name, column_name="time_stamp"):

    query = """
        SELECT EXISTS (
            SELECT 1
            FROM pg_indexes
            WHERE tablename = %s
            AND indexdef ILIKE %s
        );
    """

    with conn.cursor() as cursor:
        cursor.execute(
            query,
            (
                table_name,
                f"%UNIQUE%({column_name})%"
            )
        )

        return cursor.fetchone()[0]
    
def apply_unique_constraint(conn, table_name, column_name="time_stamp"):

    try:

        if unique_constraint_exists(conn, table_name, column_name):
            print(f"UNIQUE constraint already exists on {table_name}.{column_name}")
            return

        with conn.cursor() as cursor:

            index_name = f"idx_{table_name}_{column_name}_unique"

            cursor.execute(f"""
                CREATE UNIQUE INDEX {index_name}
                ON {table_name} ({column_name});
            """)

            conn.commit()

            print(f"UNIQUE constraint applied on {table_name}.{column_name}")

    except Exception as e:

        conn.rollback()

        print(
            f"Failed applying UNIQUE constraint on "
            f"{table_name}.{column_name}: {e}"
        )

def alter_table_to_add_columns(conn, table_name, new_columns):
    with conn.cursor() as cursor:

        # Add missing columns
        for col_name, col_type in new_columns.items():

            alter_query = f"""
                ALTER TABLE {table_name}
                ADD COLUMN IF NOT EXISTS "{col_name}" {col_type};
            """

            cursor.execute(alter_query)
            print(f"  ➔ Added column '{col_name}' to table '{table_name}'.")

        # Add UNIQUE constraint/index on time_stamp
        cursor.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS
            idx_{table_name}_timestamp_unique
            ON {table_name} (time_stamp);
        """)

        conn.commit()

def create_tables_from_json():
    # Directly resolve the path to the plant_architecture folder
    plant_arch_path = DataPaths.resolve("plant_architecture")
    
    print(f"Scanning for JSON files in: {plant_arch_path}")

    if not os.path.exists(plant_arch_path):
        print(f"Error: Path does not exist: {plant_arch_path}")
        return

    json_files = []
    
    # Use os.walk ON the resolved path to find files in all subdirectories
    for root, dirs, files in os.walk(plant_arch_path):
        for file_name in files:
            if file_name.endswith(".json"):
                full_path = os.path.join(root, file_name)
                json_files.append(full_path)

    if not json_files:
        print(f"No JSON files found in {plant_arch_path}. Nothing to create.")
        return
    
    print(f"Found {len(json_files)} JSON file(s). Creating or updating tables...")
    

    for json_path in json_files:
        file_name = os.path.basename(json_path)
        table_name = os.path.splitext(file_name)[0].lower()

        try:
            # Connection is now handled by get_correct_connection() based on DB_ROLE
            conn = get_correct_connection()
            cursor = conn.cursor()

            if file_name == "alarms.json" or file_name == "events.json":
                predefined_columns = {
                    "device_id": "TEXT",
                    "tag_name": "TEXT",
                    "time_stamp": "TIMESTAMP",
                    "severity": "TEXT",
                    "alarm_type": "TEXT",
                    "value": "NUMERIC(12,3)",
                    "status": "TEXT",
                    "comment": "TEXT",
                    "CLR_timestamp": "TIMESTAMP",
                    "ACK_timestamp": "TIMESTAMP",
                    "remark": "TEXT"
                }
            else:
                with open(json_path, "r") as file:
                    data = json.load(file)

                predefined_columns = {}
                for key, value in data.items():
                    enable = value.get("enable", True)

                    # If "enable" is explicitly set to false, skip this column
                    if not enable:
                        continue

                    col_name = value.get("alias", key).lower()
                    json_type = value.get("dataType", "text")
                    pg_type = map_data_type(json_type)
                    predefined_columns[col_name] = pg_type
                
                # Add Application status column to show communication state => 200 for healty, 500 for comm failure
                predefined_columns["app_status"] = "DOUBLE PRECISION"

            if table_exists(conn, table_name):
                print(f"Table '{table_name}' already exists. Checking for missing columns...")
                alter_table_to_add_columns(conn, table_name, predefined_columns)
                apply_unique_constraint(
                    conn,
                    table_name,
                    "time_stamp"
                )
            else:
                columns_str = ", ".join([f"{col} {dtype}" for col, dtype in predefined_columns.items()])
                create_table_query = f"""
                CREATE TABLE {table_name} (
                    {columns_str}
                );
                """
                cursor.execute(create_table_query)
                conn.commit()

                apply_unique_constraint(
                    conn,
                    table_name,
                    "time_stamp"
                )
                print(f"Table '{table_name}' created successfully.")

        except Exception as e:
            print(f"Error processing '{file_name}': {e}")

        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()


def create_tables_from_json_center():
    root_folder = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    print("Initailizing Table Creation in Centralized view!!")
    print(f"Scanning for JSON files in 'plant_architecture' folder inside: {root_folder}")

    json_files = []
    for root, dirs, files in os.walk(root_folder):
        if "plant_architecture" in root.replace("\\", "/").lower():
            for file_name in files:
                if file_name.endswith(".json"):
                    json_files.append(os.path.join(root, file_name))

    if not json_files:
        print("No JSON files found in 'plant_architecture'. Nothing to create.")
        return

    print(f"Found {len(json_files)} JSON file(s). Creating or updating tables...")

    for json_path in json_files:
        file_name = os.path.basename(json_path)
        table_name = os.path.splitext(file_name)[0].lower()

        try:
            # Connection is now handled by get_correct_connection() based on DB_ROLE
            conn = get_correct_connection()
            cursor = conn.cursor()

            if file_name == "alarms.json":
                predefined_columns = {
                    "device_id": "TEXT",
                    "tag_name": "TEXT",
                    "time_stamp": "TIMESTAMP",
                    "severity": "TEXT",
                    "value": "FLOAT"
                }
            else:
                with open(json_path, "r") as file:
                    data = json.load(file)

                predefined_columns = {}
                for key, value in data.items():
                    col_name = value.get("alias", key)
                    json_type = value.get("dataType", "text")
                    pg_type = map_data_type(json_type)
                    predefined_columns[col_name] = pg_type

            if table_exists(conn, table_name):
                print(f"Table '{table_name}' already exists. Checking for missing columns...")
                alter_table_to_add_columns(conn, table_name, predefined_columns)
            else:

                # Ensure time_stamp exists
                if "time_stamp" not in predefined_columns:
                    predefined_columns["time_stamp"] = "TIMESTAMP"

                columns_str = ", ".join([
                    f'"{col}" {dtype}'
                    for col, dtype in predefined_columns.items()
                ])

                create_table_query = f"""
                CREATE TABLE {table_name} (
                    {columns_str}
                );
                """

                cursor.execute(create_table_query)

                # Create UNIQUE index
                cursor.execute(f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS
                    idx_{table_name}_timestamp_unique
                    ON {table_name} (time_stamp);
                """)

                conn.commit()

                print(f"Table '{table_name}' created successfully.")

        except Exception as e:
            print(f"Error processing '{file_name}': {e}")

        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()