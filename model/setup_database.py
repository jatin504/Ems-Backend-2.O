import psycopg2
from psycopg2 import sql
from utils.db_config import db_config


def create_single_db(config, label):
    try:
        setup_config = config.copy()
        db_name = setup_config.pop("database")

        # connect to default DB
        setup_config["database"] = "postgres"

        print(f"🔄 Connecting to {label} server...")

        conn = psycopg2.connect(**setup_config)
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (db_name,)
        )

        if not cursor.fetchone():
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            print(f"{label} DB created: {db_name}")
        else:
            print(f"ℹ️ {label} DB already exists: {db_name}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"{label} DB error: {e}")


def create_database():
    # PRIMARY (localhost)
    create_single_db(db_config("primary"), "PRIMARY")

    # STANDBY (remote IP)
    create_single_db(db_config("standby"), "STANDBY")

