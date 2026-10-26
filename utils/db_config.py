import os
from dotenv import load_dotenv

load_dotenv()

def db_config(role=os.getenv("DB_ROLE")):
    if role == "primary":
        return {
            "host": os.getenv("PRIMARY_DB_HOST"),
            "port": int(os.getenv("PRIMARY_DB_PORT")),
            "database": os.getenv("PRIMARY_DB_NAME"),
            "user": os.getenv("PRIMARY_DB_USER"),
            "password": os.getenv("PRIMARY_DB_PASSWORD"),
        }
    else:
        return {
            "host": os.getenv("STANDBY_DB_HOST"),
            "port": int(os.getenv("STANDBY_DB_PORT")),
            "database": os.getenv("STANDBY_DB_NAME"),
            "user": os.getenv("STANDBY_DB_USER"),
            "password": os.getenv("STANDBY_DB_PASSWORD"),
        }