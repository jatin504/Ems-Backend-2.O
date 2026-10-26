import os
from dotenv import load_dotenv

load_dotenv()

def is_redundant_enabled() -> bool:
    val = os.getenv("REDUNDANT_DB", "false")
    return val.strip().lower() in ("1", "true", "yes", "on")