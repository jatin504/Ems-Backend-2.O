import os

from dotenv import load_dotenv

load_dotenv()

PUBLISH_SSE=os.getenv("PUBLISH_SSE")
CREATE_DB_SCHEMA_TABLE=os.getenv("CREATE_DB_SCHEMA_TABLE")
TIMESERIES_DB=os.getenv("TIMESERIES_DB")