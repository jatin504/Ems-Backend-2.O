from app.utils.db_utils import DB

from controller_layer.writers.file_writer import FileService

async def warm_up_db_connection():
    print("Warming up DB connection...")
    async with DB() as db:
        await db.fetch("SELECT 1")

    # Writing Flag for DB Health
    await FileService.update_flag("is_db_healthy", 1)
    print("DB connection warmed up.")