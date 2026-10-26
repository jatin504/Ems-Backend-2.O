from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv()

class DataPaths:
    @staticmethod
    def root() -> Path:
        root = os.getenv("EMS_DATA_DIR")
        if not root:
            raise RuntimeError("EMS_DATA_DIR is not set")
        return Path(root)

    @staticmethod
    def resolve(relative_path: str) -> Path:
        """
        Resolves logical EMS path relative to EMS_DATA_DIR
        """
        path = DataPaths.root() / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
    
    @staticmethod
    def define(relative_path: str) -> Path:
        """
        Resolves logical EMS path relative to EMS_DATA_DIR
        """
        path = DataPaths.root() / relative_path
        path.mkdir(parents=True, exist_ok=True)
        return path
