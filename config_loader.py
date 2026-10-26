import json
import logging
import os

from runtime_paths import DataPaths

logger = logging.getLogger(__name__)

class ConfigLoader:
    _instance = None
    _config = {}
    _last_modified_time = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigLoader, cls).__new__(cls)
        return cls._instance

    def load_config(self, config_path):
        try:
            config_path = DataPaths.resolve(config_path)
            current_modified_time = os.path.getmtime(config_path)
            if self._last_modified_time.get(config_path) != current_modified_time:
                with open(config_path, "r") as f:
                    self._config[config_path] = json.load(f)
                    self._last_modified_time[config_path] = current_modified_time
                    logger.info(f"Configuration reloaded from {config_path}")
            return self._config[config_path]
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise