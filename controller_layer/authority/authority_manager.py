import json
from pathlib import Path
import logging

from runtime_paths import DataPaths

logger = logging.getLogger(__name__)

class AuthorityManager:

    STATUS_FILE = DataPaths.resolve("status/ems_status.json")
    manual_override_file = DataPaths.resolve("controller_layer/manual_override/manual_override_config.json")

    @staticmethod
    def _load_flags():
        """
        Load flags safely from JSON.
        Returns dict with boolean values.
        """
        try:
            with open(AuthorityManager.STATUS_FILE, "r") as f:
                data = json.load(f)
            return data.get("flags", {})
        except Exception:
            return {}   # in case file missing or corrupted

    @staticmethod
    def _load_manual_override():
        """Load manual override config from JSON file."""
        try:
            with open(AuthorityManager.manual_override_file, 'r') as f:
                data = json.load(f)
            return data
        except:
            return {"enabled": False}

    @staticmethod
    def can_proceed(requesting: str) -> bool:
        """
        requesting: one of "internal", "external", "manual"
        
        Returns True if allowing execution,
        False if blocked due to higher priority active.
        """

        flags = AuthorityManager._load_flags()
        manual = AuthorityManager._load_manual_override()

        manual_active   = manual.get("enabled", False) #flags.get("manual_override_enabled", False)
        external_active = flags.get("external_schedule_executing", False)
        internal_active = flags.get("internal_controlling_executing", False)

        # -------------------------
        # PRIORITY LOGIC
        # -------------------------
        
        # REQUESTING MANUAL
        if requesting.lower() == "manual":
            if manual_active:
                logger.info("[AUTHORITY] Manual override has highest priority.")
                return True
            return False

        # REQUESTING EXTERNAL
        if requesting.lower() == "external":
            if manual_active:
                return False  # blocked by higher priority
            
            logger.info("[AUTHORITY] External scheduler has highest priority.")
            return True

        # REQUESTING INTERNAL
        if requesting.lower() == "internal":
            if manual_active:
                return False
            if external_active:
                return False
            
            logger.info("[AUTHORITY] Internal controller has highest priority.")
            return True

        # default safe fallback
        logger.info("[AUTHORITY] No active authority. Blocking execution by default.")
        return False

    @staticmethod
    def who_is_running() -> str:
        """
        Returns which authority is currently active.
        One of "manual", "external", "internal", "none"
        """

        flags = AuthorityManager._load_flags()

        manual_active   = flags.get("manual_override_enabled", False)
        external_active = flags.get("external_schedule_executing", False)
        internal_active = flags.get("internal_controlling_executing", False)

        if manual_active:
            return "manual"
        if external_active:
            return "external"
        if internal_active:
            return "internal"
        return "none"