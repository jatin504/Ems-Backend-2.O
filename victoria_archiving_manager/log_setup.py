"""
Daily Date-Named Logging Setup with Automatic Retention Cleanup
===============================================================
Configures enterprise-grade logging where the log file itself is named by the date,
and log files older than a configurable threshold (default: 15 days) are automatically deleted.

Structure:
  logs/
  └── <service_name>_logs/
      ├── 22-09-2026.log
      ├── 23-09-2026.log
      └── 24-09-2026.log

Features:
  - Date as File Name: The calendar date directly forms the log file name (<DD-MM-YYYY>.log)
    without unnecessary nested subfolders.
  - Automatic 15-Day Retention & Purge: Files and legacy date-folders older than 15 days
    (configurable via `retention_days` or `LOG_RETENTION_DAYS` env var) are purged automatically.
  - Midnight Auto-Rotation: Dynamically rotates to a new date-stamped file at midnight
    even during 24/7 background daemon execution.
  - Dual Output: Writes to both the daily log file and standard output (console).
  - Clean Log Formatting: Includes timestamp, log level, module/lineno, and message.
"""

import os
import sys
import shutil
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Union

HERE = os.path.dirname(os.path.abspath(__file__))
BASE_LOG_DIR = os.getenv("LOG_DIR", os.path.join(HERE, "logs"))

FMT = "%(asctime)s | %(levelname)s | %(name)s:%(lineno)d | %(message)s"
DATE_FMT = "%Y-%m-%d %H:%M:%S"


class DailyDateLogHandler(logging.Handler):
    """
    A logging handler that writes logs directly into:
      <base_dir>/<service_name>_logs/<DD-MM-YYYY>.log

    - Automatically rotates to a new file when the calendar day changes.
    - Automatically cleans up log files older than `retention_days` (default: 15 days).
    """

    def __init__(
        self,
        category_dir: str,
        date_format: str = "%d-%m-%Y",
        retention_days: int = 15,
        encoding: str = "utf-8"
    ):
        super().__init__()
        self.category_dir = category_dir
        self.date_format = date_format
        self.retention_days = retention_days
        self.encoding = encoding
        self.current_date = None
        self._file_handler: Optional[logging.FileHandler] = None

        os.makedirs(self.category_dir, exist_ok=True)
        # Run retention cleanup on startup
        self._cleanup_old_logs()

    def _cleanup_old_logs(self):
        """
        Removes log files and legacy date directories older than retention_days (default: 15 days).
        """
        try:
            if not os.path.exists(self.category_dir):
                return

            cutoff_date = (datetime.now() - timedelta(days=self.retention_days)).date()

            with os.scandir(self.category_dir) as entries:
                for entry in entries:
                    try:
                        # 1. Clean up date-named log files: <DD-MM-YYYY>.log
                        if entry.is_file() and entry.name.endswith(".log"):
                            stem = entry.name[:-4]  # Remove '.log'
                            try:
                                file_date = datetime.strptime(stem, self.date_format).date()
                                if file_date < cutoff_date:
                                    os.remove(entry.path)
                                    continue
                            except ValueError:
                                # Fallback: check file last modification time
                                mtime = datetime.fromtimestamp(entry.stat().st_mtime).date()
                                if mtime < cutoff_date:
                                    os.remove(entry.path)
                                    continue

                        # 2. Clean up legacy subfolders from previous version: <DD-MM-YYYY>/
                        elif entry.is_dir():
                            try:
                                dir_date = datetime.strptime(entry.name, self.date_format).date()
                                if dir_date < cutoff_date:
                                    shutil.rmtree(entry.path, ignore_errors=True)
                                    continue
                            except ValueError:
                                pass
                    except Exception:
                        pass
        except Exception:
            pass

    def _get_file_handler(self) -> logging.FileHandler:
        today_str = datetime.now().strftime(self.date_format)

        # If date has changed or handler is not yet created, rotate to new date file
        if today_str != self.current_date or self._file_handler is None:
            if self._file_handler:
                self._file_handler.close()

            self.current_date = today_str
            log_path = os.path.join(self.category_dir, f"{today_str}.log")

            self._file_handler = logging.FileHandler(log_path, encoding=self.encoding)
            if self.formatter:
                self._file_handler.setFormatter(self.formatter)

            # Cleanup expired logs upon daily rotation
            self._cleanup_old_logs()

        return self._file_handler

    def emit(self, record: logging.LogRecord):
        try:
            handler = self._get_file_handler()
            handler.emit(record)
        except Exception:
            self.handleError(record)

    def close(self):
        if self._file_handler:
            self._file_handler.close()
        super().close()


# Backward compatibility alias
DailyFolderHandler = DailyDateLogHandler


def setup_logging(
    service_name: str = "archive_manager",
    level: str = "INFO",
    date_format: str = "%d-%m-%Y",
    retention_days: Optional[int] = None,
    enable_console: bool = True,
    log_dir: Optional[Union[str, Path]] = None
) -> logging.Logger:
    """
    Configure date-named file logging with automated 15-day retention cleanup.

    Parameters
    ----------
    service_name : str
        Name of the service (e.g. 'archive_manager'). Creates '<service_name>_logs' directory if log_dir not set.
    level : str
        Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
    date_format : str
        Format of the daily file name (default '%d-%m-%Y', e.g. '23-09-2026.log').
    retention_days : int, optional
        Number of days to keep logs before auto-deleting (default: 15, or LOG_RETENTION_DAYS env var).
    enable_console : bool
        Whether to also output logs to the console (stdout).
    log_dir : str or Path, optional
        Custom directory to store logs. If specified, logs are stored directly inside this directory.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """
    # Calm noisy dependencies
    for noisy in ("urllib3", "requests", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # Resolve log directory
    if log_dir is not None:
        service_log_dir = str(Path(log_dir).resolve())
    else:
        service_log_dir = os.path.join(BASE_LOG_DIR, f"{service_name}_logs")
    os.makedirs(service_log_dir, exist_ok=True)

    if retention_days is None:
        retention_days = int(os.getenv("LOG_RETENTION_DAYS", "15"))

    log_level = getattr(logging, level.upper(), logging.INFO)
    formatter = logging.Formatter(FMT, DATE_FMT)

    # 1. Date-named rotating file handler with automatic 15-day purge
    file_handler = DailyDateLogHandler(
        category_dir=service_log_dir,
        date_format=date_format,
        retention_days=retention_days,
        encoding="utf-8"
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    # 2. Console handler (optional)
    console_handler = None
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)

    # Configure both lowercase and CamelCase names
    names_to_configure = {service_name, "".join(w.capitalize() for w in service_name.split("_"))}
    primary_logger = None

    for name in names_to_configure:
        lg = logging.getLogger(name)
        lg.setLevel(log_level)
        # Clear previous handlers to avoid duplicate logs when reloaded
        lg.handlers.clear()
        lg.addHandler(file_handler)
        if console_handler:
            lg.addHandler(console_handler)

        if primary_logger is None:
            primary_logger = lg

    return primary_logger
