# log_setup.py
import logging, os
from logging.handlers import TimedRotatingFileHandler

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.getenv("LOG_DIR", os.path.join(HERE, "logs"))

CATS = {
    "root": BASE,
    # "computation": os.path.join(BASE, "computation"),
    "mapping": os.path.join(BASE, "mapping"),
    "sampling": os.path.join(BASE, "sampling"),
    "modbus_reader": os.path.join(BASE, "modbus_reader"), 
}
for d in CATS.values():
    os.makedirs(d, exist_ok=True)

FMT  = "%(asctime)s | %(levelname)s | %(name)s:%(lineno)d | %(message)s"
DATE = "%Y-%m-%d %H:%M:%S"

class PathFilter(logging.Filter):
    """Allow only records whose pathname contains ANY of the given subpaths."""
    def __init__(self, *subpaths: str):
        super().__init__()
        self.parts = tuple(p.lower().replace("\\", "/") for p in subpaths if p)

    def filter(self, record: logging.LogRecord) -> bool:
        path = (getattr(record, "pathname", "") or "").replace("\\", "/").lower()
        return any(p in path for p in self.parts)

def _daily(log_dir, prefix, level, flt=None):

    h = DailyNamedFileHandler(
        log_dir=log_dir,
        prefix=prefix
    )

    h.setLevel(level)

    if flt:
        h.addFilter(flt)

    return h

def _has(root, tail):
    tail = tail.replace("\\", "/").lower()
    for h in root.handlers:
        if isinstance(h, TimedRotatingFileHandler):
            try:
                if h.baseFilename.replace("\\", "/").lower().endswith("/" + tail):
                    return True
            except Exception:
                pass
    return False

def augment_logging(level: str = "INFO"):
    # calm noisy deps a bit
    for noisy in ("apscheduler", "urllib3", "selenium", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    root = logging.getLogger()
    lvl  = getattr(logging, level.upper(), logging.INFO)
    root.setLevel(lvl)

    # catch-all
    if not _has(root, "logs/app.log"):
        root.addHandler(
            _daily(CATS["root"], "app", lvl)
        )

   
    # computations.py -> computation
    # comp_f = PathFilter("/computations.py")
    # if not _has(root, "logs/computation/comp.log"):
    #     root.addHandler(_daily(os.path.join(CATS["computation"], "comp.log"), lvl, comp_f))

    # mapper.py -> mapping
    map_f = PathFilter("/mapper.py")
    if not _has(root, "logs/mapping/mapping.log"):
        root.addHandler(
            _daily(
                CATS["mapping"],
                "mapping",
                lvl,
                map_f
            )
        )

    # sampling_manager.py + resolution_sampling.py -> sampling
    samp_f = PathFilter("/sampling_manager.py", "/resolution_sampling.py")
    if not _has(root, "logs/sampling/sampling.log"):
        root.addHandler(
            _daily(
                CATS["sampling"],
                "sampling",
                lvl,
                samp_f
            )
        )

    # modbus reader files -> modbus_reader
    modbus_f = PathFilter(
        "/modbus/src/",               
        "/channel_processor.py",      
        "/device_reader.py",
        "/modbus_client_manager.py",
    ) 
    if not _has(root, "logs/modbus_reader/reader.log"):
        root.addHandler(
            _daily(
                CATS["modbus_reader"],
                "reader",
                lvl,
                modbus_f
            )
        )

    logging.getLogger("startup").info("logging ready")


import logging
import os
from datetime import datetime


class DailyNamedFileHandler(logging.Handler):

    def __init__(self, log_dir, prefix, encoding="utf-8"):
        super().__init__()

        self.log_dir = log_dir
        self.prefix = prefix
        self.encoding = encoding

        self.current_date = None
        self.file_handler = None

    def _ensure_handler(self):

        today = datetime.now().strftime("%Y-%m-%d")

        if today != self.current_date:

            if self.file_handler:
                self.file_handler.close()

            filename = os.path.join(
                self.log_dir,
                f"{self.prefix}_{today}.log"
            )

            self.file_handler = logging.FileHandler(
                filename,
                encoding=self.encoding
            )

            self.file_handler.setFormatter(
                logging.Formatter(FMT, DATE)
            )

            self.current_date = today

    def emit(self, record):
        self._ensure_handler()
        self.file_handler.emit(record)

    def close(self):
        if self.file_handler:
            self.file_handler.close()
        super().close()