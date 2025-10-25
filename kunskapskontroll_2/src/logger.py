from __future__ import annotations
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Projektroot = mappen ovanför src/
BASE_DIR = Path(__file__).resolve().parents[1]

LOG_DIR = BASE_DIR / "data" / "logs"
LOG_FILE = "app.log"
LOG_PATH = LOG_DIR / LOG_FILE

_logger: logging.Logger | None = None


def get_logger() -> logging.Logger:
    """
    Central logger för hela pipelinen (ETL + analys).
    - Skriver till både fil (roterande) och console.
    - Lägger inte till dubbla handlers.
    - Låser loggfilens plats till projektets egen data/logs/.
    """
    global _logger

    if _logger is not None:
        return _logger

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("etl")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Rensa ev. gamla handlers (vid reload i pytest etc.)
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)
            try:
                h.close()
            except Exception:
                pass

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = RotatingFileHandler(
        filename=str(LOG_PATH),
        maxBytes=1_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    _logger = logger
    return logger
