# src/logger.py
from __future__ import annotations
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

LOG_DIR = os.getenv("LOG_DIR", "./logs")
LOG_FILE = os.getenv("LOG_FILE", "app.log")

_logger: logging.Logger | None = None

def get_logger() -> logging.Logger:
    global _logger
    if _logger is not None:
        return _logger

    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("etl")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # undvik dubletter

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # Fil
    fh = RotatingFileHandler(
        filename=str(Path(LOG_DIR) / LOG_FILE),
        maxBytes=1_000_000, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Konsol (bra under utveckling)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    _logger = logger
    return logger
