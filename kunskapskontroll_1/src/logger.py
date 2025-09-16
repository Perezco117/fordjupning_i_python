from __future__ import annotations
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]

LOG_DIR = BASE_DIR / os.getenv("LOG_DIR", "data/logs")
LOG_FILE = os.getenv("LOG_FILE", "app.log")

LOG_PATH = LOG_DIR / LOG_FILE


_logger: logging.Logger | None = None

def get_logger() -> logging.Logger:
    """
    Skapa en 'etl' loggare på ett idempotent vis:
    - Alltid (re)konfigurerar loggaren baserad på nuvarande env värden.
    - Raderar förinställda handlers för att undvika duplicering,
      vilket kan hända efter att modulen laddar om under testning.
    """
    global _logger
    
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("etl")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # Undviker dubbelloggning via "root"

    # --- Idempotens: raderar alla gamla handlers på denna loggare ---
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)
            try:
                h.close()
            except Exception:
                pass
    # -----------------------------------------------------------

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