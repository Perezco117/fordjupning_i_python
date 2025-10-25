from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from .logger import get_logger

load_dotenv()
logger = get_logger()

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = BASE_DIR / "data" / "etl.db"
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DEFAULT_DB_PATH}")

def _ensure_db_folder(url: str) -> None:
    if url.startswith("sqlite:///"):
        db_path = url.replace("sqlite:///", "")
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

def get_engine() -> Engine:
    _ensure_db_folder(DATABASE_URL)
    return create_engine(DATABASE_URL, future=True)

def ensure_schema(engine: Engine) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS movies (
      imdb_id TEXT PRIMARY KEY,
      title   TEXT NOT NULL,
      year    INTEGER,
      type    TEXT NOT NULL,
      fetched_at TEXT NOT NULL
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))

def load_movies_refresh(engine: Engine, df: pd.DataFrame) -> None:
    """
    Full refresh that PRESERVES schema constraints:
    - Ensures schema exists
    - DELETEs all rows
    - Appends new rows (keeps PK/NOT NULL)
    """
    ensure_schema(engine)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM movies"))

    if df.empty:
        logger.info("0 rader – tabellen rensad; inget att ladda.")
        return

    df.to_sql("movies", con=engine, if_exists="append", index=False)
    logger.info(f"Laddade {len(df)} rader till 'movies' (full refresh, behåller constraints).")
