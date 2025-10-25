from __future__ import annotations
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from .logger import get_logger

logger = get_logger()

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "etl.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"


def _ensure_db_folder() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_engine() -> Engine:
    """
    Returnerar en SQLAlchemy engine mot projektets lokala SQLite-fil.
    """
    _ensure_db_folder()
    return create_engine(DATABASE_URL, future=True)


def ensure_schema(engine: Engine) -> None:
    """
    Skapar tabellen 'movies' om den inte finns.
    Kolumner matchar transform_movies()-output.
    """
    sql = """
    CREATE TABLE IF NOT EXISTS movies (
      imdb_id TEXT PRIMARY KEY,
      title   TEXT NOT NULL,
      year    INTEGER,
      type    TEXT NOT NULL,
      genre_full TEXT,
      genre_primary TEXT,
      director TEXT,
      country TEXT,
      runtime_min INTEGER,
      imdb_rating REAL,
      imdb_votes INTEGER,
      fetched_at TEXT NOT NULL
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def load_movies_refresh(engine: Engine, df: pd.DataFrame) -> None:
    """
    Full refresh: rensa tabellen och ladda om innehållet.
    Behåller PK/constraints.
    """
    ensure_schema(engine)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM movies"))

    if df.empty:
        logger.info("0 rader – tabellen rensad; inget att ladda.")
        return

    df.to_sql("movies", con=engine, if_exists="append", index=False)
    logger.info(f"Laddade {len(df)} rader till 'movies' (full refresh).")
