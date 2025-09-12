# src/load.py
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

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/etl.db")

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

def load_movies_replace(engine: Engine, df: pd.DataFrame) -> None:
    """Minimalistiskt: ersätt hela tabellen (full refresh)."""
    ensure_schema(engine)
    # skriv till temporär tabell och ersätt
    df.to_sql("movies", con=engine, if_exists="replace", index=False)
    logger.info(f"Laddade {len(df)} rader till tabell 'movies' (replace).")
