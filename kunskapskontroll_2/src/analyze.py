from __future__ import annotations
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from .load import get_engine
from .logger import get_logger

logger = get_logger()

BASE_DIR = Path(__file__).resolve().parents[1]
ANALYSIS_DIR = BASE_DIR / "data" / "analysis"
ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)


def read_movies_df() -> pd.DataFrame:
    """
    Läser hela movies-tabellen från SQLite -> pandas.
    """
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM movies"), conn)
    logger.info(f"Läste {len(df)} rader från movies (för analys).")
    return df


def genre_rating_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Grupp per genre_primary:
    - medelbetyg
    - antal titlar
    """
    out = (
        df.groupby("genre_primary", dropna=True)
          .agg(
              avg_imdb_rating=("imdb_rating", "mean"),
              count_titles=("imdb_id", "count"),
          )
          .reset_index()
          .sort_values("avg_imdb_rating", ascending=False)
    )
    return out


def year_count_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Grupp per år:
    - antal titlar
    - medelbetyg
    """
    out = (
        df.groupby("year", dropna=True)
          .agg(
              count_titles=("imdb_id", "count"),
              avg_imdb_rating=("imdb_rating", "mean"),
          )
          .reset_index()
          .sort_values("year", ascending=True)
    )
    return out


def export_analysis() -> None:
    """
    Läser movies -> gör sammanfattningar -> skriver CSV
    till data/analysis/ så Power BI kan använda filerna.
    """
    df = read_movies_df()

    gsum = genre_rating_summary(df)
    ysum = year_count_summary(df)

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    gsum_path = ANALYSIS_DIR / "genre_rating_summary.csv"
    ysum_path = ANALYSIS_DIR / "year_count_summary.csv"

    gsum.to_csv(gsum_path, index=False, encoding="utf-8")
    ysum.to_csv(ysum_path, index=False, encoding="utf-8")

    logger.info(f"Skrev {gsum_path}")
    logger.info(f"Skrev {ysum_path}")
