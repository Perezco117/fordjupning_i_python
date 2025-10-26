"""
Skapar sammanställningar för Power BI baserat på vår ETL-databas.
Exempel:
- genomsnittligt IMDb-betyg per genre
- antal filmer per år

Filnamn:
- data/analysis/genre_rating_summary.csv
- data/analysis/year_count_summary.csv
"""

import pandas as pd
from pathlib import Path
from sqlalchemy import text
from src.logger import get_logger
from src.load import get_engine

logger = get_logger()

# Basstruktur för analysfiler
BASE_DIR = Path(__file__).resolve().parents[1]
ANALYSIS_DIR = BASE_DIR / "data" / "analysis"
ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)


def read_movies_df():
    """
    Läser hela tabellen 'movies' från databasen till en pandas DataFrame.
    """
    engine = get_engine()
    query = "SELECT * FROM movies"

    try:
        df = pd.read_sql_query(text(query), engine)
        logger.info(f"Läste {len(df)} rader från movies (för analys).")
        return df
    except Exception as e:
        logger.exception(f"Fel vid läsning från databasen: {e}")
        return pd.DataFrame()


def genre_rating_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gruppanalys: genomsnittligt betyg och antal filmer per primärgenre.
    """
    if df.empty:
        logger.warning("Ingen data i DataFrame. Hoppar över genre-analys.")
        return pd.DataFrame()

    summary = (
        df.groupby("genre_primary", dropna=False)
        .agg(avg_imdb_rating=("imdb_rating", "mean"), count=("imdb_id", "count"))
        .reset_index()
        .sort_values(by="count", ascending=False)
    )

    logger.info(f"Skapade genre_rating_summary ({len(summary)} genrer).")
    return summary


def year_count_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gruppanalys: antal filmer per år.
    """
    if df.empty:
        logger.warning("Ingen data i DataFrame. Hoppar över år-analys.")
        return pd.DataFrame()

    summary = (
        df.groupby("year", dropna=False)
        .agg(count=("imdb_id", "count"))
        .reset_index()
        .sort_values(by="year", ascending=True)
    )

    logger.info(f"Skapade year_count_summary ({len(summary)} år).")
    return summary

def export_analysis():
    """
    Kör hela analysflödet:
    1. Läs movies-tabellen
    2. Skapa genre- och årssammanställning
    3. Spara som CSV i data/analysis/
    """
    df = read_movies_df()

    if df.empty:
        logger.warning("Ingen data i databasen – hoppar över analys-export.")
        # se ändå till att analyskatalogen finns, så att Power BI kan peka dit
        ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
        return

    gsum = genre_rating_summary(df)
    ysum = year_count_summary(df)

    # se till att katalogen finns (även om ANALYSIS_DIR monkeypatchats i test)
    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    try:
        if not gsum.empty:
            gsum_path = ANALYSIS_DIR / "genre_rating_summary.csv"
            gsum.to_csv(gsum_path, index=False, encoding="utf-8")
            logger.info(f"Exporterat: {gsum_path}")

        if not ysum.empty:
            ysum_path = ANALYSIS_DIR / "year_count_summary.csv"
            ysum.to_csv(ysum_path, index=False, encoding="utf-8")
            logger.info(f"Exporterat: {ysum_path}")

        logger.info("✅ Analys-export klar.")
    except Exception as e:
        logger.exception(f"Fel vid export av analysfiler: {e}")
