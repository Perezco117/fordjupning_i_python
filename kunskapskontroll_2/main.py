from __future__ import annotations
from pathlib import Path
from dotenv import load_dotenv
import os

from src.logger import get_logger, LOG_PATH
from src.extract import (
    build_dataset_for_year_range,
    ExtractError,
)
from src.transform import transform_movies, TransformError
from src.load import get_engine, load_movies_refresh
from src.analyze import export_analysis


# Projektrot = där main.py ligger
PROJECT_ROOT = Path(__file__).resolve().parent
ENV_FILE = PROJECT_ROOT / ".env"

# Ladda .env (för OMDB_API_KEY)
loaded = load_dotenv(dotenv_path=ENV_FILE, override=False)


def main() -> int:
    logger = get_logger()

    logger.info(f".env loaded from {ENV_FILE} -> {loaded}")

    api_key = os.getenv("OMDB_API_KEY")
    if not api_key:
        logger.error("OMDB_API_KEY saknas i .env. Avbryter.")
        return 1

    # 1. Sökstrategi: breda queries
    queries = [
        "life", "world", "love", "dream", "dark", "light",
        "city", "road", "story", "star", "war", "home",
        "game", "blood", "night", "fire", "sea", "music",
        "king", "heart", "man", "night"
    ]

    # Vi vill ha ~5 års historik från nu (2025 -> 2020)
    year_min_extract = 2020

    try:
        # 2. Bygg rådatasetet från OMDb
        raw = build_dataset_for_year_range(
            queries=queries,
            year_min=year_min_extract,
            max_pages_per_query=5,
            sleep_sec=0.2,
        )

        logger.info(
            f"Rådatamängd efter sampling/filter >= {year_min_extract}: {len(raw)} rader"
        )

        # 3. Transformera data → ren, filtrerad, analysklar
        # Här styr du vilka titlar du vill behålla
        transformed = transform_movies(
            raw,
            allowed_types=["movie"],          # ["movie"], ["series"], ["movie","series"], eller None
            allowed_genres=["Action"],        # ["Action"], ["Action","Thriller"], eller None
            dedupe_on="title",                # "title" ger en rad per titel
            year_min=2020,                    # vi håller konsekvent 10-årsgränsen här också
        )

        logger.info(f"Transformerad datamängd: {len(transformed)} rader")

        # 4. Ladda till SQLite (full refresh)
        engine = get_engine()
        logger.info(f"SQLite path: {engine.url.database}")
        logger.info(f"Log file path: {LOG_PATH}")

        load_movies_refresh(engine, transformed)

        # 5. Exportera analyser (Power BI-ingång)
        export_analysis()

        logger.info("✅ ETL + ANALYS klart utan fel.")
        return 0

    except (ExtractError, TransformError) as e:
        logger.exception(f"❌ ETL avbröts: {e}")
        return 1
    except Exception as e:
        logger.exception(f"❌ Oväntat fel: {e}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
