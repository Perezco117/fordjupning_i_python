from __future__ import annotations
from pathlib import Path
from dotenv import load_dotenv

# Ladda .env för att få OMDB_API_KEY (men inte för paths/databas)
PROJECT_ROOT = Path(__file__).resolve().parent
ENV_FILE = PROJECT_ROOT / ".env"
loaded = load_dotenv(dotenv_path=ENV_FILE, override=False)

from src.logger import get_logger, LOG_PATH
from src.extract import (
    build_dataset_for_year_range,
    ExtractError,
)
from src.transform import transform_movies, TransformError
from src.load import get_engine, load_movies_refresh
from src.analyze import export_analysis


def main() -> int:
    logger = get_logger()

    logger.info(f".env loaded from {ENV_FILE} -> {loaded}")

    # 1. Din "insamlingsstrategi"
    # Bredaste / vanligaste ord + årtalstriggers. Du kan justera listan.
    queries = [
    # breda ord
    "life", "world", "love", "dream", "dark", "light",
    "city", "road", "story", "star", "war", "home",
    "game", "blood", "night", "fire", "sea", "music",
    "king", "heart",
    # ord kombinerade med årtal
    "life 2025", "love 2025", "dream 2024", "war 2024",
    "story 2023", "city 2023", "man 2022", "night 2022",
    "home 2021", "king 2021"
    ]

    # Vi vill ha typ senaste ~5 åren. Justera om du vill.
    year_min = 2021

    try:
        # 2. Hämta rådata från många queries och många pages
        raw = build_dataset_for_year_range(
            queries=queries,
            year_min=year_min,
            max_pages_per_query=5,  # du kan öka om du vill ha ännu mer data
            sleep_sec=0.2,
        )

        logger.info(f"Rådatamängd efter sampling/filter >= {year_min}: {len(raw)} rader")

        # 3. Transformera till rent analysvänligt schema
        trf = transform_movies(raw)
        logger.info(f"Transformerad datamängd: {len(trf)} rader")

        # 4. Ladda till SQLite (full refresh)
        engine = get_engine()
        logger.info(f"SQLite path: {engine.url.database}")
        logger.info(f"Log file path: {LOG_PATH}")

        load_movies_refresh(engine, trf)

        # 5. Exportera analyser (CSV som Power BI kan läsa)
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
