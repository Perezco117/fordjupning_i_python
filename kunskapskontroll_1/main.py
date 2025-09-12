# main.py
from __future__ import annotations
import os
from dotenv import load_dotenv
from src.logger import get_logger
from src.extract import fetch_movies, ExtractError
from src.transform import transform_movies, TransformError
from src.load import get_engine, load_movies_replace

def main() -> int:
    load_dotenv()
    logger = get_logger()

    query = os.getenv("SEARCH_QUERY", "Batman")
    try:
        raw = fetch_movies(query=query, page=1)
        trf = transform_movies(raw)
        engine = get_engine()
        load_movies_replace(engine, trf)
        logger.info("✅ ETL klart utan fel.")
        return 0
    except (ExtractError, TransformError) as e:
        logger.exception(f"❌ ETL avbröts: {e}")
        return 1
    except Exception as e:
        logger.exception(f"❌ Oväntat fel: {e}")
        return 2

if __name__ == "__main__":
    raise SystemExit(main())
