from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv

# --- Ladda upp .env explicit från project root och kör över alla existerande env vars---
PROJECT_ROOT = Path(__file__).resolve().parent  # Denna filen är i project root
ENV_FILE = PROJECT_ROOT / ".env"
loaded = load_dotenv(dotenv_path=ENV_FILE, override=True)
# -------------------------------------------------------------------------------------

# Importerar moduler efter .env är uppladdad så dem ser rätt inställningar.
from src.logger import get_logger, LOG_PATH
from src.extract import fetch_movies, ExtractError
from src.transform import transform_movies, TransformError
from src.load import get_engine, load_movies_refresh

def main() -> int:
    logger = get_logger()

    # För att visa vilken env som används och vilken query som används.
    logger.info(f".env loaded from {ENV_FILE} -> {loaded}")
    query = os.getenv("SEARCH_QUERY", "Batman")
    if "SEARCH_QUERY" in os.environ:
        logger.info(f"SEARCH_QUERY = {query!r}")
    else:
        logger.warning("SEARCH_QUERY not set; using default 'Batman'")

    try:
        raw = fetch_movies(query=query, page=1)
        trf = transform_movies(raw)
        engine = get_engine()
        if engine.url.get_backend_name() == "sqlite":
            db_fs_path = Path(engine.url.database).resolve()
            logger.info(f"SQLite path: {db_fs_path}")
        logger.info(f"Log file path: {LOG_PATH}")

        load_movies_refresh(engine, trf)
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
