from __future__ import annotations
import pandas as pd
from datetime import datetime, timezone
from .logger import get_logger

logger = get_logger()

class TransformError(Exception):
    pass

def transform_movies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tar DF med imdbID, Title, Year, Type och returnerar:
    imdb_id, title, year(int eller NaN), type, fetched_at(UTC ISO8601)
    Duplicat (imdb_id) tas bort.
    """
    required = {"imdbID", "Title", "Year", "Type"}
    missing = required - set(df.columns)
    if missing:
        raise TransformError(f"Saknar kolumner: {sorted(missing)}")

    out = df.copy()

    out = out.rename(columns={
        "imdbID": "imdb_id",
        "Title": "title",
        "Year": "year",
        "Type": "type",
    })

    # Year kan vara "1994" eller "1994–1996". För enkelhet: plocka första nummerserien.
    out["year"] = (
        out["year"]
        .astype(str)
        .str.extract(r"(\d{4})", expand=False)
        .astype("Int64")  # tillåter NA
    )

    out["title"] = out["title"].astype(str).str.strip()
    out["type"] = out["type"].astype(str).str.strip().str.lower()

    # unika på imdb_id
    out = out.drop_duplicates(subset=["imdb_id"]).reset_index(drop=True)

    out["fetched_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    logger.info(f"Transformerade {len(out)} rader.")
    return out
