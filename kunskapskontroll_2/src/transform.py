from __future__ import annotations
import pandas as pd
from datetime import datetime, timezone
from .logger import get_logger

logger = get_logger()

class TransformError(Exception):
    pass


def _to_int_year(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.extract(r"(\d{4})", expand=False)
        .astype("Int64")
    )


def _runtime_to_minutes(runtime_series: pd.Series) -> pd.Series:
    return (
        runtime_series.astype(str)
        .str.extract(r"(\d+)", expand=False)
        .astype("Int64")
    )


def _to_float_safely(series: pd.Series) -> pd.Series:
    out = pd.to_numeric(series.replace("N/A", pd.NA), errors="coerce")
    return out.astype("Float64")


def _votes_to_int(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .replace("N/A", pd.NA)
    )
    return pd.to_numeric(cleaned, errors="coerce").astype("Int64")


def transform_movies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tar en DataFrame med kolumner från extract-steget, t.ex.:
        imdbID, Title, Year, Type, Genre, Director, Country,
        Runtime, imdbRating, imdbVotes
    Returnerar en städad DataFrame med konsekvent schema:
        imdb_id, title, year, type,
        genre_full, genre_primary,
        director, country,
        runtime_min,
        imdb_rating, imdb_votes,
        fetched_at
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
        "Genre": "genre_full",
        "Director": "director",
        "Country": "country",
        "Runtime": "runtime",
        "imdbRating": "imdb_rating",
        "imdbVotes": "imdb_votes",
    })

    # trim strings
    for col in ["title", "type", "genre_full", "director", "country"]:
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip()

    # normalisera type
    out["type"] = out["type"].astype(str).str.lower()

    # år → Int64
    out["year"] = _to_int_year(out["year"])

    # runtime → minuter
    if "runtime" in out.columns:
        out["runtime_min"] = _runtime_to_minutes(out["runtime"])
    else:
        out["runtime_min"] = pd.Series([], dtype="Int64")

    # imdb_rating → Float64
    if "imdb_rating" in out.columns:
        out["imdb_rating"] = _to_float_safely(out["imdb_rating"])
    else:
        out["imdb_rating"] = pd.Series([], dtype="Float64")

    # imdb_votes → Int64
    if "imdb_votes" in out.columns:
        out["imdb_votes"] = _votes_to_int(out["imdb_votes"])
    else:
        out["imdb_votes"] = pd.Series([], dtype="Int64")

    # första genren
    if "genre_full" in out.columns:
        out["genre_primary"] = (
            out["genre_full"]
            .astype(str)
            .str.split(",")
            .str[0]
            .str.strip()
            .replace("N/A", pd.NA)
        )
    else:
        out["genre_primary"] = pd.NA

    # ta unika imdb_id
    out = out.drop_duplicates(subset=["imdb_id"]).reset_index(drop=True)

    # tidsstämpel
    out["fetched_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # stabil kolumnordning
    final_cols = [
        "imdb_id",
        "title",
        "year",
        "type",
        "genre_full",
        "genre_primary",
        "director",
        "country",
        "runtime_min",
        "imdb_rating",
        "imdb_votes",
        "fetched_at",
    ]
    for c in final_cols:
        if c not in out.columns:
            out[c] = pd.NA

    out = out[final_cols]

    logger.info(f"Transformerade {len(out)} rader (utökat schema).")
    return out
