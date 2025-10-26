import pandas as pd
from typing import List, Optional
from datetime import datetime


class TransformError(Exception):
    pass


def transform_movies(
    df: pd.DataFrame,
    allowed_types: Optional[List[str]] = None,
    allowed_genres: Optional[List[str]] = None,
    dedupe_on: str = "title",
    year_min: Optional[int] = None,
) -> pd.DataFrame:
    """
    Tar rådataframe från extract-steget och:
    - normaliserar kolumner
    - konverterar datatyper (år, runtime, rating, votes)
    - filtrerar på årtal (year_min, t.ex. >= 2015)
    - filtrerar på typ (movie/series/etc) om allowed_types anges
    - filtrerar på genre om allowed_genres anges
    - tar bort rader som saknar imdb_rating eller imdb_votes
    - deduplikerar på valfri kolumn (default: title)
    Returnerar en analysklar DataFrame.
    Kastar TransformError om kritiska kolumner saknas.
    """

    required_cols = [
        "imdbID",
        "Title",
        "Year",
        "Type",
        "Genre",
        "Director",
        "Country",
        "Runtime",
        "imdbRating",
        "imdbVotes",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise TransformError(f"Saknar kolumner i rådata: {missing}")

    # jobba på en kopia
    df = df.copy()

    # 1. Normalisera kolumnnamn
    df = df.rename(
        columns={
            "imdbID": "imdb_id",
            "Title": "title",
            "Year": "year",
            "Type": "type",
            "Genre": "genre",
            "Director": "director",
            "Country": "country",
            "Runtime": "runtime",
            "imdbRating": "imdb_rating",
            "imdbVotes": "imdb_votes",
        }
    )

    # 2. Typkonverteringar

    # "2024" eller "2024– " -> hämta första 4 siffrorna som float
    df["year"] = (
        df["year"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
        .astype("float")
    )

    # "123 min" -> 123 som float
    df["runtime_min"] = (
        df["runtime"]
        .astype(str)
        .str.extract(r"(\d+)")
        [0]
        .astype("float")
    )

    # Rating -> float
    df["imdb_rating"] = pd.to_numeric(df["imdb_rating"], errors="coerce")

    # Votes "12,345" -> 12345 som float
    df["imdb_votes"] = (
        df["imdb_votes"]
        .astype(str)
        .str.replace(",", "", regex=False)
    )
    df["imdb_votes"] = pd.to_numeric(df["imdb_votes"], errors="coerce")

    # Primärgenre = första genren i listan
    df["genre_primary"] = (
        df["genre"]
        .astype(str)
        .str.split(",")
        .str[0]
        .str.strip()
    )

    # fetched_at timestamp
    df["fetched_at"] = datetime.utcnow().isoformat(timespec="seconds")

    # 3. Filtrering

    # 3a. Årsfilter (behåll filmer >= year_min)
    if year_min is not None:
        df = df[df["year"] >= float(year_min)]

    # 3b. Filtrera på typ (movie/series/etc)
    if allowed_types is not None:
        allowed_types_norm = {t.lower() for t in allowed_types}
        df = df[df["type"].str.lower().isin(allowed_types_norm)]

    # 3c. Filtrera på genre (Action, Thriller, ...)
    if allowed_genres is not None and len(allowed_genres) > 0:
        import re
        escaped = [re.escape(g) for g in allowed_genres]
        pattern = r"(" + "|".join(escaped) + r")"

        df = df[
            df["genre"]
            .astype(str)
            .str.contains(pattern, case=False, na=False)
        ]

    # 3d. Ta bort poster som saknar rating eller votes
    # Vi kräver att båda finns (inte NaN)
    df = df[df["imdb_rating"].notna() & df["imdb_votes"].notna()]

    # 4. Dedupe (t.ex. title)
    if dedupe_on not in df.columns:
        raise TransformError(
            f"Kan inte deduplicera på '{dedupe_on}' eftersom kolumnen saknas."
        )

    df = df.drop_duplicates(subset=[dedupe_on], keep="first").reset_index(drop=True)

    # 5. Returnera bestämt schema
    return df[
        [
            "imdb_id",
            "title",
            "year",
            "type",
            "genre",
            "genre_primary",
            "director",
            "country",
            "runtime_min",
            "imdb_rating",
            "imdb_votes",
            "fetched_at",
        ]
    ]
