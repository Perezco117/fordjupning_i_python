from __future__ import annotations
import os
import time
from pathlib import Path
import requests
import pandas as pd
from dotenv import load_dotenv
from .logger import get_logger

# Ladda .env om den finns i projektroten så vi kan plocka OMDB_API_KEY
# (Detta stör inte paths; paths styrs i load/logger.)
BASE_DIR = Path(__file__).resolve().parents[1]
ENV_FILE = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_FILE, override=False)

logger = get_logger()

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OMDB_URL = "https://www.omdbapi.com/"

class ExtractError(Exception):
    pass


def _check_api_key():
    if not OMDB_API_KEY:
        raise ExtractError("OMDB_API_KEY saknas i .env")


def fetch_movies_basic(query: str, page: int = 1) -> pd.DataFrame:
    """
    Hämtar en enkel söklista från OMDb API med parametern 's'.
    Returnerar DataFrame med kolumner: imdbID, Title, Year, Type
    OBS: detta är OFULL info (ingen Genre, Rating, osv.).
    """
    _check_api_key()

    params = {"apikey": OMDB_API_KEY, "s": query, "page": page}
    try:
        r = requests.get(OMDB_URL, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        logger.error(f"HTTP-fel vid hämtning (query={query}, page={page}): {e}")
        raise ExtractError("Nätverksfel mot OMDb") from e

    if data.get("Response") != "True":
        msg = data.get("Error", "Okänt fel från OMDb")
        logger.warning(f"OMDb fel (query={query}, page={page}): {msg}")
        return pd.DataFrame(columns=["imdbID", "Title", "Year", "Type"])

    items = data.get("Search", [])
    df = pd.DataFrame(items, columns=["imdbID", "Title", "Year", "Type"])
    logger.info(f"Hämtade {len(df)} rader från OMDb för '{query}' (page={page}).")
    return df


def fetch_movie_details(imdb_id: str) -> dict:
    """
    Hämtar detaljerad info för en enskild titel via ID (?i=<imdb_id>).
    Returnerar en dict med t.ex. Genre, Director, Runtime, imdbRating osv.
    Vid fel returnerar {} istället för att krascha.
    """
    _check_api_key()

    params = {"apikey": OMDB_API_KEY, "i": imdb_id, "plot": "short"}
    try:
        r = requests.get(OMDB_URL, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        logger.error(f"HTTP-fel vid detaljhämntning imdb_id={imdb_id}: {e}")
        return {}

    if data.get("Response") != "True":
        logger.warning(f"Detalj misslyckades för {imdb_id}: {data.get('Error')}")
        return {}

    return data


def fetch_movies_full(query: str, page: int = 1, sleep_sec: float = 0.2) -> pd.DataFrame:
    """
    (Enkel variant, en sida.)
    1. Hämtar search results för en sida
    2. Hämtar detaljer för varje imdbID
    3. Returnerar DataFrame med utökade kolumner.
    Behålls för bakåtkompatibilitet / tester.
    """
    base_df = fetch_movies_basic(query=query, page=page)

    if base_df.empty:
        logger.warning("Inget resultat från sökningen - returnerar tom DF.")
        return pd.DataFrame(columns=[
            "imdbID", "Title", "Year", "Type",
            "Genre", "Director", "Country",
            "Runtime", "imdbRating", "imdbVotes",
        ])

    details_list: list[dict] = []
    for imdb_id in base_df["imdbID"].unique():
        d = fetch_movie_details(imdb_id)
        if d:
            details_list.append(d)
        time.sleep(sleep_sec)

    if not details_list:
        logger.warning("Inga detaljer kunde hämtas - returnerar basic info.")
        return base_df

    details_df = pd.DataFrame(details_list)

    cols_we_want = [
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

    for c in cols_we_want:
        if c not in details_df.columns:
            details_df[c] = pd.NA

    final_df = details_df[cols_we_want].copy()
    logger.info(f"Hämtade detaljer (1 sida) för {len(final_df)} titlar.")
    return final_df


def fetch_all_movies_full(query: str, max_pages: int = 10, sleep_sec: float = 0.2) -> pd.DataFrame:
    """
    Hämtar FLERA sidor för en sökterm.
    Loopar page=1..max_pages tills vi inte får fler resultat.
    För varje imdbID hämtas detaljer (Genre, Runtime, Rating, Votes, etc).
    Returnerar en DataFrame med full info för alla unika titlar.
    """
    all_details: list[dict] = []
    seen_ids: set[str] = set()

    for page in range(1, max_pages + 1):
        page_df = fetch_movies_basic(query=query, page=page)

        # Sluta om den här sidan inte gav något
        if page_df.empty:
            logger.info(f"Inga fler resultat för query={query} efter page={page-1}. Stoppar.")
            break

        for imdb_id in page_df["imdbID"].unique():
            if imdb_id in seen_ids:
                continue

            det = fetch_movie_details(imdb_id)
            if det:
                all_details.append(det)
                seen_ids.add(imdb_id)

            time.sleep(sleep_sec)

    if not all_details:
        logger.warning(f"Inga detaljerade poster alls för query={query}")
        return pd.DataFrame(columns=[
            "imdbID","Title","Year","Type",
            "Genre","Director","Country",
            "Runtime","imdbRating","imdbVotes",
        ])

    details_df = pd.DataFrame(all_details)

    cols_we_want = [
        "imdbID", "Title", "Year", "Type",
        "Genre", "Director", "Country",
        "Runtime", "imdbRating", "imdbVotes",
    ]
    for c in cols_we_want:
        if c not in details_df.columns:
            details_df[c] = pd.NA

    final_df = details_df[cols_we_want].copy()
    logger.info(f"[{query}] Totalt {len(final_df)} unika titlar över flera sidor.")
    return final_df


def build_dataset_for_year_range(
    queries: list[str],
    year_min: int,
    max_pages_per_query: int = 5,
    sleep_sec: float = 0.2,
) -> pd.DataFrame:
    """
    Kör fetch_all_movies_full för flera sökord (t.ex. ["the","love","night","2024","2023"])
    Slår ihop alla resultat, tar bort dubbletter och filtrerar på år >= year_min.

    Poängen: OMDb stödjer inte 'ge mig alla filmer per år', så vi bygger ett
    representativt dataset själva genom breda söktermer.
    """

    all_batches: list[pd.DataFrame] = []

    for q in queries:
        df_q = fetch_all_movies_full(query=q, max_pages=max_pages_per_query, sleep_sec=sleep_sec)
        if not df_q.empty:
            df_q["__source_query__"] = q
            all_batches.append(df_q)

    if not all_batches:
        logger.warning("Inget data hittades alls för de givna queries.")
        return pd.DataFrame(columns=[
            "imdbID","Title","Year","Type",
            "Genre","Director","Country",
            "Runtime","imdbRating","imdbVotes",
            "__source_query__",
        ])

    big = pd.concat(all_batches, ignore_index=True)

    # Ta bort dubbletter på imdbID
    big = big.drop_duplicates(subset=["imdbID"]).reset_index(drop=True)

    # Filtrera på år (Year kan vara '2021–2022', '2024', 'N/A'...)
    year_extracted = (
        big["Year"]
        .astype(str)
        .str.extract(r"(\d{4})", expand=False)
        .astype("Int64")
    )
    mask_recent = year_extracted >= year_min
    big = big[mask_recent].reset_index(drop=True)

    logger.info(
        f"Efter sammanslagning och filtrering på year >= {year_min} "
        f"finns {len(big)} rader kvar."
    )
    return big
