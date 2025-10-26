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


def fetch_all_movies_full(
    query: str,
    max_pages: int = 10,
    sleep_sec: float = 0.2,
    year_min: int | None = None,
    global_seen_ids: set[str] | None = None,
) -> pd.DataFrame:
    """
    Hämtar FLERA sidor för en sökterm.
    Loopar page=1..max_pages tills vi inte får fler resultat.
    För varje imdbID hämtas detaljer (Genre, Runtime, Rating, Votes, etc).

    OPTIMERINGAR (för färre API-anrop):
    - Vi hoppar över titlar som är äldre än year_min INNAN vi ringer fetch_movie_details.
    - Vi hoppar över imdbID som redan finns i global_seen_ids
      (titlar vi redan har hämtat via en annan sida eller en annan query).

    Returnerar en DataFrame med full info för alla (nya, relevanta) titlar.
    """
    all_details: list[dict] = []

    # Om main/bygg-steget skickar in en delad set så använder vi den.
    # Annars skapar vi en lokal set för den här queryn.
    if global_seen_ids is None:
        global_seen_ids = set()

    for page in range(1, max_pages + 1):
        page_df = fetch_movies_basic(query=query, page=page)

        # Sluta om den här sidan inte gav något
        if page_df.empty:
            logger.info(f"Inga fler resultat för query={query} efter page={page-1}. Stoppar.")
            break

        # Vi går rad för rad (istället för bara unique()) så vi kan läsa Year per titel
        for _, row in page_df.iterrows():
            imdb_id = row.get("imdbID")
            year_raw = row.get("Year")

            # Försök tolka första 4 siffrorna i Year (t.ex. "2022–", "2021-2023")
            year_clean = None
            if isinstance(year_raw, str) and len(year_raw) >= 4 and year_raw[:4].isdigit():
                year_clean = int(year_raw[:4])

            # 1. Årsfilter: hoppa över äldre titlar innan vi ens slår detaljer
            if year_min is not None and year_clean is not None and year_clean < year_min:
                continue

            # 2. Dublettfilter över hela körningen:
            #    hoppa om vi redan har hämtat detaljer för detta imdbID
            if imdb_id in global_seen_ids:
                continue

            # Om den överlever båda filtren -> hämta detaljer
            det = fetch_movie_details(imdb_id)
            if det:
                all_details.append(det)
                global_seen_ids.add(imdb_id)

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
    logger.info(f"[{query}] Totalt {len(final_df)} titlar efter filtrering/avdubblering.")
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

    OPTIMERINGAR (för färre API-anrop totalt):
    - Vi skapar en gemensam global_seen_ids = set() här,
      och skickar in samma set till varje fetch_all_movies_full().
      Det betyder att om t.ex. 'The Dark Knight' dyker upp i både "dark" och "night"
      så hämtar vi detaljer EN gång, inte två.
    - Vi skickar också ner year_min så att fetch_all_movies_full
      inte hämtar detaljer alls för gamla titlar.
    """

    all_batches: list[pd.DataFrame] = []

    # Delad set över ALLA queries i denna körning
    global_seen_ids: set[str] = set()

    for q in queries:
        df_q = fetch_all_movies_full(
            query=q,
            max_pages=max_pages_per_query,
            sleep_sec=sleep_sec,
            year_min=year_min,
            global_seen_ids=global_seen_ids,
        )
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

    # Ta bort dubbletter på imdbID (ska i princip redan vara unikt pga global_seen_ids,
    # detta är mest en sista säkerhetsspärr)
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
