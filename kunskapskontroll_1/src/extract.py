from __future__ import annotations
import os
import requests
import pandas as pd
from dotenv import load_dotenv
from .logger import get_logger

load_dotenv()
logger = get_logger()

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OMDB_URL = "https://www.omdbapi.com/"

class ExtractError(Exception):
    pass

def fetch_movies(query: str, page: int = 1) -> pd.DataFrame:
    """
    Hämtar filmer via OMDb 's' (search). Returnerar DataFrame med kolumner:
    imdbID, Title, Year, Type
    """
    if not OMDB_API_KEY:
        raise ExtractError("OMDB_API_KEY saknas i .env")

    params = {"apikey": OMDB_API_KEY, "s": query, "page": page}
    try:
        r = requests.get(OMDB_URL, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        logger.error(f"HTTP-fel vid hämtning: {e}")
        raise ExtractError("Nätverksfel mot OMDb") from e

    if data.get("Response") != "True":
        # OMDb svarar med {"Response":"False","Error":"Movie not found!"} etc.
        msg = data.get("Error", "Okänt fel från OMDb")
        logger.warning(f"OMDb fel: {msg}")
        # Returnera tom DF för minimalism (istället för exception)
        return pd.DataFrame(columns=["imdbID", "Title", "Year", "Type"])

    items = data.get("Search", [])
    df = pd.DataFrame(items, columns=["imdbID", "Title", "Year", "Type"])
    logger.info(f"Hämtade {len(df)} rader från OMDb för '{query}' (page={page}).")
    return df
