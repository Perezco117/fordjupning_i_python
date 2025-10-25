import requests
import pandas as pd
import src.extract as ex
import pytest

class DummyResp:
    def __init__(self, payload: dict, status=200):
        self._payload = payload
        self.status_code = status
    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(self.status_code)
    def json(self):
        return self._payload


def test_fetch_movies_basic_success(monkeypatch):
    # Säkerställ att vi har en "API-nyckel" så _check_api_key inte bråkar
    monkeypatch.setenv("OMDB_API_KEY", "TESTKEY")
    # Vi behöver reloada modulen så att ex.OMDB_API_KEY uppdateras
    import importlib
    importlib.reload(ex)

    sample_payload = {
        "Response": "True",
        "Search": [
            {"imdbID": "tt0001", "Title": "Movie A", "Year": "1999", "Type": "movie"},
            {"imdbID": "tt0002", "Title": "Movie B", "Year": "2001–2003", "Type": "series"},
        ],
    }

    def fake_get(url, params=None, timeout=None):
        # kontrollera att rätt endpoint kallas
        assert "apikey" in params
        assert "s" in params  # search query
        return DummyResp(sample_payload, 200)

    monkeypatch.setattr(requests, "get", fake_get)

    df = ex.fetch_movies_basic(query="Batman", page=1)

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["imdbID", "Title", "Year", "Type"]
    assert len(df) == 2
    assert df.loc[0, "imdbID"] == "tt0001"


def test_fetch_movies_basic_no_results(monkeypatch):
    monkeypatch.setenv("OMDB_API_KEY", "TESTKEY")
    import importlib
    importlib.reload(ex)

    payload_no_results = {
        "Response": "False",
        "Error": "Movie not found!"
    }

    def fake_get(url, params=None, timeout=None):
        return DummyResp(payload_no_results, 200)

    monkeypatch.setattr(requests, "get", fake_get)

    df = ex.fetch_movies_basic(query="asdasdNoHit", page=1)
    assert df.empty
    assert list(df.columns) == ["imdbID", "Title", "Year", "Type"]


def test_fetch_movie_details_success(monkeypatch):
    monkeypatch.setenv("OMDB_API_KEY", "TESTKEY")
    import importlib
    importlib.reload(ex)

    detail_payload = {
        "Response": "True",
        "imdbID": "tt1234",
        "Title": "X",
        "Year": "2020",
        "Genre": "Action, Crime",
        "Type": "movie",
        "Director": "Me",
        "Country": "USA",
        "Runtime": "123 min",
        "imdbRating": "7.5",
        "imdbVotes": "12,345",
    }

    def fake_get(url, params=None, timeout=None):
        # här testar vi att funktionen använder 'i' (imdbID) och inte bara 's'
        assert "i" in params
        return DummyResp(detail_payload, 200)

    monkeypatch.setattr(requests, "get", fake_get)

    out = ex.fetch_movie_details("tt1234")
    assert out["imdbID"] == "tt1234"
    assert out["Genre"] == "Action, Crime"
    assert out["Runtime"] == "123 min"


def test_fetch_movies_full_happy_path(monkeypatch):
    """
    fetch_movies_full ska:
    1. anropa fetch_movies_basic -> få en lista med två imdbID
    2. anropa fetch_movie_details för varje imdbID
    3. returnera DataFrame med utökade kolumner
    Vi mockar requests.get så att:
       - om params har "s": returnera söklista
       - om params har "i": returnera detalj för just den imdbID:n
    """
    monkeypatch.setenv("OMDB_API_KEY", "TESTKEY")
    import importlib
    importlib.reload(ex)

    search_payload = {
        "Response": "True",
        "Search": [
            {"imdbID": "tt0001", "Title": "Movie A", "Year": "1999", "Type": "movie"},
            {"imdbID": "tt0002", "Title": "Movie B", "Year": "2001", "Type": "series"},
        ],
    }

    details_payloads = {
        "tt0001": {
            "Response": "True",
            "imdbID": "tt0001",
            "Title": "Movie A",
            "Year": "1999",
            "Type": "movie",
            "Genre": "Action, Thriller",
            "Director": "Dir A",
            "Country": "USA",
            "Runtime": "110 min",
            "imdbRating": "7.1",
            "imdbVotes": "10,000",
        },
        "tt0002": {
            "Response": "True",
            "imdbID": "tt0002",
            "Title": "Movie B",
            "Year": "2001",
            "Type": "series",
            "Genre": "Drama",
            "Director": "Dir B",
            "Country": "UK",
            "Runtime": "45 min",
            "imdbRating": "8.2",
            "imdbVotes": "25,500",
        },
    }

    def fake_get(url, params=None, timeout=None):
        if "s" in params:
            # sök
            return DummyResp(search_payload, 200)
        elif "i" in params:
            imdb_id = params["i"]
            return DummyResp(details_payloads[imdb_id], 200)
        else:
            raise AssertionError("Unexpected params to requests.get")

    monkeypatch.setattr(requests, "get", fake_get)

    df = ex.fetch_movies_full(query="Batman", page=1, sleep_sec=0)

    # Kolumner vi lovade i fetch_movies_full
    expected_cols = [
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
    assert all(col in df.columns for col in expected_cols)
    assert len(df) == 2
    assert set(df["imdbID"]) == {"tt0001", "tt0002"}


def test_fetch_all_movies_full_multiple_pages(monkeypatch):
    """
    Vi mockar fetch_movies_basic och fetch_movie_details för att simulera
    2 sidor med 3 titlar totalt. Vi testar att funktionen returnerar alla rader unikt.
    """

    # fake data för två sidor
    page1 = pd.DataFrame([
        {"imdbID": "tt1", "Title": "Film1", "Year": "2023", "Type": "movie"},
        {"imdbID": "tt2", "Title": "Film2", "Year": "2022", "Type": "movie"},
    ])
    page2 = pd.DataFrame([
        {"imdbID": "tt3", "Title": "Film3", "Year": "2021", "Type": "series"},
    ])

    # monkeypatcha functions
    monkeypatch.setattr(ex, "fetch_movies_basic", lambda query, page: page1 if page == 1 else (page2 if page == 2 else pd.DataFrame()))
    monkeypatch.setattr(ex, "fetch_movie_details", lambda imdb_id: {"imdbID": imdb_id, "Title": imdb_id, "Year": "2023", "Type": "movie"})

    df = ex.fetch_all_movies_full(query="test", max_pages=3)
    assert len(df) == 3
    assert set(df["imdbID"]) == {"tt1", "tt2", "tt3"}


def test_build_dataset_for_year_range_filters_years(monkeypatch):
    """
    Vi mockar fetch_all_movies_full så att varje query ger olika årtal.
    Vi kontrollerar att funktionen bara behåller titlar >= year_min.
    """

    def fake_fetch_all_movies_full(query, max_pages, sleep_sec):
        if query == "old":
            return pd.DataFrame([{"imdbID": "tt_old", "Title": "Oldie", "Year": "2018", "Type": "movie"}])
        else:
            return pd.DataFrame([{"imdbID": "tt_new", "Title": "Newie", "Year": "2023", "Type": "movie"}])

    monkeypatch.setattr(ex, "fetch_all_movies_full", fake_fetch_all_movies_full)

    df = ex.build_dataset_for_year_range(
        queries=["old", "new"],
        year_min=2021,
        max_pages_per_query=1,
        sleep_sec=0
    )

    # endast "new" ska vara kvar eftersom 2018 < 2021
    assert len(df) == 1
    assert df.iloc[0]["imdbID"] == "tt_new"
