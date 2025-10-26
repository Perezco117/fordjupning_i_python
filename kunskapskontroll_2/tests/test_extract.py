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
    # säkerställ API_KEY så reload funkar
    monkeypatch.setenv("OMDB_API_KEY", "TESTKEY")
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
        # vi vill säkerställa att funktionen använder ?s= och inte ?i=
        assert "s" in params
        assert "apikey" in params
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
        # vi vill säkerställa att funktionen använder ?i= och inte ?s=
        assert "i" in params
        return DummyResp(detail_payload, 200)

    monkeypatch.setattr(requests, "get", fake_get)

    out = ex.fetch_movie_details("tt1234")
    assert out["imdbID"] == "tt1234"
    assert out["Genre"] == "Action, Crime"
    assert out["Runtime"] == "123 min"


def test_fetch_all_movies_full_multiple_pages(monkeypatch):
    # page1 har två titlar, page2 har en tredje, page3 är tom => loopen bryter
    page1 = pd.DataFrame([
        {"imdbID": "tt1", "Title": "Film1", "Year": "2023", "Type": "movie"},
        {"imdbID": "tt2", "Title": "Film2", "Year": "2022", "Type": "movie"},
    ])
    page2 = pd.DataFrame([
        {"imdbID": "tt3", "Title": "Film3", "Year": "2021", "Type": "series"},
    ])

    def fake_basic(query, page):
        if page == 1:
            return page1
        elif page == 2:
            return page2
        else:
            return pd.DataFrame()

    def fake_details(imdb_id):
        # mocka minimalt svar. Resten av kolumner fylls som pd.NA i funktionen.
        return {
            "imdbID": imdb_id,
            "Title": imdb_id,
            "Year": "2023",
            "Type": "movie",
        }

    monkeypatch.setattr(ex, "fetch_movies_basic", fake_basic)
    monkeypatch.setattr(ex, "fetch_movie_details", fake_details)

    df = ex.fetch_all_movies_full(query="test", max_pages=3, sleep_sec=0)
    assert set(df["imdbID"]) == {"tt1", "tt2", "tt3"}
    assert len(df) == 3


def test_fetch_all_movies_full_respects_year_min(monkeypatch):
    """
    Viktigt: vi testar att year_min hindrar gamla titlar från att ens hämta detaljer.
    """
    calls = {"details_calls": []}

    page_only = pd.DataFrame([
        {"imdbID": "tt_new", "Title": "NewFilm", "Year": "2023", "Type": "movie"},
        {"imdbID": "tt_old", "Title": "OldFilm", "Year": "2015", "Type": "movie"},
    ])

    def fake_basic(query, page):
        # bara en sida, sen tom
        return page_only if page == 1 else pd.DataFrame()

    def fake_details(imdb_id):
        # logga vilka som faktiskt försöker hämta detaljer
        calls["details_calls"].append(imdb_id)
        return {
            "imdbID": imdb_id,
            "Title": imdb_id,
            "Year": "2023",
            "Type": "movie",
        }

    monkeypatch.setattr(ex, "fetch_movies_basic", fake_basic)
    monkeypatch.setattr(ex, "fetch_movie_details", fake_details)

    df = ex.fetch_all_movies_full(
        query="test",
        max_pages=5,
        sleep_sec=0,
        year_min=2020,          # <- core grej
        global_seen_ids=None,
    )

    # Endast tt_new ska komma med (tt_old är från 2015)
    assert list(df["imdbID"]) == ["tt_new"]
    # Och viktigt: vi ska aldrig ens försöka hämta detaljer för tt_old
    assert calls["details_calls"] == ["tt_new"]


def test_fetch_all_movies_full_respects_global_seen_ids(monkeypatch):
    """
    Viktigt: vi testar att global_seen_ids gör att vi INTE hämtar detaljer igen
    för samma imdbID.
    """
    calls = {"details_calls": []}

    page_only = pd.DataFrame([
        {"imdbID": "tt1", "Title": "Film1", "Year": "2024", "Type": "movie"},
    ])

    def fake_basic(query, page):
        # Samma sida varje gång -> borde bryta efter första loop ändå.
        return page_only if page == 1 else pd.DataFrame()

    def fake_details(imdb_id):
        calls["details_calls"].append(imdb_id)
        return {
            "imdbID": imdb_id,
            "Title": imdb_id,
            "Year": "2024",
            "Type": "movie",
        }

    monkeypatch.setattr(ex, "fetch_movies_basic", fake_basic)
    monkeypatch.setattr(ex, "fetch_movie_details", fake_details)

    # Säg att vi redan har samlat tt1 från en tidigare query
    already_seen = {"tt1"}

    df = ex.fetch_all_movies_full(
        query="test",
        max_pages=5,
        sleep_sec=0,
        year_min=None,
        global_seen_ids=already_seen,
    )

    # Om den respekterar global_seen_ids ska vi inte ens kalla fake_details
    assert df.empty
    assert calls["details_calls"] == []


def test_build_dataset_for_year_range_filters_years(monkeypatch):
    """
    Den här testar build_dataset_for_year_range.
    Vi mockar fetch_all_movies_full så att:
    - query "old" ger bara 2018
    - query "new" ger bara 2023
    build_dataset_for_year_range ska bara behålla >= 2021.
    """

    def fake_fetch_all_movies_full(query, max_pages, sleep_sec, year_min=None, global_seen_ids=None):
        if query == "old":
            return pd.DataFrame([
                {"imdbID": "tt_old", "Title": "Oldie", "Year": "2018", "Type": "movie"}
            ])
        else:
            return pd.DataFrame([
                {"imdbID": "tt_new", "Title": "Newie", "Year": "2023", "Type": "movie"}
            ])

    monkeypatch.setattr(ex, "fetch_all_movies_full", fake_fetch_all_movies_full)

    df = ex.build_dataset_for_year_range(
        queries=["old", "new"],
        year_min=2021,
        max_pages_per_query=1,
        sleep_sec=0
    )

    assert len(df) == 1
    assert df.iloc[0]["imdbID"] == "tt_new"
    # check that __source_query__ column is still added for context
    assert "__source_query__" in df.columns
    assert df.iloc[0]["__source_query__"] == "new"
