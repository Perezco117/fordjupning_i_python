import requests
import src.extract as ex  # Importerar modulen så vi kan patch attributen

class DummyResp:
    def __init__(self, payload: dict, status=200):
        self._payload = payload
        self.status_code = status
    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(self.status_code)
    def json(self):
        return self._payload

def test_fetch_movies_monkeypatch(monkeypatch):
    # Patchar attributet när vi importerade modulen.
    monkeypatch.setattr(ex, "OMDB_API_KEY", "TESTKEY")

    sample = {
        "Response": "True",
        "Search": [
            {"imdbID": "tt0001", "Title": "A", "Year": "1999",       "Type": "movie"},
            {"imdbID": "tt0002", "Title": "B", "Year": "2001–2003", "Type": "series"},
        ],
    }

    def fake_get(url, params=None, timeout=None):
        return DummyResp(sample, 200)

    monkeypatch.setattr(requests, "get", fake_get)

    df = ex.fetch_movies("X", page=1)
    assert list(df.columns) == ["imdbID", "Title", "Year", "Type"]
    assert len(df) == 2

def test_fetch_movies_handles_omdb_error(monkeypatch):
    # Samma som tidigare, för att undvika att den failar på key.
    monkeypatch.setattr(ex, "OMDB_API_KEY", "TESTKEY")

    # Simulerar att OMDbs svar Response=False
    sample = {"Response": "False", "Error": "Movie not found!"}

    def fake_get(url, params=None, timeout=None):
        return DummyResp(sample, 200)

    monkeypatch.setattr(requests, "get", fake_get)

    df = ex.fetch_movies("NORESULTS", page=1)
    assert df.empty
    assert list(df.columns) == ["imdbID", "Title", "Year", "Type"]
