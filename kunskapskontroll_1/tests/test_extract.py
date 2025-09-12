import json
import types
import pandas as pd
import builtins
import requests
import os
from src.extract import fetch_movies

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
    os.environ["OMDB_API_KEY"] = "TESTKEY"
    sample = {
        "Response": "True",
        "Search": [
            {"imdbID":"tt0001","Title":"A","Year":"1999","Type":"movie"},
            {"imdbID":"tt0002","Title":"B","Year":"2001–2003","Type":"series"},
        ],
    }
    def fake_get(url, params=None, timeout=None):
        return DummyResp(sample, 200)
    monkeypatch.setattr(requests, "get", fake_get)

    df = fetch_movies("X", page=1)
    assert list(df.columns) == ["imdbID","Title","Year","Type"]
    assert len(df) == 2
