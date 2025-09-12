import pandas as pd
from src.transform import transform_movies

def test_transform_movies_basic():
    raw = pd.DataFrame([
        {"imdbID":"tt1","Title":"  Foo ","Year":"2000–2002","Type":"Series"},
        {"imdbID":"tt1","Title":"Foo dup","Year":"2001","Type":"Series"},  # dup id
        {"imdbID":"tt2","Title":"Bar","Year":"1999","Type":"movie"},
    ])
    out = transform_movies(raw)
    assert set(out.columns) == {"imdb_id","title","year","type","fetched_at"}
    # dubbletten borttagen -> 2 rader
    assert len(out) == 2
    # Year extraheras som första 4 siffror
    assert out.loc[out["imdb_id"]=="tt1","year"].item() == 2000
