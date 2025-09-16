import pandas as pd
import pytest
from datetime import datetime
from src.transform import transform_movies, TransformError

def test_transform_movies_basic():
    raw = pd.DataFrame([
        {"imdbID":"tt1","Title":"  Foo ","Year":"2000–2002","Type":"Series"},
        {"imdbID":"tt1","Title":"Foo dup","Year":"2001","Type":"Series"},  # imdbID-dublett
        {"imdbID":"tt2","Title":"Bar","Year":"1999","Type":"movie"},
    ])
    out = transform_movies(raw)

    # 1) Korrekta kolumner
    assert set(out.columns) == {"imdb_id","title","year","type","fetched_at"}

    # 2) Duplikat skall vara borta
    assert len(out) == 2

    # 3) Year: tar endast 4 första siffror (2000–2002 -> 2000), dtype ska vara "nullable Int64"
    assert out.loc[out["imdb_id"]=="tt1","year"].item() == 2000
    assert str(out["year"].dtype) == "Int64"

    # 4) Titeln nedtrimmad och endast gemener
    assert out.loc[out["imdb_id"]=="tt1","title"].item() == "Foo"
    assert out.loc[out["imdb_id"]=="tt1","type"].item() == "series"
    assert out.loc[out["imdb_id"]=="tt2","type"].item() == "movie"

    # 5) fetched_at värde finns och är ISO parseable (ser ut såhär 'YYYY-MM-DDTHH:MM:SS+00:00')
    ts = out.loc[out.index[0], "fetched_at"]
    datetime.fromisoformat(ts)  # kommer raise om inte giltig ISO

def test_transform_movies_missing_required_raises():
    # TransformError dyker upp om vi saknar obligatoriska kolumner
    raw = pd.DataFrame([{"imdbID":"tt1","Title":"A"}])  # Year/Type saknas
    with pytest.raises(TransformError):
        transform_movies(raw)

def test_transform_movies_handles_na_year():
    # Year som inte är 4 siffror ska bli NA (nullable Int64)
    raw = pd.DataFrame([
        {"imdbID":"ttX","Title":"NA year","Year":"N/A","Type":"movie"},
    ])
    out = transform_movies(raw)
    assert pd.isna(out.loc[0, "year"])
    assert str(out["year"].dtype) == "Int64"

def test_transform_movies_empty_input():
    # En tom DF med rätt kolumner ska ge oss en tom DF med transformerat schema
    raw = pd.DataFrame(columns=["imdbID","Title","Year","Type"])
    out = transform_movies(raw)
    assert list(out.columns) == ["imdb_id","title","year","type","fetched_at"]
    assert len(out) == 0
    # dtype ska fortfarande vara "nullable Int64" även när tom
    assert str(out["year"].dtype) == "Int64"
