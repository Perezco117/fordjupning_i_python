import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from src.load import ensure_schema, load_movies_refresh


def _sample_extended_df():
    return pd.DataFrame([
        {
            "imdb_id": "tt1",
            "title": "Foo",
            "year": 2000,
            "type": "movie",
            "genre": "Action, Thriller",
            "genre_primary": "Action",
            "director": "Dir A",
            "country": "USA",
            "runtime_min": 110,
            "imdb_rating": 7.1,
            "imdb_votes": 10000,
            "fetched_at": "2025-01-01T00:00:00Z",
        },
        {
            "imdb_id": "tt2",
            "title": "Bar",
            "year": 1999,
            "type": "series",
            "genre": "Drama",
            "genre_primary": "Drama",
            "director": "Dir B",
            "country": "UK",
            "runtime_min": 45,
            "imdb_rating": 8.2,
            "imdb_votes": 25500,
            "fetched_at": "2025-01-01T00:00:00Z",
        },
    ])


def test_load_refresh_preserves_pk_and_counts():
    engine = create_engine("sqlite:///:memory:", future=True)
    ensure_schema(engine)

    df = _sample_extended_df()
    load_movies_refresh(engine, df)

    # kolla att vi fick 2 rader
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM movies")).scalar_one()
    assert rows == 2

    # prova skriva in en dublett med samma imdb_id -> ska kasta IntegrityError
    dup = pd.DataFrame([df.iloc[0].to_dict()])
    with pytest.raises(IntegrityError):
        dup.to_sql("movies", con=engine, if_exists="append", index=False)

    # antalet rader ska forfarande vara 2
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM movies")).scalar_one()
    assert rows == 2


def test_load_refresh_is_full_replace_but_keeps_schema():
    engine = create_engine("sqlite:///:memory:", future=True)
    ensure_schema(engine)

    load_movies_refresh(engine, _sample_extended_df())

    # refresha med en ny df som bara har en rad (tt3)
    df2 = pd.DataFrame([{
        "imdb_id": "tt3",
        "title": "Baz",
        "year": 2010,
        "type": "movie",
        "genre": "Action",
        "genre_primary": "Action",
        "director": "Dir C",
        "country": "USA",
        "runtime_min": 100,
        "imdb_rating": 9.0,
        "imdb_votes": 99999,
        "fetched_at": "2025-01-02T00:00:00Z",
    }])
    load_movies_refresh(engine, df2)

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM movies")).scalar_one()
        only_id = conn.execute(text("SELECT imdb_id FROM movies")).scalar_one()
    assert rows == 1
    assert only_id == "tt3"


def test_load_refresh_handles_empty_dataframe():
    engine = create_engine("sqlite:///:memory:", future=True)
    ensure_schema(engine)

    load_movies_refresh(engine, _sample_extended_df())

    # empty refresh
    empty_df = _sample_extended_df().iloc[0:0]
    load_movies_refresh(engine, empty_df)

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM movies")).scalar_one()
        # kolla att tabellen fortfarande finns och har alla kolumner
        pragma_rows = conn.execute(text("PRAGMA table_info('movies')")).fetchall()

    assert rows == 0

    cols = [r[1] for r in pragma_rows]
    assert {
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
    }.issubset(set(cols))
