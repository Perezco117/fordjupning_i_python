import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from src.load import ensure_schema, load_movies_refresh

def _base_df():
    return pd.DataFrame([
        {"imdb_id":"tt1","title":"Foo","year":2000,"type":"movie","fetched_at":"2025-01-01T00:00:00Z"},
        {"imdb_id":"tt2","title":"Bar","year":1999,"type":"series","fetched_at":"2025-01-01T00:00:00Z"},
    ])

def test_load_refresh_preserves_pk_and_counts():
    engine = create_engine("sqlite:///:memory:", future=True)
    ensure_schema(engine)

    df = _base_df()
    load_movies_refresh(engine, df)

    # För att se att det är två uppladdade som vi gjorde utanför funktionen.
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM movies")).scalar_one()
    assert rows == 2

    #Försöka sätta in duplikat imdb_id via pandas, borde få IntegrityError (Primary key)
    dup = pd.DataFrame([df.iloc[0].to_dict()])
    with pytest.raises(IntegrityError):
        dup.to_sql("movies", con=engine, if_exists="append", index=False)

    # För att se att det är fortfarande två
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM movies")).scalar_one()
    assert rows == 2

def test_load_refresh_is_full_replace_but_keeps_schema():
    engine = create_engine("sqlite:///:memory:", future=True)
    ensure_schema(engine)

    load_movies_refresh(engine, _base_df())

    # Nu "refreshar" vi med ett nytt DF, borde ta bort och sen lägga till.
    df2 = pd.DataFrame([
        {"imdb_id":"tt3","title":"Baz","year":2010,"type":"movie","fetched_at":"2025-01-02T00:00:00Z"},
    ])
    load_movies_refresh(engine, df2)

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM movies")).scalar_one()
        only_id = conn.execute(text("SELECT imdb_id FROM movies")).scalar_one()
    assert rows == 1
    assert only_id == "tt3"

def test_load_refresh_handles_empty_dataframe():
    engine = create_engine("sqlite:///:memory:", future=True)
    ensure_schema(engine)

    load_movies_refresh(engine, _base_df())

    #För att refresh med tomma värden men struktur av tabell är kvar.
    empty = _base_df().iloc[0:0]  # tom med rätta kolumner
    load_movies_refresh(engine, empty)

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM movies")).scalar_one()
        # Verifierar att strukturen är kvar genom att välja kolumner(blir error om tabellen är borta).
        cols = [r[1] for r in conn.execute(text("PRAGMA table_info('movies')")).fetchall()]
    assert rows == 0
    assert {"imdb_id","title","year","type","fetched_at"}.issubset(set(cols))
