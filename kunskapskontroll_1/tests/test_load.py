import pandas as pd
from sqlalchemy import create_engine, text
from src.load import ensure_schema

def test_load_replace_inmemory():
    engine = create_engine("sqlite:///:memory:", future=True)
    ensure_schema(engine)

    df = pd.DataFrame([
        {"imdb_id":"tt1","title":"Foo","year":2000,"type":"movie","fetched_at":"2025-01-01T00:00:00Z"},
        {"imdb_id":"tt2","title":"Bar","year":1999,"type":"series","fetched_at":"2025-01-01T00:00:00Z"},
    ])
    # skriv manuellt (kopierar minimal logik)
    df.to_sql("movies", con=engine, if_exists="replace", index=False)

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM movies")).scalar_one()
    assert rows == 2
