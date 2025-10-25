import pandas as pd
import pytest
from sqlalchemy import create_engine, text
import src.analyze as an
import src.load as ld


def _prep_in_memory_db():
    """
    Skapar en in-memory sqlite engine med samma schema
    som load.ensure_schema() definierar, och fyller med lite testdata.
    Returnerar engine.
    """
    engine = create_engine("sqlite:///:memory:", future=True)
    ld.ensure_schema(engine)

    sample = pd.DataFrame([
        {
            "imdb_id": "tt1",
            "title": "Foo",
            "year": 2000,
            "type": "movie",
            "genre_full": "Action, Thriller",
            "genre_primary": "Action",
            "director": "Dir A",
            "country": "USA",
            "runtime_min": 110,
            "imdb_rating": 7.0,
            "imdb_votes": 10000,
            "fetched_at": "2025-01-01T00:00:00Z",
        },
        {
            "imdb_id": "tt2",
            "title": "Bar",
            "year": 2000,
            "type": "movie",
            "genre_full": "Drama",
            "genre_primary": "Drama",
            "director": "Dir B",
            "country": "UK",
            "runtime_min": 90,
            "imdb_rating": 9.0,
            "imdb_votes": 20000,
            "fetched_at": "2025-01-01T00:00:00Z",
        },
        {
            "imdb_id": "tt3",
            "title": "Baz",
            "year": 1999,
            "type": "series",
            "genre_full": "Drama",
            "genre_primary": "Drama",
            "director": "Dir C",
            "country": "SE",
            "runtime_min": 45,
            "imdb_rating": 8.0,
            "imdb_votes": 15000,
            "fetched_at": "2025-01-02T00:00:00Z",
        },
    ])
    sample.to_sql("movies", con=engine, if_exists="append", index=False)
    return engine


def test_read_movies_df(monkeypatch):
    # mocka get_engine så analyze använder vår in-memory DB
    engine = _prep_in_memory_db()

    def fake_get_engine():
        return engine

    monkeypatch.setattr(an, "get_engine", fake_get_engine)

    df = an.read_movies_df()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df.columns) >= {
        "imdb_id",
        "title",
        "year",
        "genre_primary",
        "imdb_rating",
    }


def test_genre_rating_summary_and_year_count_summary(monkeypatch):
    engine = _prep_in_memory_db()

    def fake_get_engine():
        return engine

    monkeypatch.setattr(an, "get_engine", fake_get_engine)

    df = an.read_movies_df()

    gsum = an.genre_rating_summary(df)
    # genre_primary Action: bara tt1 (7.0)
    # genre_primary Drama: tt2 (9.0) och tt3 (8.0) -> mean = 8.5
    assert set(gsum["genre_primary"]) == {"Action", "Drama"}
    drama_row = gsum.loc[gsum["genre_primary"] == "Drama"]
    assert pytest.approx(drama_row["avg_imdb_rating"].item(), rel=1e-6) == 8.5
    assert drama_row["count_titles"].item() == 2

    ysum = an.year_count_summary(df)
    # år 1999: 1 titel (Baz, imdb_rating=8.0)
    # år 2000: 2 titlar (7.0, 9.0) -> mean 8.0
    row1999 = ysum.loc[ysum["year"] == 1999]
    assert row1999["count_titles"].item() == 1
    assert pytest.approx(row1999["avg_imdb_rating"].item(), rel=1e-6) == 8.0

    row2000 = ysum.loc[ysum["year"] == 2000]
    assert row2000["count_titles"].item() == 2
    assert pytest.approx(row2000["avg_imdb_rating"].item(), rel=1e-6) == 8.0


def test_export_analysis_creates_csv(monkeypatch, tmp_path):
    """
    export_analysis() ska:
    - läsa DB via read_movies_df()
    - räkna gsum / ysum
    - skriva 2 csv-filer till ANALYSIS_DIR
    Vi mockar:
        - get_engine -> in-memory
        - ANALYSIS_DIR -> tmp_path / "analysis"
    """
    engine = _prep_in_memory_db()

    def fake_get_engine():
        return engine

    # 1. mocka så analyze använder vår in-memory engine
    monkeypatch.setattr(an, "get_engine", fake_get_engine)

    # 2. mocka analyssökvägen
    new_analysis_dir = tmp_path / "analysis"
    # se till att mappen finns så pandas kan skriva
    new_analysis_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(an, "ANALYSIS_DIR", new_analysis_dir)

    # 3. kör export_analysis
    an.export_analysis()

    genre_csv = new_analysis_dir / "genre_rating_summary.csv"
    year_csv = new_analysis_dir / "year_count_summary.csv"

    assert genre_csv.exists()
    assert year_csv.exists()

    gdf = pd.read_csv(genre_csv)
    ydf = pd.read_csv(year_csv)

    # kolla att kolumnerna är de vi förväntar oss (bra för Power BI)
    assert set(gdf.columns) == {"genre_primary", "avg_imdb_rating", "count_titles"}
    assert set(ydf.columns) == {"year", "count_titles", "avg_imdb_rating"}

    # sanity: filer ska inte vara tomma
    assert len(gdf) >= 1
    assert len(ydf) >= 1
