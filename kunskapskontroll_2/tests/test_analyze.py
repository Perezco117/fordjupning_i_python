import pandas as pd
import pytest
import importlib
from pathlib import Path

import src.analyze as an


def _sample_movies_df():
    """
    Skapa en liten fejkad movies-tabell som liknar vad som ligger i SQLite.
    Viktigt: kolumnnamnen ska matcha vad load.py skriver in.
    """
    return pd.DataFrame([
        {
            "imdb_id": "tt1",
            "title": "Action Film",
            "year": 2024.0,
            "type": "movie",
            "genre": "Action, Thriller",
            "genre_primary": "Action",
            "director": "Dir A",
            "country": "USA",
            "runtime_min": 120.0,
            "imdb_rating": 7.5,
            "imdb_votes": 12345.0,
            "fetched_at": "2025-10-26T12:00:00",
        },
        {
            "imdb_id": "tt2",
            "title": "Drama Film",
            "year": 2024.0,
            "type": "movie",
            "genre": "Drama",
            "genre_primary": "Drama",
            "director": "Dir B",
            "country": "UK",
            "runtime_min": 100.0,
            "imdb_rating": 8.0,
            "imdb_votes": 5555.0,
            "fetched_at": "2025-10-26T12:05:00",
        },
        {
            "imdb_id": "tt3",
            "title": "Old Thriller",
            "year": 2022.0,
            "type": "movie",
            "genre": "Thriller, Crime",
            "genre_primary": "Thriller",
            "director": "Dir C",
            "country": "CA",
            "runtime_min": 90.0,
            "imdb_rating": 6.0,
            "imdb_votes": 2000.0,
            "fetched_at": "2025-10-26T12:10:00",
        },
    ])


def test_genre_rating_summary_and_year_count_summary(monkeypatch):
    """
    genre_rating_summary() ska räkna snittbetyg och count per genre_primary.
    year_count_summary() ska räkna hur många filmer per år.
    """

    df = _sample_movies_df()

    gsum = an.genre_rating_summary(df)
    ysum = an.year_count_summary(df)

    # genre_rating_summary: vi ska ha Action, Drama, Thriller
    assert set(gsum["genre_primary"]) == {"Action", "Drama", "Thriller"}

    # checka t.ex. Drama-raden (med imdb_rating 8.0 och bara en rad)
    drama_row = gsum.loc[gsum["genre_primary"] == "Drama"].iloc[0]
    # pytest.approx för att tåla floatjitter
    assert pytest.approx(drama_row["avg_imdb_rating"], rel=1e-6) == 8.0
    assert drama_row["count"] == 1

    # year_count_summary: ska innehålla 2024 och 2022
    assert set(ysum["year"]) == {2024.0, 2022.0}
    # Filmerna år 2024: tt1 och tt2 -> count = 2
    row_2024 = ysum.loc[ysum["year"] == 2024.0].iloc[0]
    assert row_2024["count"] == 2


def test_export_analysis_creates_csv(monkeypatch, tmp_path):
    """
    export_analysis() ska:
    - läsa data via read_movies_df()
    - göra gsum / ysum
    - skriva två csv:er till ANALYSIS_DIR
    Vi mockar:
      - read_movies_df() -> sample df
      - ANALYSIS_DIR -> tmp_path / "analysis"
    """

    # 1. mocka read_movies_df() så vi inte slår riktig DB
    monkeypatch.setattr(an, "read_movies_df", lambda: _sample_movies_df())

    # 2. peka ANALYSIS_DIR till tempmapp
    new_analysis_dir = tmp_path / "analysis_dir"
    monkeypatch.setattr(an, "ANALYSIS_DIR", new_analysis_dir)

    # Säkerställ att directory inte redan finns, vi testar mkdir i analyze.export_analysis()
    assert not new_analysis_dir.exists()

    # 3. Kör export_analysis
    an.export_analysis()

    # 4. Kontrollera att outputfiler skapades
    gsum_path = new_analysis_dir / "genre_rating_summary.csv"
    ysum_path = new_analysis_dir / "year_count_summary.csv"

    assert gsum_path.exists(), "genre_rating_summary.csv borde ha skapats"
    assert ysum_path.exists(), "year_count_summary.csv borde ha skapats"

    # 5. Läs tillbaka och sanity-kolla innehållet
    gsum_loaded = pd.read_csv(gsum_path)
    ysum_loaded = pd.read_csv(ysum_path)

    # Vi vet att sample-datan har Action / Drama / Thriller
    assert set(gsum_loaded["genre_primary"]) == {"Action", "Drama", "Thriller"}

    # Vi vet att sample-datan har år 2024 och 2022
    assert set(ysum_loaded["year"]) == {2024.0, 2022.0}


def test_export_analysis_handles_empty(monkeypatch, tmp_path):
    """
    Om databasen är tom ska export_analysis():
    - inte krascha
    - inte skriva några filer
    """

    empty_df = pd.DataFrame(
        columns=[
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
        ]
    )

    # mocka read_movies_df() -> tom df
    monkeypatch.setattr(an, "read_movies_df", lambda: empty_df)

    # peka analyskatalogen mot tmp_path
    new_analysis_dir = tmp_path / "analysis_dir_empty"
    monkeypatch.setattr(an, "ANALYSIS_DIR", new_analysis_dir)

    # Kör export_analysis
    an.export_analysis()

    # Katalogen ska skapas ändå (mkdir), men inga filer ska skapas
    assert new_analysis_dir.exists()
    assert list(new_analysis_dir.iterdir()) == []
