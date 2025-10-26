import pandas as pd
import pytest
from src.transform import transform_movies, TransformError


def _make_raw_df():
    # Bygger en fejkad rå-DataFrame lik den som extract returnerar
    return pd.DataFrame([
        {
            "imdbID": "tt1",
            "Title": "Action Film",
            "Year": "2024",
            "Type": "movie",
            "Genre": "Action, Thriller",
            "Director": "Dir A",
            "Country": "USA",
            "Runtime": "120 min",
            "imdbRating": "7.5",
            "imdbVotes": "12,345",
        },
        {
            # Dublett på TITLE men annat ID (ska bort om dedupe_on='title')
            "imdbID": "tt1b",
            "Title": "Action Film",
            "Year": "2023",
            "Type": "movie",
            "Genre": "Action, Thriller",
            "Director": "Dir A2",
            "Country": "USA",
            "Runtime": "110 min",
            "imdbRating": "8.0",
            "imdbVotes": "9,999",
        },
        {
            # Fel typ (series) - ska filtreras bort om allowed_types=["movie"]
            "imdbID": "tt2",
            "Title": "Series Thing",
            "Year": "2022",
            "Type": "series",
            "Genre": "Action, Drama",
            "Director": "Dir B",
            "Country": "UK",
            "Runtime": "45 min",
            "imdbRating": "9.0",
            "imdbVotes": "1,234",
        },
        {
            # Saknar rating/votes -> ska bort
            "imdbID": "tt3",
            "Title": "No Rating Yet",
            "Year": "2024",
            "Type": "movie",
            "Genre": "Action, Sci-Fi",
            "Director": "Dir C",
            "Country": "CA",
            "Runtime": "100 min",
            "imdbRating": "N/A",
            "imdbVotes": "N/A",
        },
        {
            # Äldre än year_min (2010 < 2015) -> ska bort
            "imdbID": "tt4",
            "Title": "Old Action",
            "Year": "2010",
            "Type": "movie",
            "Genre": "Action, Crime",
            "Director": "Dir D",
            "Country": "USA",
            "Runtime": "95 min",
            "imdbRating": "6.5",
            "imdbVotes": "5,000",
        },
        {
            # Inte Action i genren -> ska bort vid allowed_genres=["Action"]
            "imdbID": "tt5",
            "Title": "Romantic Comedy",
            "Year": "2024",
            "Type": "movie",
            "Genre": "Romance, Comedy",
            "Director": "Dir E",
            "Country": "USA",
            "Runtime": "100 min",
            "imdbRating": "7.1",
            "imdbVotes": "2,200",
        },
    ])


def test_transform_movies_filters_and_dedupes():
    raw = _make_raw_df()

    result = transform_movies(
        raw,
        allowed_types=["movie"],
        allowed_genres=["Action"],
        dedupe_on="title",
        year_min=2015,
    )

    # Förväntat kvar:
    # - "Action Film" (movie, Action, recent, har rating+votes)
    # Ej kvar:
    # - "Action Film" (andra raden) -> tas bort pga dedupe_on="title"
    # - "Series Thing" -> typ "series"
    # - "No Rating Yet" -> saknar rating/votes
    # - "Old Action" -> 2010 < 2015
    # - "Romantic Comedy" -> inte Action-genre
    titles = list(result["title"])
    assert titles == ["Action Film"]

    row = result.iloc[0]

    # Kolla typkonvertering
    # year -> float
    # runtime_min -> float
    # imdb_rating -> float
    # imdb_votes -> float
    assert row["year"] == pytest.approx(2024.0)
    assert row["runtime_min"] == pytest.approx(120.0)
    assert row["imdb_rating"] == pytest.approx(7.5)
    assert row["imdb_votes"] == pytest.approx(12345.0)

    # Kolla att obligatoriska kolumner finns i slutresultatet
    for col in [
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
    ]:
        assert col in result.columns


def test_transform_movies_raises_if_missing_columns():
    # skapa en dataframe som saknar viktiga kolumner
    bad = pd.DataFrame([
        {"imdbID": "ttX", "Title": "X film"}
    ])
    with pytest.raises(TransformError):
        transform_movies(bad)
