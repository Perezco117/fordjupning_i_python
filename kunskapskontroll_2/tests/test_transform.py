import pandas as pd
import pytest
from src.transform import transform_movies, TransformError


def _raw_df():
    return pd.DataFrame([
        {
            "imdbID": "tt1",
            "Title": "  Foo Movie ",
            "Year": "2000–2002",
            "Type": "Series",
            "Genre": "Drama, Crime",
            "Director": "Someone",
            "Country": "USA, UK",
            "Runtime": "123 min",
            "imdbRating": "8.7",
            "imdbVotes": "12,345",
        },
        {
            # dubblett på imdbID -> ska droppas
            "imdbID": "tt1",
            "Title": "Foo Movie DUP",
            "Year": "2001",
            "Type": "Series",
            "Genre": "Drama, Crime",
            "Director": "Someone Else",
            "Country": "USA, UK",
            "Runtime": "111 min",
            "imdbRating": "8.0",
            "imdbVotes": "11,000",
        },
        {
            "imdbID": "tt2",
            "Title": "Bar Film",
            "Year": "1999",
            "Type": "movie",
            "Genre": "Action",
            "Director": "Dir B",
            "Country": "Sweden",
            "Runtime": "99 min",
            "imdbRating": "N/A",
            "imdbVotes": "N/A",
        },
    ])


def test_transform_movies_basic_schema_and_cleanup():
    df_in = _raw_df()
    out = transform_movies(df_in)

    # Kolla att alla slutliga kolumner finns
    expected_cols = [
        "imdb_id",
        "title",
        "year",
        "type",
        "genre_full",
        "genre_primary",
        "director",
        "country",
        "runtime_min",
        "imdb_rating",
        "imdb_votes",
        "fetched_at",
    ]
    assert list(out.columns) == expected_cols

    # Duplikat på imdb_id ska vara borta -> bara 2 kvar
    assert len(out) == 2
    assert set(out["imdb_id"]) == {"tt1", "tt2"}

    # year ska vara första fyrsiffriga året -> Int64
    row_tt1_year = out.loc[out["imdb_id"] == "tt1", "year"].item()
    assert row_tt1_year == 2000
    assert str(out["year"].dtype) == "Int64"

    # runtime_min ska vara Int64
    row_tt1_runtime = out.loc[out["imdb_id"] == "tt1", "runtime_min"].item()
    assert row_tt1_runtime == 123
    assert str(out["runtime_min"].dtype) == "Int64"

    # imdb_rating -> Float64, N/A -> NA
    tt1_rating = out.loc[out["imdb_id"] == "tt1", "imdb_rating"].item()
    tt2_rating = out.loc[out["imdb_id"] == "tt2", "imdb_rating"].item()
    assert pytest.approx(tt1_rating, rel=1e-6) == 8.7
    assert pd.isna(tt2_rating)
    assert str(out["imdb_rating"].dtype) == "Float64"

    # imdb_votes -> Int64, "12,345" -> 12345, "N/A" -> NA
    tt1_votes = out.loc[out["imdb_id"] == "tt1", "imdb_votes"].item()
    tt2_votes = out.loc[out["imdb_id"] == "tt2", "imdb_votes"].item()
    assert tt1_votes == 12345
    assert pd.isna(tt2_votes)
    assert str(out["imdb_votes"].dtype) == "Int64"

    # genre_primary = första genren före kommatecken
    tt1_genre = out.loc[out["imdb_id"] == "tt1", "genre_primary"].item()
    tt2_genre = out.loc[out["imdb_id"] == "tt2", "genre_primary"].item()
    assert tt1_genre == "Drama"
    assert tt2_genre == "Action"

    # title ska vara trimmad (inte "  Foo Movie ")
    tt1_title = out.loc[out["imdb_id"] == "tt1", "title"].item()
    assert tt1_title == "Foo Movie"

    # type lowercase
    tt1_type = out.loc[out["imdb_id"] == "tt1", "type"].item()
    assert tt1_type == "series"
    tt2_type = out.loc[out["imdb_id"] == "tt2", "type"].item()
    assert tt2_type == "movie"

    # fetched_at ska finnas och se ut som ISO8601 ungefär
    ts_val = out.loc[out.index[0], "fetched_at"]
    # .fromisoformat() kastar om det inte är giltigt
    from datetime import datetime
    datetime.fromisoformat(ts_val)


def test_transform_movies_missing_required_raises():
    df_missing = pd.DataFrame([
        {"imdbID": "tt1", "Title": "X"}  # Year och Type saknas
    ])
    with pytest.raises(TransformError):
        transform_movies(df_missing)


def test_transform_movies_handles_empty_input():
    df_empty = pd.DataFrame(columns=[
        "imdbID", "Title", "Year", "Type",
        "Genre", "Director", "Country",
        "Runtime", "imdbRating", "imdbVotes"
    ])
    out = transform_movies(df_empty)

    assert len(out) == 0
    assert "imdb_id" in out.columns
    # dtype check på year ska fortfarande vara nullable Int64
    assert str(out["year"].dtype) == "Int64"
