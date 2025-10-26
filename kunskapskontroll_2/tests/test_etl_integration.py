import pandas as pd
from sqlalchemy import create_engine, text

from src.transform import transform_movies
from src.load import ensure_schema, load_movies_refresh
from src.analyze import genre_rating_summary, year_count_summary


def _fake_extract_output():
    """
    Mockerad "rå" data så som extract-steget levererar den,
    dvs före transform_movies(). Viktigt: kolumnnamnen här
    matchar output från extract.fetch_all_movies_full().
    """
    return pd.DataFrame([
        {
            "imdbID": "tt001",
            "Title": "Action Hero",
            "Year": "2024",
            "Type": "movie",
            "Genre": "Action, Thriller",
            "Director": "Jane Director",
            "Country": "USA",
            "Runtime": "123 min",
            "imdbRating": "7.5",
            "imdbVotes": "12,345",
        },
        {
            # den här raden ska filtreras bort av transform_movies
            # eftersom den inte är "movie" (den är "series")
            "imdbID": "tt002",
            "Title": "Not A Movie",
            "Year": "2023",
            "Type": "series",
            "Genre": "Action, Drama",
            "Director": "Bob Showrunner",
            "Country": "UK",
            "Runtime": "45 min",
            "imdbRating": "8.8",
            "imdbVotes": "9,999",
        },
        {
            # den här raden ska också bort, för den är gammal (<2015)
            "imdbID": "tt003",
            "Title": "Old Action Classic",
            "Year": "2010",
            "Type": "movie",
            "Genre": "Action, Crime",
            "Director": "Old Director",
            "Country": "USA",
            "Runtime": "95 min",
            "imdbRating": "8.1",
            "imdbVotes": "55,000",
        },
        {
            # den här raden saknar imdbVotes -> ska filtreras bort
            "imdbID": "tt004",
            "Title": "No Votes Yet",
            "Year": "2024",
            "Type": "movie",
            "Genre": "Action, Sci-Fi",
            "Director": "New Dir",
            "Country": "USA",
            "Runtime": "130 min",
            "imdbRating": "6.5",
            "imdbVotes": "N/A",
        },
        {
            # den här raden är en giltig actionfilm >=2015
            "imdbID": "tt005",
            "Title": "Action Revenge",
            "Year": "2022",
            "Type": "movie",
            "Genre": "Action, Thriller",
            "Director": "Jane Director",
            "Country": "USA",
            "Runtime": "110 min",
            "imdbRating": "7.9",
            "imdbVotes": "44,000",
        },
        {
            # duplicerad titel ("Action Revenge") med lite annan data,
            # ska bort i dedupe_on="title"
            "imdbID": "tt006",
            "Title": "Action Revenge",
            "Year": "2022",
            "Type": "movie",
            "Genre": "Action, Thriller",
            "Director": "Someone Else",
            "Country": "USA",
            "Runtime": "111 min",
            "imdbRating": "7.8",
            "imdbVotes": "12,000",
        },
    ])


def test_full_etl_flow_integration(tmp_path):
    """
    Integrationstest för hela kedjan:
    raw extract df -> transform_movies -> load_movies_refresh -> analys
    """

    # === 1. Fake "extract"-output (rådata före transform) ===
    raw_df = _fake_extract_output()

    # === 2. Kör transform_movies, precis som main.py gör ===
    transformed = transform_movies(
        raw_df,
        allowed_types=["movie"],         # bara filmer
        allowed_genres=["Action"],       # bara sånt som innehåller "Action"
        dedupe_on="title",               # 1 rad per titel
        year_min=2015,                   # klipp bort innan 2015
    )

    # Förväntat här:
    # - "Not A Movie" borta (Type=series)
    # - "Old Action Classic" borta (Year 2010 < 2015)
    # - "No Votes Yet" borta (saknar imdbVotes numeriskt)
    # - Dubbel "Action Revenge" -> endast första behålls
    # Kvar borde vara:
    #   "Action Hero" (tt001, Year=2024)
    #   "Action Revenge" (tt005, Year=2022)
    titles_after_transform = set(transformed["title"].tolist())
    assert titles_after_transform == {"Action Hero", "Action Revenge"}
    assert set(transformed.columns.tolist()) == {
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
    }

    # === 3. Skapa en in-memory SQLite och sätt schema ===
    engine = create_engine("sqlite:///:memory:", future=True)
    ensure_schema(engine)

    # === 4. Ladda in den transformerade datan i databasen ===
    load_movies_refresh(engine, transformed)

    # Kontroll: läs tillbaka allt som ligger i databasen
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT imdb_id, title, year, type, genre, "
                "genre_primary, director, country, runtime_min, "
                "imdb_rating, imdb_votes, fetched_at "
                "FROM movies ORDER BY title"
            )
        ).fetchall()

    # Vi förväntar oss exakt 2 rader i DB, sorterade alfabetiskt:
    # "Action Hero", "Action Revenge"
    assert len(rows) == 2
    assert rows[0][1] == "Action Hero"
    assert rows[1][1] == "Action Revenge"

    # Kolla att genre/genre_primary mappades rätt in i tabellen
    # (dvs inga krasch pga genre vs genre_full mismatch)
    assert "Action" in rows[0][4]  # genre innehåller "Action"
    assert rows[0][5] == "Action"  # genre_primary = "Action"

    # === 5. Analys-steget på DataFrame-nivå (som analyze.py gör) ===
    # Vi kan läsa om tabellen till pandas för analys
    df_from_db = pd.read_sql_query(text("SELECT * FROM movies"), engine)

    # genre_rating_summary: gruppa på genre_primary
    genre_summary = genre_rating_summary(df_from_db)
    # year_count_summary: gruppa på year
    year_summary = year_count_summary(df_from_db)

    # genre_summary ska innehålla vår primärgenre "Action"
    assert "Action" in set(genre_summary["genre_primary"].tolist())
    # year_summary ska innehålla 2022 och 2024 (inte 2010)
    assert set(year_summary["year"].tolist()).issuperset({2022.0, 2024.0})
    assert 2010.0 not in set(year_summary["year"].tolist())
