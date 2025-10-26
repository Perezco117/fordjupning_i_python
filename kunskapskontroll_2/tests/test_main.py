import pytest
from unittest.mock import MagicMock
import pandas as pd
import main as mainmod


def test_main_runs_clean(monkeypatch):
    """
    Testar att main() körs utan undantag, med mockade moduler.

    Vi verifierar:
    - att main() returnerar 0 (dvs "allt gick bra")
    - att transform_movies anropas med de parametrar vi förväntar oss
    - att vi inte rör nätverk eller riktig databas
    """

    # 1. Mocka bort .env-hantering och API-nyckel
    # load_dotenv() ska bara "se ut att funka"
    monkeypatch.setattr(mainmod, "load_dotenv", lambda *a, **kw: True)

    # os.getenv("OMDB_API_KEY") ska alltid ge en fejk-nyckel
    monkeypatch.setenv("OMDB_API_KEY", "fakekey")

    # 2. Fake DataFrame som om extract gav oss rådata
    fake_df = pd.DataFrame([
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
        }
    ])

    # 3. Samla parametrarna som skickas till transform_movies, så vi kan asserta
    called_kwargs = {}

    def fake_transform_movies(df, **kwargs):
        called_kwargs.update(kwargs)
        # returnera en redan-transformerad df (det kan bara vara df själv här)
        return df

    # 4. Mocka funktionerna som main() anropar i pipelinen

    # I main.py gjorde du: from src.extract import build_dataset_for_year_range
    # -> därför måste vi patcha mainmod.build_dataset_for_year_range, INTE src.extract.build_dataset_for_year_range
    monkeypatch.setattr(mainmod, "build_dataset_for_year_range", lambda **kw: fake_df)

    # I main.py gjorde du: from src.transform import transform_movies
    monkeypatch.setattr(mainmod, "transform_movies", fake_transform_movies)

    # I main.py gjorde du: from src.load import get_engine, load_movies_refresh
    monkeypatch.setattr(
        mainmod,
        "get_engine",
        lambda: MagicMock(url=MagicMock(database="fake.db")),
    )
    monkeypatch.setattr(
        mainmod,
        "load_movies_refresh",
        lambda *a, **kw: None,
    )

    # I main.py gjorde du: from src.analyze import export_analysis
    monkeypatch.setattr(mainmod, "export_analysis", lambda: None)

    # 5. Kör main()
    rc = mainmod.main()

    # 6. Assertions
    # Steg lyckades
    assert rc == 0

    # transform_movies ska ha fått rätt parametrar
    # De här måste matcha exakt det du skickar i main.py
    assert called_kwargs["allowed_types"] == ["movie"]
    assert called_kwargs["allowed_genres"] == ["Action"]
    assert called_kwargs["dedupe_on"] == "title"
    assert called_kwargs["year_min"] == 2015
