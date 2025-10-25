import pandas as pd
import main as mainmod  # <-- viktigt: importera main.py direkt

def test_main_runs_end_to_end(monkeypatch):
    """
    Simulerar en hel pipelinekörning utan nätverk eller riktig extern analys-export.
    Vi patchar bort build_dataset_for_year_range (API-hämtningen) och export_analysis (Power BI-export).
    Då testar vi att main() kan köra klart och returnera exit code 0.
    """

    # Fake "rådata" som om den kom från OMDb via build_dataset_for_year_range
    fake_raw = pd.DataFrame([
        {
            "imdbID": "tt1",
            "Title": "Film1",
            "Year": "2024",
            "Type": "movie",
            "Genre": "Action, Thriller",
            "Director": "John Doe",
            "Country": "USA",
            "Runtime": "120 min",
            "imdbRating": "7.5",
            "imdbVotes": "12,345",
        }
    ])

    # Patcha bort nätverksinsamlingen med vår fake_raw
    monkeypatch.setattr(mainmod, "build_dataset_for_year_range", lambda **kwargs: fake_raw)

    # Patcha bort export_analysis så vi inte skriver riktiga CSV i testet
    monkeypatch.setattr(mainmod, "export_analysis", lambda: None)

    exit_code = mainmod.main()
    assert exit_code == 0
