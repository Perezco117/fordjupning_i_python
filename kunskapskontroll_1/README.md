## Beskrivning

Detta projekt är en minimalistisk ETL-pipeline som hämtar filmdata från OMDb API, transformerar informationen och laddar in den i en SQLite-databas.
API-nyckeln sparas lokalt i .env (privat) medan .env.example visar vilka variabler som krävs.
Skapa en .env-fil baserad på .env.example och fyll i din egen OMDb API-nyckel.
Flödet består av moduler för extract, transform, load och logger, och körs via main.py som även hanterar fel och loggning. Vid körning skapas databasen etl.db i en data-mapp samt loggar i logs/app.log.
Dessa filer är exkluderade från GitHub via .gitignore.
Projektet körs med main.py, man kan ändra vad man vill söka för filmtitel på .env filen.

## Paket som används som krävs installation
python-dotenv requests pandas sqlalchemy pytest

## Snabbstart
1. Kopiera `.env.example` → `.env` och fyll i `OMDB_API_KEY`.
2. Sätt `DATABASE_URL` till en **absolut** sökväg (exempel finns i `.env.example`).
3. Ändra filmtitel i .env med instruktioner från .env.example.
4. Kör: `python main.py`
   - Loggar: `data/logs/app.log`
   - Exakt DB-sökväg skrivs i loggen som “SQLite path: …”

## Tester
Kör alla tester med pytest:
`pytest -q`

(Om du kör via Anaconda/VS Code: välj din miljö med installerade paket och kör via VS Code's testing flik)

## Katalogstrukturen
project-root/
  main.py
  src/ (extract/transform/load/logger)
  tests/ (test_*.py)
  .env  / .env.example
  data/ (skapas automatiskt)

## Öppna .db filen i Power BI utan installeringar
1. Spara fullständiga filvägen till din etl.db fil.
2. Öppna Power BI Desktop -> File -> Options and Settings -> Options -> Python Scripting
   Home directory: sätt vägen till den Python du faktiskt använder(Du kan hitta i cmd ruta med `where python` och då visas vart du har
   Python installerat om du inte använder Anaconda. Använder du Anaconda får du gå genom Anaconda Prompt med samma prompt som ovan.)
3. Få upp Python script för att få in datan:
   Home -> Get Data -> More... -> Other -> Python Script -> Connect
4. Kopiera nedan kod för att få se vad det finns för tabeller:
  `import sqlite3, pandas as pd
   db_path = r"C:\PATH\TO\YOUR\etl.db"  # ← change this

   with sqlite3.connect(db_path) as conn:
      dataset = pd.read_sql(
         "SELECT name AS table_name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;",
         conn
      )`
   Klicka sedan OK -> Dataset -> Load (eller Transform Data)
5. Nu när du ser vilka tabeller som finns, kan du ladda upp den tabell du vill ha med följande kod(kör steg 3 igen):
  `import sqlite3, pandas as pd
   db_path = r"C:\PATH\TO\YOUR\etl.db"     # ← change this
   table   = "movies"                       # ← change to your table name exactly as listed
   
   with sqlite3.connect(db_path) as conn:
      dataset = pd.read_sql(f'SELECT * FROM "{table}"', conn)`