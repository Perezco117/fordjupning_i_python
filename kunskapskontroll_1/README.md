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

  ## Uppdateras här sedan med hur du kör .db filen i Power BI.