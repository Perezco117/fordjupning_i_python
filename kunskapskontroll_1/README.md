## Beskrivning

Detta projekt är en minimalistisk ETL-pipeline som hämtar filmdata från OMDb API, transformerar informationen och laddar in den i en SQLite-databas.
API-nyckeln sparas lokalt i .env (privat) medan .env.example visar vilka variabler som krävs.
Skapa en .env-fil baserad på .env.example och fyll i din egen OMDb API-nyckel.
Flödet består av moduler för extract, transform, load och logger, och körs via main.py som även hanterar fel och loggning. Vid körning skapas databasen etl.db i en data-mapp samt loggar i logs/app.log.
Dessa filer är exkluderade från GitHub via .gitignore.
Projektet kan köras manuellt med kommandot python main.py eller automatiseras via run_etl.bat och Windows Task Scheduler. Tester finns i mappen tests/ och körs med pytest för att säkerställa att varje del fungerar korrekt.

## Installation

Projektet är byggt i Python 3.11 (fungerar även med andra 3.x-versioner). Följande externa bibliotek behövs och kan installeras med följande kod:

pip install python-dotenv requests pandas SQLAlchemy pytest