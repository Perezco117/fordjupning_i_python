## Kunskapskontroll 2 – Python Fördjupning

**Projekt:** ETL- och analys-pipeline mot OMDb API med Power BI-integration

**Student:** Anthony Ryan Perez Andersson

**Utbildning:** Data Scientist – EC Utbildning

**Datum:** 26 oktober 2025

---


### Översikt


Detta projekt är en komplett ETL- och analys-pipeline i Python som hämtar filminformation från OMDb API, behandlar datan och skapar färdiga analysfiler för visualisering i Power BI.

Systemet är byggt med tydlig moduluppdelning (Extract, Transform, Load, Analyze) och innehåller automatisk loggning, konfigurationshantering via .env fil, samt en full testsvit med Pytest.

---


### Syfte och mål


Målet var att självständigt utveckla en produktionslik pipeline som:


1. Hämtar verklig data via API (OMDb).

2. Rensar och standardiserar datan.

3. Lagrar den i en lokal SQLite-databas.

4. Skapar analysfiler för Power BI.

5. Säkerställs med loggning och tester.


Projektet skulle också visa förståelse för Python-modularitet, datarensning med pandas och automatiserad kvalitetssäkring.

---


### Teknisk struktur


Projektet består av följande moduler:


* **`extract.py`** – Hämtar data från OMDb API, hanterar paginering, årsfilter och dubblettkontroll (för minimerade API-anrop).


* **`transform.py`** – Rensar, konverterar och filtrerar data. Nu parametriserad för valfri genre, typ (film/serie) och unik kolumn.


* **`load.py`** – Skriver data till SQLite (`data/etl.db`) via SQLAlchemy.


* **`analyze.py`** – Läser data från databasen och exporterar färdiga analyser till CSV för Power BI.


* **`logger.py`** – Central logger med roterande loggfiler i `data/logs/app.log`.


* **`main.py`** – Orkestrerar hela flödet: ETL, analys, och loggning.


* **`tests/`** – Innehåller 17 Pytest-tester som verifierar varje modul.


Alla filer sparas alltid **inom samma mapp som `main.py`** – oberoende av miljö eller IDE. Detta gör systemet portabelt mellan datorer och GitHub.

---


### Genomförande


#### 1. Extract – datainsamling


Data hämtas från OMDb API med flera breda sökord (ex: *life, dream, city, night, war, love, man*) samt paginering (flera sidor per sökord).

För att minska antalet API-anrop och undvika “Too many results” används nu två optimeringar:

1. **Årsfilter (`year_min`)** – filmer äldre än ett visst år (t.ex. 2020) hämtas inte ens i detalj.

2. **Global dubblettkontroll** – samma `imdbID` hämtas bara en gång, även om den dyker upp i flera sökord.

Vid fel hanteras undantag (`ExtractError`) med loggning istället för att krascha.

Från och med denna version hämtas filmer från **de senaste 5 åren (2020–2025)**.

---


#### 2. Transform – databeredning


Den nya versionen av `transform_movies()` gör mer än bara rensning:


* Kolumner standardiseras (`imdb_id`, `title`, `year`, `genre_primary`, etc.).


* Årtal, runtime och votes konverteras till numeriska typer.


* Primärgenre extraheras från `Genre`-fältet.


* Filtrering tillåter användaren att specificera:

  * `allowed_types`: t.ex. `["movie"]` eller `["series"]`

  * `allowed_genres`: t.ex. `["Action", "Thriller"]`

  * `dedupe_on`: t.ex. `"title"` (unika titlar)


* Filmer utan IMDb-betyg eller röstdata (`imdb_rating`, `imdb_votes`) tas bort.


Allt detta görs **parametriskt**, så att man enkelt kan styra analysens inriktning via `main.py` utan att ändra i koden.


---


#### 3. Load – databaslagring


Data laddas in i SQLite (`data/etl.db`) via SQLAlchemy.

Vid varje körning görs en **full refresh** för att säkerställa konsekvens.

---


#### 4. Analyze – sammanställning


Modulen `analyze.py` läser data från databasen via `get_engine()` och skapar två sammanställningar:


* `genre_rating_summary.csv` – genomsnittligt betyg per primärgenre.


* `year_count_summary.csv` – antal filmer per år.


Om data saknas hanteras det med varningar i loggen utan att krascha.

Filerna exporteras till `data/analysis/` för enkel Power BI-import.

---


#### 5. Loggning


Alla händelser loggas i `data/logs/app.log`.

Loggern använder `RotatingFileHandler` och sparar både till fil och konsol.

Loggar inkluderar tidsstämplar, statusmeddelanden och felspårning.

Exempel:


```

2025-10-26 13:12:10 [INFO] Rådatamängd efter sampling/filter >= 2015: 2175 rader

2025-10-26 13:12:25 [INFO] Transformerad datamängd: 1893 rader

2025-10-26 13:12:26 [INFO] SQLite path: data/etl.db

2025-10-26 13:12:27 [INFO] ✅ ETL + ANALYS klart utan fel.

```

---


### Testning och kvalitetssäkring


Projektet har **18 automatiserade Pytest-tester** som tillsammans säkerställer att hela ETL-flödet fungerar som avsett — från API-hämtning till färdiga analysfiler.

Tester finns för:


* **Extract** – API-hantering, felhantering, sidhantering.


* **Transform** – numerisk konvertering, genrefilter, årsfilter, deduplikation.


* **Load** – korrekt databasstruktur och refresh funktion och PK-begränsningar upprätthålls.


* **Analyze** – gruppsammanställning och CSV-export.


* **Logger** – handler-konfiguration och idempotens.


* **Main** – full pipeline testad med mockad data utan nätverk.


* **ETL-integration** – end-to-end-flöde där verkliga funktioner för transform_movies, ensure_schema, load_movies_refresh och analyssteget används mot en riktig in-memory-SQLite-databas.

Detta test verifierar att alla moduler fungerar ihop (schema-matchning, datatyper, filtrering och analysresultat).

Alla tester är gröna.

---


### Resultat och Power BI


Efter körning skapas följande:


* `data/etl.db` – SQLite-databas med rena filmdata


* `data/logs/app.log` – detaljerad körlogg


* `data/analysis/*.csv` – färdiga analyser


Power BI används sedan för att visualisera resultat, exempelvis:


* Genomsnittligt IMDb-betyg per genre


* Antal filmer per år


* Trend för Action-filmer över tid


---


### Körning


1. Skapa en `.env` i projektroten med hjälp av mallen .evn.example med din API-nyckel:


   ```

   OMDB_API_KEY=din_api_nyckel_här

   ```


2. Kör projektet:


   ```

   python main.py

   ```


3. Efter körning:


   * Databas → `data/etl.db`

   * Loggar → `data/logs/app.log`

   * Analysfiler → `data/analysis/*.csv`

---


För att koppla SQLlite-filen(etl.db) till Power BI utan att installera extra drivrutiner, gör så här:


1. I Power BI Desktop:

   Gå till 
      
      Files -> Options & Settings -> Options -> Python Scripting -> Set a Python home directory

   Här lägger du in fullständiga filepath till ditt Python environment.


Detta talar om för Power BI vilket Python den ska använda när den kör skript.


2. Efter du gjort ovan steg, kan du nu importera .db filen genom att använda kopiera följande Python-kod:


```

import sqlite3
import pandas as pd
import sys

# === Ändra detta till din faktiska sökväg till databasen ===
db_path = r"C:\full\path\till\din\repo-mapp\data\etl.db"

# Anslut till databasen
conn = sqlite3.connect(db_path)

# Hämta alla användartabeller (ignorerar interna sqlite_-tabeller)
tables = pd.read_sql_query(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';",
    conn
)

# Läs in varje tabell som en DataFrame och exponera dem som globala variabler
for table_name in tables['name']:
    df = pd.read_sql_query(f"SELECT * FROM '{table_name}'", conn)
    # Expose each as a top-level variable so Power BI can see it
    globals()[table_name] = df
    print(f"Loaded table: {table_name} ({len(df)} rows)")

conn.close()

```


3. För att ladda in i Power BI:
   
   Gå till 

         "Home -> Get Data -> More... -> Python script -> Connect"
   
   Klistra in Pythonkoden du kopierade (med rätt sökväg till etl.db)
   
   Tryck OK
   
4. Nu kommer Power BI att visa alla tabeller från din SQLite-databas(t.ex. movies) i Navigator-fönstret.
   
   Markera de tabeller du vill ha och ladda in dem.
   

Det här sättet fungerar helt offline och du slipper installera någon separat SQLite-driver i Windows. Power BI använder Python (sqlite3 + pandas) för att läsa in datan åt dig.



### Lärdomar och reflektion


Projektet visar hur man i Python kan bygga en komplett ETL-pipeline med robust struktur, felhantering, testning och analys.


Jag lärde mig att:


* hantera API-begränsningar,


* bygga modulär kod för data science,


* skapa testbara pipelines,


* och implementera verklig logghantering och .env-konfiguration.


Systemet är flexibelt och kan enkelt utökas till andra API:er eller schemaläggas för automatisk körning.

---


### Sammanfattning


Detta projekt demonstrerar ett komplett, robust och testat ETL-flöde i Python för Data Science-bruk.

Koden är modulär, parameterstyrd, och kan användas som mall för fler datakällor, inklusive Power BI-integration.

Pipeline-strukturen, testningen och dokumentationen gör projektet redo för användning i professionella sammanhang.

Problemet med just denna API är dock att den är väldigt begränsad i antal sökningar. Därför blir analysdelen inte bra pga för lite data.

De senaste förbättringarna inkluderar smart filtrering av gamla filmer och en global dubblettkontroll som drastiskt minskar antalet API-anrop utan att påverka resultatet.