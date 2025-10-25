Kunskapskontroll 2 – Python Fördjupning

Projekt: ETL- och analys-pipeline mot OMDb API med Power BI-integration

Student: Anthony Ryan Perez Andersson

Utbildning: Data Scientist – EC Utbildning

Datum: 25-10-2025

---

Översikt

Detta projekt är en komplett ETL- och analys-pipeline i Python som hämtar filminformation från OMDb API, behandlar datan och skapar färdiga analysfiler för visualisering i Power BI.

Syftet är att visa ett realistiskt arbetsflöde för en data scientist eller data engineer med automatiserad datainsamling (Extract), datarensning och strukturering (Transform), databasladdning (Load) samt analys och export av CSV-filer för Power BI.

---

Syfte och mål

Målet var att självständigt utveckla en produktionslik pipeline som:

1. Hämtar verklig data via API.

2. Rensar och standardiserar datan.

3. Lagrar den i en lokal SQLite-databas.

4. Skapar automatiska analysrapporter redo för Power BI.

5. Säkerställs med enhetstester (Pytest) och loggning.

---

Teknisk struktur

Projektet består av flera Pythonmoduler uppdelade enligt följande:

• extract.py – Hämtar data från OMDb API

• transform.py – Rensar och standardiserar data

• load.py – Skriver data till SQLite

• analyze.py – Skapar sammanställningar och exporterar CSV-filer

• logger.py – Central logger med roterande loggfiler

• main.py – Orkestrerar hela pipelinen

Därtill finns en testmapp med 17 automatiserade Pytest-tester och en datamapp med databas, loggar och analysfiler.

---

Genomförande

1. Extract – datainsamling

   Rådata hämtas från OMDb API genom en uppsättning breda men kontrollerade sökord.
   
   För att undvika felet “Too many results” används både allmänna och årsspecifika sökningar, till exempel: life, world, love, dream, dark, light, city, road, story, war, home, game, night, fire, music, king, heart, samt kombinationer som “love 2025”, “dream 2024”, “city 2023” och “man 2022”.

   Varje query hämtar flera sidor med resultat och filtreras till år 2021 och framåt. Resultatet blir ett varierat dataset på cirka 1000–1500 titlar per körning.
   
   Tyvärr är API:et begränsat, vilket gör att det blir svårt att få till stort databas för analys, men strukturen är här för om man hittar ett bättre API.

2. Transform – databeredning

   Funktionen transform_movies() standardiserar och renar rådatan. Kolumnnamn byts till enhetliga (imdb_id, title, year, genre_primary osv.), textvärden konverteras till numeriska typer, “N/A” ersätts med NaN, primärgenre extraheras, dubbletter tas bort och en tidsstämpel läggs till.
   
   Resultatet är en ren, analysvänlig tabell som kan laddas in i databasen.

3. Load – databaslagring

   Den transformerade datan skrivs till SQLite-databasen data/etl.db via SQLAlchemy.
   
   Varje körning gör en full refresh där tabellen återskapas från grunden, vilket säkerställer datakvalitet och reproducerbarhet.

4. Analyze och Export – sammanställning

   Funktionen export_analysis() läser in data från databasen och skapar två färdiga analysfiler:
   
   • genre_rating_summary.csv – visar genomsnittligt IMDb-betyg och antal titlar per genre.
   
   • year_count_summary.csv – visar antal titlar och snittbetyg per år.
   
   Filerna sparas i mappen data/analysis och kan importeras direkt i Power BI för visualisering.

5. Loggning och felsäkerhet

   Alla körningar loggas i data/logs/app.log med tidsstämplar och loggnivåer.
   
   Loggern använder en RotatingFileHandler för att undvika för stora filer. Exempel på hur loggutdrag kan se ut:
   
   2025-10-25 22:45:31 [INFO] Startar ETL-pipeline
   
   2025-10-25 22:45:45 [INFO] Transformerad data klar (1000 rader)
   
   2025-10-25 22:45:50 [INFO] ETL och analys slutförd utan fel.

---

Testning och kvalitetssäkring

Projektet täcks av 17 automatiserade Pytest-tester som kontrollerar att alla delar fungerar som de ska.

Testerna verifierar:

• Extract – API-hämtning, sidhantering och årfilter

• Transform – konvertering, genrehantering, datavalidering

• Load – databasstruktur, primärnycklar och full refresh

• Analyze – gruppberäkningar och CSV-export

• Logger – korrekt fil- och streamhandlers utan duplication

• Main – end-to-end körning utan fel

Alla tester passerar, vilket visar att systemet är robust och reproducerbart.

---

Resultat och Power BI-integration

Efter körning genereras databas, loggar och analysfiler i mappen data/.

Dessa används i Power BI för att skapa diagram över genomsnittligt betyg per genre, antal titlar per år och trender för filmer från 2021 till 2025.

Exempel på resultat i loggfilen:

[INFO] Rådatamängd efter sampling/filter >= 2021: 1250 rader

[INFO] Transformerad datamängd: 1187 rader

[INFO] SQLite path: data/etl.db

[INFO] ETL + analys klart utan fel.

---

Lärdomar och diskussion

Projektet visar hur man med Python kan bygga en komplett data-pipeline med verklig data och praktisk tillämpning för analys.

Jag fick erfarenhet av API-hantering, begränsningar (Too many results), datarensning i pandas, databasdesign i SQLAlchemy, loggning och testdriven utveckling.

Systemet är skalbart och lätt att vidareutveckla, till exempel genom schemaläggning, fler analyser eller automatiserad Power BI-uppdatering.

---

Körning

1. Skapa en .env-fil i projektroten med din OMDb API-nyckel:

   OMDB_API_KEY=din_api_nyckel_här

2. Kör projektet med kommandot:

   python main.py

3. Resultatet sparas i data/etl.db, data/logs/app.log och data/analysis/.

---

Sammanfattning

Projektet uppfyller målet att självständigt bygga en Data Science-relevant Python-lösning. Koden är modulär, testad och praktiskt användbar i verkliga ETL-flöden.

Helheten motsvarar ett professionellt arbetssätt för en junior data scientist eller data engineer.

Systemet är lätt att underhålla och vidareutveckla för andra datakällor.