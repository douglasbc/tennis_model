#!/bin/bash
source venv/bin/activate
python3 ingestion/main.py
# python3 data_collection/flashscore/scrape_flashscore_stats.py
# python3 data_collection/flashscore/ingest_flashscore_stats_scraped.py
cd dbt_tennis
dbt run
cd ..
wait
deactivate