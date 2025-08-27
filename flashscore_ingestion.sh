#!/bin/bash
source venv/bin/activate
python3 data_collection/flashscore/scrape_flashscore_stats.py
python3 data_collection/flashscore/ingest_flashscore_stats_scraped.py
wait
deactivate