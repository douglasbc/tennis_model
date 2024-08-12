#!/bin/bash
source venv/bin/activate
python3 odds_api/pinnacle_to_json.py
python3 odds_api/json_to_bq.py
cd dbt_tennis
# dbt run --select bets all_bets
dbt run --select all_bets
cd ..
# python3 email/main.py
deactivate