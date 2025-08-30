#!/bin/bash
source venv/bin/activate
python3 data_collection/oncourt_ingestion/main.py
# python3 data_collection/pinnacle_odds/pinnacle_to_json.py
python3 data_collection/pinnacle_odds/json_to_bq.py
cd dbt_tennis
dbt run
dbt test --select new_pinnacle_player
cd ..
wait
deactivate