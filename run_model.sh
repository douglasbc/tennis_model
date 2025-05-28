#!/bin/bash
source venv/bin/activate
python3 ingestion/main.py
cd dbt_tennis
dbt run
cd ..
# python3 -i elo_model/atp_model.py &
python3 elo_model/atp_model.py &
# python3 elo_model/wta_model.py &
wait
deactivate