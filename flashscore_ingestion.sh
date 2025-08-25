#!/bin/bash
source venv/bin/activate
python3 ingestion/main.py
# python3 elo_model/atp_backtest_model.py &
# python3 elo_model/wta_backtest_model.py &
cd dbt_tennis
dbt run
cd ..
wait
deactivate