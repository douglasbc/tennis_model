#!/bin/bash
source venv/bin/activate
python3 odds_api/pinnacle_to_json.py
python3 odds_api/json_to_bq.py
cd dbt_tennis
dbt run --select atp_bets wta_bets atp_roi wta_roi streamlit_bets streamlit_player_roi streamlit_matches
cd ..
# python3 email/main.py
deactivate