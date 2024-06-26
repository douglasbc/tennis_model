#!/bin/bash
source venv/bin/activate
python3 ingestion/main.py
cd dbt_tennis
dbt run --select atp_entry atp_odds_m atp_odds_p atp_players atp_stats atp_tournaments atp_matches atp_input atp_today atp_matches_count wta_entry wta_odds_m wta_odds_p wta_players wta_stats wta_tournaments wta_matches wta_input wta_today wta_matches_count &&
# dbt run --select atp_players atp_stats atp_tournaments atp_matches atp_input atp_today atp_matches_count wta_players wta_stats wta_tournaments wta_matches wta_input wta_today wta_matches_count &&
cd ..
python3 elo_model/atp_model.py &
python3 elo_model/wta_model.py & 
wait
deactivate
