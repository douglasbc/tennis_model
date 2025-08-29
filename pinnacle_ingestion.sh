#!/bin/bash
source venv/bin/activate
python3 data_collection/pinnacle_odds/pinnacle_to_json.py
# python3 data_collection/pinnacle_odds/json_to_bq.py
deactivate