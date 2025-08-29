import json
import requests
from datetime import datetime, timezone

URL = "https://pinnacle-odds.p.rapidapi.com/kit/v1/markets"
QUERY_STRING = {"sport_id":"2","is_have_odds":"true"}
HEADERS = {
	"X-RapidAPI-Key": "472687c9acmsh78be36a0807e901p19bad6jsnf73540bb45c9",
	"X-RapidAPI-Host": "pinnacle-odds.p.rapidapi.com"
}


def export_odds_to_json():
	# Get current timestamp in UTC (timezone-aware)
	current_timestamp = datetime.now(timezone.utc)
	
	json_data = requests.request("GET", URL, headers=HEADERS, params=QUERY_STRING).json()
	json_data = json_data['events']
	
	# Format timestamp as YYYY-MM-DD-HHMMSS
	formatted_timestamp = current_timestamp.strftime('%Y-%m-%d-%H%M%S')
	filename = f'data_collection/pinnacle_odds/json_data/tennis_odds_{formatted_timestamp}.json'

	with open(filename, 'w') as json_file:
		json.dump(json_data, json_file, indent=2)


if __name__ == "__main__":
    export_odds_to_json()