import json
import requests

URL = "https://pinnacle-odds.p.rapidapi.com/kit/v1/markets"
QUERY_STRING = {"sport_id":"2","is_have_odds":"true"}
HEADERS = {
	"X-RapidAPI-Key": "472687c9acmsh78be36a0807e901p19bad6jsnf73540bb45c9",
	"X-RapidAPI-Host": "pinnacle-odds.p.rapidapi.com"
}


def export_odds_to_json():
	json_data = requests.request("GET", URL, headers=HEADERS, params=QUERY_STRING).json()
	json_data = json_data['events']

	with open('odds_api/tennis_odds.json', 'w') as json_file:
		json.dump(json_data, json_file, indent=2)


if __name__ == "__main__":
    export_odds_to_json()