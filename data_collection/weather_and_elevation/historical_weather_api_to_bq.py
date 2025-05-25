
import requests_cache
import pandas as pd
from retry_requests import retry

import openmeteo_requests
from utils import bigquery_client, get_input_data, load_weather_data_to_bq


# Setup the BigQuery client
client = bigquery_client()
# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)
URL = "https://archive-api.open-meteo.com/v1/archive"


def historical_weather_api_to_df(latitude, longitude, start_date, end_date):
	
	params = {
		"latitude": latitude,
		"longitude": longitude,
		"start_date": start_date,
		"end_date": end_date,
		"hourly": ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "wind_speed_10m"],
		"timezone": "auto"
	}
	responses = openmeteo.weather_api(URL, params=params)

	# Process hourly data. The order of variables needs to be the same as requested.
	response = responses[0]
	hourly = response.Hourly()
	hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
	hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
	hourly_apparent_temperature = hourly.Variables(2).ValuesAsNumpy()
	hourly_wind_speed_10m = hourly.Variables(3).ValuesAsNumpy()
	elevation = response.Elevation()
	timezone = response.Timezone()
	timezone_offset_seconds = response.UtcOffsetSeconds()


	hourly_data = {"timestamp": pd.date_range(
		start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
		end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
		freq = pd.Timedelta(seconds = hourly.Interval()),
		inclusive = "left"
	)}
	# hourly_data["latitude"] = latitude
	# hourly_data["longitude"] = longitude
	hourly_data["temperature"] = hourly_temperature_2m
	hourly_data["relative_humidity"] = hourly_relative_humidity_2m
	hourly_data["apparent_temperature"] = hourly_apparent_temperature
	hourly_data["wind_speed"] = hourly_wind_speed_10m
	hourly_data["elevation"] = elevation
	hourly_data["timezone"] = timezone
	hourly_data["timezone_offset_seconds"] = timezone_offset_seconds

	return pd.DataFrame(data = hourly_data)


def iterate_historical_weather_df_to_bq(input_table):
	historical_weather_input_df = get_input_data(client, input_table)

	output_table = 'tennis-358702.raw_layer.historical_weather'

	df = pd.DataFrame()

	for index, row in historical_weather_input_df.iterrows():	
		unique_key = row["unique_key"],
		latitude = row["tournament_latitude"],
		longitude = row["tournament_longitude"]
		start_date = row["start_date"],
		end_date = row["end_date"],

		new_df = historical_weather_api_to_df(latitude, longitude, start_date, end_date)
		new_df.insert(0, 'unique_key', str(unique_key))
		new_df.insert(2, 'latitude', str(latitude))
		new_df.insert(3, 'longitude', str(longitude))
	
		df = pd.concat([df, new_df], ignore_index=True)
		
	load_weather_data_to_bq(client, df, output_table)
		

def main():
    iterate_historical_weather_df_to_bq("historical_weather_input_last_week")


if __name__ == "__main__":
    main()
