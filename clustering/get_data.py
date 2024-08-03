from bq_client import bigquery_client

client = bigquery_client()


def get_player_clustering_data(tour):
    query = f'''
select
  player_name,
  unreturned_pct,
  rally_agression_score,
from `tennis-358702.treated_layer.{tour}_clustering_input`
 '''
    query_job = client.query(query)
    return query_job.result().to_dataframe()


def get_elevation_clustering_data():
    query = f'''
select
  elevation
from `tennis-358702.treated_layer.tournament_elevation`
 '''
    query_job = client.query(query)
    return query_job.result().to_dataframe()


def get_apparent_temperature_clustering_data():
    query = f'''
select
  weather_id,
  avg_apparent_temperature
  from `tennis-358702.treated_layer.historical_weather`
 '''
    query_job = client.query(query)
    return query_job.result().to_dataframe()

def get_wind_speed_clustering_data():
    query = f'''
select
  weather_id,
  avg_wind_speed
  from `tennis-358702.treated_layer.historical_weather`
 '''
    query_job = client.query(query)
    return query_job.result().to_dataframe()