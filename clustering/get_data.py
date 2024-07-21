from bq_client import bigquery_client

client = bigquery_client()


# def get_clustering_data(tour):
#     query = f'''
# select
#   player_name,
#   unreturned_pct,
#   second_serve_agression_score,
#   returned_pct
#   slice_returns_pct,
#   avg_rally_lenght,
#   sliced_per_backhand_groundstroke,
#   net_points_pct,
#   rally_agression_score,
#   return_agression_score
# from `tennis-358702.treated_layer.{tour}_clustering_input`
#  '''
#     query_job = client.query(query)
#     return query_job.result().to_dataframe()


def get_clustering_data(tour):
    query = f'''
select
  player_name,
  unreturned_pct,
  rally_agression_score,
from `tennis-358702.treated_layer.{tour}_clustering_input`
 '''
    query_job = client.query(query)
    return query_job.result().to_dataframe()