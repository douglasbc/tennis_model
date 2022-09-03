import os
os.environ['CUDA_VISIBLE_DEVICES'] = ''

from jax_elo.models.correlated_skills_model import calculate_ratings
from jax_elo.models.correlated_skills_model import fit
from jax_elo.models.correlated_skills_model import predict
from jax_elo.utils.bq_client import bigquery_client

from google.cloud import bigquery
import numpy as np
import pandas as pd


client = bigquery_client()

input_query = 'SELECT * FROM `tennis-358702.model_layer.wta_input`'
df = client.query(input_query).to_dataframe()

df = df.sort_values(['match_date', 'round'])
df = df.reset_index(drop=True)

# Fit the model
params, opt_info = fit(df['p1_name'], df['p2_name'], df['surface'].values, 
                       margins=df['margins'].values, verbose=True)


# We can now calculate the rating history:
history, final_rating_dict, mark_names = calculate_ratings(params, df['p1_name'], df['p2_name'],
                        df['surface'].values, df['margins'].values)


def predict_match(p1, p2, surface):
    return predict(final_rating_dict, params, p1, p2, surface, mark_names)


next_matches_query = 'SELECT * FROM `tennis-358702.model_layer.wta_today`'
next_matches = client.query(next_matches_query).to_dataframe()
next_matches = next_matches.sort_values(['tournament_name', 'round', 'match_date'])

predictions_df = next_matches.copy()

for index,row in predictions_df.iterrows():
    match_prediction = predict_match(row['p1_name'], row['p2_name'], row['surface'])
    predictions_df.loc[index,'p1_probability'] = match_prediction
    predictions_df.loc[index,'p2_probability'] = 1-match_prediction
    predictions_df.loc[index,'p1_fair_odds'] = 1/match_prediction
    predictions_df.loc[index,'p2_fair_odds'] = 1/(1-match_prediction)

table_id = 'tennis-358702.model_layer.wta_predictions'

schema = [
    bigquery.SchemaField('match_date', 'DATETIME'),
    bigquery.SchemaField('p1_name', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('p2_name', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('round', 'STRING'),
    bigquery.SchemaField('tournament_name', 'STRING'),
    bigquery.SchemaField('surface', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('p1_probability', 'FLOAT64'),
    bigquery.SchemaField('p2_probability', 'FLOAT64'),
    bigquery.SchemaField('p1_fair_odds', 'FLOAT64'),
    bigquery.SchemaField('p2_fair_odds', 'FLOAT64')
]

job_config = bigquery.LoadJobConfig(schema=schema)

job = client.load_table_from_dataframe(predictions_df, table_id, job_config=job_config)
job.result()

print(f"Loaded table {table_id}")
