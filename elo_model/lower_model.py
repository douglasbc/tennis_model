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

def get_input_data(tour, lower_level):
    if tour.lower() == 'atp':
        lower_tournaments = 'Future'
    if tour.lower() == 'wta':
        lower_tournaments = 'Lower Level'

    if lower_level:
        input_query = f'SELECT * FROM `tennis-358702.model_layer.{tour}_input`'
    else:
        input_query = f'''
                          SELECT * FROM `tennis-358702.model_layer.{tour}_input`
                          WHERE tournament_level <> {lower_tournaments}
                       '''
               
    df = client.query(input_query).to_dataframe()

    df = df.sort_values(['match_date', 'round'])
    df = df.reset_index(drop=True)
    
    return df


def fit_model(tour, lower_level):

    df = get_input_data(tour, lower_level)

    params, opt_info = fit(df['p1_name'], df['p2_name'], df['surface'].values, 
                        margins=df['margins'].values, verbose=True)

    history, final_rating_dict, mark_names = calculate_ratings(params, df['p1_name'], df['p2_name'],
                            df['surface'].values, df['margins'].values)

    return params, final_rating_dict, mark_names


def predict_match(p1, p2, surface):
    return predict(final_rating_dict, params, p1, p2, surface, mark_names)


def get_todays_matches(tour, lower_level):

    if lower_level:
        next_matches_query = f'SELECT * FROM `tennis-358702.model_layer.{tour}_today`'
    else:
        next_matches_query = f'''
                          SELECT * FROM `tennis-358702.model_layer.{tour}_today`
                          WHERE tournament_level = 'Main Tour'
                       '''
               
    df = client.query(next_matches_query).to_dataframe()

    df = df.sort_values(['tournament_name', 'round', 'match_date'])
    # predictions_df = next_matches.copy()
    return df


def predict_todays_matches(tour, lower_level):

    params, final_rating_dict, mark_names = fit_model(tour, lower_level)
    df = get_todays_matches(tour, lower_level)
    
    for index,row in df.iterrows():
        match_prediction = predict_match(row['p1_name'], row['p2_name'], row['surface'])
        df.loc[index,'p1_probability'] = match_prediction
        df.loc[index,'p2_probability'] = 1-match_prediction
        df.loc[index,'p1_fair_odds'] = 1/match_prediction
        df.loc[index,'p2_fair_odds'] = 1/(1-match_prediction)

    table_id = f'tennis-358702.model_layer.{tour}_predictions'

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

    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_TRUNCATE')

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded table {table_id}")


if __name__ == "__main__":
    predict_todays_matches(tour='wta', lower_level=False)
    