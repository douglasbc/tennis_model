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
        lower_level = 'Future'
    if tour.lower() == 'wta':
        lower_level = 'Lower Level'

    if lower_level:
        input_query = f'''
                          SELECT * FROM `tennis-358702.model_layer.{tour}_backtest_input`
                          WHERE match_date <= '2022-03-31'
                       '''
    else:
        input_query = f'''
                          SELECT * FROM `tennis-358702.model_layer.{tour}_backtest_input`
                          WHERE
                            match_date <= '2022-03-31'
                            and tournament_level <> {lower_level}
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


def get_test_matches(tour, lower_level, date_range):

    if lower_level:
        next_matches_query = f'''
                          with players_fitted as (
                            select distinct p1_name as player
                            from `tennis-358702.model_layer.{tour}_backtest_input`
                            where match_date <= '2022-03-31'
                            union all
                            select distinct p2_name as player
                            from `tennis-358702.model_layer.{tour}_backtest_input`
                            where match_date <= '2022-03-31'
                          )
                          SELECT * FROM `tennis-358702.model_layer.{tour}_backtest_input`
                          WHERE
                            {date_range}
                            and p1_name in (select player from players_fitted)
                            and p2_name in (select player from players_fitted)
                          '''
    else:
        next_matches_query = f'''
                          with players_fitted as (
                            select distinct p1_name as player
                            from `tennis-358702.model_layer.{tour}_backtest_input`
                            where match_date <= '2022-03-31'
                            union all
                            select distinct p2_name as player
                            from `tennis-358702.model_layer.{tour}_backtest_input`
                            where match_date <= '2022-03-31'
                          )
                          SELECT * FROM `tennis-358702.model_layer.{tour}_backtest_input`
                          WHERE
                            {date_range}
                            and p1_name in (select player from players_fitted)
                            and p2_name in (select player from players_fitted)
                            and tournament_level = 'Main Tour'
                          '''
    df = client.query(next_matches_query).to_dataframe()

    df = df.sort_values(['match_date', 'tournament_level', 'round'])
    return df


def predict_test_matches(tour, lower_level, date_range, prefix, final_rating_dict, params, mark_names):

    df = get_test_matches(tour, lower_level, date_range)
    
    for index,row in df.iterrows():
        match_prediction = predict(final_rating_dict, params, row['p1_name'],
                                   row['p2_name'], row['surface'], mark_names
                                   )        
        df.loc[index,'p1_fair_pct'] = match_prediction
        df.loc[index,'p2_fair_pct'] = 1-match_prediction
        df.loc[index,'p1_fair_odds'] = 1/match_prediction
        df.loc[index,'p2_fair_odds'] = 1/(1-match_prediction)
        df.loc[index,'p1_pct_diff'] = match_prediction - df.loc[index,'p1_odds_pct']
        df.loc[index,'p2_pct_diff'] = 1-match_prediction - df.loc[index,'p2_odds_pct'] 

    if lower_level:
        table_id = f'tennis-358702.model_layer.{tour}_{prefix}_lower_backtest_predictions'
    else:
        table_id = f'tennis-358702.model_layer.{tour}_{prefix}_backtest_predictions'

    # schema = [
    #     bigquery.SchemaField('match_date', 'DATE'),
    #     bigquery.SchemaField('p1_name', 'STRING', mode='REQUIRED'),
    #     bigquery.SchemaField('p2_name', 'STRING', mode='REQUIRED'),
    #     bigquery.SchemaField('round', 'STRING'),
    #     bigquery.SchemaField('surface', 'STRING', mode='REQUIRED'),
    #     bigquery.SchemaField('p1_fair_pct', 'FLOAT64'),
    #     bigquery.SchemaField('p2_fair_pct', 'FLOAT64'),
    #     bigquery.SchemaField('p1_fair_odds', 'FLOAT64'),
    #     bigquery.SchemaField('p2_fair_odds', 'FLOAT64'),
    #     bigquery.SchemaField('p1_pct_diff', 'FLOAT64'),
    #     bigquery.SchemaField('p2_pct_diff', 'FLOAT64'),
    # ]

    # job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_TRUNCATE')

    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded table {table_id}")


def main(tour, lower_level):

    params, final_rating_dict, mark_names = fit_model(tour, lower_level)

    quarter_range = "match_date >= '2022-04-01' and match_date <= '2022-06-30'" 
    predict_test_matches(tour, lower_level, quarter_range, 'q2', final_rating_dict, params, mark_names)


if __name__ == "__main__":
    main(tour='wta', lower_level=False)






