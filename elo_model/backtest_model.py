import os
os.environ['CUDA_VISIBLE_DEVICES'] = ''

from jax_elo.models.correlated_skills_model import calculate_ratings
from jax_elo.models.correlated_skills_model import fit
from jax_elo.models.correlated_skills_model import predict_match
from jax_elo.utils.bq_client import bigquery_client            
from jax_elo.utils.convert_p import convert_bo3_p_to_bo5

from google.cloud.bigquery import LoadJobConfig

input_start = '"2014-01-01"'
input_end = '"2022-03-31"'
test_end = '"2022-07-31"'
year_start = '2014_to_2022_april_to_july'


client = bigquery_client()

def get_input_data(tour):
    input_query = f'''
                     select * from `tennis-358702.model_layer.{tour}_input`
                     where match_date >= {input_start}
                     and match_date <= {input_end}
                    '''

               
    df = client.query(input_query).to_dataframe()

    df = df.sort_values(['match_date', 'round'])
    df = df.reset_index(drop=True)
    
    return df


def fit_model(tour):

    df = get_input_data(tour)

    params, opt_info = fit(df['p1_name'], df['p2_name'], df['surface'].values, 
                        margins=df['margins'].values, verbose=True)

    final_rating_dict, mark_names = calculate_ratings(params, df['p1_name'], df['p2_name'],
                            df['surface'].values, df['margins'].values)

    return params, final_rating_dict, mark_names


def get_test_matches(tour):

    next_matches_query = f'''
                            with players_fitted as (
                              select distinct p1_name as player
                              from `tennis-358702.model_layer.{tour}_input`
                              where match_date <= {input_end}
                              union all
                              select distinct p2_name as player
                              from `tennis-358702.model_layer.{tour}_input`
                              where match_date <= {input_end}
                            )
                            select * from `tennis-358702.model_layer.{tour}_input`
                            where
                              match_date > {input_end}
                              and match_date <= {test_end}
                              and p1_name in (select player from players_fitted)
                              and p2_name in (select player from players_fitted)
                              and p1_odds > 1
                              and p2_odds > 1
                        '''

    df = client.query(next_matches_query).to_dataframe()

    df = df.sort_values(['match_date', 'round'])
    return df


def predict_test_matches(tour, year_start, final_rating_dict, params, mark_names):

    df = get_test_matches(tour)
    
    for index,row in df.iterrows():
        match_prediction = predict_match(final_rating_dict, params, row['p1_name'],
                                   row['p2_name'], row['surface'], mark_names
                                   )
        if df.loc[index,'tournament_tier'] == 'Grand Slam':
            match_prediction = convert_bo3_p_to_bo5(match_prediction)

        df.loc[index,'p1_odds_pct'] = 1/df.loc[index,'p1_odds']
        df.loc[index,'p2_odds_pct'] = 1/df.loc[index,'p2_odds']
        df.loc[index,'p1_fair_pct'] = match_prediction
        df.loc[index,'p2_fair_pct'] = 1-match_prediction
        df.loc[index,'p1_fair_odds'] = 1/match_prediction
        df.loc[index,'p2_fair_odds'] = 1/(1-match_prediction)
        df.loc[index,'p1_pct_diff'] = match_prediction - df.loc[index,'p1_odds_pct']
        df.loc[index,'p2_pct_diff'] = 1-match_prediction - df.loc[index,'p2_odds_pct'] 

    table_id = f'tennis-358702.model_layer.{tour}_{year_start}_backtest_predictions'

    job_config = LoadJobConfig(write_disposition='WRITE_TRUNCATE')

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded table {table_id}")


def main(tour):
    global params
    global final_rating_dict
    global mark_names

    params, final_rating_dict, mark_names = fit_model(tour)
    predict_test_matches(tour, year_start, final_rating_dict, params, mark_names)


if __name__ == "__main__":
    main('wta')
