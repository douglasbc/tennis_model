import os
os.environ['CUDA_VISIBLE_DEVICES'] = ''

from google.cloud.bigquery import LoadJobConfig

from jax_elo.models.correlated_skills_model import calculate_ratings
from jax_elo.models.correlated_skills_model import fit
from jax_elo.models.correlated_skills_model import predict_match
from jax_elo.utils.bq_client import bigquery_client            
from jax_elo.utils.convert_p import convert_bo3_p_to_bo5


client = bigquery_client()

def get_input_data(tour):
    input_query = f'''
                    select
                      match_date,
                      p1_name,
                      p2_name,
                      surface,
                      tournament_tier,
                      round,
                      margins   
                    from `tennis-358702.model_layer.{tour}_input`                 
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


def get_todays_matches(tour):

    next_matches_query = f'''
                            with players_fitted as (
                              select distinct p1_name as player
                              from `tennis-358702.model_layer.{tour}_input`
                              union all
                              select distinct p2_name as player
                              from `tennis-358702.model_layer.{tour}_input`
                            )
                            select * from `tennis-358702.model_layer.{tour}_today`
                            where
                              p1_name in (select player from players_fitted)
                              and p2_name in (select player from players_fitted)
                            '''

    df = client.query(next_matches_query).to_dataframe()

    df = df.sort_values(['tournament_tier', 'round', 'match_date'])
    return df


def predict_todays_matches(tour, final_rating_dict, params, mark_names):

    df = get_todays_matches(tour)
    
    for index,row in df.iterrows():
        match_prediction = predict_match(final_rating_dict, params, row['p1_name'],
                                   row['p2_name'], row['surface'], mark_names
                                   )   
        if tour == 'atp' and df.loc[index,'tournament_tier'] == 'Grand Slam':
            match_prediction = convert_bo3_p_to_bo5(match_prediction)
        
        df.loc[index,'p1_probability'] = match_prediction
        df.loc[index,'p2_probability'] = 1-match_prediction
        df.loc[index,'p1_fair_odds'] = 1/match_prediction
        df.loc[index,'p2_fair_odds'] = 1/(1-match_prediction)

    table_id = f'tennis-358702.raw_layer.{tour}_predictions'

    job_config = LoadJobConfig(write_disposition='WRITE_TRUNCATE')

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded table {table_id}")


def predict(player_1, player_2, surface='Hard'):
    match_prediction = predict_match(final_rating_dict, params, player_1, player_2,
                                surface, mark_names
                                )
    print(f'''
    Match: {player_1} x {player_2}
    Surface: {surface}

    Win probability:
    {player_1}: {match_prediction})
    {player_2}: {1-match_prediction})

    Fair Odds:
    {player_1}: {1/match_prediction})
    {player_2}: {1/(1-match_prediction)})
    ''')


def main(tour):    
    global params
    global final_rating_dict
    global mark_names

    params, final_rating_dict, mark_names = fit_model(tour)
    predict_todays_matches(tour, final_rating_dict, params, mark_names)


if __name__ == "__main__":
    main('atp')
    