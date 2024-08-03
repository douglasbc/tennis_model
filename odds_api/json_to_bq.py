import json
from typing import Dict


from google.cloud import bigquery
import pandas as pd

from bq_client import bigquery_client

        
def load_json_as_df():
    with open('odds_api/tennis_odds.json', 'r') as f:
      odds_data = json.load(f)
      odds_data = odds_data[4:]

    return pd.json_normalize(odds_data)


def load_odds_to_bq(client, df):
    table_id = f'tennis-358702.raw_layer.pinnacle_odds'

    columns = ['event_id', 'parent_id', 'league_name',
               'starts', 'home', 'away', 
               'periods.num_0.money_line.home',
               'periods.num_0.money_line.away',      
               'event_type', 'resulting_unit',
               'periods.num_0.period_status'
                ]
    df = df[columns]
    df.columns = ['event_id', 'parent_id',
                'tournament_round', 'match_start_at',
                'p1_name', 'p2_name',
                'p1_pinnacle_odds',  'p2_pinnacle_odds',
                'event_type', 'resulting_unit',
                'line_period'
                ]

    # df['match_start_at'] = pd.to_datetime(df['match_start_at'], format='%Y/%m/%dT%H:%M:%S')
    df['match_start_at'] = pd.to_datetime(df['match_start_at'], format='mixed')
    

    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded table {table_id}")


def main():
    client = bigquery_client()

    df = load_json_as_df()

    load_odds_to_bq(client, df)


if __name__ == "__main__":
    main()