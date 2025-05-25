import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from bq_client import bigquery_client

# Configuration
CSV_PATH = os.path.join('data_collection', 'flashscore', 'manual_input', 'flashscore_input - all.csv')
PROJECT_ID = 'tennis-358702'
DATASET = 'raw_layer'
table_name = 'flashscore_stats_manual'
table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"

def main():
    # Read CSV with proper data types
    df = pd.read_csv(
        CSV_PATH,
        parse_dates=['tournament_date', 'match_date'],
        dtype={
            'match_id': 'string',
            'tournament_name': 'string',
            'tournament_country': 'string',
            'p1_name': 'string',
            'p2_name': 'string',
            'round': 'string'
        }
    )

    # Convert numeric columns to pandas nullable integer type
    int_cols = [
        'p1_winners', 'p1_unforced_errors', 'p1_net_points_won', 'p1_net_points_played',
        'p2_winners', 'p2_unforced_errors', 'p2_net_points_won', 'p2_net_points_played'
    ]
    df[int_cols] = df[int_cols].astype('Int64')

    # Define BigQuery schema
    schema = [
        bigquery.SchemaField('match_id', 'STRING'),
        bigquery.SchemaField('tournament_date', 'DATE'),
        bigquery.SchemaField('tournament_name', 'STRING'),
        bigquery.SchemaField('tournament_country', 'STRING'),
        bigquery.SchemaField('match_date', 'DATE'),
        bigquery.SchemaField('p1_name', 'STRING'),
        bigquery.SchemaField('p2_name', 'STRING'),
        bigquery.SchemaField('round', 'STRING'),
        bigquery.SchemaField('p1_winners', 'INT64'),
        bigquery.SchemaField('p1_unforced_errors', 'INT64'),
        bigquery.SchemaField('p1_net_points_won', 'INT64'),
        bigquery.SchemaField('p1_net_points_played', 'INT64'),
        bigquery.SchemaField('p2_winners', 'INT64'),
        bigquery.SchemaField('p2_unforced_errors', 'INT64'),
        bigquery.SchemaField('p2_net_points_won', 'INT64'),
        bigquery.SchemaField('p2_net_points_played', 'INT64'),
    ]

    # Configure load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Initialize client and load data
    client = bigquery_client()
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Successfully loaded {len(df)} rows to {table_id}")

if __name__ == '__main__':
    main()