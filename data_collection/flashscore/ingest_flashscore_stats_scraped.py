import os
import pandas as pd
from google.cloud import bigquery
from bq_client import bigquery_client

# Configuration
CSV_PATH = os.path.join('data_collection', 'flashscore', 'flashscore_atp_stats.csv')
PROJECT_ID = 'tennis-358702'
DATASET = 'raw_layer'
table_name = 'flashscore_stats_scraped'
table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"

def main():
    # Read CSV with proper data types
    df = pd.read_csv(
        CSV_PATH,
        dtype={
            'match_id': 'string',
            'p1_name': 'string',
            'p2_name': 'string'
        }
    )

    # Convert epoch timestamps to datetime with microsecond precision
    df['match_timestamp'] = pd.to_datetime(df['match_timestamp'], unit='s', utc=True).dt.tz_localize(None)
    df['data_added_timestamp'] = pd.to_datetime(df['data_added_timestamp'], unit='s', utc=True).dt.tz_localize(None)

    # Convert numeric columns to pandas nullable integer type
    int_cols = [
        'p1_avg_1st_sv_speed', 'p2_avg_1st_sv_speed', 'p1_avg_2nd_sv_speed', 'p2_avg_2nd_sv_speed',
        'p1_winners', 'p2_winners', 'p1_unforced_errors', 'p2_unforced_errors',
        'p1_net_points_won', 'p2_net_points_won', 'p1_net_points_played', 'p2_net_points_played'
    ]
    df[int_cols] = df[int_cols].astype('Int64')

    # Define BigQuery schema
    schema = [
        bigquery.SchemaField('match_id', 'STRING'),
        bigquery.SchemaField('match_timestamp', 'TIMESTAMP'),
        bigquery.SchemaField('p1_name', 'STRING'),
        bigquery.SchemaField('p2_name', 'STRING'),
        bigquery.SchemaField('p1_avg_1st_sv_speed', 'INT64'),
        bigquery.SchemaField('p2_avg_1st_sv_speed', 'INT64'),
        bigquery.SchemaField('p1_avg_2nd_sv_speed', 'INT64'),
        bigquery.SchemaField('p2_avg_2nd_sv_speed', 'INT64'),
        bigquery.SchemaField('p1_winners', 'INT64'),
        bigquery.SchemaField('p2_winners', 'INT64'),
        bigquery.SchemaField('p1_unforced_errors', 'INT64'),
        bigquery.SchemaField('p2_unforced_errors', 'INT64'),
        bigquery.SchemaField('p1_net_points_won', 'INT64'),
        bigquery.SchemaField('p2_net_points_won', 'INT64'),
        bigquery.SchemaField('p1_net_points_played', 'INT64'),
        bigquery.SchemaField('p2_net_points_played', 'INT64'),
        bigquery.SchemaField('data_added_timestamp', 'TIMESTAMP')
    ]

    # Configure load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Initialize client and load data
    client = bigquery_client()

    # Convert pandas timestamps to microseconds precision
    for ts_col in ['match_timestamp', 'data_added_timestamp']:
        df[ts_col] = df[ts_col].astype('datetime64[us]')

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Successfully loaded {len(df)} rows to {table_id}")

if __name__ == '__main__':
    main()