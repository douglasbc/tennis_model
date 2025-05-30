import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from typing import List
from bq_client import bigquery_client


PROJECT_ID = 'tennis-358702'
DATASET = 'raw_layer'
CSV_PATH = os.path.join('data_collection', 'match_charting_project', 'tennis_MatchChartingProject-master')


def process_file1(file_name: str) -> pd.DataFrame:
    """Process the first CSV file to prepare for BigQuery ingestion."""
    df = pd.read_csv(os.path.join(CSV_PATH, file_name))
    df = df[df['set'] == 'Total'].copy()

    # Split match_id and handle variable-length parts
    parts = df['match_id'].str.split('-')
    mask = parts.str.len() >= 6
    df = df[mask].copy()
    parts = parts[mask]

    # Extract components by position
    df['match_date'] = parts.str[0]
    df['tour'] = parts.str[1]
    df['tournament_name'] = parts.str[2:-3].str.join('-')  # Middle parts
    df['round'] = parts.str[-3]
    df['p1_name'] = parts.str[-2]
    df['p2_name'] = parts.str[-1]

    # Convert match_date to date
    df['match_date'] = pd.to_datetime(df['match_date'], format='%Y%m%d', errors='coerce').dt.date
    df.dropna(subset=['match_date'], inplace=True)

    # Clean names
    for col in ['tournament_name', 'p1_name', 'p2_name']:
        df[col] = df[col].str.replace('_', ' ')

    # DEFINE split_cols HERE (this was missing)
    split_cols = ['match_date', 'tour', 'tournament_name', 'round', 'p1_name', 'p2_name']

    # Split into player 1 and player 2 data
    df_p1 = df[df['player'] == df['p1_name']].copy()
    df_p2 = df[df['player'] == df['p2_name']].copy()

    # Columns to rename (excluding split columns and identifiers)
    stats_cols = df.columns.difference(['match_id', 'player', 'set'] + split_cols).tolist()

    # Rename stats columns with prefixes
    df_p1.rename(columns={col: f'p1_{col}' for col in stats_cols}, inplace=True)
    df_p2.rename(columns={col: f'p2_{col}' for col in stats_cols}, inplace=True)

    # Merge player 1 and player 2 data
    merged_df = pd.merge(
        df_p1[['match_id'] + split_cols + [f'p1_{col}' for col in stats_cols]],
        df_p2[['match_id'] + [f'p2_{col}' for col in stats_cols]],
        on='match_id'
    )
    return merged_df

def process_file2(file_name: str, merged_df1: pd.DataFrame) -> pd.DataFrame:
    """Process the second CSV file and join with processed File1 data."""
    df = pd.read_csv(os.path.join(CSV_PATH, file_name))
    df = df[df['row'] == 'NetPoints']
    df = df[['match_id', 'player', 'net_pts', 'pts_won']]

    # Merge with File1 data to get p1_name and p2_name
    df_merged = df.merge(
        merged_df1[['match_id', 'p1_name', 'p2_name']],
        on='match_id',
        how='inner'
    )

    # Create p1 and p2 specific columns
    df_merged['p1_net_pts'] = df_merged.apply(
        lambda x: x['net_pts'] if x['player'] == x['p1_name'] else None,
        axis=1
    )
    df_merged['p1_net_pts_won'] = df_merged.apply(
        lambda x: x['pts_won'] if x['player'] == x['p1_name'] else None,
        axis=1
    )
    df_merged['p2_net_pts'] = df_merged.apply(
        lambda x: x['net_pts'] if x['player'] == x['p2_name'] else None,
        axis=1
    )
    df_merged['p2_net_pts_won'] = df_merged.apply(
        lambda x: x['pts_won'] if x['player'] == x['p2_name'] else None,
        axis=1
    )

    # Aggregate to one row per match_id
    df_grouped = df_merged.groupby('match_id').agg({
        'p1_net_pts': 'first',
        'p1_net_pts_won': 'first',
        'p2_net_pts': 'first',
        'p2_net_pts_won': 'first'
    }).reset_index()
    return df_grouped

def main():
    merged_df1 = process_file1('charting-w-stats-Overview.csv')
    df2_grouped = process_file2('charting-w-stats-NetPoints.csv', merged_df1)

    # Merge to final DataFrame
    final_df = merged_df1.merge(df2_grouped, on='match_id', how='left')

    # Define BigQuery schema
    schema = [
        bigquery.SchemaField('match_date', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('tour', 'STRING'),
        bigquery.SchemaField('tournament_name', 'STRING'),
        bigquery.SchemaField('round', 'STRING'),
        bigquery.SchemaField('p1_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('p2_name', 'STRING', mode='REQUIRED')
    ]

    # Add other columns (assuming all other fields are INTEGER)
    numeric_cols = final_df.columns.difference([
        'match_id', 'match_date', 'tour', 'tournament_name', 'round', 'p1_name', 'p2_name'
    ])
    for col in numeric_cols:
        schema.append(bigquery.SchemaField(col, 'INT64'))

    # Upload to BigQuery
    client = bigquery_client()
    table_name = 'wta_match_charting_repo_stats'
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(final_df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into {table_id}")

if __name__ == '__main__':
    main()