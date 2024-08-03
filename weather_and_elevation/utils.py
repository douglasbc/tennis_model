import os

from google.cloud import bigquery
from google.oauth2 import service_account


def bigquery_client():
    KEY_PATH = os.path.join('config', 'key.json')
    credentials = service_account.Credentials.from_service_account_file(
        KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id)
    

def get_input_data(client, table_id):
    query = f"select * from `tennis-358702.treated_layer.{table_id}`"

    query_job = client.query(query)
    return query_job.result().to_dataframe()


# def load_weather_data_to_bq(client, df, table_id, write_disposition='WRITE_TRUNCATE'):
def load_weather_data_to_bq(client, df, table_id):
    job_config = bigquery.LoadJobConfig(write_disposition='WRITE_APPEND')
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded data to table {table_id}")