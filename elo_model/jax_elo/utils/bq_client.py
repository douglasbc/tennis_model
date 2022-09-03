import os

from google.cloud import bigquery
from google.oauth2 import service_account


def bigquery_client():
    KEY_PATH = os.path.join('config', 'key.json')
    credentials = service_account.Credentials.from_service_account_file(
        KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=credentials, project=credentials.project_id)
    