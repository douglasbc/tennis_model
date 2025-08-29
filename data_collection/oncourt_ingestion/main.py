import os

from extract import backup_csv, extract_mdb_to_csv
from load_to_bq import bigquery_client, full_refresh_to_bq

def main():
    backup_csv()
    extract_mdb_to_csv()
    full_refresh_to_bq()


if __name__ == "__main__":
    main()
