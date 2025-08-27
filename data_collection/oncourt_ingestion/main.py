import os

from extract import backup_csv, extract_mdb_to_csv
from load_to_bq import bigquery_client, full_refresh_to_bq


BACKUP_PATH = os.path.join('raw', 'backup')
CSV_PATH = os.path.join('raw', 'csv')
MDB_PATH = os.path.join('raw', 'OnCourt', 'OnCourt.mdb')


def main():
    backup_csv()
    extract_mdb_to_csv()
    full_refresh_to_bq()


if __name__ == "__main__":
    main()
