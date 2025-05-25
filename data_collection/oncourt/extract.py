import os
import shutil
import subprocess


BACKUP_PATH = os.path.join('data', 'backup')
CSV_PATH = os.path.join('data', 'csv')
MDB_PATH = os.path.expanduser(
    os.path.join('~', '.wine32', 'drive_c', 'Program Files', 'OnCourt', 'OnCourt.mdb')
)

def backup_csv():
    if not os.path.exists(CSV_PATH):
        os.makedirs(CSV_PATH)
    
    for file_name in os.listdir(CSV_PATH):
        source = os.path.join(CSV_PATH, file_name)
        destination = os.path.join(BACKUP_PATH, file_name)

        if os.path.isfile(source):
            shutil.move(source, destination)
            print(f'Moved {file_name} from {CSV_PATH} to {BACKUP_PATH}')

def extract_mdb_to_csv(remove_quotes= False):
    export_args = '-Q ' if remove_quotes else ''
    
    sh_command = f'''
    mdb-tables -1 "{MDB_PATH}" \
    | xargs -I{{}} bash -c 'mdb-export {export_args}"{MDB_PATH}" "$1" > "{CSV_PATH}/$1.csv"' -- {{}}
    '''
    
    subprocess.run(sh_command, shell=True, check=True, executable='/bin/bash')
    print(f'Extracted tables to {CSV_PATH}')