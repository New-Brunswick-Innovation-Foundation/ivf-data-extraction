from datetime import datetime
import logging
import os
import pyodbc
from dotenv import load_dotenv
import pandas as pd

logging.basicConfig(level=logging.INFO)
load_dotenv()

db_host = os.getenv("AZURE_DB_HOST")
db_name = os.getenv("AZURE_DB_NAME")
db_user = os.getenv("AZURE_DB_USERNAME")
db_password = os.getenv("AZURE_DB_PASSWORD")
db_driver = os.getenv("AZURE_DB_DRIVER")
db_backup_dir = os.getenv("DB_BACKUP_DIR")

def connect_to_db(autocommit):
    try:
        conn_args = {
            "driver": db_driver,
            "server": db_host,
            "database": db_name,
            "uid": db_user,
            "pwd": db_password,
            "Encrypt": "yes",
            "TrustServerCertificate": "no",
            "Connection Timeout": 30
        }
        if autocommit:
            conn_args["autocommit"] = autocommit
        return pyodbc.connect(**conn_args)
    except pyodbc.Error as e:
        logging.error(f"Error connecting to database: {e}")
        return None
    
def backup_db():
    conn = connect_to_db(True)
    if conn:
        try:
            with conn.cursor() as cursor:
                db_backup_file = db_name + '_' + str(datetime.now().strftime('%Y%m%d_%H%M%S')) + '.bak'
                db_backup_command = f'BACKUP DATABASE [{db_name}] TO DISK=\'' + os.path.join(db_backup_dir, db_backup_file) + '\''
                cursor.execute(db_backup_command)

                db_backup_details = {'database': [db_name], 'backup_file': [db_backup_file], 'backup_datetime': [datetime.now()]}
                db_backup_df = pd.DataFrame(data=db_backup_details)
                db_backup_details_file = os.path.join(db_backup_dir, 'db_backup_details.csv')
                if os.path.exists(db_backup_details_file):
                    db_backup_df.to_csv(db_backup_details_file, mode='a', index=False, header=False)
                else:
                    db_backup_df.to_csv(db_backup_details_file, index=False)
        finally:
            conn.close()
    else:
        logging.error("Could not connect to DB")