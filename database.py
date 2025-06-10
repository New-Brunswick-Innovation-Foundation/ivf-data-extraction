from datetime import datetime
import os
import logging
import pyodbc
import pandas as pd
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
load_dotenv()

db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_driver = os.getenv("DB_DRIVER")
db_backup_dir = os.getenv("DB_BACKUP_DIR")

def connect_to_db(autocommit):
    try:
        if autocommit:
            return pyodbc.connect(
                driver=db_driver,
                server=db_host,
                database=db_name,
                uid=db_user,
                pwd=db_password,
                autocommit=autocommit
            )
        else:
            return pyodbc.connect(
                driver=db_driver,
                server=db_host,
                database=db_name,
                uid=db_user,
                pwd=db_password
            )
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

def load_existing_records(research_fund_id, conn):
    if not conn:
        return pd.DataFrame()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT RefNum FROM Investment WHERE ResearchFundID LIKE ?", research_fund_id)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame([dict(zip(columns, row)) for row in rows])
    except pyodbc.Error as e:
        logging.error(f"Error fetching existing records: {e}")
        return pd.DataFrame()

def split_insert_update(new_df, existing_df):
    existing_refnums = set(existing_df['RefNum'].dropna())
    insert_df = new_df[~new_df['RefNum'].isin(existing_refnums)].copy()
    update_df = new_df[new_df['RefNum'].isin(existing_refnums)].copy()
    return insert_df, update_df

def insert_new_records(insert_df, conn):
    with conn.cursor() as cursor:
        for _, row in insert_df.iterrows():
            cursor.execute("""
                INSERT INTO Investment (
                    RefNum, ApplTitle, ExecSum, FiscalYear, ResearchFundID,
                    ApplDate, DecisionDate, AmtRqstd, AmtAwarded, TotalLevAmt,
                    PrivSectorLev, FedLeverage, OtherLeverage, FTE, PTE,
                    NBIFSectorID, Notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tuple(None if pd.isna(x) else x for x in row.values))
        conn.commit()

def update_existing_records(update_df, conn):
    with conn.cursor() as cursor:
        for _, row in update_df.iterrows():
            cursor.execute("""
                UPDATE Investment SET
                    ApplTitle = ?, ExecSum = ?, FiscalYear = ?, ResearchFundID = ?,
                    ApplDate = ?, DecisionDate = ?, AmtRqstd = ?, AmtAwarded = ?, TotalLevAmt = ?,
                    PrivSectorLev = ?, FedLeverage = ?, OtherLeverage = ?, FTE = ?, PTE = ?,
                    NBIFSectorID = ?, Notes = ?
                WHERE RefNum = ?
            """, tuple(None if pd.isna(x) else x for x in row.drop("RefNum").values) + (row["RefNum"],))
        conn.commit()

def sync_with_database(df, research_fund_id):
    conn = connect_to_db(False)
    if conn:
        try:
            existing_df = load_existing_records(research_fund_id, conn)
            insert_df, update_df = split_insert_update(df, existing_df)
            insert_new_records(insert_df, conn)
            update_existing_records(update_df, conn)
            logging.info(f"Inserted: {len(insert_df)}, Updated: {len(update_df)}")
        finally:
            conn.close()
    else:
        logging.error("Could not connect to DB")
