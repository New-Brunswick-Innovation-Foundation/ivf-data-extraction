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

def connect_to_db():
    try:
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

def fetch_sql(sql):
    conn = connect_to_db()
    if not conn:
        return None
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    except pyodbc.Error as e:
        logging.error(f"Error executing query: {e}")
        return None
    finally:
        conn.close()

def load_existing_records(research_fund_id):
    sql = f"SELECT RefNum FROM Investment where ResearchFundID like '{research_fund_id}'"
    existing_records = fetch_sql(sql)
    return pd.DataFrame(existing_records)

def split_insert_update(new_df, existing_df):
    existing_refnums = set(existing_df['RefNum'].dropna())
    insert_df = new_df[~new_df['RefNum'].isin(existing_refnums)].copy()
    update_df = new_df[new_df['RefNum'].isin(existing_refnums)].copy()
    return insert_df, update_df

def insert_new_records(insert_df, conn):
    cursor = conn.cursor()
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
    cursor = conn.cursor()
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

def sync_with_database(df):
    existing_df = load_existing_records('IVF')
    insert_df, update_df = split_insert_update(df, existing_df)
    
    conn = connect_to_db()
    if conn:
        try:
            insert_new_records(insert_df, conn)
            update_existing_records(update_df, conn)
            logging.info(f"Inserted: {len(insert_df)}, Updated: {len(update_df)}")
        finally:
            conn.close()
    else:
        logging.error("Could not connect to DB")
