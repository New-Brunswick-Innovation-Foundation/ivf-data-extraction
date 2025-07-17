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

# Table configurations
TABLE_CONFIGS = {
    'Investment': {
        'unique_column': 'RefNum',
        'filter_column': 'ResearchFundID',
        'columns': [
            'RefNum', 'ApplTitle', 'ExecSum', 'FiscalYear', 'ResearchFundID',
            'ApplDate', 'DecisionDate', 'AmtRqstd', 'AmtAwarded', 'TotalLevAmt',
            'PrivSectorLev', 'FedLeverage', 'OtherLeverage', 'FTE', 'PTE',
            'NBIFSectorID', 'Notes'
        ]
    },
    'VoucherCompany': {
        'unique_column': 'CompanyName',
        'filter_column': None,
        'columns': [
            'CompanyName', 'Address', 'City', 'Province', 
            'PostalCode', 'Country', 'Region', 'IncorporationDate'
        ]
    },
    'PeopleInfo': {
        'unique_column': 'Email',
        'filter_column': None,
        'columns': [
            'LastName', 'FirstName', 'Email', 'Phone', 
            'Note', 'CommOptOut'
        ]
    }
}

def connect_to_db(autocommit):
    try:
        conn_args = {
            "driver": db_driver,
            "server": db_host,
            "database": db_name,
            "uid": db_user,
            "pwd": db_password
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

def load_existing_records(table_name, filter_value=None, conn=None):
    """Load existing records from the specified table."""
    if not conn:
        return pd.DataFrame()
    
    config = TABLE_CONFIGS.get(table_name)
    if not config:
        logging.error(f"Unknown table: {table_name}")
        return pd.DataFrame()
    
    try:
        with conn.cursor() as cursor:
            unique_column = config['unique_column']
            filter_column = config['filter_column']
            
            if filter_column and filter_value:
                query = f"SELECT {unique_column} FROM {table_name} WHERE {filter_column} LIKE ?"
                cursor.execute(query, filter_value)
            else:
                query = f"SELECT {unique_column} FROM {table_name}"
                cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame([dict(zip(columns, row)) for row in rows])
    except pyodbc.Error as e:
        logging.error(f"Error fetching existing records from {table_name}: {e}")
        return pd.DataFrame()

def split_insert_update(new_df, existing_df, unique_column):
    """Split dataframe into records to insert and records to update, case-insensitively for strings."""
    if new_df[unique_column].dtype == object:
        # Normalize to lowercase for comparison
        existing_values = set(existing_df[unique_column].dropna().str.lower())
        new_df['_normalized'] = new_df[unique_column].str.lower()
        insert_df = new_df[~new_df['_normalized'].isin(existing_values)].copy()
        update_df = new_df[new_df['_normalized'].isin(existing_values)].copy()
        insert_df.drop(columns=['_normalized'], inplace=True)
        update_df.drop(columns=['_normalized'], inplace=True)
    else:
        existing_values = set(existing_df[unique_column].dropna())
        insert_df = new_df[~new_df[unique_column].isin(existing_values)].copy()
        update_df = new_df[new_df[unique_column].isin(existing_values)].copy()

    return insert_df, update_df

def generate_insert_query(table_name, columns):
    """Generate INSERT query for the specified table."""
    placeholders = ', '.join(['?' for _ in columns])
    column_names = ', '.join(columns)
    return f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

def generate_update_query(table_name, columns, unique_column):
    """Generate UPDATE query for the specified table."""
    set_clause = ', '.join([f"{col} = ?" for col in columns if col != unique_column])
    return f"UPDATE {table_name} SET {set_clause} WHERE {unique_column} = ?"

def insert_new_records(insert_df, table_name, conn):
    """Insert new records into the specified table."""
    config = TABLE_CONFIGS.get(table_name)
    if not config:
        logging.error(f"Unknown table: {table_name}")
        return
    
    columns = config['columns']
    insert_query = generate_insert_query(table_name, columns)
    
    with conn.cursor() as cursor:
        for _, row in insert_df.iterrows():
            values = tuple(None if pd.isna(x) else x for x in [row.get(col) for col in columns])
            cursor.execute(insert_query, values)
        conn.commit()

def update_existing_records(update_df, table_name, conn):
    """Update existing records in the specified table."""
    config = TABLE_CONFIGS.get(table_name)
    if not config:
        logging.error(f"Unknown table: {table_name}")
        return
    
    columns = config['columns']
    unique_column = config['unique_column']
    update_query = generate_update_query(table_name, columns, unique_column)
    
    with conn.cursor() as cursor:
        for _, row in update_df.iterrows():
            # Get values for all columns except the unique column, then add unique column value at the end
            update_values = tuple(None if pd.isna(x) else x for x in [row.get(col) for col in columns if col != unique_column])
            unique_value = row.get(unique_column)
            cursor.execute(update_query, update_values + (unique_value,))
        conn.commit()

def sync_with_database(df, table_name, filter_value=None):
    """
    Sync dataframe with the specified database table.
    
    Args:
        df: DataFrame containing the data to sync
        table_name: Name of the table to sync with ('Investment' or 'VoucherCompany')
        filter_value: Optional filter value (e.g., research_fund_id for Investment table)
    """
    config = TABLE_CONFIGS.get(table_name)
    if not config:
        logging.error(f"Unknown table: {table_name}")
        return
    
    conn = connect_to_db(False)
    if conn:
        try:
            existing_df = load_existing_records(table_name, filter_value, conn)
            unique_column = config['unique_column']
            insert_df, update_df = split_insert_update(df, existing_df, unique_column)
            
            insert_new_records(insert_df, table_name, conn)
            update_existing_records(update_df, table_name, conn)
            
            logging.info(f"{table_name} - Inserted: {len(insert_df)}, Updated: {len(update_df)}")
        finally:
            conn.close()
    else:
        logging.error("Could not connect to DB")

# Convenience functions for backward compatibility and ease of use
def sync_investment_data(df, research_fund_id):
    """Convenience function to sync Investment data."""
    sync_with_database(df, 'Investment', research_fund_id)

def sync_voucher_company_data(df):
    """Convenience function to sync VoucherCompany data."""
    sync_with_database(df, 'VoucherCompany')

def sync_people_info_data(df):
    sync_with_database(df, 'PeopleInfo')
