from constants import TABLE_CONFIGS
import pandas as pd
import logging
import pyodbc

logging.basicConfig(level=logging.INFO)

def get_existing_records(table_name, filter_value=None, conn=None):
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
    
def get_existing_records_with_ids(table_name, filter_value=None, conn=None):
    """Enhanced version that loads records with their IDs for better duplicate matching."""
    if not conn:
        return pd.DataFrame()
    
    config = TABLE_CONFIGS.get(table_name)
    if not config:
        logging.error(f"Unknown table: {table_name}")
        return pd.DataFrame()
    
    try:
        with conn.cursor() as cursor:
            if table_name == 'VoucherCompany':
                # Load company records with CompanyID
                query = f"SELECT CompanyID, CompanyName, Address, City, Province FROM {table_name}"
                cursor.execute(query)
            elif table_name == 'PeopleInfo':
                # Load person records with PersonID
                query = f"SELECT PersonID, LastName, FirstName, Email FROM {table_name}"
                cursor.execute(query)
            else:
                # Use original logic for other tables
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

def get_company_id_by_name(company_name, conn):
    query = "SELECT CompanyID FROM VoucherCompany WHERE CompanyName = ?"
    with conn.cursor() as cursor:
        cursor.execute(query, company_name)
        row = cursor.fetchone()
        return row[0] if row else None

def get_person_id_by_email(email, conn):
    query = "SELECT PersonID FROM PeopleInfo WHERE Email = ?"
    with conn.cursor() as cursor:
        cursor.execute(query, email)
        row = cursor.fetchone()
        return row[0] if row else None