import logging
import pandas as pd
from backend.constants import TABLE_CONFIGS
from database.connection import connect_to_db
from database.duplicates import handle_company_duplicates, handle_person_duplicates
from database.get import get_existing_records, get_existing_records_with_ids
from database.insert import insert_new_records, split_insert_update
from database.update import update_existing_records, update_existing_records_by_id

logging.basicConfig(level=logging.INFO)

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
            existing_df = get_existing_records(table_name, filter_value, conn)
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

def sync_voucher_company_data(df, interactive=True, similarity_threshold=0.8):
    """Enhanced version with ID-based duplicate detection and updates."""
    conn = connect_to_db(False)
    if conn:
        try:
            existing_df = get_existing_records_with_ids('VoucherCompany', conn=conn)
            
            # Handle duplicates with ID storage  
            insert_df, skip_df, update_df = handle_company_duplicates(
                df, existing_df, interactive, similarity_threshold
            )
            
            if not insert_df.empty:
                insert_new_records(insert_df, 'VoucherCompany', conn)
            
            if not update_df.empty:
                update_existing_records_by_id(update_df, 'VoucherCompany', conn)  # Use ID-based updates
            
            logging.info(f"VoucherCompany - Inserted: {len(insert_df)}, "
                        f"Skipped: {len(skip_df)}, Updated: {len(update_df)}")
            
            if not skip_df.empty:
                logging.info("Skipped companies (potential duplicates):")
                for _, company in skip_df.iterrows():
                    logging.info(f"  - {company['CompanyName']}")
                    
        finally:
            conn.close()
            return insert_df, skip_df, update_df
    else:
        logging.error("Could not connect to DB")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
def sync_people_info_data(df, interactive=True, similarity_threshold=0.8):
    """Enhanced version with ID-based duplicate detection and updates."""
    conn = connect_to_db(False)
    if conn:
        try:
            existing_df = get_existing_records_with_ids('PeopleInfo', conn=conn)
            
            # Handle duplicates with ID storage
            insert_df, skip_df, update_df = handle_person_duplicates(
                df, existing_df, interactive, similarity_threshold
            )
            
            if not insert_df.empty:
                insert_new_records(insert_df, 'PeopleInfo', conn)
            
            if not update_df.empty:
                update_existing_records_by_id(update_df, 'PeopleInfo', conn)  # Use ID-based updates
            
            logging.info(f"PeopleInfo - Inserted: {len(insert_df)}, "
                        f"Skipped: {len(skip_df)}, Updated: {len(update_df)}")
            
            if not skip_df.empty:
                logging.info("Skipped people (potential duplicates):")
                for _, person in skip_df.iterrows():
                    full_name = f"{person.get('FirstName', '')} {person.get('LastName', '')}"
                    logging.info(f"  - {full_name}")
                    
        finally:
            conn.close()
            return insert_df, skip_df, update_df
    else:
        logging.error("Could not connect to DB")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()