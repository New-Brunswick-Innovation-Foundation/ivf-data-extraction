import logging
import pandas as pd
from constants import TABLE_CONFIGS

logging.basicConfig(level=logging.INFO)

def update_existing_records(update_df, table_name, conn):
    """
    Enhanced update function that handles updates based on the _update_target field.
    """
    if update_df.empty:
        return
        
    config = TABLE_CONFIGS.get(table_name)
    if not config:
        logging.error(f"Unknown table: {table_name}")
        return
    
    columns = config['columns']
    unique_column = config['unique_column']
    
    with conn.cursor() as cursor:
        for _, row in update_df.iterrows():
            # Use _update_target to identify which record to update
            target_value = row.get('_update_target')
            if not target_value:
                continue
                
            # Build the SET clause for all columns except the unique column
            set_clauses = []
            update_values = []
            
            for col in columns:
                if col != unique_column:  # Don't update the unique identifier
                    set_clauses.append(f"{col} = ?")
                    value = row.get(col)
                    update_values.append(None if pd.isna(value) else value)
            
            if not set_clauses:
                continue
                
            set_clause = ', '.join(set_clauses)
            update_query = f"UPDATE {table_name} SET {set_clause} WHERE {unique_column} = ?"
            
            try:
                cursor.execute(update_query, update_values + [target_value])
                print(f"✅ Updated {table_name} record: {target_value}")
            except Exception as e:
                logging.error(f"Error updating {table_name} record {target_value}: {e}")
        
        conn.commit()

def update_existing_records_by_id(update_df, table_name, conn):
    """
    Update records using their primary key IDs instead of names/emails.
    """
    if update_df.empty:
        return
        
    config = TABLE_CONFIGS.get(table_name)
    if not config:
        logging.error(f"Unknown table: {table_name}")
        return
    
    columns = config['columns']
    
    # Determine ID column and table specifics
    if table_name == 'VoucherCompany':
        id_column = 'CompanyID'
    elif table_name == 'PeopleInfo':
        id_column = 'PersonID'
    else:
        logging.error(f"ID-based updates not supported for table: {table_name}")
        return
    
    with conn.cursor() as cursor:
        for _, row in update_df.iterrows():
            # Get the target ID
            target_id = row.get('_update_target_id')
            
            if not target_id:
                print(f"⚠️  No target ID found for update record")
                continue
            
            print(f"\n=== DEBUG: Updating {table_name} record by ID ===")
            print(f"Target ID: {target_id}")
            
            # Show BEFORE state
            cursor.execute(f"SELECT * FROM {table_name} WHERE {id_column} = ?", (target_id,))
            before_record = cursor.fetchone()
            print(f"BEFORE: {before_record}")
            
            # Build the SET clause for all columns
            set_clauses = []
            update_values = []
            
            print("Fields being updated:")
            for col in columns:
                set_clauses.append(f"{col} = ?")
                value = row.get(col)
                clean_value = None if pd.isna(value) else value
                update_values.append(clean_value)
                print(f"  {col}: {clean_value}")
            
            if not set_clauses:
                continue
                
            set_clause = ', '.join(set_clauses)
            update_query = f"UPDATE {table_name} SET {set_clause} WHERE {id_column} = ?"
            query_values = update_values + [target_id]
            
            print(f"SQL Query: {update_query}")
            print(f"Query Values: {query_values}")
            
            try:
                cursor.execute(update_query, query_values)
                affected_rows = cursor.rowcount
                
                # Show AFTER state
                cursor.execute(f"SELECT * FROM {table_name} WHERE {id_column} = ?", (target_id,))
                after_record = cursor.fetchone()
                print(f"AFTER: {after_record}")
                
                if affected_rows > 0:
                    print(f"✅ Updated {table_name} record ID {target_id} ({affected_rows} row(s))")
                else:
                    print(f"⚠️  No rows updated for {table_name} ID {target_id}")
            except Exception as e:
                logging.error(f"Error updating {table_name} record ID {target_id}: {e}")
                print(f"Error details: {e}")
        
        print(f"\n=== Committing changes to {table_name} ===")
        conn.commit()