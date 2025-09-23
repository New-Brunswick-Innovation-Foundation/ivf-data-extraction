from constants import TABLE_CONFIGS
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)

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
            try:
                cursor.execute(insert_query, values)
            except Exception as e:
                print(f"❌ Insert failed into {table_name}: {e}\nValues: {values}")
        conn.commit()

def insert_into_project_asgmt(refnum, person_id, batch_id, loaded_at, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM staging.ProjectAsgmt WHERE RefNum = ? AND PersonID = ?", (refnum, person_id))
    if cursor.fetchone() is None:
        cursor.execute(
            "INSERT INTO staging.ProjectAsgmt (RefNum, PersonID, Participation, PersonTitle, BatchID, LoadedAt) VALUES (?, ?, 'PI', 'F', ?, ?)",
            (refnum, person_id, batch_id, loaded_at)
        )
        conn.commit()
        print(f"Linked {refnum} to PersonID: {person_id}")

def insert_into_company_asgmt(refnum, company_id, batch_id, loaded_at, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM staging.CompanyAsgmt WHERE RefNum = ? AND CompanyID = ?", (refnum, company_id))
    if cursor.fetchone() is None:
        cursor.execute(
            "INSERT INTO staging.CompanyAsgmt (RefNum, CompanyID, BatchID, LoadedAt) VALUES (?, ?, ?, ?)",
            (refnum, company_id, batch_id, loaded_at)
        )
        conn.commit()
        print(f"Linked {refnum} to CompanyID: {company_id}")