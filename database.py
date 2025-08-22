from datetime import datetime
import os
import logging
import pyodbc
import pandas as pd
from dotenv import load_dotenv
import re
from difflib import SequenceMatcher

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
    }
    ,
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

def load_existing_records_enhanced(table_name, filter_value=None, conn=None):
    """Enhanced version that loads more company data for similarity checking."""
    if not conn:
        return pd.DataFrame()
    
    config = TABLE_CONFIGS.get(table_name)
    if not config:
        logging.error(f"Unknown table: {table_name}")
        return pd.DataFrame()
    
    try:
        with conn.cursor() as cursor:
            if table_name == 'VoucherCompany':
                # Load full company records for similarity checking
                query = f"SELECT CompanyName, Address, City, Province FROM {table_name}"
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return pd.DataFrame([dict(zip(columns, row)) for row in rows])
            elif table_name == 'PeopleInfo':
                # Load full company records for similarity checking
                query = f"SELECT LastName, FirstName, Email FROM {table_name}"
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return pd.DataFrame([dict(zip(columns, row)) for row in rows])
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
    
def load_existing_records_with_ids(table_name, filter_value=None, conn=None):
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
            cursor.execute(insert_query, values)
        conn.commit()

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
    
def insert_into_project_asgmt(refnum, person_id, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM ProjectAsgmt WHERE RefNum = ? AND PersonID = ?", (refnum, person_id))
    if cursor.fetchone() is None:
        cursor.execute(
            "INSERT INTO ProjectAsgmt (RefNum, PersonID, Participation, PersonTitle) VALUES (?, ?, 'PI', 'F')",
            (refnum, person_id)
        )
        conn.commit()
        print(f"Linked {refnum} to PersonID: {person_id}")

def insert_into_company_asgmt(refnum, company_id, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM CompanyAsgmt WHERE RefNum = ? AND CompanyID = ?", (refnum, company_id))
    if cursor.fetchone() is None:
        cursor.execute(
            "INSERT INTO CompanyAsgmt (RefNum, CompanyID) VALUES (?, ?)",
            (refnum, company_id)
        )
        conn.commit()
        print(f"Linked {refnum} to CompanyID: {company_id}")

def normalize_company_name(company_name):
    """Normalize company name for better duplicate detection."""
    if not company_name:
        return ""
    
    normalized = company_name.lower().strip()
    
    # Remove common business suffixes (more comprehensive list)
    business_suffixes = [
        r'\binc\.?$', r'\bincorporated$', r'\bcorp\.?$', r'\bcorporation$',
        r'\bltd\.?$', r'\blimited$', r'\bllc$', r'\bco\.?$', r'\bcompany$',
        r'\benterprises?$', r'\benterprise$', r'\bgroup$', r'\bholdings?$',
        r'\bassociates?$', r'\bpartners?$', r'\bsolutions?$', r'\bservices?$',
        r'\btechnologies$', r'\btechnology$', r'\btech$', r'\bsystems?$',
        r'\bindustries$', r'\bindustrial$', r'\bmanufacturing$', r'\bmfg$'
    ]

    changed = True
    while changed:
        old = normalized
        for suffix in business_suffixes:
            normalized = re.sub(suffix, '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        changed = (normalized != old)

    
    # Handle punctuation more carefully - preserve meaningful separators
    # Replace hyphens and underscores with spaces first
    normalized = re.sub(r'[-_]', ' ', normalized)
    
    # Remove other punctuation except dots (which might be meaningful in tech names)
    normalized = re.sub(r'[^\w\s\.]', ' ', normalized)
    
    # Clean up multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized).strip()

    normalized = re.sub(r'\.', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized

def normalize_person_name(person_name):
    """Normalize person name for better duplicate detection."""
    if not person_name:
        return ""
    
    normalized = person_name.lower().strip()
    
    # Remove common person name prefixes and suffixes
    person_name_affixes = [
        r'^\s*mr\.?\s+', r'^\s*mrs\.?\s+', r'^\s*ms\.?\s+', r'^\s*dr\.?\s+',   # prefixes
        r'\s+phd\.?\s*$', r'\s+md\.?\s*$', r'\s+jr\.?\s*$', r'\s+sr\.?\s*$', r'\s+mba\.?\s*$'    # suffixes
    ]

    for affix in person_name_affixes:
        normalized = re.sub(affix, '', normalized)

    
    # Remove punctuation and extra spaces
    normalized = re.sub(r'[^\w\s]', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized

def extract_operating_name(company_name):
    """Extract the main operating name from complex company descriptions."""
    if not company_name:
        return company_name
    
    operating_patterns = [
        r'operating\s+business\s+name:\s*(.+?)(?:,|$)',
        r'dba\s*(.+?)(?:,|$)',
        r'doing\s+business\s+as\s*(.+?)(?:,|$)',
        r'operating\s+as\s*(.+?)(?:,|$)'
    ]
    
    for pattern in operating_patterns:
        match = re.search(pattern, company_name, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    return company_name

def find_similar_companies(new_company, existing_df, similarity_threshold=0.8):
    """Find similar companies and return their IDs."""
    if existing_df.empty:
        return []
    
    new_operating = extract_operating_name(new_company['CompanyName'])
    new_normalized = normalize_company_name(new_operating)
    
    similar_companies = []
    
    for _, existing in existing_df.iterrows():
        existing_operating = extract_operating_name(existing['CompanyName'])
        existing_normalized = normalize_company_name(existing_operating)
        
        similarity = SequenceMatcher(None, new_normalized, existing_normalized).ratio()
        # if 'dot' in existing_normalized:
        #     print('New Normalized:' + new_normalized)
        #     print('Existing Normalized:' + existing_normalized)
        #     print('Similarity: ' + str(similarity))
        
        if similarity >= similarity_threshold:
            similar_companies.append({
                'company_id': existing['CompanyID'],
                'existing_company': existing['CompanyName'],
                'similarity': similarity,
                'address': existing.get('Address', ''),
                'city': existing.get('City', ''),
                'province': existing.get('Province', '')
            })
    
    return sorted(similar_companies, key=lambda x: x['similarity'], reverse=True)



def find_similar_people(new_person, existing_df, similarity_threshold=0.8):
    """Find similar people and return their IDs."""
    if existing_df.empty:
        return []
    
    new_email = (new_person.get('Email') or "").strip().lower()
    new_full_name = new_person['FirstName'] + new_person['LastName']
    new_normalized = normalize_person_name(new_full_name)

    similar_people = []
    
    for _, existing in existing_df.iterrows():
        existing_email = (existing.get('Email') or "").strip().lower()
        existing_full_name = existing['FirstName'] + existing['LastName']
        existing_normalized = normalize_person_name(existing_full_name)

        if new_email and existing_email and new_email == existing_email:
            similarity = 1.0
        else:
            similarity = SequenceMatcher(None, new_normalized, existing_normalized).ratio()
        
        if similarity >= similarity_threshold:
            similar_people.append({
                'person_id': existing['PersonID'],  # Store the ID!
                'existing_last_name': existing['LastName'],
                'existing_first_name': existing['FirstName'],
                'similarity': similarity,
                'email': existing.get('Email', ''),
            })
    
    return sorted(similar_people, key=lambda x: x['similarity'], reverse=True)

def handle_company_duplicates(df, existing_df, interactive=True, similarity_threshold=0.8):
    """
    Handle potential company duplicates with corrected logic.
    
    Logic:
    - No matches above threshold → INSERT as new
    - Matches above threshold → Interactive review or auto-skip if non-interactive
    """
    insert_companies = []
    skip_companies = []
    update_companies = []
    
    for _, new_company in df.iterrows():
        similar = find_similar_companies(new_company, existing_df, similarity_threshold)
        
        if not similar:
            # No matches above threshold found - INSERT as new company
            print(f"✅ Auto-inserting (no matches above {similarity_threshold}): '{new_company['CompanyName']}'")
            insert_companies.append(new_company)
            continue
        
        # Found similar companies above threshold
        if interactive:
            # Show for interactive review
            print(f"\n🔍 Potential duplicate found for: '{new_company['CompanyName']}'")
            print(f"   Address: {new_company.get('Address', 'N/A')}, {new_company.get('City', 'N/A')}")
            
            for i, match in enumerate(similar[:3], 1):
                print(f"   {i}. '{match['existing_company']}' (similarity: {match['similarity']:.2f}) [ID: {match['company_id']}]")
                print(f"      Address: {match['address']}, {match['city']}")
            
            choice_made = False
            while not choice_made:
                choice = input("\nWhat would you like to do?\n"
                             "1. Insert as new company\n"
                             "2. Skip (it's a duplicate)\n"
                             "3. Update existing record\n"
                             "4. Show more details\n"
                             "Enter choice (1-4): ").strip()
                
                if choice == '1':
                    insert_companies.append(new_company)
                    choice_made = True
                elif choice == '2':
                    # User confirms it's a duplicate - skip and store reference
                    if len(similar) == 1:
                        selected_match = similar[0]
                    else:
                        print("\nWhich existing company is this a duplicate of?")
                        for i, match in enumerate(similar[:3], 1):
                            print(f"   {i}. '{match['existing_company']}' [ID: {match['company_id']}]")
                        
                        while True:
                            try:
                                selection = int(input("Enter number (1-{}): ".format(min(3, len(similar)))).strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_match = similar[selection-1]
                                    break
                                else:
                                    print("Invalid selection. Please try again.")
                            except ValueError:
                                print("Please enter a valid number.")
                    
                    skip_record = new_company.copy()
                    skip_record['_matched_existing_id'] = selected_match['company_id']
                    skip_record['_matched_existing_name'] = selected_match['existing_company']
                    skip_companies.append(skip_record)
                    choice_made = True
                elif choice == '3':
                    # Update existing record
                    if len(similar) == 1:
                        selected_match = similar[0]
                    else:
                        print("\nSelect which existing company to update:")
                        for i, match in enumerate(similar[:3], 1):
                            print(f"   {i}. '{match['existing_company']}' [ID: {match['company_id']}]")
                        
                        while True:
                            try:
                                selection = int(input("Enter number (1-{}): ".format(min(3, len(similar)))).strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_match = similar[selection-1]
                                    break
                                else:
                                    print("Invalid selection. Please try again.")
                            except ValueError:
                                print("Please enter a valid number.")
                    
                    update_record = new_company.copy()
                    update_record['_update_target_id'] = selected_match['company_id']
                    update_companies.append(update_record)
                    print(f"✅ Will update '{selected_match['existing_company']}' (ID: {selected_match['company_id']})")
                    choice_made = True
                elif choice == '4':
                    # Show more details
                    print(f"\nNew company details:")
                    for key, value in new_company.items():
                        print(f"  {key}: {value}")
                    print(f"\nExisting company details:")
                    for i, match in enumerate(similar[:3], 1):
                        print(f"\n  Match {i}: '{match['existing_company']}' [ID: {match['company_id']}]")
                        print(f"    Address: {match['address']}")
                        print(f"    City: {match['city']}")
                        print(f"    Province: {match['province']}")
                        print(f"    Similarity: {match['similarity']:.2f}")
                else:
                    print("Invalid choice. Please enter 1, 2, 3, or 4.")
        else:
            # Non-interactive mode: auto-skip potential duplicates
            print(f"⭐️ Auto-skipping potential duplicate: '{new_company['CompanyName']}' "
                  f"(similarity: {similar[0]['similarity']:.2f} with '{similar[0]['existing_company']}')")
            
            # Store reference to the most similar existing company
            skip_record = new_company.copy()
            skip_record['_matched_existing_id'] = similar[0]['company_id']
            skip_record['_matched_existing_name'] = similar[0]['existing_company']
            skip_companies.append(skip_record)
    
    return pd.DataFrame(insert_companies), pd.DataFrame(skip_companies), pd.DataFrame(update_companies)

def handle_person_duplicates(df, existing_df, interactive=True, similarity_threshold=0.8):
    """
    Handle potential person duplicates with ID-based updates.
    """
    insert_people = []
    skip_people = []
    update_people = []
    
    for _, new_person in df.iterrows():
        similar = find_similar_people(new_person, existing_df, similarity_threshold)
        
        if not similar:
            full_name = f"{new_person.get('FirstName', '')} {new_person.get('LastName', '')}"
            print(f"✅ Auto-inserting (no matches above {similarity_threshold}): '{full_name}'")
            insert_people.append(new_person)
            continue

        
        # Found similar people above threshold - show for interactive review
        if interactive:
            full_name = f"{new_person.get('FirstName', '')} {new_person.get('LastName', '')}"
            print(f"\n🔍 Potential duplicate found for: '{full_name}'")
            print(f"   Email: {new_person.get('Email', 'N/A')}")
            
            for i, match in enumerate(similar[:3], 1):
                existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                email_display = match['email'] if match['email'] else 'None'
                print(f"   {i}. '{existing_name}' (similarity: {match['similarity']:.2f}) [ID: {match['person_id']}]")
                print(f"      Email: {email_display}")
            
            choice_made = False
            while not choice_made:
                choice = input("\nWhat would you like to do?\n"
                             "1. Insert as new person\n"
                             "2. Skip (it's a duplicate)\n"
                             "3. Update existing record\n"
                             "4. Show more details\n"
                             "Enter choice (1-4): ").strip()
                
                if choice == '1':
                    insert_people.append(new_person)
                    choice_made = True
                elif choice == '2':
                    # Let user select which existing person this is a duplicate of
                    if len(similar) == 1:
                        selected_match = similar[0]
                    else:
                        print("\nWhich existing person is this a duplicate of?")
                        for i, match in enumerate(similar[:3], 1):
                            existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                            email_display = match['email'] if match['email'] else 'None'
                            print(f"   {i}. '{existing_name}' ({email_display}) [ID: {match['person_id']}]")
                        
                        while True:
                            try:
                                selection = int(input("Enter number (1-{}): ".format(min(3, len(similar)))).strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_match = similar[selection-1]
                                    break
                                else:
                                    print("Invalid selection. Please try again.")
                            except ValueError:
                                print("Please enter a valid number.")
                    
                    # Store the matched person info in the skip record
                    skip_record = new_person.copy()
                    skip_record['_matched_existing_id'] = selected_match['person_id']
                    skip_record['_matched_existing_email'] = selected_match['email']
                    skip_people.append(skip_record)
                    choice_made = True
                elif choice == '3':
                    # Let user select which existing person to update
                    if len(similar) == 1:
                        selected_match = similar[0]
                    else:
                        print("\nSelect which existing person to update:")
                        for i, match in enumerate(similar[:3], 1):
                            existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                            email_display = match['email'] if match['email'] else 'None'
                            print(f"   {i}. '{existing_name}' ({email_display}) [ID: {match['person_id']}]")
                        
                        while True:
                            try:
                                selection = int(input("Enter number (1-{}): ".format(min(3, len(similar)))).strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_match = similar[selection-1]
                                    break
                                else:
                                    print("Invalid selection. Please try again.")
                            except ValueError:
                                print("Please enter a valid number.")
                    
                    # Create update record with ID for targeting
                    update_record = new_person.copy()
                    update_record['_update_target_id'] = selected_match['person_id']  # Store PersonID
                    
                    update_people.append(update_record)
                    selected_name = f"{selected_match['existing_first_name']} {selected_match['existing_last_name']}"
                    print(f"✅ Will update '{selected_name}' (ID: {selected_match['person_id']}) with new information")
                    choice_made = True
                elif choice == '4':
                    print(f"\nNew person details:")
                    for key, value in new_person.items():
                        print(f"  {key}: {value}")
                    print(f"\nExisting person details:")
                    for i, match in enumerate(similar[:3], 1):
                        existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                        print(f"\n  Match {i}: '{existing_name}' [ID: {match['person_id']}]")
                        print(f"    Email: {match['email']}")
                        print(f"    Similarity: {match['similarity']:.2f}")
                else:
                    print("Invalid choice. Please enter 1, 2, 3, or 4.")
        else:
            # Non-interactive mode: automatically skip entries above threshold
            full_name = f"{new_person.get('FirstName', '')} {new_person.get('LastName', '')}"
            existing_name = f"{similar[0]['existing_first_name']} {similar[0]['existing_last_name']}"
            print(f"⭐️ Auto-skipping potential duplicate: '{full_name}' "
                  f"(similarity: {similar[0]['similarity']:.2f} with '{existing_name}')")
            skip_people.append(new_person)
    
    return pd.DataFrame(insert_people), pd.DataFrame(skip_people), pd.DataFrame(update_people)

# Convenience functions for backward compatibility and ease of use
def sync_investment_data(df, research_fund_id):
    """Convenience function to sync Investment data."""
    sync_with_database(df, 'Investment', research_fund_id)

def sync_voucher_company_data(df, interactive=True, similarity_threshold=0.8):
    """Enhanced version with ID-based duplicate detection and updates."""
    conn = connect_to_db(False)
    if conn:
        try:
            existing_df = load_existing_records_with_ids('VoucherCompany', conn=conn)
            
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
            existing_df = load_existing_records_with_ids('PeopleInfo', conn=conn)
            
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

