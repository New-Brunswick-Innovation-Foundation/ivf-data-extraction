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

def get_investment_by_refnum(refnum, conn):
    query = "SELECT InvestmentID FROM Investment WHERE RefNum = ?"
    with conn.cursor() as cursor:
        cursor.execute(query, refnum)
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
    
    # Remove common business suffixes
    business_suffixes = [
        r'\binc\.?$', r'\bincorporated$', r'\bcorp\.?$', r'\bcorporation$',
        r'\bltd\.?$', r'\blimited$', r'\bllc$', r'\bco\.?$', r'\bcompany$',
        r'\benterprises?$', r'\benterprise$'
    ]
    
    for suffix in business_suffixes:
        normalized = re.sub(suffix, '', normalized)
    
    # Remove punctuation and extra spaces
    normalized = re.sub(r'[^\w\s]', ' ', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    # Handle common variations
    word_replacements = {
        'handtools': 'hand tools',
        'and': '&',
        ' & ': ' and ',
    }
    
    for old, new in word_replacements.items():
        normalized = normalized.replace(old, new)
    
    return normalized

def normalize_person_name(person_name):
    """Normalize person name for better duplicate detection."""
    if not person_name:
        return ""
    
    normalized = person_name.lower().strip()
    
    # Remove common person name prefixes and suffixes
    person_name_suffixes = [
        r'\bmr\.?$', r'\bmrs\.?$', r'\bms\.?$', r'\bdr\.?$', r'\bphd\.?$'
    ]
    
    for suffix in person_name_suffixes:
        normalized = re.sub(suffix, '', normalized)
    
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
    """Find similar companies in the existing database."""
    if existing_df.empty:
        return []
    
    new_operating = extract_operating_name(new_company['CompanyName'])
    new_normalized = normalize_person_name(new_operating)
    
    similar_companies = []
    
    for _, existing in existing_df.iterrows():
        existing_operating = extract_operating_name(existing['CompanyName'])
        existing_normalized = normalize_person_name(existing_operating)
        
        similarity = SequenceMatcher(None, new_normalized, existing_normalized).ratio()
        
        if similarity >= similarity_threshold:
            similar_companies.append({
                'existing_company': existing['CompanyName'],
                'similarity': similarity,
                'address': existing.get('Address', ''),
                'city': existing.get('City', ''),
                'province': existing.get('Province', '')
            })
    
    return sorted(similar_companies, key=lambda x: x['similarity'], reverse=True)

def find_similar_people(new_person, existing_df, similarity_threshold=0.8):
    """Find similar people in the existing database."""
    if existing_df.empty:
        return []
    
    new_full_name = new_person['FirstName'] + new_person['LastName']
    new_normalized = normalize_person_name(new_full_name)
    
    similar_people = []
    
    for _, existing in existing_df.iterrows():
        existing_full_name = existing['FirstName'] + existing['LastName']
        existing_normalized = normalize_person_name(existing_full_name)
        
        similarity = SequenceMatcher(None, new_normalized, existing_normalized).ratio()
        
        if similarity >= similarity_threshold:
            similar_people.append({
                'existing_last_name': existing['LastName'],
                'existing_first_name': existing['FirstName'],
                'similarity': similarity,
                'email': existing.get('Email', ''),
            })
    
    return sorted(similar_people, key=lambda x: x['similarity'], reverse=True)

def handle_company_duplicates(df, existing_df, interactive=True, similarity_threshold=0.8):
    """
    Handle potential company duplicates with user interaction including update functionality.
    
    Workflow:
    - Entries with matches above threshold: shown for interactive review
    - Entries without matches above threshold: automatically skipped
    
    Args:
        df: DataFrame with new company data
        existing_df: DataFrame with existing company data  
        interactive: Whether to prompt user for decisions on entries above threshold
        similarity_threshold: Minimum similarity to consider a potential duplicate
    
    Returns:
        Tuple of (companies_to_insert, companies_to_skip, companies_to_update)
    """
    insert_companies = []
    skip_companies = []
    update_companies = []
    
    for _, new_company in df.iterrows():
        similar = find_similar_companies(new_company, existing_df, similarity_threshold)
        
        if not similar:
            # No matches above threshold found - automatically skip
            print(f"⏭️  Auto-skipping (no matches above {similarity_threshold}): '{new_company['CompanyName']}'")
            skip_companies.append(new_company)
            continue
        
        # Found similar companies above threshold - show for interactive review
        if interactive:
            print(f"\n🔍 Potential duplicate found for: '{new_company['CompanyName']}'")
            print(f"   Address: {new_company.get('Address', 'N/A')}, {new_company.get('City', 'N/A')}")
            
            for i, match in enumerate(similar[:3], 1):  # Show top 3 matches
                print(f"   {i}. '{match['existing_company']}' (similarity: {match['similarity']:.2f})")
                print(f"      Address: {match['address']}, {match['city']}")
            
            while True:
                choice = input("\nWhat would you like to do?\n"
                             "1. Insert as new company\n"
                             "2. Skip (it's a duplicate)\n"
                             "3. Update existing record\n"
                             "4. Show more details\n"
                             "Enter choice (1-4): ").strip()
                
                if choice == '1':
                    insert_companies.append(new_company)
                    break
                elif choice == '2':
                    skip_companies.append(new_company)
                    break
                elif choice == '3':
                    # Let user select which existing company to update
                    if len(similar) == 1:
                        selected_company = similar[0]['existing_company']
                    else:
                        print("\nSelect which existing company to update:")
                        for i, match in enumerate(similar[:3], 1):
                            print(f"   {i}. '{match['existing_company']}'")
                        
                        while True:
                            try:
                                selection = int(input("Enter number (1-{}): ".format(min(3, len(similar)))).strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_company = similar[selection-1]['existing_company']
                                    break
                                else:
                                    print("Invalid selection. Please try again.")
                            except ValueError:
                                print("Please enter a valid number.")
                    
                    # Create update record with the existing company name as the unique identifier
                    update_record = new_company.copy()
                    update_record['_update_target'] = selected_company  # Store which company to update
                    update_companies.append(update_record)
                    print(f"✅ Will update '{selected_company}' with new information")
                    break
                elif choice == '4':
                    print(f"\nNew company details:")
                    for key, value in new_company.items():
                        print(f"  {key}: {value}")
                    print(f"\nExisting company details:")
                    for i, match in enumerate(similar[:3], 1):
                        print(f"\n  Match {i}: '{match['existing_company']}'")
                        print(f"    Address: {match['address']}")
                        print(f"    City: {match['city']}")
                        print(f"    Province: {match['province']}")
                        print(f"    Similarity: {match['similarity']:.2f}")
                    continue
                else:
                    print("Invalid choice. Please enter 1, 2, 3, or 4.")
        else:
            # Non-interactive mode: automatically skip entries above threshold
            print(f"⏭️  Auto-skipping potential duplicate: '{new_company['CompanyName']}' "
                  f"(similarity: {similar[0]['similarity']:.2f} with '{similar[0]['existing_company']}')")
            skip_companies.append(new_company)
    
    return pd.DataFrame(insert_companies), pd.DataFrame(skip_companies), pd.DataFrame(update_companies)

def handle_person_duplicates(df, existing_df, interactive=True, similarity_threshold=0.8):
    """
    Handle potential person duplicates with user interaction including update functionality.
    
    Workflow:
    - Entries with matches above threshold: shown for interactive review
    - Entries without matches above threshold: automatically skipped
    
    Args:
        df: DataFrame with new person data
        existing_df: DataFrame with existing person data  
        interactive: Whether to prompt user for decisions on entries above threshold
        similarity_threshold: Minimum similarity to consider a potential duplicate
    
    Returns:
        Tuple of (people_to_insert, people_to_skip, people_to_update)
    """
    insert_people = []
    skip_people = []
    update_people = []
    
    for _, new_person in df.iterrows():
        similar = find_similar_people(new_person, existing_df, similarity_threshold)
        
        if not similar:
            # No matches above threshold found - automatically skip
            full_name = f"{new_person.get('FirstName', '')} {new_person.get('LastName', '')}"
            print(f"⏭️  Auto-skipping (no matches above {similarity_threshold}): '{full_name}'")
            skip_people.append(new_person)
            continue
        
        # Found similar people above threshold - show for interactive review
        if interactive:
            full_name = f"{new_person.get('FirstName', '')} {new_person.get('LastName', '')}"
            print(f"\n🔍 Potential duplicate found for: '{full_name}'")
            print(f"   Email: {new_person.get('Email', 'N/A')}")
            
            for i, match in enumerate(similar[:3], 1):
                existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                print(f"   {i}. '{existing_name}' (similarity: {match['similarity']:.2f})")
                print(f"      Email: {match['email']}")
            
            while True:
                choice = input("\nWhat would you like to do?\n"
                             "1. Insert as new person\n"
                             "2. Skip (it's a duplicate)\n"
                             "3. Update existing record\n"
                             "4. Show more details\n"
                             "Enter choice (1-4): ").strip()
                
                if choice == '1':
                    insert_people.append(new_person)
                    break
                elif choice == '2':
                    skip_people.append(new_person)
                    break
                elif choice == '3':
                    # Let user select which existing person to update
                    if len(similar) == 1:
                        selected_email = similar[0]['email']
                        selected_name = f"{similar[0]['existing_first_name']} {similar[0]['existing_last_name']}"
                    else:
                        print("\nSelect which existing person to update:")
                        for i, match in enumerate(similar[:3], 1):
                            existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                            print(f"   {i}. '{existing_name}' ({match['email']})")
                        
                        while True:
                            try:
                                selection = int(input("Enter number (1-{}): ".format(min(3, len(similar)))).strip())
                                if 1 <= selection <= min(3, len(similar)):
                                    selected_email = similar[selection-1]['email']
                                    selected_name = f"{similar[selection-1]['existing_first_name']} {similar[selection-1]['existing_last_name']}"
                                    break
                                else:
                                    print("Invalid selection. Please try again.")
                            except ValueError:
                                print("Please enter a valid number.")
                    
                    # Create update record with the existing email as the unique identifier
                    update_record = new_person.copy()
                    update_record['_update_target'] = selected_email  # Store which person to update by email
                    update_people.append(update_record)
                    print(f"✅ Will update '{selected_name}' with new information")
                    break
                elif choice == '4':
                    print(f"\nNew person details:")
                    for key, value in new_person.items():
                        print(f"  {key}: {value}")
                    print(f"\nExisting person details:")
                    for i, match in enumerate(similar[:3], 1):
                        existing_name = f"{match['existing_first_name']} {match['existing_last_name']}"
                        print(f"\n  Match {i}: '{existing_name}'")
                        print(f"    Email: {match['email']}")
                        print(f"    Similarity: {match['similarity']:.2f}")
                    continue
                else:
                    print("Invalid choice. Please enter 1, 2, 3, or 4.")
        else:
            # Non-interactive mode: automatically skip entries above threshold
            full_name = f"{new_person.get('FirstName', '')} {new_person.get('LastName', '')}"
            existing_name = f"{similar[0]['existing_first_name']} {similar[0]['existing_last_name']}"
            print(f"⏭️  Auto-skipping potential duplicate: '{full_name}' "
                  f"(similarity: {similar[0]['similarity']:.2f} with '{existing_name}')")
            skip_people.append(new_person)
    
    return pd.DataFrame(insert_people), pd.DataFrame(skip_people), pd.DataFrame(update_people)

# Convenience functions for backward compatibility and ease of use
def sync_investment_data(df, research_fund_id):
    """Convenience function to sync Investment data."""
    sync_with_database(df, 'Investment', research_fund_id)

def sync_voucher_company_data(df):
    sync_with_database(df, 'VoucherCompany')

def sync_voucher_company_data_enhanced(df, interactive=True, similarity_threshold=0.8):
    """Enhanced version of sync_voucher_company_data with duplicate detection and update support."""
    conn = connect_to_db(False)
    if conn:
        try:
            existing_df = load_existing_records_enhanced('VoucherCompany', conn=conn)
            
            # Handle duplicates - now returns insert, skip, and update DataFrames
            insert_df, skip_df, update_df = handle_company_duplicates(
                df, existing_df, interactive, similarity_threshold
            )
            
            if not insert_df.empty:
                insert_new_records(insert_df, 'VoucherCompany', conn)
            
            if not update_df.empty:
                update_existing_records_enhanced(update_df, 'VoucherCompany', conn)
            
            logging.info(f"VoucherCompany - Inserted: {len(insert_df)}, "
                        f"Skipped: {len(skip_df)}, Updated: {len(update_df)}")
            
            if not skip_df.empty:
                logging.info("Skipped companies (potential duplicates):")
                for _, company in skip_df.iterrows():
                    logging.info(f"  - {company['CompanyName']}")
                    
        finally:
            conn.close()
            return insert_df, skip_df, update_df  # Return all three DataFrames
    else:
        logging.error("Could not connect to DB")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def sync_people_info_data(df):
    sync_with_database(df, 'PeopleInfo')

def sync_people_info_data_enhanced(df, interactive=True, similarity_threshold=0.8):
    """Enhanced version of sync_people_info_data with duplicate detection and update support."""
    conn = connect_to_db(False)
    if conn:
        try:
            existing_df = load_existing_records_enhanced('PeopleInfo', conn=conn)
            
            # Handle duplicates - now returns insert, skip, and update DataFrames
            insert_df, skip_df, update_df = handle_person_duplicates(
                df, existing_df, interactive, similarity_threshold
            )
            
            if not insert_df.empty:
                insert_new_records(insert_df, 'PeopleInfo', conn)
            
            if not update_df.empty:
                update_existing_records_enhanced(update_df, 'PeopleInfo', conn)
            
            logging.info(f"PeopleInfo - Inserted: {len(insert_df)}, "
                        f"Skipped: {len(skip_df)}, Updated: {len(update_df)}")
            
            if not skip_df.empty:
                logging.info("Skipped people (potential duplicates):")
                for _, person in skip_df.iterrows():
                    full_name = f"{person.get('FirstName', '')} {person.get('LastName', '')}"
                    logging.info(f"  - {full_name}")
                    
        finally:
            conn.close()
            return insert_df, skip_df, update_df  # Return all three DataFrames
    else:
        logging.error("Could not connect to DB")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

