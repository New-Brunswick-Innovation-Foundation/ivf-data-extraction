from api.joins import process_join_tables
from api.program import filter_program_applications, get_program_ID, get_program_applications, process_program_applications
from api.utils import remove_duplicates
from database.connection import backup_db
from database.sync import sync_investment_data, sync_people_info_data, sync_voucher_company_data

def main():
    program_name = 'Innovation Voucher Fund'
    ivf_program_id = get_program_ID(program_name)
    responses = get_program_applications(ivf_program_id)
    applications = filter_program_applications(responses, '2025')
    investment_df, people_info_df, voucher_company_df = process_program_applications(applications)
    
    # Remove duplicates within the current batch first
    investment_df = remove_duplicates(investment_df)
    people_info_df = remove_duplicates(people_info_df)
    voucher_company_df = remove_duplicates(voucher_company_df)

    # Backup database
    backup_db()

    sync_investment_data(investment_df, 'IVF')

    people_insert_df, people_skip_df, people_update_df = sync_people_info_data(
        people_info_df, 
        interactive=True,
        similarity_threshold=0.75
    )

    print("\n=== DEBUG: People Update DataFrame ===")
    if not people_update_df.empty:
        debug_columns = ['FirstName', 'LastName', 'Email']
        if '_update_target_id' in people_update_df.columns:
            debug_columns.append('_update_target_id')
        elif '_update_target' in people_update_df.columns:
            debug_columns.append('_update_target')
        print(people_update_df[debug_columns].to_string())
    else:
        print("No people updates")

    company_insert_df, company_skip_df, company_update_df = sync_voucher_company_data(
        voucher_company_df, 
        interactive=True,
        similarity_threshold=0.75
    )

    print("\n=== DEBUG: Company Update DataFrame ===")
    if not company_update_df.empty:
        debug_columns = ['CompanyName', 'Address']
        if '_update_target_id' in company_update_df.columns:
            debug_columns.append('_update_target_id')
        elif '_update_target' in company_update_df.columns:
            debug_columns.append('_update_target')
        print(company_update_df[debug_columns].to_string())
    else:
        print("No company updates")

    process_join_tables(
        investment_df, 
        people_insert_df, people_skip_df, people_update_df, 
        company_insert_df, company_skip_df, company_update_df
    )

if __name__ == "__main__":
    main()