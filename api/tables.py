from datetime import datetime
from api.tasks import get_application_task, get_application_task_ID, get_task_value
from api.mapping import map_fiscal_year, map_province, map_city_to_region, map_decision_date, map_selector_of_research
from constants import sector_mapping, province_mapping, city_to_region_mapping
from api.utils import clean_email, clean_value


def get_investment(application, tasks, application_form_task, id):
    if not application:
        print(f"Skipping empty application: {id}")
        return None

    research_fund_id = 'IVF'
    application_title = get_task_value(application_form_task, 'Project Information: | Title of Project:')
    executive_summary = get_task_value(application_form_task, 'Executive Summary:')
    amount_requested = clean_value(get_task_value(application_form_task, 'Requested Contribution from NBIF:'))

    selector_of_research_id = get_application_task_ID(tasks, 'Select Sector of Research')
    selector_of_research_task = get_application_task(id, selector_of_research_id)
    sector = map_selector_of_research(selector_of_research_task, sector_mapping)
    
    created_at = application.get('created_at')
    application_date = None
    if created_at:
        try:
            application_date = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S")
        except (ValueError, TypeError):
            print(f"Unexpected created_at format for application {id}: {created_at}")

    decision = application.get('decision') or {}
    awarded = decision.get('awarded')
    amount_awarded = 0.0
    if awarded:
        try:
            amount_awarded = clean_value(str(awarded))
        except Exception as e:
            print(f"Could not clean awarded value for application {id}: {awarded} ({e})")

    total_leverage_amount = amount_awarded / 4 if amount_awarded > 0 else 0.00

    refnum = ''
    fiscal_year = ''
    decision_date = ''
    for custom_field in application.get('custom_fields') or []:
        name = custom_field.get('name')
        value = custom_field.get('value')
        if name == 'NBIF Reference Number':
            refnum = value
        elif name == 'Fiscal Year':
            fiscal_year = map_fiscal_year(value)
        elif name == 'Current Date for NOD':
            if value:
                decision_date = map_decision_date(value)
    
    investment = {
        'RefNum': refnum,
        'ApplTitle': application_title,
        'ExecSum': executive_summary,
        'FiscalYear': fiscal_year,
        'ResearchFundID': research_fund_id,
        'ApplDate': application_date,
        'DecisionDate': decision_date,
        'AmtRqstd': amount_requested,
        'AmtAwarded': amount_awarded,
        'TotalLevAmt': total_leverage_amount,
        'PrivSectorLev': total_leverage_amount,
        'FedLeverage': None,
        'OtherLeverage': None,
        'FTE': None,
        'PTE': None,
        'NBIFSectorID': sector,
        'Notes': None
    }

    #Appending email and company name for inserting records into assignment tables
    email_response = get_task_value(application_form_task, 'Researcher Information: | PI E-mail Address:').strip().lower()
    email = clean_email(email_response)
    company_name = get_task_value(application_form_task, 'Company Information: | Company Name:')
    investment['Email'] = email
    investment['CompanyName'] = company_name

    return investment
    
def get_people_info(application_form_task):
    last_name = get_task_value(application_form_task, 'Researcher Information: | PI Last Name:')
    first_name = get_task_value(application_form_task, 'Researcher Information: | Principal Investigator (PI) First Name:')
    email_response = get_task_value(application_form_task, 'Researcher Information: | PI E-mail Address:').strip().lower()
    email = clean_email(email_response)

    people_info = {
        'LastName': last_name,
        'FirstName': first_name,
        'Email': email,
        'Phone': None,
        'Note': None,
        'CommOptOut': None
    }

    return people_info

def get_voucher_company(application_form_task):
    company_name = get_task_value(application_form_task, 'Company Information: | Company Name:')
    address = get_task_value(application_form_task, 'Company Information: | Company Street Address:')
    city = get_task_value(application_form_task, 'Company Information: | City:')
    province_index = get_task_value(application_form_task, 'Company Information: | Province:')
    postal_code = get_task_value(application_form_task, 'Company Information: | Postal Code:')
    incorporation_date = get_task_value(application_form_task, 'Company Information: | Date of Incorporation:').replace('/', '-')

    
    province = map_province(province_index, province_mapping, company_name)
    region = map_city_to_region(city, city_to_region_mapping)

    voucher_company = {
        'CompanyName': company_name,
        'Address': address,
        'City': city,
        'Province': province,
        'PostalCode': postal_code,
        'Country': 'Canada',
        'IncorporationDate': incorporation_date,
        'Region': region
    }

    return voucher_company