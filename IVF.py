import requests
import json
import pandas as pd
import re
from datetime import datetime
from database import sync_investment_data, sync_people_info_data_enhanced, sync_voucher_company_data_enhanced, backup_db, get_company_id_by_name, get_person_id_by_email, insert_into_company_asgmt, insert_into_project_asgmt

numeric_columns = ['FedLeverage', 'OtherLeverage', 'FTE', 'PTE']


sector_mapping = {
    "Environment & Agriculture - Select Sector": [
        "Environmental Technology & Resource Management",
        "Fisheries & Marine Sciences",
        "Agriculture, Forestry, Food & Beverage"
    ],
    "Information Technology - Select Sector": [
        "ICT",
        "Energy & Electronics",
        "Manufacturing & Materials",
        "Aerospace & Defense",
        "Precision Sciences"
    ],
    "BioScience and Health - Select Sector": [
        "Bioscience & Biotechnology",
        "Health & Medicine"
    ],
    "Business Operations - Select Sector": [
        "Statistics & Data Analytics",
        "Finance, Economics & Business Sciences",
        "Consumer Goods & Services",
        "Media, Tourism & Entertainment"
    ],
    "Social Sciences - Select Sector": [
        "Social Sciences & Humanities, Psychology"
    ]
}

city_to_region_mapping = {
    "Fredericton": "SW",
    "Moncton": "SE",
    "Saint John": "SW",
    "Bathurst": "NE",
    "Campbellton": "NE",
    "Miramichi": "NE",
    "Edmundston": "NW",
    "Caraquet": "NE",
    "Shippagan": "NE",
    "Dieppe": "SE",
    "Riverview": "SE",
    "Tracadie-Sheila": "NE",
    "Sackville": "SE",
    "St. Stephen": "SW",
    "Sussex": "SE",
    "Quispamsis": "SW",
    "Rothesay": "SW",
    "Hanwell": "SW",
    "Clair": "NW",
    "St. Andrews": "SW",
}

province_mapping = {
    0: "AB",
    1: "BC",
    2: "MB",
    3: "NB",
    4: "NL",
    5: "NT",
    6: "NS",
    7: "NU",
    8: "ON",
    9: "PE",
    10: "QC",
    11: "SK",
    12: "YK"
}

def api2JSON(data: dict):
    with open('program_info.json', 'w') as f:
        json.dump(data, f, indent=4)

def load_api_info():
    with open('program_info.json') as f:
        return json.load(f)

def refresh_token(api_info):
    response = requests.post('https://nbif-finb.smapply.io/api/o/token/', data=api_info['api']).json()
    api_info['api']['access_token'] = response['access_token']
    api_info['api']['refresh_token'] = response['refresh_token']
    api2JSON(api_info)
    return api_info

def get_session(api_info):
    session = requests.Session()
    session.headers = {'Authorization': f"Bearer {api_info['api']['access_token']}"}
    return session

def get_paginated(session, base_url, endpoint, params):
    if params is None:
        params = {}
    responses = []
    try:
        response = session.get(f"{base_url}{endpoint}", params=params).json()
    except json.decoder.JSONDecodeError:
        return None
    responses.append(response)
    for page in range(2, response.get("num_pages", 1) + 1):
        params['page'] = page
        responses.append(session.get(f"{base_url}{endpoint}", params=params).json())
    return responses

def getProgramId(name):
    data = load_api_info()
    session = get_session(data)
    base_url = "https://nbif-finb.smapply.io/api/"
    endpoint = 'programs'
    params = None

    responses = get_paginated(session, base_url, endpoint, params)
    if responses is None:
        data = refresh_token(data)
        session = get_session(data)
        responses = get_paginated(session, base_url, endpoint, params)

    for page in responses:
        for result in page.get('results', []):
            if result['name'].strip().lower() == name.strip().lower():
                return result['id']

def getProgramApplications(id):
    data = load_api_info()
    session = get_session(data)
    base_url = "https://nbif-finb.smapply.io/api/"
    endpoint = 'applications'
    params = {
        'program': id,
    }

    responses = get_paginated(session, base_url, endpoint, params)
    if responses is None:
        data = refresh_token(data)
        session = get_session(data)
        responses = get_paginated(session, base_url, endpoint, params)
    return responses

def filterProgramApplications(responses, fiscal_year):
    applications = []
    for page in responses:
        for result in page.get('results', []):
            has_fiscal_year = False
            has_refnum = False
            for custom_field in result.get('custom_fields', []):
                if (custom_field['name'] == 'Fiscal Year' and custom_field['value'] == fiscal_year):
                    has_fiscal_year = True
                if (custom_field['name'] == 'NBIF Reference Number' and custom_field['value']):
                    has_refnum = True
            if has_fiscal_year and has_refnum:
                applications.append(result)
    return applications

def processProgramApplications(applications):
    investment_data = []
    people_info_data = []
    voucher_company_data = []

    for application in applications:
        id = application['id']
        tasks = getApplicationTasks(id)
        application_form_id = getApplicationTaskID(tasks, 'IVF - Application Form')
        application_form_task = getApplicationTask(id, application_form_id)

        investment_data.append(getInvestment(application, tasks, application_form_task, id))
        people_info_data.append(getPeopleInfo(application_form_task))
        voucher_company_data.append(getVoucherCompany(application_form_task))

    investment_df = pd.DataFrame(investment_data)
    people_info_df = pd.DataFrame(people_info_data)
    voucher_company_df = pd.DataFrame(voucher_company_data)

    for col in numeric_columns:
        investment_df[col] = pd.to_numeric(investment_df[col], errors='coerce')
    return investment_df, people_info_df, voucher_company_df

def getInvestment(application, tasks, application_form_task, id):
    research_fund_id = 'IVF'
    application_title = getTaskValue(application_form_task, 'Project Information: | Title of Project:')
    executive_summary = getTaskValue(application_form_task, 'Executive Summary:')
    amount_requested = cleanValue(getTaskValue(application_form_task, 'Requested Contribution from NBIF:'))

    selector_of_research_id = getApplicationTaskID(tasks, 'Select Sector of Research')
    selector_of_research_task = getApplicationTask(id, selector_of_research_id)
    sector = mapSelectorOfResearch(selector_of_research_task, sector_mapping)
    
    application_date = datetime.strptime(application.get('created_at'), "%Y-%m-%dT%H:%M:%S")
    amount_awarded = cleanValue(application.get('decision', {}).get('awarded'))
    total_leverage_amount = amount_awarded / 4 if amount_awarded > 0 else 0.00

    refnum = ''
    fiscal_year = ''
    decision_date = ''
    for custom_field in application.get('custom_fields', []):
        name = custom_field.get('name')
        value = custom_field.get('value')
        if name == 'NBIF Reference Number':
            refnum = value
        elif name == 'Fiscal Year':
            fiscal_year = value
        elif name == 'Current Date for NOD':
            if value:
                decision_date = datetime.strptime(value, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
    
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
    email = getTaskValue(application_form_task, 'Researcher Information: | PI E-mail Address:').strip().lower()
    company_name = getTaskValue(application_form_task, 'Company Information: | Company Name:')
    investment['Email'] = email
    investment['CompanyName'] = company_name

    return investment
    
def getPeopleInfo(application_form_task):
    last_name = getTaskValue(application_form_task, 'Researcher Information: | PI Last Name:')
    first_name = getTaskValue(application_form_task, 'Researcher Information: | Principal Investigator (PI) First Name:')
    email = getTaskValue(application_form_task, 'Researcher Information: | PI E-mail Address:').strip().lower()

    people_info = {
        'LastName': last_name,
        'FirstName': first_name,
        'Email': email,
        'Phone': None,
        'Note': None,
        'CommOptOut': None
    }

    return people_info

def getVoucherCompany(application_form_task):
    company_name = getTaskValue(application_form_task, 'Company Information: | Company Name:')
    address = getTaskValue(application_form_task, 'Company Information: | Company Street Address:')
    city = getTaskValue(application_form_task, 'Company Information: | City:')
    province_index = getTaskValue(application_form_task, 'Company Information: | Province:')
    postal_code = getTaskValue(application_form_task, 'Company Information: | Postal Code:')
    incorporation_date = getTaskValue(application_form_task, 'Company Information: | Date of Incorporation:').replace('/', '-')

    
    province = mapProvince(province_index, province_mapping, company_name)
    region = mapCityToRegion(city, city_to_region_mapping)

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


            
def getApplicationTasks(id):
    data = load_api_info()
    session = get_session(data)
    base_url = "https://nbif-finb.smapply.io/api/"
    endpoint = f"applications/{id}/tasks"
    params = None

    responses = get_paginated(session, base_url, endpoint, params)
    if responses is None:
        data = refresh_token(data)
        session = get_session(data)
        responses = get_paginated(session, base_url, endpoint, params)
    return responses

def getApplicationTask(id, task_id):
    data = load_api_info()
    session = get_session(data)
    base_url = "https://nbif-finb.smapply.io/api/"
    endpoint = f"applications/{id}/tasks/{task_id}"
    params = None

    responses = get_paginated(session, base_url, endpoint, params)
    if responses is None:
        data = refresh_token(data)
        session = get_session(data)
        responses = get_paginated(session, base_url, endpoint, params)

    return responses

def getApplicationTaskID(task_wrappers, task_name):
    for wrapper in task_wrappers:
        for task in wrapper.get("results", []):
            if task.get("name") == task_name:
                return task.get("id")
            
def getTaskValue(responses, label):
    for response in responses:
        data = response.get("data", {})
        for field in data.values():
            if field.get("label") == label:
                return field.get("response")

def mapSelectorOfResearch(selector_of_research_task, sector_mapping):
    data = selector_of_research_task[0].get("data", {})

    for field in data.values():
        label = field.get("label")
        response = field.get("response")
        if response is not None and label in sector_mapping:
            sector_list = sector_mapping[label]
            if 0 <= response < len(sector_list):
                return sector_list[response]
    
    return None  #for when no valid mapping was found

def mapCityToRegion(city, city_to_region_mapping):
    normalized_city = city.strip().title()  # Normalize formatting
    if normalized_city in city_to_region_mapping:
        return city_to_region_mapping[normalized_city]
    else:
        region = input(f"Enter region for city '{city}' (NE/NW/SE/SW): ").strip().upper()
        while region not in ["NE", "NW", "SE", "SW"]:
            region = input("Invalid input. Please enter NE, NW, SE, or SW: ").strip().upper()
        city_to_region_mapping[normalized_city] = region  # Save it for future use
        return region

def mapProvince(province_index, province_mapping, company_name):
    if province_index in province_mapping:
        return province_mapping[province_index]
    else:
        return input(f"Enter province for company '{company_name}' (NB, NS, etc.): ").strip().upper()
    
def process_join_tables(investment_df, people_insert_df, people_skip_df, people_update_df, company_insert_df, company_skip_df, company_update_df):
    from database import connect_to_db
    conn = connect_to_db(False)
    if not conn:
        print("Unable to connect to DB for join table inserts.")
        return

    try:
        for _, investment in investment_df.iterrows():
            refnum = investment["RefNum"]

            # Match PeopleInfo by email
            email = investment.get("Email", "").strip().lower()
            person_id = None
            if email:
                # Check all three DataFrames for people
                match_insert = pd.DataFrame()
                match_skip = pd.DataFrame()
                match_update = pd.DataFrame()
                
                if not people_insert_df.empty and "Email" in people_insert_df.columns:
                    match_insert = people_insert_df[people_insert_df["Email"].str.lower() == email]
                    
                if not people_skip_df.empty and "Email" in people_skip_df.columns:
                    match_skip = people_skip_df[people_skip_df["Email"].str.lower() == email]
                    
                if not people_update_df.empty and "Email" in people_update_df.columns:
                    match_update = people_update_df[people_update_df["Email"].str.lower() == email]

                if not match_insert.empty or not match_skip.empty or not match_update.empty:
                    person_id = get_person_id_by_email(email, conn)

            if person_id:
                insert_into_project_asgmt(refnum, person_id, conn)

            # Match Company by name
            company_name = investment.get("CompanyName", "").strip()
            company_id = None
            if company_name:
                # Check all three DataFrames for companies
                match_insert = pd.DataFrame()
                match_skip = pd.DataFrame()
                match_update = pd.DataFrame()
                
                if not company_insert_df.empty and "CompanyName" in company_insert_df.columns:
                    match_insert = company_insert_df[company_insert_df["CompanyName"] == company_name]
                    
                if not company_skip_df.empty and "CompanyName" in company_skip_df.columns:
                    match_skip = company_skip_df[company_skip_df["CompanyName"] == company_name]
                    
                if not company_update_df.empty and "CompanyName" in company_update_df.columns:
                    match_update = company_update_df[company_update_df["CompanyName"] == company_name]

                if not match_insert.empty or not match_skip.empty or not match_update.empty:
                    company_id = get_company_id_by_name(company_name, conn)

            if company_id:
                insert_into_company_asgmt(refnum, company_id, conn)
                
    finally:
        conn.close()


def cleanValue(string_value):
    replacements = [(",", ""), ("$", "")]
    for old, new in replacements:
        string_value = string_value.replace(old, new)
    float_value = float(string_value)
    return float_value

def removeDuplicates(df):
    df.drop_duplicates(inplace=True)
    return df

program_name = 'Innovation Voucher Fund'
ivf_program_id = getProgramId(program_name)
responses = getProgramApplications(ivf_program_id)
applications = filterProgramApplications(responses, '2025')
investment_df, people_info_df, voucher_company_df = processProgramApplications(applications)
# Remove duplicates within the current batch first
investment_df = removeDuplicates(investment_df)
people_info_df = removeDuplicates(people_info_df)
voucher_company_df = removeDuplicates(voucher_company_df)

# Save to Excel for review
investment_df.to_excel("investment.xlsx")
people_info_df.to_excel("people_info.xlsx")
voucher_company_df.to_excel("voucher_company.xlsx")

# Backup database
backup_db()

sync_investment_data(investment_df, 'IVF')

people_insert_df, people_skip_df, people_update_df = sync_people_info_data_enhanced(
    people_info_df, 
    interactive=True,
    similarity_threshold=0.75
)

company_insert_df, company_skip_df, company_update_df = sync_voucher_company_data_enhanced(
    voucher_company_df, 
    interactive=True,
    similarity_threshold=0.75
)

process_join_tables(investment_df, people_insert_df, people_skip_df, people_update_df, company_insert_df, company_skip_df, company_update_df)



