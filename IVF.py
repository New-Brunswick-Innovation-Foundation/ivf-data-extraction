import requests
import json
import pandas as pd
from datetime import datetime
from database import sync_with_database

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
        return None  # Let caller handle the refresh
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
    applications_data = []

    for application in applications:
        research_fund_id = 'IVF'
        id = application['id']
        tasks = getApplicationTasks(id)
        application_form_id = getApplicationTaskID(tasks, 'IVF - Application Form')
        application_form_task = getApplicationTask(id, application_form_id)

        application_title = getTaskValue(application_form_task, 'Project Information: | Title of Project:')
        executive_summary = getTaskValue(application_form_task, 'Executive Summary:')
        amount_requested = clean_value(getTaskValue(application_form_task, 'Requested Contribution from NBIF:'))

        selector_of_research_id = getApplicationTaskID(tasks, 'Select Sector of Research')
        selector_of_research_task = getApplicationTask(id, selector_of_research_id)
        sector = map_selector_of_research(selector_of_research_task, sector_mapping)
        
        application_date = datetime.strptime(application.get('created_at'), "%Y-%m-%dT%H:%M:%S")
        amount_awarded = clean_value(application.get('decision', {}).get('awarded'))
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
                decision_date = datetime.strptime(value, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
        
        application_data = {
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

        applications_data.append(application_data)

    df = pd.DataFrame(applications_data)
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    # print(df[['FedLeverage', 'OtherLeverage', 'FTE', 'PTE']].head())
    df.to_excel('output.xlsx', index=False)
    return df
    
            
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

def map_selector_of_research(selector_of_research_task, sector_mapping):
    data = selector_of_research_task[0].get("data", {})

    for field in data.values():
        label = field.get("label")
        response = field.get("response")
        if response is not None and label in sector_mapping:
            sector_list = sector_mapping[label]
            if 0 <= response < len(sector_list):
                return sector_list[response]
    
    return None  #for when no valid mapping was found

def clean_value(string_value):
    replacements = [(",", ""), ("$", "")]
    for old, new in replacements:
        string_value = string_value.replace(old, new)
    float_value = float(string_value)
    return float_value




program_name = 'Innovation Voucher Fund'
ivf_program_id = getProgramId(program_name)
responses = getProgramApplications(ivf_program_id)
applications = filterProgramApplications(responses, '2025')
df = processProgramApplications(applications)
sync_with_database(df)
#getApplicationTasks('30898584')
#getApplicationTask('31077701', '1840220')
