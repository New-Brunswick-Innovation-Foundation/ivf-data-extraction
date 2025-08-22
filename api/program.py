from api.tasks import get_application_task, get_application_task_ID, get_application_tasks
from api.tables import get_investment, get_people_info, get_voucher_company
from api.client import get_paginated, get_session, load_api_info, refresh_token
from constants import numeric_columns
import pandas as pd


def get_program_ID(name):
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

def get_program_applications(id):
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

def filter_program_applications(responses, fiscal_year):
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

def process_program_applications(applications):
    investment_data = []
    people_info_data = []
    voucher_company_data = []

    for application in applications:
        id = application['id']
        tasks = get_application_tasks(id)
        application_form_id = get_application_task_ID(tasks, 'IVF - Application Form')
        application_form_task = get_application_task(id, application_form_id)

        investment_data.append(get_investment(application, tasks, application_form_task, id))
        people_info_data.append(get_people_info(application_form_task))
        voucher_company_data.append(get_voucher_company(application_form_task))

    investment_df = pd.DataFrame(investment_data)
    people_info_df = pd.DataFrame(people_info_data)
    voucher_company_df = pd.DataFrame(voucher_company_data)

    for col in numeric_columns:
        investment_df[col] = pd.to_numeric(investment_df[col], errors='coerce')
    return investment_df, people_info_df, voucher_company_df