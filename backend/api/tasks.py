from api.client import get_paginated, get_session, load_api_info, refresh_token

def get_application_tasks(id):
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

def get_application_task(id, task_id):
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

def get_application_task_ID(task_wrappers, task_name):
    for wrapper in task_wrappers:
        for task in wrapper.get("results", []):
            if task.get("name") == task_name:
                return task.get("id")
            
def get_task_value(responses, label):
    for response in responses:
        data = response.get("data", {})
        for field in data.values():
            if field.get("label") == label:
                return field.get("response")