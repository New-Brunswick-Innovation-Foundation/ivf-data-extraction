import json
import requests

def api_2_JSON(data: dict):
    with open('program_info.json', 'w') as f:
        json.dump(data, f, indent=4)

def load_api_info():
    with open('program_info.json') as f:
        return json.load(f)

def refresh_token(api_info):
    response = requests.post('https://nbif-finb.smapply.io/api/o/token/', data=api_info['api']).json()
    api_info['api']['access_token'] = response['access_token']
    api_info['api']['refresh_token'] = response['refresh_token']
    api_2_JSON(api_info)
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