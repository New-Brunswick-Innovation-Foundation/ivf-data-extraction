import os
import json
import requests
import threading
import logging
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# Create a lock for thread-safe operations
token_lock = threading.Lock()

# --- Azure Blob Configuration ---
AZURE_STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
# Extract the container name to the environment variables to prevent cross-project collisions
CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME") 
BLOB_NAME = "program_info.json"

def get_blob_client():
    """Helper to initialize the Azure Blob Client."""
    if not AZURE_STORAGE_CONNECTION_STRING:
        logging.critical("AZURE_STORAGE_CONNECTION_STRING is missing from environment variables.")
        raise EnvironmentError("Missing Azure Storage connection string.")
    
    if not CONTAINER_NAME:
        logging.critical("AZURE_CONTAINER_NAME is missing from environment variables.")
        raise EnvironmentError("Missing Azure Container Name.")
        
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    return blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

def load_api_info():
    """Reads the dynamic tokens from Blob and merges with static environment variables."""
    with token_lock:
        blob_client = get_blob_client()
        
        try:
            blob_data = blob_client.download_blob().readall()
            dynamic_state = json.loads(blob_data)
        except Exception as e:
            logging.error(f"Failed to read from Blob Storage: {e}")
            raise

        # Merge the static secrets from App Settings with the dynamic tokens from Blob
        return {
            "api": {
                "client_id": os.environ.get("CLIENT_ID"),
                "client_secret": os.environ.get("CLIENT_SECRET"),
                "grant_type": "refresh_token",
                "refresh_token": dynamic_state.get("refresh_token"),
                "access_token": dynamic_state.get("access_token")
            }
        }

def refresh_token(api_info):
    """Refreshes the token via SMA and saves the new dynamic state back to Blob Storage."""
    with token_lock:
        blob_client = get_blob_client()
        
        # 1. Re-read the blob to ensure another thread hasn't already refreshed it
        blob_data = blob_client.download_blob().readall()
        current_dynamic_state = json.loads(blob_data)
            
        # 2. If the token in the blob is newer than the one this thread crashed on, just return!
        if current_dynamic_state.get('access_token') != api_info['api'].get('access_token'):
            api_info['api']['access_token'] = current_dynamic_state['access_token']
            api_info['api']['refresh_token'] = current_dynamic_state['refresh_token']
            return api_info

        # 3. If it's truly expired, ask SurveyMonkey for a new one
        response = requests.post('https://nbif-finb.smapply.io/api/o/token/', data=api_info['api']).json()
        
        if 'access_token' not in response:
            logging.warning(f"\n[!] Token Refresh Failed! SMA Response: {response}")
            raise KeyError("Failed to retrieve access_token from SurveyMonkey Apply.")
            
        # 4. Write ONLY the dynamic tokens back to Blob Storage
        new_dynamic_state = {
            "access_token": response['access_token'],
            "refresh_token": response['refresh_token']
        }
        
        blob_client.upload_blob(json.dumps(new_dynamic_state, indent=4), overwrite=True)
        
        # 5. Update the dictionary in memory so the current thread can continue
        api_info['api']['access_token'] = response['access_token']
        api_info['api']['refresh_token'] = response['refresh_token']
            
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
        
    if 'error' in response or ('detail' in response and 'credentials' in response['detail']):
        return None
        
    responses.append(response)
    for page in range(2, response.get("num_pages", 1) + 1):
        params['page'] = page
        responses.append(session.get(f"{base_url}{endpoint}", params=params).json())
    return responses