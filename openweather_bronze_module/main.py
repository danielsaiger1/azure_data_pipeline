import sys
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any
import config.dt.config as config
import logging
import json
from azure.identity import InteractiveBrowserCredential
import os


# sys.path to be able to read functions from utils folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from utils.io import write_json
from utils.auth import get_keyvault_secrets

# 1 ---------------------------- VARIABLES ----------------------------
# Variable for environment
env = 'dt'

# Variable for environment of run execution
run_environment = 'vs_code'
if run_environment not in ['vs_code', 'synapse']:
    raise ValueError(f"Invalid value for run_environment: {run_environment}. Must be 'vs_code' or 'synapse'.")

# Variable for KeyVault Name which contains the secret for the Storage Account
keyvault_name = 'kvcrimeweather'



# 2 ---------------------------- CONFIGURATION FILES ----------------------------
# configuration files contain information about source and destination of data
config_path_src = os.path.join(os.path.dirname(__file__), f'config/{env}/src.json')
with open(config_path_src, 'r') as config_file:
    src_config = json.load(config_file)

config_path_dstn = os.path.join(os.path.dirname(__file__), f'config/{env}/dstn_adls.json')
with open(config_path_dstn, 'r') as config_file:
    dstn_config = json.load(config_file)

api_base_url = src_config.get('api_base_url')

# 3 ---------------------------- AUTHENTICATION ----------------------------
credential = InteractiveBrowserCredential(additionally_allowed_tenants="*")

# Log in with your Azure Account to receive stored secret in KeyVault 
dstn_storage_account_secret = get_keyvault_secrets(
    keyvault_name = keyvault_name, 
    run_environment = run_environment,
    secret_name = dstn_config.get('storage_account_secret_name'), 
    credential = credential,
)


# 4 ---------------------------- READ DATA FROM SOURCE ----------------------------

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_weather_data(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return {}
    
def fetch_weather_data_in_intervals(url: str, lat: float, lon: float, start_date: datetime, end_date: datetime, interval_days: int, api_key: str) -> list:
    """Fetch weather data in intervals and return a combined list of results."""
    all_data = []  # To store all results
    current_start = start_date

    while current_start < end_date:
        #Define the interval's end
        current_end = min(current_start + timedelta(days=interval_days), end_date)
        logging.info(f"Fetching data from {current_start} to {current_end}")

        #Convert dates to UNIX timestamps
        start_timestamp = round(current_start.timestamp())
        end_timestamp = round(current_end.timestamp())

        #Set API parameters
        params = {
            "lat": lat,
            "lon": lon,
            "type": "hour",
            "start": start_timestamp,
            "end": end_timestamp,
            "appid": api_key
        }

        # Fetch data
        response = fetch_weather_data(url, params)
        if response:
            all_data.extend(response.get('list', []))  #Append the 'list' key content
        else:
            logging.warning(f"No data fetched for interval {current_start} to {current_end}")

        #Move to the next interval
        current_start = current_end

    return all_data

def main():
    url = src_config.get('api_base_url')
    lat = src_config.get('lat')
    lon = src_config.get('lon')
    api_key = config.api_key

    if not api_key:
        logging.error("API key is missing. Please set the OPENWEATHER_API_KEY in your .env file.")
        return

    # Get date range
    start_date = datetime(2023, 12, 1)  
    end_date = datetime.now()           # Use current date as the end date

    # Fetch weather data in intervals (5 days per call)
    interval_days = 5
    weather_data = fetch_weather_data_in_intervals(url, lat, lon, start_date, end_date, interval_days, api_key)

    # Write combined data to a JSON file
    if weather_data:
        current_file_path = dstn_config.get('file_path') + f'openweather_raw.json'
        write_json(
            data=weather_data,
            storage_account_name=dstn_config.get('storage_account_name'), 
            storage_account_secret=dstn_storage_account_secret, 
            container_name=dstn_config.get('container_name'), 
            file_path=current_file_path,
        )
        logging.info(f"JSON file successfully written to {dstn_config.get('container_name')}/{current_file_path} in {dstn_config.get('storage_account_name')}.")
    else:
        logging.error("No weather data was fetched.")

if __name__ == "__main__":
    main()