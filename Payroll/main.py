import os
import requests
from fuzzywuzzy import fuzz
import arrow
import calendar
import logging
from requests.exceptions import HTTPError, RequestException
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path='../.env')

# Setup logging
logging.basicConfig(level=logging.INFO)

# Fetch API keys and IDs from environment variables
DEEL_API_KEY = os.getenv('DEEL_API_KEY')
HARVEST_API_KEY = os.getenv('HARVEST_API_KEY')
HARVEST_ACC_ID = os.getenv('HARVEST_ACCOUNT_ID')

# Headers for API requests
headers_deel = {
    'Authorization': f"Bearer {DEEL_API_KEY}",
    'accept': 'application/json'
}

headers_harvest = {
    "Harvest-Account-Id": HARVEST_ACC_ID,
    'Authorization': f'Bearer {HARVEST_API_KEY}'
}

logging.info(f"DEEL_API_KEY Loaded: {'Yes' if DEEL_API_KEY else 'No'}")
logging.info(f"HARVEST_API_KEY Loaded: {'Yes' if HARVEST_API_KEY else 'No'}")
logging.info(f"HARVEST_ACC_ID Loaded: {'Yes' if HARVEST_ACC_ID else 'No'}")
logging.info(f"HARVEST_ACC_ID Value: {HARVEST_ACC_ID}")


def get_previous_semi_month_dates():
    """Get start and end dates for the previous semi-month period."""
    today = arrow.now()
    first_day_of_current_month = today.replace(day=1)

    if today.day <= 15:
        last_day_of_previous_month = first_day_of_current_month.shift(days=-1)
        start_date = last_day_of_previous_month.replace(day=16)
        end_date = first_day_of_current_month.shift(days=-1)
    else:
        start_date = today.replace(day=1)
        end_date = today.replace(day=15)

    return start_date, end_date


def fetch_harvest_entries(start_date, end_date):
    """Fetch time entries from Harvest API within the specified date range."""
    url_harvest = "https://api.harvestapp.com/v2/time_entries"
    params = {
        "from": start_date.format('YYYY-MM-DD'),
        "to": end_date.format('YYYY-MM-DD')
    }
    try:
        logging.info(f"Fetching Harvest entries with params: {params}")
        response_harvest = requests.get(url_harvest, headers=headers_harvest, params=params)
        logging.info(f"Harvest response URL: {response_harvest.url}")
        response_harvest.raise_for_status()
        return response_harvest.json()['time_entries']
    except (HTTPError, RequestException) as e:
        logging.error(f"Error fetching Harvest entries: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response status code: {e.response.status_code}")
            logging.error(f"Response content: {e.response.content}")
        return []


def calculate_time_sum(entries):
    """Calculate Time Sum"""
    time_sum_by_person = {}
    for entry in entries:
        person_name = entry['user']['name']
        hours = entry['hours']
        time_sum_by_person.setdefault(person_name, 0)
        time_sum_by_person[person_name] += hours
    return time_sum_by_person


def fetch_contracts():
    """Fetch contracts from Deel API."""
    all_contracts = []
    after_cursor = None
    url_deel = 'https://api.letsdeel.com/rest/v1/contracts'

    while True:
        params_deel = {
            'types': 'pay_as_you_go_time_based',
            'after_cursor': after_cursor
        }
        try:
            response = requests.get(url_deel, headers=headers_deel, params=params_deel)
            response.raise_for_status()
            data = response.json()
            if 'data' in data:
                contracts = data['data']
                all_contracts.extend(contracts)
            if not data['data']:
                break
            after_cursor = data['page']['cursor']
        except (HTTPError, RequestException) as e:
            logging.error(f"Error fetching Deel contracts: {e}")
            break

    return all_contracts


def submit_timesheet(contract_id, hours, date):
    """Submit timesheet to Deel API."""
    payload = {
        "data": {
            "contract_id": contract_id,
            "description": "Uploaded",
            "date_submitted": date.format('YYYY-MM-DD'),
            "quantity": hours
        }
    }
    headers_timesheets = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f'Bearer {DEEL_API_KEY}'
    }
    try:
        response = requests.post("https://api.letsdeel.com/rest/v1/timesheets", json=payload,
                                 headers=headers_timesheets)
        response.raise_for_status()
        logging.info(f"Timesheet submitted for contract {contract_id} with hours {hours}")
    except (HTTPError, RequestException) as e:
        logging.error(f"Error submitting timesheet for contract {contract_id}: {e}")


def find_matching_contracts(time_sum_by_person, contracts, date):
    """Find matching contracts and submit timesheets."""
    for person_name, hours in time_sum_by_person.items():
        for contract in contracts:
            similarity_ratio = max(fuzz.token_set_ratio(contract['title'], person_name),
                                   fuzz.token_set_ratio(person_name, contract['title']))
            if similarity_ratio > 90:
                if contract['id']:
                    if contract['status'] == 'in_progress':
                        submit_timesheet(contract['id'], hours, date)


def process_payroll():
    """Main function to process payment."""
    start_date1, end_date1 = get_previous_semi_month_dates()
    entries = fetch_harvest_entries(start_date1, end_date1)
    if entries:
        time_sum_by_person = calculate_time_sum(entries)
        logging.info(time_sum_by_person)
        contracts = fetch_contracts()
        if contracts:
            find_matching_contracts(time_sum_by_person, contracts, start_date1)


def payroll_trigger(request):
    """Cloud Function entry point for payroll."""
    logging.info("Payroll workflow triggered.")
    process_payroll()
    return "Payroll workflow executed successfully."

# if __name__ == "__main__":
#     process_payroll()
