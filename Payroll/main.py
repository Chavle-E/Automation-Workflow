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
HARVEST_ACC_ID = os.getenv('HARVEST_ACC_ID')

# Headers for API requests
headers_deel = {
    'Authorization': f"Bearer {DEEL_API_KEY}",
    'accept': 'application/json'
}

headers_harvest = {
    "Harvest-Account-ID": HARVEST_ACC_ID,
    'Authorization': f'Bearer {HARVEST_API_KEY}'
}


def get_semi_month_dates():
    """Get start and end dates for the current semi-month period."""
    today = arrow.now()
    _, last_day_of_month = calendar.monthrange(today.year, today.month)

    if today.day <= 15:
        start = today.replace(day=1)
        end = today.replace(day=15)
    else:
        start = today.replace(day=16)
        end = today.replace(day=last_day_of_month)

    return start, end


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
        response_harvest = requests.get(url_harvest, headers=headers_harvest, params=params)
        response_harvest.raise_for_status()
        return response_harvest.json()['time_entries']
    except (HTTPError, RequestException) as e:
        logging.error(f"Error fetching Harvest entries: {e}")
        return []


def calculate_time_sum(entries):
    """Calculate the sum of hours for each person from the time entries."""
    time_sum_by_person = {}
    for entry in entries:
        person_name = entry['user']['name']
        hours = entry['hours']
        time_sum_by_person[person_name] = time_sum_by_person.get(person_name, 0) + hours
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


def find_matching_contracts(time_sum_by_person, contracts):
    """Find matching contracts and submit timesheets."""
    for person_name, hours in time_sum_by_person.items():
        for contract in contracts:
            similarity_ratio = max(fuzz.token_set_ratio(contract['title'], person_name),
                                   fuzz.token_set_ratio(person_name, contract['title']))
            if similarity_ratio > 90:
                if contract['id'] in ['3j4enzw']:
                    if contract['status'] == 'in_progress':
                        submit_timesheet(contract['id'], hours)


def submit_timesheet(contract_id, hours):
    """Submit timesheet to Deel API."""
    payload = {
        "data": {
            "contract_id": contract_id,
            "description": "Uploaded",
            "date_submitted": arrow.now().format('YYYY-MM-DD'),
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


if __name__ == "__main__":
    start_date1, end_date1 = get_previous_semi_month_dates()
    entries = fetch_harvest_entries(start_date1, end_date1)
    if entries:
        time_sum_by_person = calculate_time_sum(entries)
        logging.info(time_sum_by_person)
        contracts = fetch_contracts()
        if contracts:
            find_matching_contracts(time_sum_by_person, contracts)
