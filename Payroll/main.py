import os
import requests
from fuzzywuzzy import fuzz
import arrow
import logging
from requests.exceptions import HTTPError, RequestException
from dotenv import load_dotenv
from ratelimit import limits, sleep_and_retry

# Load environment variables from .env file
load_dotenv(dotenv_path='.env')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
    'Harvest-Account-Id': f"{HARVEST_ACC_ID}",
    'Authorization': f'Bearer {HARVEST_API_KEY}'
}

# Validate environment variables
if not all([DEEL_API_KEY, HARVEST_API_KEY, HARVEST_ACC_ID]):
    logging.error("Missing environment variables")
    raise EnvironmentError("One or more environment variables are missing")


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


@sleep_and_retry
@limits(calls=5, period=1)  # 5 calls per second
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
        response_harvest.raise_for_status()
        return response_harvest.json()['time_entries']
    except (HTTPError, RequestException) as e:
        logging.error(f"Error fetching Harvest entries: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response status code: {e.response.status_code}")
            logging.error(f"Response content: {e.response.content}")
        return []


def calculate_time_sum(entries):
    """Calculate total hours worked by each person."""
    time_sum_by_person = {}
    for entry in entries:
        person_name = entry['user']['name']
        hours = entry['hours']
        time_sum_by_person.setdefault(person_name, 0)
        time_sum_by_person[person_name] += hours
    return time_sum_by_person


@sleep_and_retry
@limits(calls=5, period=1)  # 5 calls per second
def fetch_contracts():
    """Fetch contracts from Deel API, filtering for 'pay_as_you_go_time_based' contracts."""
    all_contracts = []
    after_cursor = None
    url_deel = 'https://api.letsdeel.com/rest/v2/contracts'

    while True:
        params_deel = {'after_cursor': after_cursor}
        try:
            response = requests.get(url_deel, headers=headers_deel, params=params_deel)
            response.raise_for_status()
            data = response.json()

            if 'data' in data:
                contracts = [contract for contract in data['data']
                             if contract['type'] == 'pay_as_you_go_time_based']
                all_contracts.extend(contracts)

            if not data['data']:
                break
            after_cursor = data['page']['cursor']

        except (HTTPError, RequestException) as e:
            logging.error(f"Error fetching Deel contracts: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"Response status code: {e.response.status_code}")
                logging.error(f"Response content: {e.response.content}")
            break

    return all_contracts


def get_valid_submission_date(timesheet_date, contract_start):
    """Return the later date between timesheet_date and contract_start."""
    return max(timesheet_date, contract_start)


@sleep_and_retry
@limits(calls=5, period=1)  # 5 calls per second
def submit_timesheet(contract_id, hours, submission_date, contract_start):
    """Submit timesheet to Deel API."""
    payload = {
        "data": {
            "contract_id": contract_id,
            "description": "Uploaded",
            "date_submitted": submission_date.format('YYYY-MM-DD'),
            "quantity": hours
        }
    }
    headers_timesheets = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f'Bearer {DEEL_API_KEY}'
    }
    try:
        response = requests.post("https://api.letsdeel.com/rest/v2/timesheets", json=payload,
                                 headers=headers_timesheets)
        response.raise_for_status()
        logging.info(
            f"Timesheet submitted for contract {contract_id} with hours {hours} for date {submission_date.format('YYYY-MM-DD')}")
    except (HTTPError, RequestException) as e:
        error_message = e.response.json().get('errors', [{}])[0].get('message', 'Unknown error')
        logging.error(f"Error submitting timesheet for contract {contract_id}: {error_message}")


def find_matching_contracts(time_sum_by_person, contracts, submission_date):
    """Find matching contracts and submit timesheets."""
    for person_name, hours in time_sum_by_person.items():
        for contract in contracts:
            similarity_ratio = max(
                fuzz.token_set_ratio(contract['title'].lower(), person_name.lower()),
                fuzz.token_set_ratio(person_name.lower(), contract['title'].lower())
            )
            if similarity_ratio > 85 and contract['status'] == 'in_progress':
                contract_start = arrow.get(contract['created_at'])
                logging.info(
                    f"Processing timesheet for {person_name} - Contract ID: {contract['id']}, Hours: {hours}, Submission Date: {submission_date.format('YYYY-MM-DD')}, Contract Start: {contract_start.format('YYYY-MM-DD')}")
                submit_timesheet(contract['id'], hours, submission_date, contract_start)


def process_payroll(dry_run=False):
    """Main function to process payment."""
    start_date, end_date = get_previous_semi_month_dates()
    logging.info(f"Processing payroll for period: {start_date.format('YYYY-MM-DD')} to {end_date.format('YYYY-MM-DD')}")

    entries = fetch_harvest_entries(start_date, end_date)
    if entries:
        time_sum_by_person = calculate_time_sum(entries)
        logging.info(f"Time sum by person: {time_sum_by_person}")

        contracts = fetch_contracts()
        if contracts:
            if dry_run:
                logging.info("Dry run mode: Timesheets will not be submitted")
                for person, hours in time_sum_by_person.items():
                    logging.info(
                        f"Would submit timesheet for {person}: {hours} hours for date {start_date.format('YYYY-MM-DD')}")
            else:
                find_matching_contracts(time_sum_by_person, contracts, end_date)
        else:
            logging.warning("No contracts fetched from Deel")
    else:
        logging.warning("No time entries fetched from Harvest")


if __name__ == "__main__":
    process_payroll(dry_run=False)  # Set to True for a dry run, False to actually submit timesheets


def payroll_trigger(request):
    """Cloud Function entry point for payroll."""
    logging.info("Payroll workflow triggered.")
    process_payroll()
    return "Payroll workflow executed successfully."


