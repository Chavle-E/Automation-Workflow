import os
import logging
import requests
import arrow
import calendar
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path='../.env')

# Setup logging
logging.basicConfig(level=logging.INFO)

# Environment variables
HARVEST_API_KEY = os.getenv('HARVEST_API_KEY')
HARVEST_ACCOUNT_ID = os.getenv('HARVEST_ACCOUNT_ID')

headers = {
    'Harvest-Account-ID': HARVEST_ACCOUNT_ID,
    'Authorization': f'Bearer {HARVEST_API_KEY}',
    "Content-Type": "application/json"
}


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


def get_client_ids():
    """Fetch active client IDs from Harvest API."""
    url = "https://api.harvestapp.com/v2/clients"
    try:
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        clients = res.json().get("clients", [])
        return [client['id'] for client in clients if client["is_active"]]
    except requests.RequestException as e:
        logging.error(f"Error fetching clients: {e}")
        return []


def get_project_ids():
    """Fetch project IDs and their associated client IDs from Harvest API."""
    url = "https://api.harvestapp.com/v2/projects"
    try:
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        projects = res.json().get('projects', [])
        return {project['id']: project['client']['id'] for project in projects}
    except requests.RequestException as e:
        logging.error(f"Error fetching projects: {e}")
        return {}


def check_time_entries_exist(project_id, start_date, end_date):
    """Check if there are time entries for a project within the specified date range."""
    url = f"https://api.harvestapp.com/v2/time_entries"
    params = {
        "project_id": project_id,
        "from": start_date.format("YYYY-MM-DD"),
        "to": end_date.format("YYYY-MM-DD")
    }
    try:
        res = requests.get(url, headers=headers, params=params)
        res.raise_for_status()
        time_entries = res.json().get("time_entries", [])
        return len(time_entries) > 0
    except requests.RequestException as e:
        logging.error(f"Error checking time entries for project {project_id}: {e}")
        return False


def create_invoice(event, context):
    """Function to create invoices in Harvest."""
    invoice_url = "https://api.harvestapp.com/v2/invoices"
    client_ids = get_client_ids()
    project_ids = get_project_ids()
    start_date, end_date = get_previous_semi_month_dates()

    for client_id in client_ids:
        for project_id, associated_client_id in project_ids.items():
            if associated_client_id == client_id and check_time_entries_exist(project_id, start_date, end_date):
                if project_id not in [36506766, 34951635, 39801484]:
                    payload = {
                        "client_id": client_id,
                        "notes": "Thank you for choosing ThirstySprout!",
                        "payment_term": "upon receipt",
                        "line_items_import": {
                            "project_ids": [project_id],
                            "time": {"summary_type": "people", "from": start_date.format("YYYY-MM-DD"),
                                     "to": end_date.format("YYYY-MM-DD")},
                            "expenses": {"summary_type": "category"}
                        }
                    }
                    try:
                        res = requests.post(invoice_url, headers=headers, json=payload)
                        res.raise_for_status()
                        logging.info(f"Invoice created for client {client_id} and project {project_id}")
                    except requests.RequestException as e:
                        logging.error(f"Error creating invoice for client {client_id} and project {project_id}: {e}")
