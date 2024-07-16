import os
import logging
import requests
import arrow
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path='../.env')

# Setup logging
logging.basicConfig(level=logging.INFO)

# Environment variables
HARVEST_API_KEY = os.getenv('HARVEST_API_KEY')
HARVEST_ACCOUNT_ID = os.getenv('HARVEST_ACCOUNT_ID')

headers = {
    'Harvest-Account-ID': f'{HARVEST_ACCOUNT_ID}',
    'Authorization': f'Bearer {HARVEST_API_KEY}',
    "Content-Type": "application/json"
}

# Define special billing preferences
SPECIAL_BILLING_CLIENTS = {
    '13363422': {
        'project_id': [35848992],  # Add specific project IDs for this client if needed
        'billing_day': 16,
        'due_date_offset': 5  # Due date is 5 days after billing day
    }
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


def get_custom_billing_dates():
    """Get start and end dates for custom billing period (16th of last month to 15th of this month)."""
    today = arrow.now()
    start_date = today.replace(day=16).shift(months=-1)
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


def create_invoice(client_id, project_id, start_date, end_date, payment_term):
    """Function to create invoices in Harvest."""
    invoice_url = "https://api.harvestapp.com/v2/invoices"
    payload = {
        "client_id": client_id,
        "notes": "Thank you for choosing ThirstySprout!",
        "payment_term": payment_term,
        "line_items_import": {
            "project_ids": [project_id],
            "time": {"summary_type": "people", "from": start_date.format("YYYY-MM-DD"), "to": end_date.format("YYYY-MM-DD")},
            "expenses": {"summary_type": "category"}
        }
    }
    try:
        res = requests.post(invoice_url, headers=headers, json=payload)
        res.raise_for_status()
        logging.info(f"Invoice created for client {client_id} and project {project_id}")
    except requests.RequestException as e:
        logging.error(f"Error creating invoice for client {client_id} and project {project_id}: {e}")


def process_invoices():
    """Main function to process invoices."""
    client_ids = get_client_ids()
    project_ids = get_project_ids()
    today = arrow.now()

    for client_id in client_ids:
        special_billing = SPECIAL_BILLING_CLIENTS.get(client_id)
        if special_billing:
            if today.day == special_billing['billing_day']:
                start_date, end_date = get_custom_billing_dates()
                payment_term = f"net {special_billing['due_date_offset']}"
                for project_id in special_billing['project_id']:
                    if check_time_entries_exist(project_id, start_date, end_date):
                        create_invoice(client_id, project_id, start_date, end_date, payment_term)
        else:
            start_date, end_date = get_previous_semi_month_dates()
            for project_id, associated_client_id in project_ids.items():
                if associated_client_id == client_id and check_time_entries_exist(project_id, start_date, end_date):
                    create_invoice(client_id, project_id, start_date, end_date, "upon receipt")


def invoicing_trigger(request):
    """Cloud Function entry point for invoicing."""
    logging.info("Invoicing workflow triggered.")
    process_invoices()


# if __name__ == "__main__":
#     invoicing_trigger(None, None)
