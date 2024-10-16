import os
import logging
import requests
import arrow
from dotenv import load_dotenv
import traceback

# Load environment variables from .env file
load_dotenv(dotenv_path='../.env')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
HARVEST_API_KEY = os.getenv('HARVEST_API_KEY')
HARVEST_ACCOUNT_ID = os.getenv('HARVEST_ACCOUNT_ID')

# Check if the environment variables are loaded correctly
if not HARVEST_API_KEY or not HARVEST_ACCOUNT_ID:
    logging.error("Missing HARVEST_API_KEY or HARVEST_ACCOUNT_ID environment variables")
    raise ValueError("Missing HARVEST_API_KEY or HARVEST_ACCOUNT_ID environment variables")

headers = {
    'Harvest-Account-ID': HARVEST_ACCOUNT_ID,
    'Authorization': f'Bearer {HARVEST_API_KEY}',
    "Content-Type": "application/json"
}

# Define special billing preferences
SPECIAL_BILLING_CLIENTS = {
    '13363422': {
        'project_ids': [35848992],  # Add specific project IDs for this client
        'billing_day': 16,
        'due_date_offset': 5  # Due date is 5 days after billing day
    }
    # Add more special billing clients here if needed
}


def get_billing_dates(client_id, today):
    """Get start and end dates based on client billing preference."""
    special_billing = SPECIAL_BILLING_CLIENTS.get(str(client_id))

    if special_billing:
        billing_day = special_billing['billing_day']
        if today.day == billing_day:
            end_date = today.shift(days=-1)
            start_date = end_date.replace(day=billing_day).shift(months=-1)
            due_date = today.shift(days=special_billing['due_date_offset'])
        else:
            # If it's not the billing day, return the previous period
            end_date = today.replace(day=billing_day).shift(days=-1)
            start_date = end_date.replace(day=billing_day).shift(months=-1)
            due_date = end_date.shift(days=special_billing['due_date_offset'])
        logging.info(f"Special billing dates for client {client_id}: {start_date} to {end_date}, due {due_date}")
        return start_date, end_date, due_date, "custom"
    else:
        if today.day <= 15:
            # For the first half of the month, bill for the previous month's second half
            end_date = today.replace(day=1).shift(days=-1)
            start_date = end_date.replace(day=16)
        else:
            # For the second half of the month, bill for the current month's first half
            end_date = today.replace(day=15)
            start_date = end_date.replace(day=1)
        logging.info(f"Regular billing dates for client {client_id}: {start_date} to {end_date}, due upon receipt")
        return start_date, end_date, end_date, "upon receipt"


def get_client_ids():
    """Fetch active client IDs from Harvest API."""
    url = "https://api.harvestapp.com/v2/clients"
    try:
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        clients = res.json().get("clients", [])
        active_clients = [client['id'] for client in clients if client["is_active"]]
        logging.info(f"Retrieved {len(active_clients)} active clients")
        return active_clients
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
        project_client_map = {project['id']: project['client']['id'] for project in projects}
        logging.info(f"Retrieved {len(project_client_map)} projects")
        return project_client_map
    except requests.RequestException as e:
        logging.error(f"Error fetching projects: {e}")
        return {}


def check_time_entries_exist(project_id, start_date, end_date):
    """Check if there are time entries for a project within the specified date range."""
    url = "https://api.harvestapp.com/v2/time_entries"
    params = {
        "project_id": project_id,
        "from": start_date.format("YYYY-MM-DD"),
        "to": end_date.format("YYYY-MM-DD")
    }
    try:
        res = requests.get(url, headers=headers, params=params)
        res.raise_for_status()
        time_entries = res.json().get("time_entries", [])
        entry_count = len(time_entries)
        logging.info(f"Found {entry_count} time entries for project {project_id} from {start_date} to {end_date}")
        return entry_count > 0
    except requests.RequestException as e:
        logging.error(f"Error checking time entries for project {project_id}: {e}")
        return False


def create_invoice(client_id, project_id, start_date, end_date, due_date, payment_term):
    """Function to create invoices in Harvest."""
    invoice_url = "https://api.harvestapp.com/v2/invoices"
    payload = {
        "client_id": client_id,
        "notes": "Thank you for choosing ThirstySprout!",
        "payment_term": payment_term,
        "due_date": due_date.format("YYYY-MM-DD"),
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
        invoice_data = res.json()
        logging.info(
            f"Invoice created for client {client_id} and project {project_id}. Invoice ID: {invoice_data.get('id')}")
        logging.info(
            f"Invoice details: Total amount: {invoice_data.get('amount')}, Line items: {len(invoice_data.get('line_items', []))}")
    except requests.RequestException as e:
        logging.error(f"Error creating invoice for client {client_id} and project {project_id}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response content: {e.response.content}")


def process_invoices():
    """Main function to process invoices."""
    client_ids = get_client_ids()
    project_ids = get_project_ids()
    today = arrow.now()

    for client_id in client_ids:
        start_date, end_date, due_date, payment_term = get_billing_dates(client_id, today)
        logging.info(f"Processing billing for client {client_id} from {start_date} to {end_date}")

        special_billing = SPECIAL_BILLING_CLIENTS.get(str(client_id))
        if special_billing:
            logging.info(f"Client {client_id} has special billing configuration")
            for project_id in special_billing['project_ids']:
                if check_time_entries_exist(project_id, start_date, end_date):
                    create_invoice(client_id, project_id, start_date, end_date, due_date, payment_term)
                else:
                    logging.warning(
                        f"No time entries found for special billing client {client_id}, project {project_id}")
        else:
            logging.info(f"Processing regular billing for client {client_id}")
            for project_id, associated_client_id in project_ids.items():
                if associated_client_id == client_id:
                    if check_time_entries_exist(project_id, start_date, end_date):
                        if project_id not in [36506766, 34951635, 39801484]:
                            create_invoice(client_id, project_id, start_date, end_date, due_date, payment_term)
                        else:
                            logging.info(f"Skipping invoice creation for excluded project {project_id}")
                    else:
                        logging.warning(f"No time entries found for client {client_id}, project {project_id}")


def invoicing_trigger(request):
    """Main function to trigger invoicing workflow."""
    logging.info("Invoicing workflow triggered.")
    try:
        process_invoices()
        return "Invoicing workflow executed successfully."
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        logging.error(traceback.format_exc())
        return f"An error occurred: {str(e)}"
