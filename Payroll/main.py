import os
import requests
import arrow
import logging
from requests.exceptions import HTTPError, RequestException
from dotenv import load_dotenv
from ratelimit import limits, sleep_and_retry

# Import our new modules
from database import MappingDatabase
from deel_client import DeelClient
from matcher import UserMatcher

# For Cloud Functions, download/upload DB from Cloud Storage
try:
    from cloud_storage_db import CloudStorageDB

    USE_CLOUD_STORAGE = True
except ImportError:
    USE_CLOUD_STORAGE = False

# Load environment variables from .env file
load_dotenv(dotenv_path='../.env')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fetch API keys and IDs from environment variables
DEEL_API_KEY = os.getenv('DEEL_API_KEY')
HARVEST_API_KEY = os.getenv('HARVEST_API_KEY')
HARVEST_ACC_ID = os.getenv('HARVEST_ACCOUNT_ID')
GCS_BUCKET = os.getenv('GCS_BUCKET_NAME', 'your-bucket-name')  # Add to your .env

# Harvest headers
headers_harvest = {
    'Harvest-Account-Id': f"{HARVEST_ACC_ID}",
    'Authorization': f'Bearer {HARVEST_API_KEY}',
    'User-Agent': 'PayrollSync (contact@yourcompany.com)'
}


# Validate environment variables only when actually processing payroll
# (not during import for other Cloud Functions in the same directory)
def validate_env_vars():
    """Validate that all required environment variables are set."""
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
        logging.info(
            f"Fetching Harvest entries from {start_date.format('YYYY-MM-DD')} to {end_date.format('YYYY-MM-DD')}")
        response_harvest = requests.get(url_harvest, headers=headers_harvest, params=params)
        response_harvest.raise_for_status()
        return response_harvest.json()['time_entries']
    except (HTTPError, RequestException) as e:
        logging.error(f"Error fetching Harvest entries: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response status code: {e.response.status_code}")
            logging.error(f"Response content: {e.response.content}")
        return []


@sleep_and_retry
@limits(calls=5, period=1)
def get_harvest_users():
    """Fetch all active Harvest users."""
    users = []
    url = "https://api.harvestapp.com/v2/users"
    params = {"is_active": "true"}

    while url:
        response = requests.get(url, headers=headers_harvest, params=params)
        response.raise_for_status()
        data = response.json()
        users.extend(data["users"])

        # Handle pagination
        url = data.get("links", {}).get("next")
        params = {}  # Clear params for next page

    logging.info(f"Fetched {len(users)} Harvest users")
    return users


def calculate_time_sum(entries):
    """Calculate total hours worked by each person (grouped by user ID)."""
    time_sum_by_user_id = {}
    for entry in entries:
        user_id = entry['user']['id']
        hours = entry['hours']
        time_sum_by_user_id.setdefault(user_id, 0)
        time_sum_by_user_id[user_id] += hours
    return time_sum_by_user_id


def sync_timesheets_to_deel(time_sum_by_user_id, db, deel_client, end_date, auto_match=True):
    """
    Submit timesheets to Deel using stored mappings.

    Args:
        time_sum_by_user_id: Dict of {harvest_user_id: total_hours}
        db: MappingDatabase instance
        deel_client: DeelClient instance
        end_date: Arrow date for timesheet submission
        auto_match: If True, try to auto-match unmapped users
    """
    submitted_count = 0
    failed_count = 0

    # Get all Harvest users for name lookup
    harvest_users = {u['id']: u for u in get_harvest_users()}

    for harvest_user_id, hours in time_sum_by_user_id.items():
        harvest_user = harvest_users.get(harvest_user_id)
        if not harvest_user:
            logging.warning(f"Could not find Harvest user for ID: {harvest_user_id}")
            continue

        user_name = f"{harvest_user.get('first_name', '')} {harvest_user.get('last_name', '')}"

        # Step 1: Try database lookup
        deel_contract_id = db.get_deel_contract_by_harvest_id(harvest_user_id)

        # Step 2: Try Deel external_id lookup as fallback
        if not deel_contract_id:
            logging.info(f"No mapping in DB for {user_name}, trying Deel external_id lookup")
            contract = deel_client.find_contract_by_external_id(harvest_user_id)
            if contract:
                deel_contract_id = contract['id']
                logging.info(f"Found contract via external_id for {user_name}")

                # Save to database for next time
                db.create_mapping(
                    harvest_user_id=harvest_user_id,
                    harvest_email=harvest_user.get('email'),
                    harvest_name=user_name,
                    deel_contract_id=deel_contract_id,
                    deel_email=contract.get('worker', {}).get('email') if contract.get('worker') else None,
                    deel_name=contract.get('title'),
                    match_method='external_id_lookup',
                    confidence_score=1.0,
                    match_signals={'method': 'external_id'},
                    verification_status='auto_matched'
                )

        # Step 3: Try auto-matching if enabled and still no match
        if not deel_contract_id and auto_match:
            logging.info(f"No mapping found for {user_name}, attempting auto-match")
            deel_contracts = deel_client.get_all_contracts()
            matcher = UserMatcher(auto_accept_threshold=0.90, review_threshold=0.70)
            match_result = matcher.find_best_match(harvest_user, deel_contracts)

            if match_result and match_result.decision == 'auto_accept':
                matched_contract = next(
                    (c for c in deel_contracts if c['id'] == match_result.deel_contract_id),
                    None
                )

                if matched_contract:
                    deel_contract_id = matched_contract['id']
                    logging.info(
                        f"Auto-matched {user_name} to {matched_contract['title']} ({match_result.confidence:.2%})")

                    # Save mapping
                    db.create_mapping(
                        harvest_user_id=harvest_user_id,
                        harvest_email=harvest_user.get('email'),
                        harvest_name=user_name,
                        deel_contract_id=deel_contract_id,
                        deel_email=matched_contract.get('worker', {}).get('email') if matched_contract.get(
                            'worker') else None,
                        deel_name=matched_contract.get('title'),
                        match_method='auto_match',
                        confidence_score=match_result.confidence,
                        match_signals=match_result.signals,
                        verification_status='auto_matched'
                    )

                    # Set external_id
                    deel_client.set_external_id(deel_contract_id, harvest_user_id)

        # Step 4: Submit timesheet if we have a contract
        if deel_contract_id:
            success = deel_client.submit_timesheet(
                contract_id=deel_contract_id,
                hours=hours,
                date=end_date.format('YYYY-MM-DD'),
                description="Uploaded from Harvest"
            )

            if success:
                logging.info(f"✅ Submitted {hours}h for {user_name}")
                submitted_count += 1
            else:
                logging.error(f"❌ Failed to submit timesheet for {user_name}")
                failed_count += 1
        else:
            logging.error(f"❌ No Deel contract found for {user_name} (Harvest ID: {harvest_user_id})")
            failed_count += 1

    logging.info(f"\nSummary: {submitted_count} submitted, {failed_count} failed")
    return submitted_count, failed_count


def process_payroll(dry_run=False, use_cloud_storage=None):
    """Main function to process payroll."""
    # Validate environment variables
    validate_env_vars()

    # Setup database (with Cloud Storage support)
    # For local testing, force use_cloud_storage=False
    if use_cloud_storage is None:
        use_cloud_storage = USE_CLOUD_STORAGE

    if use_cloud_storage:
        storage_db = CloudStorageDB(GCS_BUCKET)
        storage_db.download_db()
        db = MappingDatabase(db_path=storage_db.get_db_path())
    else:
        db = MappingDatabase()

    # Initialize Deel client
    deel = DeelClient(DEEL_API_KEY)

    # Get date range
    start_date, end_date = get_previous_semi_month_dates()
    logging.info(f"Processing payroll for period: {start_date.format('YYYY-MM-DD')} to {end_date.format('YYYY-MM-DD')}")

    # Fetch Harvest time entries
    entries = fetch_harvest_entries(start_date, end_date)

    if not entries:
        logging.warning("No time entries fetched from Harvest")
        return

    # Calculate hours by user
    time_sum_by_user_id = calculate_time_sum(entries)
    logging.info(f"Time entries for {len(time_sum_by_user_id)} users")

    if dry_run:
        logging.info("DRY RUN MODE - Timesheets will not be submitted")
        for user_id, hours in time_sum_by_user_id.items():
            logging.info(f"Would submit {hours}h for user ID {user_id}")
    else:
        # Sync to Deel
        sync_timesheets_to_deel(time_sum_by_user_id, db, deel, end_date, auto_match=True)

    # Upload database back to Cloud Storage
    if use_cloud_storage:
        storage_db.upload_db()


def payroll_trigger(request):
    """Cloud Function entry point for payroll."""
    logging.info("Payroll workflow triggered.")
    try:
        process_payroll(dry_run=False)
        return "Payroll workflow executed successfully."
    except Exception as e:
        logging.error(f"Error in payroll workflow: {e}")
        return f"Error: {str(e)}", 500


if __name__ == "__main__":
    # Import reminder function for Cloud Functions deployment
    try:
        from Payroll_Reminders.main import reminder_trigger
    except ImportError:
        # If slack_reminders can't be imported, provide a stub
        def reminder_trigger(request):
            return "Reminder function not available", 500

    # For local testing, run in dry-run mode (without Cloud Storage)
    process_payroll(dry_run=True, use_cloud_storage=False)

    # Uncomment to run for real locally:
    # process_payroll(dry_run=False, use_cloud_storage=False)
