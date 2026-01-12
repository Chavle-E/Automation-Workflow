import os
import logging
from dotenv import load_dotenv

load_dotenv(dotenv_path='../.env')

from database import MappingDatabase
from deel_client import DeelClient
from matcher import UserMatcher
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import requests

# For Cloud Functions, download/upload DB from Cloud Storage
try:
    from cloud_storage_db import CloudStorageDB

    USE_CLOUD_STORAGE = True
except ImportError:
    USE_CLOUD_STORAGE = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DEEL_API_KEY = os.getenv('DEEL_API_KEY')
HARVEST_API_KEY = os.getenv('HARVEST_API_KEY')
HARVEST_ACC_ID = os.getenv('HARVEST_ACCOUNT_ID')
SLACK_TOKEN = os.getenv('SLACK_TOKEN')
GCS_BUCKET = os.getenv('GCS_BUCKET_NAME', 'your-bucket-name')

# Your Slack notification settings
NOTIFY_SLACK_USER = "Guga Chavleshvili"

headers_harvest = {
    'Harvest-Account-Id': HARVEST_ACC_ID,
    'Authorization': f'Bearer {HARVEST_API_KEY}',
    'User-Agent': 'MappingSync'
}


def get_all_harvest_users():
    """Fetch all active Harvest users."""
    users = []
    url = "https://api.harvestapp.com/v2/users"
    params = {"is_active": "true"}

    while url:
        response = requests.get(url, headers=headers_harvest, params=params)
        response.raise_for_status()
        data = response.json()
        users.extend(data["users"])
        url = data.get("links", {}).get("next")
        params = {}

    return users


def find_slack_user_by_name(name):
    """Find Slack user ID by name."""
    if not SLACK_TOKEN:
        logging.warning("SLACK_TOKEN not set, cannot send notifications")
        return None

    slack = WebClient(token=SLACK_TOKEN)

    try:
        users_list = slack.users_list()
        for member in users_list['members']:
            if member.get('deleted') or member.get('is_bot'):
                continue
            profile = member.get('profile', {})
            real_name = profile.get('real_name', '').lower()
            display_name = profile.get('display_name', '').lower()

            if name.lower() in real_name or name.lower() in display_name:
                logging.info(f"Found Slack user: {real_name} ({member['id']})")
                return member['id']
    except SlackApiError as e:
        logging.error(f"Error finding Slack user: {e.response['error']}")

    return None


def send_slack_report(results, dry_run=False):
    """Send mapping sync results to Slack DM."""
    if not SLACK_TOKEN:
        logging.warning("SLACK_TOKEN not set, skipping Slack notification")
        return False

    slack = WebClient(token=SLACK_TOKEN)
    user_id = find_slack_user_by_name(NOTIFY_SLACK_USER)

    if not user_id:
        logging.error(f"Could not find Slack user: {NOTIFY_SLACK_USER}")
        return False

    # Build the message blocks
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{'🧪 DRY RUN: ' if dry_run else ''}📊 Harvest → Deel Mapping Sync"
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Already Mapped:*\n{len(results['already_mapped'])}"},
                {"type": "mrkdwn", "text": f"*Auto-Matched:*\n{len(results['auto_matched'])}"},
                {"type": "mrkdwn", "text": f"*Needs Review:*\n{len(results['needs_review'])}"},
                {"type": "mrkdwn", "text": f"*No Match:*\n{len(results['no_match'])}"}
            ]
        }
    ]

    # Add auto-matched section if any
    if results['auto_matched']:
        matched_list = "\n".join([f"• {name}" for name in results['auto_matched'][:10]])
        if len(results['auto_matched']) > 10:
            matched_list += f"\n_...and {len(results['auto_matched']) - 10} more_"

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*✅ Auto-Matched:*\n{matched_list}"}
        })

    # Add needs review section if any
    if results['needs_review']:
        review_list = "\n".join([
            f"• {item['harvest']} → {item['deel']} ({item['confidence']:.0%})"
            for item in results['needs_review'][:10]
        ])
        if len(results['needs_review']) > 10:
            review_list += f"\n_...and {len(results['needs_review']) - 10} more_"

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*⚠️ Needs Manual Review:*\n{review_list}"}
        })
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "_Run `python review_matches.py` to approve/reject these matches_"}
        })

    # Add no match section if any
    if results['no_match']:
        no_match_list = "\n".join([
            f"• {item['name']} ({item['email']})"
            for item in results['no_match'][:10]
        ])
        if len(results['no_match']) > 10:
            no_match_list += f"\n_...and {len(results['no_match']) - 10} more_"

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*❌ No Match Found (check Deel contracts):*\n{no_match_list}"}
        })

    # Add success message if everything is good
    if not results['needs_review'] and not results['no_match']:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "✨ *All users are mapped!* Payroll sync should work smoothly."}
        })

    # Send the message
    try:
        slack.chat_postMessage(
            channel=user_id,
            text="Harvest → Deel Mapping Sync Report",
            blocks=blocks
        )
        logging.info(f"✅ Sent Slack report to {NOTIFY_SLACK_USER}")
        return True
    except SlackApiError as e:
        logging.error(f"Error sending Slack message: {e.response['error']}")
        return False


def sync_user_mappings(dry_run=False, send_slack=True, use_cloud_storage=None):
    """
    Sync all Harvest users to Deel contracts.
    Run this BEFORE payroll to ensure everyone is mapped.
    """
    # Setup database (with Cloud Storage support)
    if use_cloud_storage is None:
        use_cloud_storage = USE_CLOUD_STORAGE

    if use_cloud_storage:
        storage_db = CloudStorageDB(GCS_BUCKET)
        storage_db.download_db()
        db = MappingDatabase(db_path=storage_db.get_db_path())
    else:
        db = MappingDatabase()

    deel = DeelClient(DEEL_API_KEY)
    matcher = UserMatcher(auto_accept_threshold=0.85, review_threshold=0.60)

    # Fetch all data
    harvest_users = get_all_harvest_users()
    deel_contracts = deel.get_all_contracts()

    logging.info(f"Found {len(harvest_users)} Harvest users and {len(deel_contracts)} Deel contracts")

    # Track results
    results = {
        'already_mapped': [],
        'auto_matched': [],
        'needs_review': [],
        'no_match': []
    }

    for user in harvest_users:
        user_id = str(user['id'])
        user_name = f"{user.get('first_name', '')} {user.get('last_name', '')}"
        user_email = user.get('email', '')

        # Step 1: Check if already mapped
        existing = db.get_deel_contract_by_harvest_id(user_id)
        if existing:
            results['already_mapped'].append(user_name)
            continue

        # Step 2: Check Deel external_id
        contract = deel.find_contract_by_external_id(user_id)
        if contract:
            logging.info(f"Found existing external_id link for {user_name}")
            if not dry_run:
                db.create_mapping(
                    harvest_user_id=user_id,
                    harvest_email=user_email,
                    harvest_name=user_name,
                    deel_contract_id=contract['id'],
                    deel_email=contract.get('worker', {}).get('email'),
                    deel_name=contract.get('title'),
                    match_method='external_id_lookup',
                    confidence_score=1.0,
                    match_signals={'method': 'external_id'},
                    verification_status='auto_matched'
                )
            results['auto_matched'].append(user_name)
            continue

        # Step 3: Try auto-matching
        match_result = matcher.find_best_match(user, deel_contracts)

        if not match_result:
            logging.warning(f"❌ No match found for {user_name} ({user_email})")
            results['no_match'].append({'name': user_name, 'email': user_email})
            continue

        matched_contract = next(
            (c for c in deel_contracts if c['id'] == match_result.deel_contract_id),
            None
        )

        if not matched_contract:
            results['no_match'].append({'name': user_name, 'email': user_email})
            continue

        deel_name = matched_contract.get('title', 'Unknown')
        deel_email = matched_contract.get('worker', {}).get('email', 'Unknown')

        if match_result.decision == 'auto_accept':
            logging.info(f"✅ Auto-matched: {user_name} → {deel_name} ({match_result.confidence:.0%})")

            if not dry_run:
                db.create_mapping(
                    harvest_user_id=user_id,
                    harvest_email=user_email,
                    harvest_name=user_name,
                    deel_contract_id=match_result.deel_contract_id,
                    deel_email=deel_email,
                    deel_name=deel_name,
                    match_method='auto_match',
                    confidence_score=match_result.confidence,
                    match_signals=match_result.signals,
                    verification_status='auto_matched'
                )
                deel.set_external_id(match_result.deel_contract_id, user_id)

            results['auto_matched'].append(user_name)

        elif match_result.decision == 'needs_review':
            logging.info(f"⚠️ Needs review: {user_name} → {deel_name} ({match_result.confidence:.0%})")

            if not dry_run:
                db.create_mapping(
                    harvest_user_id=user_id,
                    harvest_email=user_email,
                    harvest_name=user_name,
                    deel_contract_id=match_result.deel_contract_id,
                    deel_email=deel_email,
                    deel_name=deel_name,
                    match_method='auto_match',
                    confidence_score=match_result.confidence,
                    match_signals=match_result.signals,
                    verification_status='needs_review'
                )

            results['needs_review'].append({
                'harvest': user_name,
                'deel': deel_name,
                'confidence': match_result.confidence
            })
        else:
            results['no_match'].append({'name': user_name, 'email': user_email})

    # Print summary to logs
    print("\n" + "=" * 60)
    print("MAPPING SYNC SUMMARY")
    print("=" * 60)
    print(f"\n✅ Already mapped: {len(results['already_mapped'])}")
    print(f"✅ Auto-matched:   {len(results['auto_matched'])}")
    print(f"⚠️  Needs review:   {len(results['needs_review'])}")
    print(f"❌ No match:       {len(results['no_match'])}")

    if results['needs_review']:
        print("\n⚠️  NEEDS MANUAL REVIEW:")
        for item in results['needs_review']:
            print(f"   {item['harvest']} → {item['deel']} ({item['confidence']:.0%})")

    if results['no_match']:
        print("\n❌ NO MATCH FOUND (check Deel contracts exist):")
        for item in results['no_match']:
            print(f"   {item['name']} ({item['email']})")

    print("\n" + "=" * 60)

    # Upload database back to Cloud Storage
    if use_cloud_storage:
        storage_db.upload_db()

    # Send Slack notification
    if send_slack:
        send_slack_report(results, dry_run=dry_run)

    return results


def mapping_sync_trigger(request):
    """Cloud Function entry point for mapping sync."""
    logging.info("Mapping sync triggered.")
    try:
        results = sync_user_mappings(dry_run=False, send_slack=True, use_cloud_storage=True)
        return f"Sync complete. Auto-matched: {len(results['auto_matched'])}, Needs review: {len(results['needs_review'])}, No match: {len(results['no_match'])}"
    except Exception as e:
        logging.error(f"Error in mapping sync: {e}")
        return f"Error: {str(e)}", 500


if __name__ == "__main__":
    # Run locally - uses local user_mappings.db (not Cloud Storage)
    # dry_run=True to test without making changes
    # send_slack=True to get the DM
    sync_user_mappings(dry_run=False, send_slack=True, use_cloud_storage=False)