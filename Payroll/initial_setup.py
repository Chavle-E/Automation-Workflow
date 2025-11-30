import os
import logging
import requests
from dotenv import load_dotenv
from database import MappingDatabase
from matcher import UserMatcher
from deel_client import DeelClient

# Load environment variables
load_dotenv(dotenv_path='../.env')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
DEEL_API_KEY = os.getenv('DEEL_API_KEY')
HARVEST_API_KEY = os.getenv('HARVEST_API_KEY')
HARVEST_ACC_ID = os.getenv('HARVEST_ACCOUNT_ID')

# Harvest headers
harvest_headers = {
    'Harvest-Account-Id': HARVEST_ACC_ID,
    'Authorization': f'Bearer {HARVEST_API_KEY}',
    'User-Agent': 'TimesheetSync (contact@yourcompany.com)'
}


def get_harvest_users():
    """Fetch all active Harvest users."""
    users = []
    url = "https://api.harvestapp.com/v2/users"
    params = {"is_active": "true"}

    while url:
        response = requests.get(url, headers=harvest_headers, params=params)
        response.raise_for_status()
        data = response.json()
        users.extend(data["users"])

        # Handle pagination
        url = data.get("links", {}).get("next")
        params = {}  # Clear params for next page (already in URL)

    logging.info(f"Fetched {len(users)} Harvest users")
    return users


def initial_bulk_match(dry_run=True):
    """
    Match all existing Harvest users to Deel contracts.

    Args:
        dry_run: If True, only show what would be matched without saving
    """
    logging.info("Starting initial bulk matching...")

    # Initialize components
    db = MappingDatabase()
    matcher = UserMatcher(auto_accept_threshold=0.80, review_threshold=0.65)
    deel = DeelClient(DEEL_API_KEY)

    # Fetch data from both systems
    harvest_users = get_harvest_users()
    deel_contracts = deel.get_all_contracts()

    logging.info(f"Matching {len(harvest_users)} Harvest users against {len(deel_contracts)} Deel contracts")

    # Track results
    auto_matched = []
    needs_review = []
    no_match = []

    for h_user in harvest_users:
        # Skip non-contractors if you only want contractors
        # if not h_user.get('is_contractor'):
        #     continue

        # Find best match
        match_result = matcher.find_best_match(h_user, deel_contracts)

        if not match_result:
            no_match.append(h_user)
            logging.warning(f"No match found for: {h_user['first_name']} {h_user['last_name']} ({h_user['email']})")
            continue

        # Get the matched Deel contract details
        matched_contract = next(
            (c for c in deel_contracts if c['id'] == match_result.deel_contract_id),
            None
        )

        if match_result.decision == 'auto_accept':
            auto_matched.append({
                'harvest_user': h_user,
                'deel_contract': matched_contract,
                'match': match_result
            })
            logging.info(
                f"✓ AUTO-MATCH: {h_user['first_name']} {h_user['last_name']} → "
                f"{matched_contract['title']} (confidence: {match_result.confidence:.2%})"
            )

            if not dry_run:
                # Save to database
                db.create_mapping(
                    harvest_user_id=h_user['id'],
                    harvest_email=h_user.get('email'),
                    harvest_name=f"{h_user['first_name']} {h_user['last_name']}",
                    deel_contract_id=matched_contract['id'],
                    deel_email=matched_contract.get('worker', {}).get('email') if matched_contract.get('worker') else None,
                    deel_name=matched_contract['title'],
                    match_method='auto_match',
                    confidence_score=match_result.confidence,
                    match_signals=match_result.signals,
                    verification_status='auto_matched'
                )

                # Set external_id in Deel
                deel.set_external_id(matched_contract['id'], h_user['id'])

        elif match_result.decision == 'needs_review':
            needs_review.append({
                'harvest_user': h_user,
                'deel_contract': matched_contract,
                'match': match_result
            })
            logging.warning(
                f"? NEEDS REVIEW: {h_user['first_name']} {h_user['last_name']} → "
                f"{matched_contract['title']} (confidence: {match_result.confidence:.2%})"
            )

            if not dry_run:
                # Save to database with needs_review status
                db.create_mapping(
                    harvest_user_id=h_user['id'],
                    harvest_email=h_user.get('email'),
                    harvest_name=f"{h_user['first_name']} {h_user['last_name']}",
                    deel_contract_id=matched_contract['id'],
                    deel_email=matched_contract.get('worker', {}).get('expected_email'),
                    deel_name=matched_contract['title'],
                    match_method='fuzzy_match',
                    confidence_score=match_result.confidence,
                    match_signals=match_result.signals,
                    verification_status='needs_review'
                )

    # Print summary
    print("\n" + "=" * 80)
    print("BULK MATCHING SUMMARY")
    print("=" * 80)
    print(f"Total Harvest users: {len(harvest_users)}")
    print(f"Total Deel contracts: {len(deel_contracts)}")
    print(f"\nAuto-matched (≥90% confidence): {len(auto_matched)}")
    print(f"Needs review (70-90% confidence): {len(needs_review)}")
    print(f"No match found (<70% confidence): {len(no_match)}")

    if needs_review:
        print("\n" + "-" * 80)
        print("MATCHES NEEDING REVIEW:")
        print("-" * 80)
        for item in needs_review:
            h = item['harvest_user']
            d = item['deel_contract']
            m = item['match']
            print(f"\nHarvest: {h['first_name']} {h['last_name']} ({h['email']})")
            print(f"Deel:    {d['title']} ({d.get('worker', {}).get('expected_email', 'N/A')})")
            print(f"Confidence: {m.confidence:.2%}")
            print(f"Signals: {m.signals}")

    if dry_run:
        print("\n" + "=" * 80)
        print("DRY RUN MODE - No changes were saved")
        print("Run with dry_run=False to save these mappings")
        print("=" * 80)


if __name__ == "__main__":
    # Run in dry-run mode first to review matches
    # initial_bulk_match(dry_run=True)

    # After reviewing, uncomment to actually save:
    initial_bulk_match(dry_run=False)
