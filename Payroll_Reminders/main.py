import os
import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path='../.env')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_env_vars():
    """Get and validate environment variables."""
    slack_token = os.getenv('SLACK_TOKEN')
    harvest_api_key = os.getenv('HARVEST_API_KEY')
    harvest_acc_id = os.getenv('HARVEST_ACCOUNT_ID')

    if not all([slack_token, harvest_api_key, harvest_acc_id]):
        raise EnvironmentError(
            "Missing required environment variables: SLACK_TOKEN, HARVEST_API_KEY, HARVEST_ACCOUNT_ID")

    return slack_token, harvest_api_key, harvest_acc_id


def get_users_with_missing_timesheets(days=7, min_hours=32):
    """
    Find users who haven't logged enough hours in Harvest.

    Args:
        days: Number of days to look back (default: 7)
        min_hours: Minimum expected hours (default: 32 for a typical week)

    Returns:
        List of users with missing timesheets
    """
    _, harvest_api_key, harvest_acc_id = get_env_vars()

    harvest_headers = {
        'Harvest-Account-Id': harvest_acc_id,
        'Authorization': f'Bearer {harvest_api_key}',
        'User-Agent': 'TimesheetReminder (contact@yourcompany.com)'
    }

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    # Get all active users
    users_url = "https://api.harvestapp.com/v2/users?is_active=true"
    users_resp = requests.get(users_url, headers=harvest_headers)
    users_resp.raise_for_status()
    all_users = users_resp.json()['users']

    logging.info(f"Fetched {len(all_users)} active Harvest users")

    # Get time report for the period
    report_url = "https://api.harvestapp.com/v2/reports/time/team"
    params = {
        "from": start_date.strftime("%Y%m%d"),
        "to": end_date.strftime("%Y%m%d")
    }
    report_resp = requests.get(report_url, headers=harvest_headers, params=params)
    report_resp.raise_for_status()

    # Build hours by user ID
    hours_by_user = {}
    for result in report_resp.json().get('results', []):
        hours_by_user[result['user_id']] = result['total_hours']

    logging.info(f"Found time entries for {len(hours_by_user)} users")

    # Find users with insufficient hours
    missing = []
    for user in all_users:
        hours = hours_by_user.get(user['id'], 0)
        if hours < min_hours:
            missing.append({
                'user_id': user['id'],
                'email': user['email'],
                'name': f"{user['first_name']} {user['last_name']}",
                'hours_logged': hours,
                'hours_expected': min_hours
            })

    return missing


def find_slack_user(email, name):
    """
    Find Slack user by email, with fallback to name search.

    Args:
        email: User's email address
        name: User's full name

    Returns:
        Slack user ID or None
    """
    slack_token, _, _ = get_env_vars()
    slack = WebClient(token=slack_token)

    # Try email lookup first
    try:
        user_lookup = slack.users_lookupByEmail(email=email)
        return user_lookup['user']['id']
    except SlackApiError as e:
        if e.response['error'] == 'users_not_found':
            logging.warning(f"No Slack user found for email: {email}")

            # Fallback: try name search
            try:
                users_list = slack.users_list()
                for member in users_list['members']:
                    if member.get('deleted') or member.get('is_bot'):
                        continue

                    profile = member.get('profile', {})
                    real_name = profile.get('real_name', '').lower()
                    display_name = profile.get('display_name', '').lower()

                    if name.lower() in real_name or name.lower() in display_name:
                        logging.info(f"Found Slack user by name match: {name} -> {real_name}")
                        return member['id']
            except SlackApiError as e2:
                logging.error(f"Error searching Slack users: {e2.response['error']}")
        else:
            logging.error(f"Slack API error: {e.response['error']}")

    return None


def send_slack_dm(user_id, message, hours_logged, hours_expected):
    """
    Send DM reminder to a Slack user.

    Args:
        user_id: Slack user ID
        message: Text message to send
        hours_logged: Hours the user has logged
        hours_expected: Hours they should have logged
    """
    slack_token, _, _ = get_env_vars()
    slack = WebClient(token=slack_token)

    try:
        slack.chat_postMessage(
            channel=user_id,
            text=message,
            blocks=[
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "⏰ Timesheet Reminder"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": message
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Hours Logged:*\n{hours_logged:.1f} hours"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Expected:*\n{hours_expected} hours"
                        }
                    ]
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "📝 Submit Timesheet"},
                            "url": "https://thirstysprout.harvestapp.com",
                            "style": "primary"
                        }
                    ]
                }
            ]
        )
        logging.info(f"✓ Sent reminder to user {user_id}")
        return True
    except SlackApiError as e:
        logging.error(f"✗ Error sending message to {user_id}: {e.response['error']}")
        return False


def send_reminders(dry_run=False, days=7, min_hours=32):
    """
    Main function to send timesheet reminders.

    Args:
        dry_run: If True, only show who would get reminders without sending
        days: Number of days to check
        min_hours: Minimum expected hours
    """
    logging.info(f"Checking for missing timesheets (last {days} days, min {min_hours} hours)")

    # Get users with missing timesheets
    missing = get_users_with_missing_timesheets(days=days, min_hours=min_hours)

    if not missing:
        logging.info("✓ No missing timesheets - everyone is up to date!")
        return

    logging.info(f"Found {len(missing)} users with insufficient hours")

    sent_count = 0
    failed_count = 0

    for user in missing:
        message = (
            f"Hi! You've logged *{user['hours_logged']:.1f} hours* this week. "
            f"Please submit your remaining hours in Harvest to reach the expected *{user['hours_expected']} hours*. "
            f"Thanks! 🙏"
        )

        if dry_run:
            logging.info(
                f"[DRY RUN] Would send to: {user['name']} ({user['email']}) - {user['hours_logged']:.1f}h logged")
            continue

        # Find Slack user
        slack_user_id = find_slack_user(user['email'], user['name'])

        if slack_user_id:
            success = send_slack_dm(
                slack_user_id,
                message,
                user['hours_logged'],
                user['hours_expected']
            )
            if success:
                sent_count += 1
            else:
                failed_count += 1
        else:
            logging.warning(f"✗ Could not find Slack user for: {user['name']} ({user['email']})")
            failed_count += 1

    logging.info(f"\nSummary: {sent_count} reminders sent, {failed_count} failed")


def reminder_trigger(request):
    """Cloud Function entry point for timesheet reminders."""
    logging.info("Timesheet reminder workflow triggered.")
    try:
        send_reminders(dry_run=False, days=7, min_hours=32)
        return "Reminders sent successfully."
    except Exception as e:
        logging.error(f"Error in reminder workflow: {e}")
        return f"Error: {str(e)}", 500


if __name__ == "__main__":
    # Run in dry-run mode first to see who would get reminders
    send_reminders(dry_run=True)

    # After reviewing, uncomment to actually send:
    # send_reminders(dry_run=False)
