import os
import logging
import datetime
import calendar
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
from flask import escape, request

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)

# Environment variables
SLACK_TOKEN = os.getenv('SLACK_TOKEN')

slack_client = WebClient(token=SLACK_TOKEN)


def is_last_three_days_of_month():
    today = datetime.datetime.utcnow().date()
    _, last_day = calendar.monthrange(today.year, today.month)
    return today.day in [last_day, last_day - 1, last_day - 2]


def post_message_to_slack(request):
    """HTTP Cloud Function to post a message to Slack on 13th, 14th, 15th, and last three days of the month."""
    today = datetime.datetime.utcnow().day

    if today in [13, 14, 15] or is_last_three_days_of_month():
        message = "<!channel> Hello everyone! Please don't forget to submit your timesheets"

        try:
            response = slack_client.chat_postMessage(channel="#announcements", text=message)
            logging.info(f"Message posted to #announcements: {response['message']['text']}")
            return escape(f"Message posted to #announcements: {response['message']['text']}")
        except SlackApiError as e:
            logging.error(f"Error posting message to Slack: {e.response['error']}")
            return escape(f"Error posting message to Slack: {e.response['error']}")
    else:
        return escape("Not a scheduled day for posting.")
