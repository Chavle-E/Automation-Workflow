import os
import logging
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv(dotenv_path='../.env')

# Setup logging
logging.basicConfig(level=logging.INFO)

# Environment variables
SLACK_TOKEN = os.getenv('SLACK_TOKEN')

slack_client = WebClient(token=SLACK_TOKEN)


def post_message_to_slack(event):
    """Function to post a message to Slack."""
    message = "<!channel> Hello everyone! Please don't forget to submit your timesheets"
    try:
        response = slack_client.chat_postMessage(channel="#announcements", text=message)
        logging.info(f"Message posted to #announcements: {response['message']['text']}")
    except SlackApiError as e:
        logging.error(f"Error posting message to Slack: {e.response['error']}")


