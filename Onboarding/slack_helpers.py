"""
Slack side of provisioning: workspace invites + notifying the humans.

Invite reality check: programmatic workspace invites (`admin.users.invite`) only
work with an org-admin token on paid/Grid plans. We TRY the API with whatever
token is configured and, if Slack refuses (missing_scope / not_allowed / free
plan), fall back to DMing the operators to send the invite by hand — provisioning
must degrade to notify-a-human, never fail silently (David's decision).

Notifications go as DMs to the operators named in ONBOARDING_NOTIFY_USERS
(comma-separated Slack real/display names; defaults to Guga — add David's Slack
name via the env var once confirmed). Name lookup mirrors Payroll/sync_mappings.py.
Non-sensitive notifications are ALSO posted to ONBOARDING_NOTIFY_CHANNEL
(default #onboarding); messages carrying credentials (the Zoho one-time
password) are DM-only via sensitive=True.
"""
import logging
import os

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# David's confirmed default-channel set for new contractors.
DEFAULT_CHANNELS = ("announcements", "thirstysprout-projects-and-off-topic-stuff")

NOTIFY_USERS = [n.strip() for n in
                os.getenv("ONBOARDING_NOTIFY_USERS", "Guga Chavleshvili").split(",") if n.strip()]
NOTIFY_CHANNEL = os.getenv("ONBOARDING_NOTIFY_CHANNEL", "onboarding").strip().lstrip("#")


def _find_user_id_by_name(slack: WebClient, name: str):
    try:
        for member in slack.users_list()["members"]:
            if member.get("deleted") or member.get("is_bot"):
                continue
            profile = member.get("profile", {})
            if (name.lower() in profile.get("real_name", "").lower()
                    or name.lower() in profile.get("display_name", "").lower()):
                return member["id"]
    except SlackApiError as e:
        logging.error(f"Slack user lookup failed: {e.response['error']}")
    return None


def notify_operators(slack: WebClient, text: str, sensitive: bool = False) -> bool:
    """DM every configured operator; non-sensitive messages also go to the
    notify channel. Returns True if at least one message went out."""
    sent = False
    for name in NOTIFY_USERS:
        user_id = _find_user_id_by_name(slack, name)
        if not user_id:
            logging.error(f"Slack operator not found: {name!r}")
            continue
        try:
            slack.chat_postMessage(channel=user_id, text=text)
            sent = True
        except SlackApiError as e:
            logging.error(f"DM to {name} failed: {e.response['error']}")
    if not sensitive and NOTIFY_CHANNEL:
        sent = _post_to_channel(slack, NOTIFY_CHANNEL, text) or sent
    if not sent:
        logging.error(f"NO operator notified — message was: {text}")
    return sent


def _post_to_channel(slack: WebClient, channel_name: str, text: str) -> bool:
    # Post by NAME first: it works for private channels the bot is a member of
    # (#onboarding is private, so the public-only conversations_list lookup
    # can't see it — this is how backfill's notifications post too).
    try:
        slack.chat_postMessage(channel=channel_name, text=text)
        return True
    except SlackApiError as e:
        if e.response.get("error") != "channel_not_found":
            logging.error(f"Post to #{channel_name} failed: {e.response['error']}")
            return False
    ids = _channel_ids(slack, [channel_name])
    if not ids:
        logging.error(f"Notify channel #{channel_name} not found")
        return False
    try:
        slack.chat_postMessage(channel=ids[0], text=text)
        return True
    except SlackApiError as e:
        if e.response.get("error") == "not_in_channel":
            try:
                slack.conversations_join(channel=ids[0])
                slack.chat_postMessage(channel=ids[0], text=text)
                return True
            except SlackApiError as e2:
                logging.error(f"Post to #{channel_name} failed after join: {e2.response['error']}")
        else:
            logging.error(f"Post to #{channel_name} failed: {e.response['error']}")
    return False


def _channel_ids(slack: WebClient, names):
    wanted = {n.lstrip("#").lower() for n in names}
    found = {}
    cursor = None
    try:
        while True:
            resp = slack.conversations_list(types="public_channel", limit=200, cursor=cursor)
            for ch in resp["channels"]:
                if ch["name"].lower() in wanted:
                    found[ch["name"].lower()] = ch["id"]
            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor or len(found) == len(wanted):
                break
    except SlackApiError as e:
        logging.error(f"conversations_list failed: {e.response['error']}")
    return list(found.values())


def invite_to_workspace(slack: WebClient, email: str, channels=DEFAULT_CHANNELS):
    """
    Try the admin invite API. Returns (invited: bool, detail: str) — invited=False
    means the caller must fall back to a manual-invite notification.
    """
    try:
        team_id = slack.team_info()["team"]["id"]
        channel_ids = _channel_ids(slack, channels)
        if not channel_ids:
            return False, "default channels not found via API"
        slack.admin_users_invite(team_id=team_id, email=email,
                                 channel_ids=",".join(channel_ids))
        logging.info(f"Slack workspace invite sent to {email}")
        return True, "invited via admin.users.invite"
    except SlackApiError as e:
        error = e.response.get("error", str(e))
        logging.warning(f"Slack API invite unavailable for {email}: {error}")
        return False, f"api_invite_failed:{error}"
