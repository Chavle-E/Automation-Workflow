"""
Onboarding digest — posts the "still onboarding" list to the private #onboarding
Slack channel so you and David have visibility in one place.

Team-facing only (the channel is private). Contractor-facing nudges are handled
separately — Deel already emails onboarding reminders natively, and overdue cases
(like a 5-week-stale onboarding) should get a human eyeball before any "no pay"
message goes out.

Meant to run on a DAILY schedule, not every poll, to avoid noise.
Dry-run by default; pass --post to actually send.
"""
import os
import sys
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from onboarding_poll import fetch_all_people, select_candidates

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SLACK_TOKEN = os.getenv("SLACK_TOKEN")
# Prefer the channel ID (bulletproof for private channels). Falls back to the name.
ONBOARDING_CHANNEL = os.getenv("ONBOARDING_CHANNEL", "#onboarding")


def _days_until(start_str):
    """Negative = started N days ago (overdue); positive = starts in N days."""
    try:
        start = datetime.fromisoformat(start_str.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        return None
    return (start - datetime.now(timezone.utc).date()).days


def _line(profile):
    name = f"{profile['first_name']} {profile['last_name']}"
    d = _days_until(profile["start_date"])
    if d is None:
        when = "start date unknown"
    elif d > 0:
        when = f"starts in {d}d"
    elif d == 0:
        when = "starts today"
    else:
        when = f"⚠️ OVERDUE — started {abs(d)}d ago"
    return f"• *{name}* — {when}  _(start {profile['start_date']})_"


def build_blocks(to_remind):
    if not to_remind:
        return [{"type": "section",
                 "text": {"type": "mrkdwn", "text": "✅ Nobody is mid-onboarding right now."}}]
    ordered = sorted(to_remind, key=lambda pr: (_days_until(pr[0]["start_date"]) or 0))
    lines = "\n".join(_line(p) for p, _ in ordered)
    return [
        {"type": "header",
         "text": {"type": "plain_text", "text": f"🚀 Onboarding in progress ({len(to_remind)})"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": lines}},
        {"type": "context", "elements": [{"type": "mrkdwn",
            "text": "Haven't completed Deel onboarding yet — *not* provisioned. "
                    "Overdue ones may need a personal nudge."}]},
    ]


def _resolve_channel(slack, channel):
    """Best-effort: turn a #name into a channel ID. Returns the input on failure."""
    if not channel.startswith("#"):
        return channel
    target = channel.lstrip("#")
    cursor = None
    try:
        while True:
            resp = slack.conversations_list(
                types="public_channel,private_channel", limit=200, cursor=cursor)
            for ch in resp["channels"]:
                if ch["name"] == target:
                    return ch["id"]
            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
    except SlackApiError as e:
        logging.warning(f"Couldn't list channels ({e.response['error']}); posting by name. "
                        f"If that fails, set ONBOARDING_CHANNEL to the channel ID.")
    return channel


def post_digest(to_remind, channel=ONBOARDING_CHANNEL, dry_run=True):
    if dry_run:
        print(f"[DRY RUN] Would post to {channel}:")
        for p, _ in to_remind:
            print("  ", _line(p))
        return

    if not SLACK_TOKEN:
        raise SystemExit("SLACK_TOKEN not set (.env)")
    slack = WebClient(token=SLACK_TOKEN)
    chan = _resolve_channel(slack, channel)

    try:
        slack.chat_postMessage(channel=chan, text="Onboarding in progress",
                               blocks=build_blocks(to_remind))
        logging.info(f"Posted onboarding digest to {channel}")
    except SlackApiError as e:
        err = e.response["error"]
        if err in ("not_in_channel", "channel_not_found"):
            logging.error(f"Slack '{err}': invite the bot to {channel} "
                          f"(type '/invite @yourbot' in the channel), or use the channel ID.")
        else:
            logging.error(f"Slack error: {err}")


if __name__ == "__main__":
    people = fetch_all_people()
    _, to_remind = select_candidates(people)
    post_digest(to_remind, dry_run="--post" not in sys.argv)



