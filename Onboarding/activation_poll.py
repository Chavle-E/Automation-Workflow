"""
Activation poller — Cloud Function entry point `poll_activations`.

Provisioning gets accounts CREATED and invites SENT; this closes the loop by
detecting when the contractor actually SHOWS UP, and pinging the operators
(DM + #onboarding, via notify_operators) the first time each step flips. When
every detectable step is done, the hire advances not_signed_in -> active.

Detectable signals (verified live 2026-07-25):
  - Zoho : account.lastLogin > 0  -> they logged into the work mailbox
  - Slack: users.lookupByEmail(work_email) resolves -> they joined the workspace
Harvest login/acceptance is NOT exposed by the Harvest API (is_active is true
the moment we create the contractor, no last-login field), so it is not polled;
Harvest counts as satisfied once invited or skipped, which not_signed_in already
implies. Activation therefore = Zoho login AND Slack join.

Only hires in lifecycle == not_signed_in are polled (all their invites are out).
Every write is idempotent, so the daily/interval schedule is safe to re-run.
"""
import logging
import os

from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from firestore_store import OnboardingStore, _now
from slack_helpers import notify_operators
from zoho_client import ZohoMailClient

load_dotenv(dotenv_path="../.env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SLACK_TOKEN = (os.getenv("SLACK_TOKEN") or "").strip()
ZOHO_CLIENT_ID = (os.getenv("ZOHO_CLIENT_ID") or "").strip()
ZOHO_CLIENT_SECRET = (os.getenv("ZOHO_CLIENT_SECRET") or "").strip()
ZOHO_REFRESH_TOKEN = (os.getenv("ZOHO_REFRESH_TOKEN") or "").strip()

ZOHO_LOGIN_MESSAGE = ":inbox_tray: *{name}* logged into their work email ({work_email})."
SLACK_JOIN_MESSAGE = ":slack: *{name}* joined the Slack workspace ({work_email})."
ACTIVE_MESSAGE = (
    ":tada: *{name}* is fully onboarded — logged into work email and joined Slack. "
    "Marked *active*."
)


def _acct(doc, tool):
    return (doc.get("accounts") or {}).get(tool) or {}


def _slack_joined(slack: WebClient, work_email: str):
    """The hire's Slack user id if they've joined the workspace, else None."""
    try:
        resp = slack.users_lookupByEmail(email=work_email)
        return resp["user"]["id"]
    except SlackApiError as e:
        if e.response.get("error") != "users_not_found":
            logging.warning(f"Slack lookup failed for {work_email}: {e.response['error']}")
        return None


def _zoho_logged_in(zoho: ZohoMailClient, work_email: str) -> bool:
    """True once the work mailbox has a non-zero lastLogin."""
    if zoho is None:
        return False
    try:
        user = zoho.find_user_by_email(work_email)
        return bool(user and int(user.get("lastLogin") or 0) > 0)
    except Exception as e:
        logging.warning(f"Zoho login check failed for {work_email}: {e}")
        return False


def poll_one(store, slack, zoho, doc):
    """Detect newly-activated steps for one hire; return a step->status dict."""
    person_id = doc["person_id"]
    work_email = doc.get("email")
    name = doc.get("personal_name") or doc.get("contract_name") or person_id
    outcomes = {}

    zoho_done = bool(_acct(doc, "zoho").get("activated_at"))
    slack_done = bool(_acct(doc, "slack").get("joined_at"))

    # Zoho login.
    if not zoho_done and _zoho_logged_in(zoho, work_email):
        store.record_account_event(person_id, "zoho", {"activated_at": _now()},
                                   action="zoho_activated")
        notify_operators(slack, ZOHO_LOGIN_MESSAGE.format(name=name, work_email=work_email))
        zoho_done = True
        outcomes["zoho"] = "logged in"

    # Slack join.
    if not slack_done:
        uid = _slack_joined(slack, work_email)
        if uid:
            store.record_account_event(person_id, "slack",
                                       {"joined_at": _now(), "slack_user_id": uid},
                                       action="slack_joined")
            notify_operators(slack, SLACK_JOIN_MESSAGE.format(name=name, work_email=work_email))
            slack_done = True
            outcomes["slack"] = "joined"

    # Fully onboarded -> active (notify exactly once, on the flipping transition).
    if zoho_done and slack_done and store.advance_to_active(person_id):
        notify_operators(slack, ACTIVE_MESSAGE.format(name=name))
        outcomes["lifecycle"] = "active"

    return outcomes


def run_activation_poll(person_id=None):
    """Poll one hire, or every hire sitting in lifecycle=not_signed_in."""
    if not SLACK_TOKEN:
        raise EnvironmentError("SLACK_TOKEN not set")

    store = OnboardingStore()
    slack = WebClient(token=SLACK_TOKEN)
    zoho = (ZohoMailClient(ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN)
            if (ZOHO_CLIENT_ID and ZOHO_CLIENT_SECRET and ZOHO_REFRESH_TOKEN) else None)
    if zoho is None:
        logging.warning("ZOHO_* env not set — Zoho login detection disabled")

    if person_id:
        doc = store.get(person_id)
        if not doc:
            raise KeyError(f"no onboarding doc for person_id={person_id}")
        docs = [doc]
    else:
        docs = store.list_by_lifecycle("not_signed_in")

    results = {}
    for doc in docs:
        try:
            results[doc["person_id"]] = poll_one(store, slack, zoho, doc)
        except Exception as e:  # one bad hire must not stop the sweep
            logging.error(f"activation poll failed for {doc.get('person_id')}: {e}")
            results[doc.get("person_id")] = {"error": str(e)[:200]}
    logging.info(f"Activation poll: {len(docs)} checked, results={results}")
    return results


def poll_activations(request):
    """HTTP entry point. POST (optional JSON {person_id}) sweeps not_signed_in."""
    person_id = None
    if request is not None and hasattr(request, "get_json"):
        body = request.get_json(silent=True) or {}
        person_id = body.get("person_id")
    try:
        results = run_activation_poll(person_id)
        return {"results": results}, 200
    except Exception as e:
        logging.error(f"poll_activations error: {e}")
        return {"error": str(e)}, 500


if __name__ == "__main__":
    import json
    print(json.dumps(run_activation_poll(), indent=2))