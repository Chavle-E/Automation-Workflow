"""
Onboarding backfill — Cloud Function entry point `backfill_onboarding`.

Populates the Firestore `onboarding` collection from the current Deel state so the
store reflects reality before any of the later (out-of-scope) phases run. Safe to
run twice: every write goes through OnboardingStore's idempotent, transactional
upsert, which never regresses lifecycle and refreshes only changed identity fields.

How the data is assembled (see README + the Deel-field finding):
  - /people    -> the onboarding GATE (employments[].hiring_status), start_date,
                  personal-details name, work_email.  (NOT available on /contracts)
  - /contracts -> the company-typed `title` = the SOURCE for the derived email,
                  plus external_id and worker.full_name.
  Joined on  employment.id == contract.id.

Out of scope this phase: provisioning, Slack approval endpoint, activation poller,
offboarding, dashboard. Account activation timestamps stay null.
"""
import os
import logging
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from slack_sdk import WebClient

# Shared modules: cloudbuild copies deel_client.py + matcher.py from Payroll/ at
# deploy (single source of truth). Locally, fall back to the sibling package.
try:
    from deel_client import DeelClient
except ImportError:
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Payroll"))
    from deel_client import DeelClient

from naming import derive_email, name_confidence

load_dotenv(dotenv_path="../.env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DEEL_API_KEY = os.getenv("DEEL_API_KEY")
SLACK_TOKEN = (os.getenv("SLACK_TOKEN") or "").strip()
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "#onboarding")

# Onboarding gate (confirmed against the live People API + onboarding_poll.py):
#   active                          -> onboarding COMPLETE   -> needs_approval
#   onboarding / onboarding_overdue -> onboarding INCOMPLETE -> gated
STATUS_DONE = {"active"}
STATUS_ONBOARDING = {"onboarding", "onboarding_overdue"}

# Back-catalog guard (from onboarding_poll.py): a "completed onboarding" person is
# only a genuine in-flight new hire if they started recently AND have no work email
# yet. Everyone else is pre-existing staff; we still record them, but flag them so
# the later approval poller does not ping David about the whole company.
NEW_HIRE_WINDOW_DAYS = 21


def _current_employment(person):
    """The person's active (non-ended) employment, or None."""
    for emp in person.get("employments", []):
        if not emp.get("is_ended"):
            return emp
    return None


def _started_recently(start_date, days=NEW_HIRE_WINDOW_DAYS):
    if not start_date:
        return False
    try:
        start = datetime.fromisoformat(start_date.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        return False
    return start >= (datetime.now(timezone.utc).date() - timedelta(days=days))


def build_record(person, contracts_by_id):
    """
    Turn one /people record into the fields backfill upserts, or None to skip
    (no active employment / unknown gate status).
    """
    emp = _current_employment(person)
    if not emp:
        return None

    hiring_status = emp.get("hiring_status") or person.get("hiring_status")
    contract_id = emp.get("id")
    contract = contracts_by_id.get(contract_id, {})

    # Email SOURCE = company-typed contract title; fall back through worker/emp/person.
    contract_name = (
        contract.get("title")
        or (contract.get("worker") or {}).get("full_name")
        or emp.get("name")
        or person.get("full_name")
        or f"{person.get('first_name', '')} {person.get('last_name', '')}".strip()
    )
    # Confidence cross-check uses the personal-details name.
    personal_name = (
        person.get("full_name")
        or f"{person.get('first_name', '')} {person.get('last_name', '')}".strip()
    )

    # Email (David's rule): from Deel's STRUCTURED first_name/last_name — take the
    # first given name + the first (paternal) surname. Fall back to splitting the
    # contract name string only when structured fields are missing.
    first_name = person.get("first_name")
    last_name = person.get("last_name")
    if first_name and last_name:
        email = derive_email(first_name, last_name)
    else:
        email = derive_email(contract_name)

    start_date = emp.get("start_date") or person.get("start_date")

    # Lifecycle gate.
    if hiring_status in STATUS_ONBOARDING:
        lifecycle = "gated"
    elif hiring_status in STATUS_DONE:
        lifecycle = "needs_approval"   # spec's "ready"; enum-valid pre-provision state
    else:
        return None  # inactive / no_active_contracts / unknown — nothing to seed

    work_email = emp.get("work_email")
    genuine_new_hire = _started_recently(start_date) and not work_email
    back_catalog = (lifecycle == "needs_approval") and not genuine_new_hire

    return {
        "person_id": person.get("id"),
        "identity": {
            "contract_name": contract_name,
            "personal_name": personal_name,
            "email": email,
            "name_confidence": name_confidence(contract_name, personal_name),
            "start_date": start_date,
            "backfill_back_catalog": back_catalog,
        },
        "initial_lifecycle": lifecycle,
        "deel_status": emp.get("contract_status") or contract.get("status"),
    }


def _notify_new_hire(hire_data):
    """Send Slack notification when a genuine new hire is discovered."""
    if not SLACK_TOKEN or not SLACK_CHANNEL:
        logging.debug("Slack notification skipped: token or channel not configured")
        return

    try:
        client = WebClient(token=SLACK_TOKEN)
        name = hire_data["identity"]["personal_name"]
        email = hire_data["identity"]["email"]
        start_date = hire_data["identity"]["start_date"]
        confidence = hire_data["identity"]["name_confidence"]

        confidence_emoji = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(confidence, "⚪")

        message = (
            f"{confidence_emoji} *New hire discovered*: {name}\n"
            f"Email: `{email}`\n"
            f"Start date: {start_date}\n"
            f"Confidence: {confidence}\n"
            f"_Waiting for approval in the onboarding dashboard._"
        )

        client.chat_postMessage(channel=SLACK_CHANNEL, text=message, mrkdwn=True)
        logging.info(f"Slack notification sent for {email}")
    except Exception as e:
        logging.warning(f"Failed to send Slack notification: {e}")


def run_backfill(dry_run=False):
    """
    Read current Deel people+contracts and upsert a Firestore doc per active hire.
    dry_run=True logs what would be written and touches no Firestore (used for
    local validation without google-cloud-firestore configured).
    """
    if not DEEL_API_KEY:
        raise EnvironmentError("DEEL_API_KEY is not set")

    deel = DeelClient(DEEL_API_KEY)
    people = deel.get_all_people()
    contracts = deel.get_all_contracts(contract_type=None)  # all types
    contracts_by_id = {c["id"]: c for c in contracts}
    logging.info(f"Fetched {len(people)} people, {len(contracts)} contracts")

    store = None
    if not dry_run:
        from firestore_store import OnboardingStore  # imported lazily (cloud-only dep)
        store = OnboardingStore()

    summary = {"created": 0, "updated": 0, "unchanged": 0, "skipped": 0,
               "gated": 0, "needs_approval": 0, "back_catalog": 0, "low_confidence": 0}
    new_genuine_hires = []

    for person in people:
        record = build_record(person, contracts_by_id)
        if record is None:
            summary["skipped"] += 1
            continue

        summary[record["initial_lifecycle"]] += 1
        if record["identity"]["backfill_back_catalog"]:
            summary["back_catalog"] += 1
        if record["identity"]["name_confidence"] == "low":
            summary["low_confidence"] += 1
            logging.warning(
                f"LOW confidence for person {record['person_id']}: "
                f"contract={record['identity']['contract_name']!r} "
                f"personal={record['identity']['personal_name']!r} "
                f"-> proposed {record['identity']['email']} (needs human review)"
            )

        if dry_run:
            logging.info(
                f"[DRY] {record['initial_lifecycle']:<14} "
                f"{record['identity']['email']} "
                f"({record['identity']['name_confidence']}) "
                f"back_catalog={record['identity']['backfill_back_catalog']}"
            )
        else:
            result = store.upsert(
                record["person_id"],
                identity=record["identity"],
                initial_lifecycle=record["initial_lifecycle"],
                deel_status=record["deel_status"],
            )
            summary[result] += 1
            # Track newly created genuine hires (not back-catalog) for Slack notification
            if result == "created" and not record["identity"]["backfill_back_catalog"]:
                new_genuine_hires.append(record)

    # Send Slack notifications for newly discovered genuine hires
    if not dry_run:
        for hire in new_genuine_hires:
            _notify_new_hire(hire)

    logging.info(f"Backfill summary: {summary}")
    return summary


def backfill_onboarding(request):
    """Cloud Function HTTP entry point. ?dry_run=1 to preview without writing."""
    logging.info("Onboarding backfill triggered.")
    try:
        dry_run = False
        if request is not None and hasattr(request, "args"):
            dry_run = request.args.get("dry_run") in ("1", "true", "yes")
        summary = run_backfill(dry_run=dry_run)
        return f"Backfill complete: {summary}"
    except Exception as e:
        logging.error(f"Error in onboarding backfill: {e}")
        return f"Error: {str(e)}", 500


if __name__ == "__main__":
    # Local: dry run against live Deel, no Firestore writes.
    run_backfill(dry_run=True)