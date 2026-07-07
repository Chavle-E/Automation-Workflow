"""
Onboarding candidate selection — reads Deel People and decides, per person,
whether to PROVISION (onboarding done), REMIND (still onboarding), or SKIP.

Gate values confirmed empirically against the Deel People API:
    hiring_status == "active"            -> onboarding COMPLETE   -> provision
    hiring_status == "onboarding"        -> onboarding INCOMPLETE -> remind
    "inactive" / "no_active_contracts"   -> skip

SAFETY (back-catalog guard): many existing "active" workers have a null external_id
because the payroll matcher never linked everyone. We must NEVER mass-provision them.
A person is only a PROVISION candidate if they are active, have NO work email yet,
AND started recently. (For the pilot you can also pin PILOT_CONTRACT_IDS.)

This module does nothing destructive — run it directly for a dry-run summary.
"""
import os
import logging
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DEEL_BASE = "https://api.letsdeel.com/rest/v2"
TOKEN = os.getenv("DEEL_ORG_TOKEN")

STATUS_DONE = {"active"}            # add "ready_to_start" here if it ever appears
STATUS_ONBOARDING = {"onboarding"}  # incomplete states that should get reminders
NEW_HIRE_WINDOW_DAYS = 21           # back-catalog guard

# Optional hard allowlist for a supervised pilot run. Empty = no restriction.
PILOT_CONTRACT_IDS: set[str] = set()  # e.g. {"3vw95nd"}


def _headers():
    if not TOKEN:
        raise SystemExit("DEEL_ORG_TOKEN not set (.env)")
    return {"Authorization": f"Bearer {TOKEN}", "accept": "application/json"}


def fetch_all_people(page_size=100):
    """Fetch every person via offset pagination. Ends when a page comes back short."""
    people, offset = [], 0
    while True:
        params = {"limit": page_size, "offset": offset}
        r = requests.get(f"{DEEL_BASE}/people", headers=_headers(), params=params)
        r.raise_for_status()
        rows = r.json().get("data", [])
        people.extend(rows)
        if len(rows) < page_size:   # short (last) page — done
            break
        offset += page_size
    logging.info(f"Fetched {len(people)} people")
    return people


def current_employment(person):
    """The person's active (non-ended) employment, or None. Skips old contracts."""
    for emp in person.get("employments", []):
        if not emp.get("is_ended"):
            return emp
    return None


def _personal_email(person):
    emails = {e.get("type"): e.get("value") for e in person.get("emails", [])}
    return emails.get("personal") or emails.get("primary")


def _started_recently(emp, days=NEW_HIRE_WINDOW_DAYS):
    sd = emp.get("start_date")
    if not sd:
        return False
    try:
        start = datetime.fromisoformat(sd.replace("Z", "+00:00")).date()
    except ValueError:
        return False
    today = datetime.now(timezone.utc).date()
    return start >= (today - timedelta(days=days))  # includes future start dates


def extract_profile(person, emp):
    """Everything provisioning needs, from a single People record."""
    return {
        "person_id": person.get("id"),
        "worker_id": person.get("worker_id"),
        "contract_id": emp.get("id"),
        "first_name": person.get("first_name"),
        "last_name": person.get("last_name"),
        "preferred_first_name": person.get("preferred_first_name"),
        "personal_email": _personal_email(person),
        "work_email": emp.get("work_email"),
        "start_date": emp.get("start_date"),
        "cost_rate": (emp.get("payment") or {}).get("rate"),   # what we PAY — NOT client billable
        "currency": (emp.get("payment") or {}).get("currency"),
        "job_title": emp.get("job_title"),
        "hiring_type": emp.get("hiring_type"),
        "hiring_status": emp.get("hiring_status"),
    }


def classify(person):
    """Return (action, reason, profile) where action in {provision, remind, skip}."""
    emp = current_employment(person)
    if not emp:
        return "skip", "no active employment", None

    profile = extract_profile(person, emp)
    status = emp.get("hiring_status")

    if PILOT_CONTRACT_IDS and profile["contract_id"] not in PILOT_CONTRACT_IDS:
        return "skip", "not in pilot allowlist", profile

    if status in STATUS_ONBOARDING:
        return "remind", "onboarding incomplete", profile

    if status in STATUS_DONE:
        if profile["work_email"]:
            return "skip", "already provisioned (work email set)", profile
        if not _started_recently(emp):
            return "skip", "active but not a recent hire (back catalog)", profile
        return "provision", "onboarding complete, no work email, recent start", profile

    return "skip", f"status={status}", profile


def select_candidates(people):
    to_provision, to_remind = [], []
    for p in people:
        action, reason, profile = classify(p)
        if action == "provision":
            to_provision.append((profile, reason))
        elif action == "remind":
            to_remind.append((profile, reason))
    return to_provision, to_remind


if __name__ == "__main__":
    people = fetch_all_people()
    to_provision, to_remind = select_candidates(people)

    print(f"\n=== TO PROVISION ({len(to_provision)}) ===")
    for prof, why in to_provision:
        print(f"  {prof['first_name']} {prof['last_name']} | start {prof['start_date']} | {why}")

    print(f"\n=== TO REMIND ({len(to_remind)}) ===")
    for prof, why in to_remind:
        print(f"  {prof['first_name']} {prof['last_name']} | start {prof['start_date']} | {why}")

    print("\n(dry run — nothing created, nothing sent)")