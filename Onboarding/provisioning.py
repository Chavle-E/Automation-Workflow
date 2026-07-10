"""
Provisioning — Cloud Function entry point `provision_onboarding`.

Runs the tool-account side of an approved hire (lifecycle == "provisioning",
`provision_request` filled in by the dashboard approve form). Deployed
AUTHENTICATED; invoked by the dashboard (OIDC) right after an approval / manual
mark, and by a daily scheduler sweep that retries anything still pending.

Per-hire steps, in the playbook order Zoho -> Slack -> Harvest:

  1. Zoho has no API access yet -> NOTIFY-A-HUMAN: DM the operators the exact
     playbook instructions (auto-gen password, credentials to the PERSONAL email,
     force password change). A human then creates the user and clicks
     "mark Zoho created" in the dashboard.
  2. Slack + Harvest invites target the WORK email, which only exists once the
     Zoho user is created — so both steps WAIT for accounts.zoho.created_at.
     Slack: try the admin invite API, fall back to a manual-invite DM.
     Harvest: create the contractor (rate/cost from the approval form, 40h,
     Member) and assign to the approved project. A full plan (seat cap) never
     fails silently: operators are DMed to add a paid seat, and the sweep/retry
     picks the hire up again after they do.

Every step is idempotent (skipped once its accounts.* marker is set) and each
outcome is written through OnboardingStore.record_account_event, which advances
the doc to "not_signed_in" when all three invites are out.

Also serves GET ?list=projects — the active Harvest projects for the dashboard's
approval-form dropdown (keeps the dashboard itself secret-free).
"""
import json
import logging
import os

from dotenv import load_dotenv
from slack_sdk import WebClient

from firestore_store import OnboardingStore, _now
from harvest_client import HarvestClient, SeatLimitError
from slack_helpers import invite_to_workspace, notify_operators

load_dotenv(dotenv_path="../.env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# .strip(): secret versions created with `echo` carry a trailing newline, which
# is invalid in HTTP header values (bit us live on harvest-acc-id).
HARVEST_API_KEY = (os.getenv("HARVEST_API_KEY") or "").strip()
HARVEST_ACCOUNT_ID = (os.getenv("HARVEST_ACCOUNT_ID") or "").strip()
SLACK_TOKEN = (os.getenv("SLACK_TOKEN") or "").strip()

ZOHO_INSTRUCTIONS = (
    ":busts_in_silhouette: *Zoho user needed for {name}* (approved by {approver})\n"
    "1. Zoho admin (`billing@thirstysprout.com`) → add user *{work_email}*, Role = *User* (never Admin)\n"
    "2. Auto-generate the password (≥8, upper+lower+number+special)\n"
    "3. CHECK “send credentials via email” → personal email *{personal_email}*\n"
    "4. CHECK “force password change on first login”\n"
    "5. Send the onboarding guide to BOTH {work_email} and {personal_email}\n"
    "Then open the dashboard → this hire → *Mark Zoho created* (that releases the "
    "Slack + Harvest invites, which go to the work email)."
)

SEAT_CAP_MESSAGE = (
    ":chair: *Harvest seat cap hit* while provisioning *{name}* ({work_email}).\n"
    "The automation cannot change seat count (billing). Please add a paid seat in "
    "Harvest, then hit *Retry provisioning* on the dashboard (or wait for the daily sweep).\n"
    "Harvest said: _{detail}_"
)

SLACK_MANUAL_MESSAGE = (
    ":slack: *Manual Slack invite needed* for *{name}*: the API invite is unavailable "
    "({detail}). Please invite *{work_email}* to the workspace with default channels "
    "#announcements + #thirstysprout-projects-and-off-topic-stuff, then *Mark Slack "
    "invited* on the dashboard."
)


def _split_name(doc):
    """Legal name = Deel personal details (David's rule); doc stores the joined string."""
    parts = (doc.get("personal_name") or doc.get("contract_name") or "").split()
    if not parts:
        return "Unknown", "Unknown"
    return parts[0], " ".join(parts[1:]) or parts[0]


def _acct(doc, tool):
    return (doc.get("accounts") or {}).get(tool) or {}


def provision_one(store: OnboardingStore, slack: WebClient, harvest: HarvestClient, doc):
    """Run all still-pending steps for one hire. Returns a step->outcome dict."""
    person_id = doc["person_id"]
    req = doc.get("provision_request") or {}
    outcomes = {}

    # Hard guards — mirror the store's, in case this is invoked directly.
    if doc.get("backfill_back_catalog"):
        return {"skipped": "back_catalog"}
    if doc.get("lifecycle") != "provisioning":
        return {"skipped": f"lifecycle={doc.get('lifecycle')}"}
    if not req:
        return {"skipped": "no provision_request (approve via the dashboard first)"}

    name = doc.get("personal_name") or doc.get("contract_name") or person_id
    work_email = doc.get("email")

    # --- 1. Zoho: notify-a-human (no API), once -------------------------------
    if not _acct(doc, "zoho").get("created_at"):
        if not _acct(doc, "zoho").get("requested_at"):
            notify_operators(slack, ZOHO_INSTRUCTIONS.format(
                name=name, approver=req.get("by", "?"), work_email=work_email,
                personal_email=req.get("personal_email", "?")))
            store.record_account_event(person_id, "zoho", {"requested_at": _now()},
                                       action="zoho_manual_requested")
            outcomes["zoho"] = "operators notified"
        else:
            outcomes["zoho"] = "waiting for manual creation"
        # Work email doesn't exist until Zoho is done — hold the invites.
        outcomes["slack"] = outcomes["harvest"] = "waiting for zoho.created_at"
        return outcomes
    outcomes["zoho"] = "created"

    # --- 2. Slack invite (work email) ------------------------------------------
    slack_acct = _acct(doc, "slack")
    if slack_acct.get("invited_at"):
        outcomes["slack"] = "already invited"
    elif slack_acct.get("manual_requested_at"):
        outcomes["slack"] = "waiting for manual invite"
    else:
        invited, detail = invite_to_workspace(slack, work_email)
        if invited:
            store.record_account_event(person_id, "slack",
                                       {"invited_at": _now(), "invite_mode": "api"},
                                       action="slack_invited")
            outcomes["slack"] = "invited via API"
        else:
            notify_operators(slack, SLACK_MANUAL_MESSAGE.format(
                name=name, work_email=work_email, detail=detail))
            store.record_account_event(person_id, "slack",
                                       {"manual_requested_at": _now(), "invite_mode": "manual"},
                                       action="slack_manual_requested")
            outcomes["slack"] = f"manual invite requested ({detail})"

    # --- 3. Harvest contractor + project ---------------------------------------
    harvest_acct = _acct(doc, "harvest")
    if harvest_acct.get("invited_at"):
        outcomes["harvest"] = "already invited"
    else:
        first, last = _split_name(doc)
        try:
            existing = harvest.find_user_by_email(work_email)
            user = existing or harvest.create_contractor(
                first, last, work_email,
                billable_rate=req["billable_rate"], cost_rate=req.get("cost_rate"))
            harvest.assign_user_to_project(req["harvest_project_id"], user["id"])
            store.record_account_event(person_id, "harvest", {
                "invited_at": _now(), "user_id": user["id"], "seat": "ok",
                "project_id": req["harvest_project_id"],
            }, action="harvest_invited")
            outcomes["harvest"] = ("existing user assigned to project" if existing
                                   else "contractor created + assigned")
        except SeatLimitError as e:
            if harvest_acct.get("seat") != "blocked_no_seat":  # notify once per block
                notify_operators(slack, SEAT_CAP_MESSAGE.format(
                    name=name, work_email=work_email, detail=str(e)[:200]))
                store.record_account_event(person_id, "harvest", {"seat": "blocked_no_seat"},
                                           action="harvest_seat_blocked")
            outcomes["harvest"] = "blocked: no free seat (operators notified)"
        except Exception as e:  # keep other hires provisionable
            logging.error(f"Harvest provisioning failed for {person_id}: {e}")
            outcomes["harvest"] = f"error: {e}"

    return outcomes


def run_provisioning(person_id=None):
    """Provision one hire, or sweep everything sitting in lifecycle=provisioning."""
    if not (HARVEST_API_KEY and HARVEST_ACCOUNT_ID and SLACK_TOKEN):
        raise EnvironmentError("HARVEST_API_KEY / HARVEST_ACCOUNT_ID / SLACK_TOKEN not set")

    store = OnboardingStore()
    slack = WebClient(token=SLACK_TOKEN)
    harvest = HarvestClient(HARVEST_API_KEY, HARVEST_ACCOUNT_ID)

    if person_id:
        doc = store.get(person_id)
        if not doc:
            raise KeyError(f"no onboarding doc for person_id={person_id}")
        docs = [doc]
    else:
        docs = store.list_by_lifecycle("provisioning")

    results = {}
    for doc in docs:
        results[doc["person_id"]] = provision_one(store, slack, harvest, doc)
    logging.info(f"Provisioning results: {results}")
    return results


def provision_onboarding(request):
    """
    HTTP entry point (authenticated).
      GET  ?list=projects          -> JSON active Harvest projects (dashboard dropdown)
      POST {"person_id": "..."}    -> provision that hire now
      POST (empty body)            -> sweep all lifecycle=provisioning docs (scheduler)
    """
    try:
        if request.args.get("list") == "projects":
            harvest = HarvestClient(HARVEST_API_KEY, HARVEST_ACCOUNT_ID)
            projects = [{"id": p["id"], "name": p["name"],
                         "client": (p.get("client") or {}).get("name", "")}
                        for p in harvest.list_projects()]
            projects.sort(key=lambda p: (p["client"], p["name"]))
            return json.dumps({"projects": projects}), 200, {"Content-Type": "application/json"}

        person_id = request.args.get("person_id")
        if not person_id and request.is_json:
            person_id = (request.get_json(silent=True) or {}).get("person_id")

        results = run_provisioning(person_id)
        return json.dumps({"results": results}), 200, {"Content-Type": "application/json"}
    except (KeyError, ValueError) as e:
        return json.dumps({"error": str(e)}), 400, {"Content-Type": "application/json"}
    except Exception as e:
        logging.error(f"Error in provisioning: {e}")
        return json.dumps({"error": str(e)}), 500, {"Content-Type": "application/json"}
