"""
Provisioning — Cloud Function entry point `provision_onboarding`.

Runs the tool-account side of an approved hire (lifecycle == "provisioning",
`provision_request` filled in by the dashboard approve form). Deployed
AUTHENTICATED; invoked by the dashboard (OIDC) right after an approval / manual
mark, and by a daily scheduler sweep that retries anything still pending.

Per-hire steps, in the playbook order Zoho -> Slack -> Harvest:

  1. Zoho: create the work mailbox via the Zoho Mail API (Self Client of
     hello@thirstysprout.ai; auto-generated password, forced change at first
     login, role=member) and DM the operators the one-time credentials to
     forward to the hire's PERSONAL email. If the API creds are missing or the
     call fails, fall back to NOTIFY-A-HUMAN: DM the exact playbook instructions;
     a human then creates the user and clicks "mark Zoho created" in the
     dashboard.
  2. Slack + Harvest invites target the WORK email, which only exists once the
     Zoho user is created — so both steps WAIT for accounts.zoho.created_at
     (set in the same run when the API path succeeds).
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
import datetime
import json
import logging
import os
import re

from dotenv import load_dotenv
from slack_sdk import WebClient

from firestore_store import OnboardingStore, _now
from harvest_client import HarvestClient, SeatLimitError
from slack_helpers import invite_to_workspace, notify_operators
from zoho_client import ZohoMailClient, generate_password

load_dotenv(dotenv_path="../.env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# .strip(): secret versions created with `echo` carry a trailing newline, which
# is invalid in HTTP header values (bit us live on harvest-acc-id).
HARVEST_API_KEY = (os.getenv("HARVEST_API_KEY") or "").strip()
HARVEST_ACCOUNT_ID = (os.getenv("HARVEST_ACCOUNT_ID") or "").strip()
SLACK_TOKEN = (os.getenv("SLACK_TOKEN") or "").strip()
ZOHO_CLIENT_ID = (os.getenv("ZOHO_CLIENT_ID") or "").strip()
ZOHO_CLIENT_SECRET = (os.getenv("ZOHO_CLIENT_SECRET") or "").strip()
ZOHO_REFRESH_TOKEN = (os.getenv("ZOHO_REFRESH_TOKEN") or "").strip()
# Standing workspace invite link (Slack admin UI; signups restricted to the
# @thirstysprout.ai domain). When set, the Slack "invite" is an email to the
# work inbox instead of the admin API / manual-DM dance.
SLACK_INVITE_LINK = (os.getenv("SLACK_INVITE_LINK") or "").strip()
# Optional: prices contractor cost in the finance report from Deel contract
# rates (the real pay rate) instead of Harvest cost_rate.
DEEL_API_KEY = (os.getenv("DEEL_API_KEY") or "").strip()

ZOHO_CREATED_MESSAGE = (
    ":white_check_mark: *Zoho mailbox created for {name}*: *{work_email}* "
    "(approved by {approver})\n"
    "One-time password: `{password}` — the user MUST change it at first login.\n"
    "Please send these credentials to the personal email *{personal_email}*, along with:\n"
    "• Onboarding guide (send to BOTH {work_email} and {personal_email})\n"
    "• Company guide: https://help.thirstysprout.com/\n"
    "(This password is only shown here — it is not stored anywhere.)\n"
    "Slack + Harvest invites to {work_email} are going out automatically now."
)

ZOHO_EXISTING_MESSAGE = (
    ":information_source: *Zoho mailbox already exists* for *{name}* ({work_email}) — "
    "marked as created and moving on to the Slack + Harvest invites. If the hire never "
    "received credentials, reset their password from the Zoho admin console and send it "
    "to *{personal_email}*."
)

ZOHO_INSTRUCTIONS = (
    ":busts_in_silhouette: *Zoho user needed for {name}* (approved by {approver})\n"
    "1. Zoho admin (`hello@thirstysprout.ai`) → add user *{work_email}*, Role = *User* (never Admin)\n"
    "2. Auto-generate the password (≥8, upper+lower+number+special)\n"
    "3. CHECK 'send credentials via email' → personal email *{personal_email}*\n"
    "4. CHECK 'force password change on first login'\n"
    "5. Send to BOTH {work_email} and {personal_email}:\n"
    "   • Onboarding guide\n"
    "   • Company guide: https://help.thirstysprout.com/\n"
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

SLACK_COMPLETED_MESSAGE = (
    ":white_check_mark: *Slack invite sent* to *{name}* ({work_email})"
)

HARVEST_COMPLETED_MESSAGE = (
    ":white_check_mark: *Harvest contractor added* for *{name}* ({work_email}) — "
    "Rate: ${rate}/hr, Project: {project}, Seat assignment complete"
)

CREDS_EMAILED_MESSAGE = (
    ":white_check_mark: *Zoho mailbox created for {name}*: *{work_email}* "
    "(approved by {approver})\n"
    "Credentials + company guide emailed to *{personal_email}*; the Slack invite "
    "goes to the work inbox next. No operator action needed."
)

SLACK_EMAILED_MESSAGE = (
    ":email: *Slack invite emailed* to *{name}*'s work inbox ({work_email})."
)

CREDENTIALS_EMAIL_SUBJECT = "Your ThirstySprout work email is ready"
CREDENTIALS_EMAIL_BODY = (
    "Hi {first_name},\n\n"
    "Welcome to ThirstySprout! Your work email account is ready:\n\n"
    "    Email:    {work_email}\n"
    "    Password: {password}\n\n"
    "This is a one-time password - you'll be asked to set your own the first time\n"
    "you log in at https://mail.zoho.com\n\n"
    "Next steps:\n"
    "  1. Log into your work inbox - a Slack invitation is waiting for you there.\n"
    "     Make sure you join Slack with your work email address.\n"
    "  2. Read the company guide: https://help.thirstysprout.com/\n\n"
    "If anything doesn't work, just reply to this email.\n\n"
    "- ThirstySprout"
)

SLACK_EMAIL_SUBJECT = "Join the ThirstySprout Slack"
SLACK_EMAIL_BODY = (
    "Hi {first_name},\n\n"
    "Join our Slack workspace here:\n\n"
    "    {invite_link}\n\n"
    "Important: sign up with this work email address ({work_email}) - the\n"
    "workspace only accepts @thirstysprout.ai addresses.\n\n"
    "See you there!\n"
    "- ThirstySprout"
)


def _split_name(doc):
    """Legal name = Deel personal details (David's rule); doc stores the joined string."""
    parts = (doc.get("personal_name") or doc.get("contract_name") or "").split()
    if not parts:
        return "Unknown", "Unknown"
    return parts[0], " ".join(parts[1:]) or parts[0]


def _acct(doc, tool):
    return (doc.get("accounts") or {}).get(tool) or {}


def provision_one(store: OnboardingStore, slack: WebClient, harvest: HarvestClient, doc,
                  zoho: ZohoMailClient = None):
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

    # --- 1. Zoho mailbox: API first, notify-a-human as fallback ----------------
    if not _acct(doc, "zoho").get("created_at"):
        created = False
        if zoho is not None:
            try:
                first, last = _split_name(doc)
                if zoho.find_user_by_email(work_email):
                    notify_operators(slack, ZOHO_EXISTING_MESSAGE.format(
                        name=name, work_email=work_email,
                        personal_email=req.get("personal_email", "?")))
                    store.record_account_event(
                        person_id, "zoho", {"created_at": _now(), "mode": "api_existing"},
                        action="zoho_found_existing")
                    outcomes["zoho"] = "already existed in Zoho (marked created)"
                else:
                    # The one-time password is never stored in Firestore or logs.
                    # Forced change at first login.
                    password = generate_password()
                    zoho.create_user(work_email, first, last, password)
                    store.record_account_event(
                        person_id, "zoho", {"created_at": _now(), "mode": "api"},
                        action="zoho_created_api")
                    personal_email = req.get("personal_email", "")
                    # Email the credentials straight to the personal inbox. On any
                    # send failure, fall back to DMing the operator the password.
                    emailed = False
                    try:
                        zoho.send_mail(
                            personal_email, CREDENTIALS_EMAIL_SUBJECT,
                            CREDENTIALS_EMAIL_BODY.format(
                                first_name=first, work_email=work_email, password=password))
                        emailed = True
                    except Exception as e:
                        logging.error(f"Credentials email failed for {person_id}: {e}")
                    if emailed:
                        store.record_account_event(
                            person_id, "zoho", {"credentials_emailed_at": _now()},
                            action="zoho_credentials_emailed")
                        notify_operators(slack, CREDS_EMAILED_MESSAGE.format(
                            name=name, approver=req.get("by", "?"), work_email=work_email,
                            personal_email=personal_email))
                        outcomes["zoho"] = "created via API + credentials emailed"
                    else:
                        # sensitive: carries the one-time password — DM-only.
                        notify_operators(slack, ZOHO_CREATED_MESSAGE.format(
                            name=name, approver=req.get("by", "?"), work_email=work_email,
                            personal_email=personal_email, password=password),
                            sensitive=True)
                        outcomes["zoho"] = "created via API (email failed — password DM'd)"
                created = True
            except Exception as e:  # fall back to the human playbook DM
                logging.error(f"Zoho API provisioning failed for {person_id}: {e}")
                outcomes["zoho_api_error"] = str(e)[:200]
        if not created:
            if not _acct(doc, "zoho").get("requested_at"):
                notify_operators(slack, ZOHO_INSTRUCTIONS.format(
                    name=name, approver=req.get("by", "?"), work_email=work_email,
                    personal_email=req.get("personal_email", "?")))
                store.record_account_event(person_id, "zoho", {"requested_at": _now()},
                                           action="zoho_manual_requested")
                outcomes["zoho"] = "operators notified (manual fallback)"
            else:
                outcomes["zoho"] = "waiting for manual creation"
            # Work email doesn't exist until Zoho is done — hold the invites.
            outcomes["slack"] = outcomes["harvest"] = "waiting for zoho.created_at"
            return outcomes
    else:
        outcomes["zoho"] = "created"

    # --- 2. Slack invite (work email) ------------------------------------------
    slack_acct = _acct(doc, "slack")
    first, _last = _split_name(doc)
    if slack_acct.get("invited_at"):
        outcomes["slack"] = "already invited"
    elif slack_acct.get("manual_requested_at"):
        outcomes["slack"] = "waiting for manual invite"
    elif SLACK_INVITE_LINK and zoho is not None:
        # Preferred path: email the standing invite link to the work inbox (the
        # mailbox exists by now). The hire self-joins with their work address.
        try:
            zoho.send_mail(
                work_email, SLACK_EMAIL_SUBJECT,
                SLACK_EMAIL_BODY.format(
                    first_name=first, invite_link=SLACK_INVITE_LINK, work_email=work_email))
            store.record_account_event(
                person_id, "slack",
                {"invited_at": _now(), "invite_mode": "email_link"},
                action="slack_invite_emailed")
            try:
                slack.chat_postMessage(channel="onboarding",
                    text=SLACK_EMAILED_MESSAGE.format(name=name, work_email=work_email),
                    mrkdwn=True)
            except Exception as e:
                logging.warning(f"Failed to post Slack notification to #onboarding: {e}")
            outcomes["slack"] = "invite link emailed to work inbox"
        except Exception as e:
            logging.error(f"Slack invite email failed for {person_id}: {e}")
            notify_operators(slack, SLACK_MANUAL_MESSAGE.format(
                name=name, work_email=work_email, detail=f"invite email failed: {e}"))
            store.record_account_event(person_id, "slack",
                                       {"manual_requested_at": _now(), "invite_mode": "manual"},
                                       action="slack_manual_requested")
            outcomes["slack"] = "manual invite requested (invite email failed)"
    else:
        invited, detail = invite_to_workspace(slack, work_email)
        if invited:
            store.record_account_event(person_id, "slack",
                                       {"invited_at": _now(), "invite_mode": "api"},
                                       action="slack_invited")
            try:
                slack.chat_postMessage(channel="onboarding", text=SLACK_COMPLETED_MESSAGE.format(name=name, work_email=work_email), mrkdwn=True)
            except Exception as e:
                logging.warning(f"Failed to send Slack notification to #onboarding: {e}")
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
    if not req.get("harvest_project_id"):
        # Approved with "No Harvest" — time is tracked outside Harvest.
        if not harvest_acct.get("skipped_at"):
            store.record_account_event(person_id, "harvest", {"skipped_at": _now()},
                                       action="harvest_skipped")
        outcomes["harvest"] = "skipped (no Harvest project — tracked outside Harvest)"
    elif harvest_acct.get("invited_at"):
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
            try:
                slack.chat_postMessage(
                    channel="onboarding",
                    text=HARVEST_COMPLETED_MESSAGE.format(
                        name=name, work_email=work_email,
                        rate=req.get("billable_rate", "?"),
                        project=req.get("harvest_project_name", "?")
                    ),
                    mrkdwn=True
                )
            except Exception as e:
                logging.warning(f"Failed to send Harvest notification to #onboarding: {e}")
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
    # Zoho creds are optional: without them the Zoho step degrades to the
    # notify-a-human playbook DM instead of failing the whole run.
    zoho = (ZohoMailClient(ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN)
            if (ZOHO_CLIENT_ID and ZOHO_CLIENT_SECRET and ZOHO_REFRESH_TOKEN) else None)
    if zoho is None:
        logging.warning("ZOHO_* env not set — Zoho step will use the manual fallback")

    if person_id:
        doc = store.get(person_id)
        if not doc:
            raise KeyError(f"no onboarding doc for person_id={person_id}")
        docs = [doc]
    else:
        docs = store.list_by_lifecycle("provisioning")

    results = {}
    for doc in docs:
        results[doc["person_id"]] = provision_one(store, slack, harvest, doc, zoho=zoho)
    logging.info(f"Provisioning results: {results}")
    return results


def _deel_hourly_rates(harvest_user_ids):
    """
    harvest_user_id -> {"rate", "currency", "contract_id"} from Deel contracts.

    The payroll sync stamps external_id = "harvest_<user id>" on Deel contracts,
    so the join is a straight index. Rates only exist on the contract DETAIL
    (compensation_details); only scale == "hourly" is usable as a cost rate.
    Best-effort: any Deel hiccup just means Harvest cost_rate fallback.
    """
    if not (DEEL_API_KEY and harvest_user_ids):
        return {}
    try:
        from deel_client import DeelClient
        deel = DeelClient(DEEL_API_KEY)
        contracts = deel.get_all_contracts(contract_type=None)
    except Exception as e:
        logging.warning(f"finance: Deel contracts unavailable ({e}) — using Harvest cost rates")
        return {}

    # Prefer a live contract when the same person has several (re-hires etc.).
    status_rank = {"in_progress": 0, "completed": 1}
    by_ext = {}
    for c in contracts:
        ext = c.get("external_id") or ""
        if not ext.startswith("harvest_"):
            continue
        rank = status_rank.get(c.get("status"), 2)
        if ext not in by_ext or rank < by_ext[ext][0]:
            by_ext[ext] = (rank, c)

    rates = {}
    for uid in harvest_user_ids:
        found = by_ext.get(f"harvest_{uid}")
        if not found:
            continue
        try:
            detail = deel.get_contract(found[1]["id"]) or {}
        except Exception:
            continue
        comp = detail.get("compensation_details") or {}
        if comp.get("scale") == "hourly" and comp.get("amount"):
            rates[uid] = {
                "rate": float(comp["amount"]),
                "currency": comp.get("currency_code") or "USD",
                "contract_id": found[1]["id"],
            }
    return rates


def _overlaps(a_start, a_end, b_start, b_end):
    """Date-string overlap; open-ended sides count as overlapping."""
    if not (a_start and a_end):
        return False
    return a_start <= b_end and a_end >= b_start


def _invoice_stats(harvest, date_from, date_to, person_names):
    """
    (per-person paid/open amounts, invoice rows) for invoices whose service
    period overlaps the report range (or, for period-less invoices such as
    milestones, whose issue_date falls in it).

    Per-person attribution parses the line items our own invoice import
    generates — "Project: Person Name (from - to)" — by name containment.
    Draft and closed (written-off) invoices are listed but never attributed.
    """
    lookback = (datetime.date.fromisoformat(date_from)
                - datetime.timedelta(days=60)).isoformat()
    invoices = harvest.list_invoices(issued_from=lookback)

    rows, attribution = [], {name: {"paid": 0.0, "open": 0.0} for name in person_names}
    for inv in invoices:
        issue = inv.get("issue_date") or ""
        if not (_overlaps(inv.get("period_start"), inv.get("period_end"), date_from, date_to)
                or (not inv.get("period_start") and date_from <= issue <= date_to)):
            continue
        state = inv.get("state")
        rows.append({
            "id": inv.get("id"), "number": inv.get("number"),
            "client": (inv.get("client") or {}).get("name", ""),
            "amount": inv.get("amount"), "state": state,
            "issue_date": issue, "due_date": inv.get("due_date"),
            "paid_date": inv.get("paid_date"),
            "period_start": inv.get("period_start"), "period_end": inv.get("period_end"),
        })
        if state not in ("paid", "open"):
            continue  # draft/closed: visible in the table, not in the numbers
        for li in inv.get("line_items") or []:
            desc = li.get("description") or ""
            for name in person_names:
                if name and name in desc:
                    attribution[name][state] += float(li.get("amount") or 0)
                    break

    rows.sort(key=lambda r: (r["issue_date"] or ""), reverse=True)
    return attribution, rows


def finance_report(harvest: HarvestClient, date_from: str, date_to: str):
    """
    Per-contractor profit for a period.

    Revenue = billable hours x the Harvest entry's billable_rate. Cost = ALL
    logged hours (contractors are paid for logged time) priced from the Deel
    contract's hourly rate (the real pay rate) when a mapped contract exists,
    falling back to the Harvest entry's cost_rate. Invoice attribution adds
    what has actually been PAID per contractor, so collected profit
    (paid - cost) is separate from billed profit. Unpriced hours are surfaced
    per person so a missing rate reads as "fix this", never as free margin.
    """
    entries = harvest.list_time_entries(date_from, date_to)
    people = {}
    for e in entries:
        user = e.get("user") or {}
        uid = user.get("id")
        if uid is None:
            continue
        p = people.setdefault(uid, {
            "user_id": uid, "name": user.get("name") or f"user {uid}",
            "hours": 0.0, "billable_hours": 0.0,
            "revenue": 0.0, "harvest_cost": 0.0,
            "harvest_hours_no_cost_rate": 0.0, "billable_hours_no_bill_rate": 0.0,
            "projects": {},
        })
        hours = float(e.get("hours") or 0)
        p["hours"] += hours
        project = (e.get("project") or {}).get("name") or "?"
        p["projects"][project] = p["projects"].get(project, 0.0) + hours

        cost_rate = e.get("cost_rate")
        if cost_rate:
            p["harvest_cost"] += hours * float(cost_rate)
        elif hours:
            p["harvest_hours_no_cost_rate"] += hours

        if e.get("billable"):
            p["billable_hours"] += hours
            bill_rate = e.get("billable_rate")
            if bill_rate:
                p["revenue"] += hours * float(bill_rate)
            elif hours:
                p["billable_hours_no_bill_rate"] += hours

    # Second pass: price cost from Deel where a mapped hourly contract exists.
    deel_rates = _deel_hourly_rates(list(people.keys()))
    paid_by_name, invoice_rows = _invoice_stats(
        harvest, date_from, date_to, [p["name"] for p in people.values()])

    rows = []
    for p in people.values():
        deel = deel_rates.get(p["user_id"])
        if deel:
            p["cost"] = p["hours"] * deel["rate"]
            p["cost_rate"] = deel["rate"]
            p["cost_rate_source"] = "deel"
            p["cost_currency"] = deel["currency"]
            p["hours_no_cost_rate"] = 0.0
        else:
            p["cost"] = p["harvest_cost"]
            p["hours_no_cost_rate"] = p["harvest_hours_no_cost_rate"]
            priced = p["hours"] - p["hours_no_cost_rate"]
            p["cost_rate"] = (p["cost"] / priced) if priced > 0 else None
            p["cost_rate_source"] = "harvest" if p["cost"] else "none"
            p["cost_currency"] = "USD"
        del p["harvest_cost"], p["harvest_hours_no_cost_rate"]

        paid = paid_by_name.get(p["name"]) or {"paid": 0.0, "open": 0.0}
        p["paid_amount"] = round(paid["paid"], 2)
        p["open_amount"] = round(paid["open"], 2)
        p["profit"] = p["revenue"] - p["cost"]
        p["collected_profit"] = p["paid_amount"] - p["cost"]
        p["margin"] = (p["profit"] / p["revenue"]) if p["revenue"] else None
        # Effective bill rate observed in the period (entry rates can vary by
        # project), averaged over the hours that actually carry a rate.
        rated_billable = p["billable_hours"] - p["billable_hours_no_bill_rate"]
        p["bill_rate"] = (p["revenue"] / rated_billable) if rated_billable > 0 else None
        p["projects"] = sorted(p["projects"].items(), key=lambda kv: -kv[1])
        for money in ("revenue", "cost", "profit", "collected_profit"):
            p[money] = round(p[money], 2)
        rows.append(p)
    rows.sort(key=lambda r: -(r["profit"]))

    totals = {
        "hours": round(sum(r["hours"] for r in rows), 2),
        "billable_hours": round(sum(r["billable_hours"] for r in rows), 2),
        "revenue": round(sum(r["revenue"] for r in rows), 2),
        "cost": round(sum(r["cost"] for r in rows), 2),
        "profit": round(sum(r["profit"] for r in rows), 2),
        "paid_amount": round(sum(r["paid_amount"] for r in rows), 2),
        "open_amount": round(sum(r["open_amount"] for r in rows), 2),
        "collected_profit": round(sum(r["collected_profit"] for r in rows), 2),
        "hours_no_cost_rate": round(sum(r["hours_no_cost_rate"] for r in rows), 2),
        "deel_rated": sum(1 for r in rows if r["cost_rate_source"] == "deel"),
    }
    totals["margin"] = (totals["profit"] / totals["revenue"]) if totals["revenue"] else None
    return {"from": date_from, "to": date_to, "people": rows, "totals": totals,
            "invoices": invoice_rows}


def provision_onboarding(request):
    """
    HTTP entry point (authenticated).
      GET  ?list=projects          -> JSON active Harvest projects (dashboard dropdown)
      GET  ?report=finance&from=YYYY-MM-DD&to=YYYY-MM-DD
                                   -> per-contractor profit report (dashboard /finance)
      POST {"person_id": "..."}    -> provision that hire now
      POST (empty body)            -> sweep all lifecycle=provisioning docs (scheduler)
    """
    try:
        if request.args.get("report") == "finance":
            date_from = request.args.get("from", "")
            date_to = request.args.get("to", "")
            if not (re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_from)
                    and re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_to)):
                return (json.dumps({"error": "from/to must be YYYY-MM-DD"}), 400,
                        {"Content-Type": "application/json"})
            harvest = HarvestClient(HARVEST_API_KEY, HARVEST_ACCOUNT_ID)
            report = finance_report(harvest, date_from, date_to)
            return json.dumps({"report": report}), 200, {"Content-Type": "application/json"}

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
