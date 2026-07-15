"""
Onboarding dashboard — a PRIVATE view of the onboarding Firestore state, now with
the approval flow.

Reads render the roster and per-hire detail straight from the `onboarding` Native
Firestore database. Writes are limited to the approval flow and go through
OnboardingStore's transactional guards:

  - POST /hire/<id>/approve     approval form (billable rate, cost rate, personal
                                email, Harvest project) -> lifecycle=provisioning,
                                then invokes the provisioning function.
  - POST /hire/<id>/mark/<step> record a manual step (Zoho created / Slack or
                                Harvest invited by hand) and re-invoke provisioning.
  - POST /hire/<id>/retry       re-invoke provisioning for a stuck hire.

The dashboard stays SECRET-FREE: it holds no Deel/Slack/Harvest keys. All tool
calls live in the `provision_onboarding` Cloud Function (PROVISION_URL), which the
dashboard invokes with an OIDC identity token minted for its own service account;
the Harvest-project dropdown is proxied from that function too. If the function
is unreachable the approval is still safely recorded in Firestore — the daily
sweep (or the Retry button) picks it up.

Privacy: intended to run on Cloud Run behind IAP (REQUIRE_IAP=1 refuses requests
missing the IAP header). While the team is on `gcloud run services proxy`, the
audit identity falls back to the email claim of the caller's own identity token —
Cloud Run IAM has already verified it upstream.
"""
import base64
import json
import logging
import os

import requests
from flask import Flask, render_template, abort, request, redirect, url_for, flash

# Shared module: copied from ../Onboarding at deploy (single source of truth for the
# Firestore database + collection). Locally, fall back to the sibling package.
try:
    from firestore_store import OnboardingStore
except ImportError:  # local dev
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Onboarding"))
    from firestore_store import OnboardingStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = Flask(__name__)
# Sessions are only used for flash messages; a per-instance random key is fine.
app.secret_key = os.getenv("FLASK_SECRET_KEY") or os.urandom(32)

REQUIRE_IAP = os.getenv("REQUIRE_IAP") in ("1", "true", "yes")
PROVISION_URL = os.getenv("PROVISION_URL", "")

# Order the roster surfaces lifecycles in: things needing attention first.
LIFECYCLE_ORDER = ["needs_approval", "gated", "provisioning", "not_signed_in",
                   "active", "offboarding", "offboarded"]
CONFIDENCE_ORDER = {"low": 0, "medium": 1, "high": 2}

# Manual steps a human may record from the detail page -> (tool, field, audit action).
MANUAL_STEPS = {
    "zoho_created": ("zoho", "created_at", "zoho_created_manual"),
    "slack_invited": ("slack", "invited_at", "slack_invited_manual"),
    "harvest_invited": ("harvest", "invited_at", "harvest_invited_manual"),
}

_store = None


def store() -> OnboardingStore:
    """Lazily construct the store so import never touches Firestore (and tests can run)."""
    global _store
    if _store is None:
        _store = OnboardingStore()
    return _store


def current_user() -> str:
    """
    The authenticated caller, for the audit trail. Prefer the IAP header
    (accounts.google.com:email); fall back to the email claim of the Bearer
    identity token that `gcloud run services proxy` forwards — Cloud Run IAM
    already verified its signature before the request reached us.
    """
    raw = request.headers.get("X-Goog-Authenticated-User-Email", "")
    if raw:
        return raw.split(":")[-1]
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        try:
            payload = auth.split(".")[1]
            claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
            return claims.get("email", "")
        except Exception:
            pass
    return ""


@app.before_request
def _enforce_iap():
    if REQUIRE_IAP and request.path != "/healthz" and not current_user():
        abort(403, "This dashboard must be accessed through Identity-Aware Proxy.")


# ---- provisioning function client -------------------------------------------

def _id_token(audience: str) -> str:
    """OIDC token for the provisioning function, minted from our own runtime SA."""
    from google.oauth2 import id_token as google_id_token
    from google.auth.transport.requests import Request as GoogleAuthRequest
    return google_id_token.fetch_id_token(GoogleAuthRequest(), audience)


def call_provisioning(params=None, payload=None, timeout=60):
    """
    Invoke provision_onboarding. Returns (ok, data-or-error-message). Never raises:
    the Firestore write has already happened, so a dead function must degrade to
    'recorded, will be retried', not to a user-facing 500.
    """
    if not PROVISION_URL:
        return False, "PROVISION_URL is not configured"
    try:
        headers = {"Authorization": f"Bearer {_id_token(PROVISION_URL)}"}
        if payload is not None:
            resp = requests.post(PROVISION_URL, params=params, json=payload,
                                 headers=headers, timeout=timeout)
        else:
            resp = requests.get(PROVISION_URL, params=params, headers=headers, timeout=timeout)
        body = resp.json() if resp.headers.get("Content-Type", "").startswith("application/json") else {}
        if resp.status_code != 200:
            return False, body.get("error", f"HTTP {resp.status_code}")
        return True, body
    except Exception as e:
        logging.warning(f"provisioning function unreachable: {e}")
        return False, str(e)


def harvest_projects():
    """Active Harvest projects for the approve form (proxied; [] if unreachable)."""
    ok, data = call_provisioning(params={"list": "projects"}, timeout=30)
    return data.get("projects", []) if ok else []


# ---- read views ---------------------------------------------------------------

def _summary(docs):
    s = {"total": len(docs), "gated": 0, "needs_approval": 0, "provisioning": 0,
         "low_confidence": 0, "back_catalog": 0, "new_hire": 0}
    for d in docs:
        life = d.get("lifecycle")
        if life in s:
            s[life] += 1
        if d.get("name_confidence") == "low":
            s["low_confidence"] += 1
        if d.get("backfill_back_catalog"):
            s["back_catalog"] += 1
        else:
            s["new_hire"] += 1
    return s


def _sort_key(d):
    life = d.get("lifecycle") or ""
    life_rank = LIFECYCLE_ORDER.index(life) if life in LIFECYCLE_ORDER else len(LIFECYCLE_ORDER)
    conf_rank = CONFIDENCE_ORDER.get(d.get("name_confidence"), 3)
    return (life_rank, conf_rank, (d.get("email") or ""))


@app.route("/")
def roster():
    docs = store().list_all()

    # Optional filters via query string.
    lifecycle = request.args.get("lifecycle") or ""
    confidence = request.args.get("confidence") or ""
    new_only = request.args.get("new") or ""
    view = docs
    if lifecycle:
        view = [d for d in view if d.get("lifecycle") == lifecycle]
    if confidence:
        view = [d for d in view if d.get("name_confidence") == confidence]
    if new_only:
        view = [d for d in view if not d.get("backfill_back_catalog")]

    view = sorted(view, key=_sort_key)
    return render_template(
        "index.html",
        rows=view,
        summary=_summary(docs),
        lifecycle=lifecycle,
        confidence=confidence,
        new_only=new_only,
        user=current_user(),
    )


@app.route("/hire/<person_id>")
def hire(person_id):
    doc = store().get(person_id)
    if not doc:
        abort(404, f"No onboarding record for {person_id}")
    approvable = doc.get("lifecycle") == "needs_approval"
    return render_template(
        "detail.html",
        doc=doc,
        user=current_user(),
        approvable=approvable,
        projects=harvest_projects() if approvable else [],
        manual_steps=MANUAL_STEPS,
    )


# ---- approval flow (writes) ----------------------------------------------------

@app.route("/hire/<person_id>/approve", methods=["POST"])
def approve(person_id):
    form = request.form
    errors = []
    try:
        billable_rate = float(form.get("billable_rate", ""))
        if billable_rate <= 0:
            errors.append("billable rate must be > 0")
    except ValueError:
        billable_rate = None
        errors.append("billable rate is required (a number)")

    cost_rate = None
    if form.get("cost_rate", "").strip():
        try:
            cost_rate = float(form["cost_rate"])
        except ValueError:
            errors.append("cost rate must be a number")

    personal_email = form.get("personal_email", "").strip()
    if "@" not in personal_email:
        errors.append("personal email is required (Zoho credentials are sent there)")

    project_id = form.get("harvest_project_id", "").strip()
    if not project_id:
        errors.append("a Harvest project is required")

    if errors:
        flash("Not approved: " + "; ".join(errors), "error")
        return redirect(url_for("hire", person_id=person_id))

    by = current_user() or "unknown"
    try:
        store().request_provision(
            person_id, by=by, billable_rate=billable_rate, cost_rate=cost_rate,
            personal_email=personal_email, harvest_project_id=project_id,
            harvest_project_name=form.get("harvest_project_name", ""),
        )
    except (ValueError, KeyError) as e:
        flash(f"Not approved: {e}", "error")
        return redirect(url_for("hire", person_id=person_id))

    ok, data = call_provisioning(payload={"person_id": person_id})
    if ok:
        flash("Approved — provisioning started (Zoho instructions sent to the operators).", "ok")
    else:
        flash(f"Approved and recorded; provisioning function not reached ({data}) — "
              "use Retry or wait for the daily sweep.", "warn")
    return redirect(url_for("hire", person_id=person_id))


@app.route("/hire/<person_id>/mark/<step>", methods=["POST"])
def mark_step(person_id, step):
    if step not in MANUAL_STEPS:
        abort(404)
    tool, field, action = MANUAL_STEPS[step]
    by = current_user() or "unknown"
    from firestore_store import _now
    try:
        store().record_account_event(person_id, tool, {field: _now()}, action=action, by=by)
    except KeyError as e:
        flash(str(e), "error")
        return redirect(url_for("hire", person_id=person_id))
    # A newly created Zoho user unblocks the Slack/Harvest invites — run them now.
    ok, _data = call_provisioning(payload={"person_id": person_id})
    flash(f"Recorded {step.replace('_', ' ')}." +
          ("" if ok else " (Provisioning function not reached — Retry later.)"),
          "ok" if ok else "warn")
    return redirect(url_for("hire", person_id=person_id))


@app.route("/hire/<person_id>/retry", methods=["POST"])
def retry(person_id):
    ok, data = call_provisioning(payload={"person_id": person_id})
    if ok:
        outcome = data.get("results", {}).get(str(person_id), {})
        flash("Provisioning ran: " + (json.dumps(outcome) if outcome else "no pending steps."), "ok")
    else:
        flash(f"Provisioning function not reached: {data}", "error")
    return redirect(url_for("hire", person_id=person_id))


@app.route("/healthz")
def healthz():
    return "ok", 200


if __name__ == "__main__":
    # Local dev server. On Cloud Run, gunicorn serves `app` (see Dockerfile).
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=True)
