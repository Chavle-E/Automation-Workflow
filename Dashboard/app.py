"""
Onboarding dashboard — a PRIVATE, read-only view of the onboarding Firestore state.

Phase: read-only. Renders the roster and per-hire detail (identity, derived email,
confidence, lifecycle, account scaffold, audit trail) straight from the `onboarding`
Native-mode Firestore database that the backfill populates. It performs NO writes and
exposes NO Deel/secret access — it only reads Firestore. Approve/offboard actions are
a later phase.

Privacy: intended to run on Cloud Run behind Identity-Aware Proxy (IAP). IAP injects
the authenticated user in `X-Goog-Authenticated-User-Email`; when REQUIRE_IAP=1 the app
refuses any request lacking it (defence in depth so it is never accidentally public).
Locally (no IAP) leave REQUIRE_IAP unset.

Reuses OnboardingStore (single source of truth for the DB/collection names + doc shape);
firestore_store.py is copied in at deploy, mirroring the Cloud Functions build.
"""
import os
import logging

from flask import Flask, render_template, abort, request

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

REQUIRE_IAP = os.getenv("REQUIRE_IAP") in ("1", "true", "yes")

# Order the roster surfaces lifecycles in: things needing attention first.
LIFECYCLE_ORDER = ["needs_approval", "gated", "provisioning", "not_signed_in",
                   "active", "offboarding", "offboarded"]
CONFIDENCE_ORDER = {"low": 0, "medium": 1, "high": 2}

_store = None


def store() -> OnboardingStore:
    """Lazily construct the store so import never touches Firestore (and tests can run)."""
    global _store
    if _store is None:
        _store = OnboardingStore()
    return _store


def current_user() -> str:
    """The IAP-authenticated user, if present (header form: accounts.google.com:email)."""
    raw = request.headers.get("X-Goog-Authenticated-User-Email", "")
    return raw.split(":")[-1] if raw else ""


@app.before_request
def _enforce_iap():
    if REQUIRE_IAP and request.path != "/healthz" and not current_user():
        abort(403, "This dashboard must be accessed through Identity-Aware Proxy.")


def _summary(docs):
    s = {"total": len(docs), "gated": 0, "needs_approval": 0,
         "low_confidence": 0, "back_catalog": 0}
    for d in docs:
        life = d.get("lifecycle")
        if life in s:
            s[life] += 1
        if d.get("name_confidence") == "low":
            s["low_confidence"] += 1
        if d.get("backfill_back_catalog"):
            s["back_catalog"] += 1
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
    view = docs
    if lifecycle:
        view = [d for d in view if d.get("lifecycle") == lifecycle]
    if confidence:
        view = [d for d in view if d.get("name_confidence") == confidence]

    view = sorted(view, key=_sort_key)
    return render_template(
        "index.html",
        rows=view,
        summary=_summary(docs),
        lifecycle=lifecycle,
        confidence=confidence,
        user=current_user(),
    )


@app.route("/hire/<person_id>")
def hire(person_id):
    doc = store().get(person_id)
    if not doc:
        abort(404, f"No onboarding record for {person_id}")
    return render_template("detail.html", doc=doc, user=current_user())


@app.route("/healthz")
def healthz():
    return "ok", 200


if __name__ == "__main__":
    # Local dev server. On Cloud Run, gunicorn serves `app` (see Dockerfile).
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=True)