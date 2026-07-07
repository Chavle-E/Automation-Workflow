# Onboarding Dashboard (read-only)

A **private, read-only** web view of the onboarding state. It renders the roster and
per-hire detail (identity, derived email, name confidence, lifecycle, the account
scaffold, and the audit trail) straight from the `onboarding` Native-mode Firestore
database that `../Onboarding` backfills.

**It performs no writes and touches no Deel/secrets — it only reads Firestore.**
Approve / offboard actions belong to later phases and are intentionally absent.

## Stack

- **Flask + gunicorn on Cloud Run** (consistent with the repo's existing Flask usage).
- Reads Firestore via **`OnboardingStore`** (`firestore_store.py`, the single source of
  truth for the DB/collection names + doc shape). At deploy it is **copied in from
  `../Onboarding`**, mirroring how the Cloud Functions copy their shared modules; locally
  the app falls back to importing it from the sibling `Onboarding/` dir.

## Privacy — Identity-Aware Proxy (IAP)

The service must sit **behind IAP** so only signed-in org users reach it; it is never
served to `allUsers`. IAP injects the caller in `X-Goog-Authenticated-User-Email`, which
the app shows in the header. Set **`REQUIRE_IAP=1`** in the Cloud Run env so the app
returns `403` for any request missing that header (defence in depth — it refuses to serve
if IAP is ever misconfigured or bypassed). `/healthz` is exempt.

## Routes

| Route | Purpose |
|-------|---------|
| `GET /` | roster; summary cards + filters (`?lifecycle=`, `?confidence=`) |
| `GET /hire/<person_id>` | full record for one hire |
| `GET /healthz` | Cloud Run health check (unauthenticated) |

## Run locally

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
# Reads the real onboarding Firestore DB via Application Default Credentials:
export GOOGLE_CLOUD_PROJECT=charged-sector-427921-v5
gcloud auth application-default login   # one-time, if not already done
python app.py     # http://localhost:8080  (REQUIRE_IAP unset for local dev)
```

## Deploy (Cloud Run + IAP)

```bash
# 1. Copy the shared store in (single source of truth), then deploy from source.
cp ../Onboarding/firestore_store.py .
gcloud run deploy onboarding-dashboard \
  --source . \
  --region europe-west1 \
  --no-allow-unauthenticated \
  --set-env-vars REQUIRE_IAP=1 \
  --project charged-sector-427921-v5

# 2. Grant the Cloud Run runtime service account Firestore read access
#    (roles/datastore.user — same role the backfill uses).
# 3. Put IAP in front of the service and grant your org users
#    roles/iap.httpsResourceAccessor. (Console: Security → Identity-Aware Proxy.)
```

The runtime service account also needs `roles/datastore.user` to read the `onboarding`
database. Keep the service **`--no-allow-unauthenticated`**; IAP handles who gets in.

## Not in this phase

Approve/offboard buttons, editing, and anything that mutates state. This view exists to
give a trustworthy picture of the onboarding pipeline before those actions are built.
