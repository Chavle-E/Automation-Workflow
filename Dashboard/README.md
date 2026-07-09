# Onboarding Dashboard

A **private** web view of the onboarding state, plus the **approval flow**. It renders
the roster and per-hire detail (identity, derived email, name confidence, lifecycle,
the account scaffold, and the audit trail) straight from the `onboarding` Native-mode
Firestore database that `../Onboarding` backfills, and lets an operator approve a hire
for provisioning.

**The dashboard stays secret-free** — it holds no Deel/Slack/Harvest keys. Its only
writes are the guarded approval-flow transactions in `OnboardingStore`; all tool calls
(Zoho instructions, Slack invite, Harvest contractor + project) live in the
`provision_onboarding` Cloud Function, which the dashboard invokes with an OIDC token
minted for its own service account. Offboarding actions are the next phase.

## Stack

- **Flask + gunicorn on Cloud Run** (consistent with the repo's existing Flask usage).
- Reads/writes Firestore via **`OnboardingStore`** (`firestore_store.py`, the single
  source of truth for the DB/collection names + doc shape). At deploy it is **copied in
  from `../Onboarding`**; locally the app falls back to importing it from the sibling dir.

## Privacy — IAP / private Cloud Run

The service is `--no-allow-unauthenticated`; until IAP is set up, view it through the
caller's own identity:

```bash
gcloud run services proxy onboarding-dashboard --region=europe-west1 \
  --project=charged-sector-427921-v5   # then http://localhost:8080
```

Audit identity: with IAP, the `X-Goog-Authenticated-User-Email` header; on the proxy
path, the email claim of the caller's identity token (Cloud Run IAM verified it
upstream). `REQUIRE_IAP=1` additionally refuses any request missing the IAP header —
set it only once real IAP is in front (it breaks the proxy path). `/healthz` is exempt.

## Routes

| Route | Purpose |
|-------|---------|
| `GET /` | roster; summary cards + filters (`?lifecycle=`, `?confidence=`) |
| `GET /hire/<person_id>` | full record; approve form when `needs_approval` (never for back-catalog) |
| `POST /hire/<id>/approve` | record billable rate / cost rate / personal email / Harvest project → `provisioning`, invoke the function |
| `POST /hire/<id>/mark/<step>` | record a manual step (`zoho_created`, `slack_invited`, `harvest_invited`) and re-invoke |
| `POST /hire/<id>/retry` | re-invoke provisioning for a stuck hire |
| `GET /healthz` | Cloud Run health check (unauthenticated) |

If the provisioning function is unreachable, approvals/marks are still recorded in
Firestore and flagged in the UI — the daily `onboarding-provision-sweep` scheduler (or
the Retry button) finishes the job.

## Run locally

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
export GOOGLE_CLOUD_PROJECT=charged-sector-427921-v5
gcloud auth application-default login   # one-time, if not already done
python app.py     # http://localhost:8080  (REQUIRE_IAP unset for local dev)
```

Locally the provisioning call needs `PROVISION_URL` plus ADC that can mint ID tokens;
without it the approve form still records to Firestore and shows the "will be retried"
warning.

## Deploy (Cloud Run)

```bash
# 1. Copy the shared store in (single source of truth), then deploy from source.
cp ../Onboarding/firestore_store.py .
gcloud run deploy onboarding-dashboard \
  --source . \
  --region europe-west1 \
  --no-allow-unauthenticated \
  --set-env-vars PROVISION_URL=$(gcloud functions describe provision_onboarding \
      --region=europe-west1 --format='value(url)') \
  --project charged-sector-427921-v5

# 2. Runtime SA needs roles/datastore.user (already granted) and invoker on
#    provision_onboarding (cloudbuild step 6b grants the compute SA).
# 3. IAP later: put IAP in front, grant users roles/iap.httpsResourceAccessor,
#    then add --set-env-vars REQUIRE_IAP=1.
```
