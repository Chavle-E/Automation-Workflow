"""
OnboardingStore — Firestore state store for onboarding/offboarding.

State store = FIRESTORE (collection `onboarding`, one doc per hire, keyed by the
Deel person_id). Chosen over the payroll SQLite-on-GCS pattern because onboarding
is event-driven (Slack button, activation poller, Deel poll can overlap) and a
download->modify->upload cycle would clobber concurrent writes. Firestore is
serverless + concurrent-safe; every mutating op here runs in a transaction.

Mirrors the spirit of Payroll/database.py's verification_status approval gate:
a doc is created in a non-active lifecycle and only advances when a human/poller
confirms — backfill never fabricates activation or regresses a doc.

Public API (Phase 1):
    get(person_id)                 -> dict | None
    upsert(person_id, ...)         -> "created" | "updated" | "unchanged"  (idempotent)
    list_by_lifecycle(lifecycle)   -> list[dict]
    append_audit(person_id, ...)   -> the audit entry
"""
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from google.cloud import firestore

COLLECTION = "onboarding"
# Named Firestore database (NOT the project's `(default)`, which is DATASTORE_MODE
# and cannot serve Native-mode reads/writes). Created Native in europe-west1.
DATABASE = "onboarding"

# Fields that backfill derives from Deel (the source of truth) and may safely
# refresh on every run. lifecycle/accounts/created_at/audit are NOT in here —
# they are lifecycle state and must never be clobbered by a re-run.
IDENTITY_FIELDS = (
    "contract_name", "personal_name", "email", "name_confidence",
    "start_date", "backfill_back_catalog",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _empty_accounts() -> Dict:
    """Account scaffold with all activation timestamps null (Phase 1)."""
    return {
        "zoho": {"created_at": None, "activated_at": None},
        "slack": {"invited_at": None, "joined_at": None},
        "harvest": {"invited_at": None, "accepted_at": None, "seat": None},
        "deel": {"status": None},
    }


class OnboardingStore:
    def __init__(self, collection: str = COLLECTION, database: str = DATABASE,
                 client: Optional[firestore.Client] = None):
        self.client = client or firestore.Client(database=database)
        self.collection = collection

    def _ref(self, person_id):
        return self.client.collection(self.collection).document(str(person_id))

    # ---- reads --------------------------------------------------------------

    def get(self, person_id) -> Optional[Dict]:
        snap = self._ref(person_id).get()
        return snap.to_dict() if snap.exists else None

    def list_by_lifecycle(self, lifecycle: str) -> List[Dict]:
        query = self.client.collection(self.collection).where("lifecycle", "==", lifecycle)
        return [snap.to_dict() for snap in query.stream()]

    # ---- writes (all transactional) -----------------------------------------

    def upsert(self, person_id, *, identity: Dict, initial_lifecycle: str,
               deel_status: Optional[str] = None, audit_action: str = "backfill") -> str:
        """
        Idempotent upsert keyed by person_id.

        - First time: creates the doc with the account scaffold (null timestamps),
          the given initial_lifecycle, created_at, and an audit entry.
        - Re-run: refreshes only IDENTITY_FIELDS that actually changed. lifecycle
          is set ONLY when still unset, so a human/poller advance (e.g. ->provisioning)
          is never regressed. If nothing changed, performs no write ("unchanged").

        Safe to run repeatedly and concurrently.
        """
        identity = {k: v for k, v in identity.items() if k in IDENTITY_FIELDS}
        ref = self._ref(person_id)
        client = self.client

        @firestore.transactional
        def _txn(txn) -> str:
            snap = ref.get(transaction=txn)
            now = _now()

            if not snap.exists:
                accounts = _empty_accounts()
                if deel_status is not None:
                    accounts["deel"]["status"] = deel_status
                doc = {
                    "person_id": str(person_id),
                    **identity,
                    "lifecycle": initial_lifecycle,
                    "accounts": accounts,
                    "audit": [{"action": f"{audit_action}_created", "by": "system", "at": now}],
                    "created_at": now,
                    "updated_at": now,
                }
                txn.set(ref, doc)
                return "created"

            existing = snap.to_dict()
            updates = {k: v for k, v in identity.items() if existing.get(k) != v}

            # lifecycle only on first sight — never regress human/poller progress
            if not existing.get("lifecycle"):
                updates["lifecycle"] = initial_lifecycle
            if deel_status is not None and (existing.get("accounts") or {}).get("deel", {}).get("status") != deel_status:
                updates["accounts.deel.status"] = deel_status

            if not updates:
                return "unchanged"

            audit = existing.get("audit", [])
            audit.append({"action": f"{audit_action}_updated", "by": "system", "at": now})
            updates["audit"] = audit
            updates["updated_at"] = now
            txn.update(ref, updates)
            return "updated"

        result = _txn(client.transaction())
        logging.info(f"upsert {person_id}: {result}")
        return result

    def append_audit(self, person_id, action: str, by: str = "system") -> Dict:
        """Append an audit entry transactionally. Raises KeyError if doc missing."""
        entry = {"action": action, "by": by, "at": _now()}
        ref = self._ref(person_id)

        @firestore.transactional
        def _txn(txn):
            snap = ref.get(transaction=txn)
            if not snap.exists:
                raise KeyError(f"onboarding doc not found for person_id={person_id}")
            audit = snap.to_dict().get("audit", [])
            audit.append(entry)
            txn.update(ref, {"audit": audit, "updated_at": _now()})

        _txn(self.client.transaction())
        return entry