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

Provisioning phase:
    request_provision(person_id, ...)    -> the stored provision_request (dashboard approve)
    record_account_event(person_id, ...) -> "updated" | "unchanged"  (provisioning function)
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

    def list_all(self) -> List[Dict]:
        """Every onboarding doc (used by the read-only dashboard)."""
        return [snap.to_dict() for snap in self.client.collection(self.collection).stream()]

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

            # lifecycle only on first sight — never regress human/poller progress.
            # Exception: gated -> needs_approval is the natural forward transition
            # (their Deel status advanced since we first saw them).
            if not existing.get("lifecycle"):
                updates["lifecycle"] = initial_lifecycle
            elif (existing.get("lifecycle") == "gated"
                    and initial_lifecycle == "needs_approval"):
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

    def request_provision(self, person_id, *, by: str, billable_rate: Optional[float],
                          personal_email: str, harvest_project_id=None,
                          harvest_project_name: str = "",
                          cost_rate: Optional[float] = None) -> Dict:
        """
        Record a human approval and advance needs_approval -> provisioning.

        Stores the approval-form inputs (David's decision: billable rate + Harvest
        project come from a human at approval time, personal email is where Zoho
        credentials go) under `provision_request` so the provisioning function has
        everything it needs, with `by` = the approving human for the audit trail.

        Guards (raise ValueError, nothing written):
          - doc must exist and be lifecycle == "needs_approval"
          - back-catalog docs (pre-existing staff) are never provisionable
        """
        # harvest_project_id=None means "No Harvest": the Harvest step is skipped
        # (time tracked outside Harvest), so billable_rate may be None too.
        if harvest_project_id and billable_rate is None:
            raise ValueError("billable_rate is required when a Harvest project is set")
        request_doc = {
            "billable_rate": float(billable_rate) if billable_rate is not None else None,
            "cost_rate": float(cost_rate) if cost_rate is not None else None,
            "personal_email": personal_email,
            "harvest_project_id": str(harvest_project_id) if harvest_project_id else None,
            "harvest_project_name": harvest_project_name,
            "by": by,
            "at": _now(),
        }
        ref = self._ref(person_id)

        @firestore.transactional
        def _txn(txn):
            snap = ref.get(transaction=txn)
            if not snap.exists:
                raise ValueError(f"no onboarding doc for person_id={person_id}")
            doc = snap.to_dict()
            if doc.get("backfill_back_catalog"):
                raise ValueError("back-catalog records cannot be provisioned")
            if doc.get("lifecycle") != "needs_approval":
                raise ValueError(f"lifecycle is {doc.get('lifecycle')!r}, expected 'needs_approval'")
            audit = doc.get("audit", [])
            audit.append({"action": "provision_requested", "by": by, "at": request_doc["at"]})
            txn.update(ref, {
                "lifecycle": "provisioning",
                "provision_request": request_doc,
                "audit": audit,
                "updated_at": request_doc["at"],
            })

        _txn(self.client.transaction())
        logging.info(f"request_provision {person_id}: approved by {by}")
        return request_doc

    # All three invites out -> the hire is just waiting on sign-ins. Harvest counts
    # as done when invited OR explicitly skipped (approved with "No Harvest").
    _INVITE_FIELDS = (("zoho", ("created_at",)), ("slack", ("invited_at",)),
                      ("harvest", ("invited_at", "skipped_at")))

    def record_account_event(self, person_id, tool: str, fields: Dict, *,
                             action: str, by: str = "system") -> str:
        """
        Set accounts.<tool>.<field> values (audit-logged, transactional). Only fields
        whose value actually changes are written ("unchanged" if none do), so the
        provisioning function can re-run safely.

        Auto-advance: when a write completes the invite set (zoho created, slack +
        harvest invited) while lifecycle == "provisioning", the doc moves to
        "not_signed_in". Activation to "active" stays with the (later) poller.
        """
        ref = self._ref(person_id)

        @firestore.transactional
        def _txn(txn) -> str:
            snap = ref.get(transaction=txn)
            if not snap.exists:
                raise KeyError(f"onboarding doc not found for person_id={person_id}")
            doc = snap.to_dict()
            accounts = doc.get("accounts") or {}
            current = accounts.get(tool) or {}

            updates = {f"accounts.{tool}.{k}": v for k, v in fields.items()
                       if current.get(k) != v}
            if not updates:
                return "unchanged"

            now = _now()
            merged = dict(accounts)
            merged[tool] = {**current, **fields}
            if (doc.get("lifecycle") == "provisioning"
                    and all(any((merged.get(t) or {}).get(f) for f in fields)
                            for t, fields in self._INVITE_FIELDS)):
                updates["lifecycle"] = "not_signed_in"

            audit = doc.get("audit", [])
            audit.append({"action": action, "by": by, "at": now})
            updates["audit"] = audit
            updates["updated_at"] = now
            txn.update(ref, updates)
            return "updated"

        result = _txn(self.client.transaction())
        logging.info(f"record_account_event {person_id} {tool} {action}: {result}")
        return result

    def advance_to_active(self, person_id, by: str = "activation_poller") -> bool:
        """
        not_signed_in -> active, transactionally. Returns True only on the
        transition that actually flips it (so the caller notifies exactly once);
        False if it was already active or isn't ready. Never regresses other
        lifecycles.
        """
        ref = self._ref(person_id)

        @firestore.transactional
        def _txn(txn) -> bool:
            snap = ref.get(transaction=txn)
            if not snap.exists:
                raise KeyError(f"onboarding doc not found for person_id={person_id}")
            if snap.to_dict().get("lifecycle") != "not_signed_in":
                return False
            now = _now()
            audit = snap.to_dict().get("audit", [])
            audit.append({"action": "activated", "by": by, "at": now})
            txn.update(ref, {"lifecycle": "active", "audit": audit, "updated_at": now})
            return True

        advanced = _txn(self.client.transaction())
        if advanced:
            logging.info(f"advance_to_active {person_id}: now active")
        return advanced

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