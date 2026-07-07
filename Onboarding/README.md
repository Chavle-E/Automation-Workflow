# Onboarding / Offboarding Automation

Automates developer **onboarding** and **offboarding** end to end, replacing a
manual process. This README covers **Phase 1** (the Firestore store, email/name
logic, and an idempotent Deel→Firestore backfill). Provisioning, the Slack
approval endpoint, the activation poller, offboarding and the dashboard are later
phases and intentionally **not** built yet.

## State store — why Firestore (not SQLite-on-GCS)

Onboarding is **event-driven**: a Slack approval button, an activation poller, and
the Deel poll can all touch the same hire concurrently. The payroll
download→modify→upload SQLite-on-GCS pattern would clobber concurrent writes.
Firestore is serverless and concurrent-safe; every mutating op in
`firestore_store.py` runs in a **transaction**.

- Collection: `onboarding`, **one doc per hire, keyed by the Deel `person_id`**.

### Document shape

```
person_id; contract_name; personal_name; email; name_confidence (high|medium|low);
lifecycle (gated|needs_approval|provisioning|not_signed_in|active|offboarding|offboarded);
start_date; backfill_back_catalog (bool);
accounts: { zoho:{created_at,activated_at}, slack:{invited_at,joined_at},
            harvest:{invited_at,accepted_at,seat}, deel:{status} };
audit:[{action,by,at}]; created_at; updated_at
```

`lifecycle=active` ONLY once zoho/slack/harvest each have an activation timestamp
("onboarded"). Phase 1 leaves all activation timestamps **null**.

## The Deel-field finding (why backfill reads BOTH endpoints)

The onboarding-complete gate is **not** reliably available on `/contracts`:

- `/contracts.status` (`in_progress`, `completed`, `cancelled`, `user_cancelled`, …)
  only tells you the contract is signed/active — not whether the worker finished
  Deel onboarding (compliance/ID/tax docs) or has actually started. `/contracts`
  also has **no `start_date`**.
- The authoritative gate is **`/people` → `employments[].hiring_status`**
  (`active` = complete; `onboarding` / `onboarding_overdue` = incomplete),
  confirmed against the live API and already used by `onboarding_poll.py`.

So backfill **joins** the two on `employment.id == contract.id`:

| Need | Source |
|------|--------|
| onboarding gate, `start_date`, **structured `first_name`/`last_name`** (= email source), work_email | `/people` |
| company-typed contract `title` (confidence cross-check + display), `external_id` | `/contracts` |

A single `deel-api-key` token authenticates both endpoints.

## Email + confidence rules (`naming.py`, reuses `matcher.py`)

- **Email (David's confirmed rule):** `username = <first given name>.<first surname>`
  `@thirstysprout.ai`, from Deel's **structured `first_name`/`last_name`** fields.
  - First-name field sometimes holds two given names (e.g. `Syed Asad`) → take the
    **first** (`syed`).
  - Last-name field sometimes holds two LATAM surnames (e.g. `Garcia Lopez`) → keep
    the **first/paternal** one (`garcia`), drop the maternal.
  - Normalization (lowercase, strip accents, alnum) reuses `UserMatcher.normalize_name`.
  - `Tazeem`/`Imran` → `tazeem.imran` · `Syed Asad`/`Ali` → `syed.ali` ·
    `Maria Fernanda`/`Garcia Lopez` → `maria.garcia`.
  - When Deel exposes no structured last name, falls back to splitting a single
    full-name string (first token + first/paternal surname). Using the structured
    fields also fixes garbage contract titles (`Untitled Contract`, `Guga` → the
    person's real `shota.kvastiani`, `gurami.chavleshvili`).
- **`name_confidence`** = token-set of the company-typed contract `title` vs the
  personal-details name: subset → `high`, partial overlap → `medium`, no shared
  tokens → `low`. A `low` result means the wrong contract is likely attached — it
  is flagged and must never be auto-provisioned.

The backfill **proposes** every email; it never creates a mailbox. The proposed
address is surfaced for human approval (later phase) so edge cases are caught.

## Lifecycle assigned by the backfill

| Deel `hiring_status` | lifecycle | notes |
|----------------------|-----------|-------|
| `onboarding` / `onboarding_overdue` | `gated` | onboarding incomplete |
| `active` | `needs_approval` | onboarding complete, awaiting email/seat approval |
| anything else / no active employment | *(skipped)* | inactive, terminated, no contract |

`needs_approval` is the enum-valid name for the spec's "ready" (post-gate,
pre-provision). `upsert` only sets `lifecycle` when it is unset, so a re-run never
regresses a human/poller advance.

### `backfill_back_catalog`

Most current `active` workers are **pre-existing staff**, not in-flight new hires.
Using the `onboarding_poll.py` guard (recent start AND no work email yet), the
backfill flags pre-existing staff with `backfill_back_catalog=true` so the later
approval poller does not ping David about the whole company. It does not change
`lifecycle`.

## Idempotency

`OnboardingStore.upsert` is transactional and safe to run repeatedly/concurrently:
on first sight it creates the doc (account scaffold + `created_at` + audit);
on re-run it refreshes only the identity fields that actually changed, never
regresses `lifecycle`, and writes nothing when nothing changed (`"unchanged"`).

## Manual onboarding playbook (reference for provisioning — LATER phases)

Captured from a real manual onboarding (contractor "Asad Ali"). Not built yet;
this is the spec the provisioning phase must automate. All three steps are
idempotent-by-check in the real flow (skip if already done).

**Gate confirmation.** "Onboarding" on Deel = the 5-step worker flow; the contractor
was on 4/5 with only *document upload* left → this is exactly our `gated` lifecycle.
Provision only once `hiring_status` flips to `active`.

**Names + email.** Use Deel's **structured** `first_name`/`last_name` (personal
details), not the free-text contract "contractor name" (which was spelled
differently — "Asad ali" — and is unreliable; this is what `name_confidence=low`
catches). Domain: **always `@thirstysprout.ai`** for these contractors (the other
domain `choppingblock.ai` is not used here). Email username = `firstname.lastname`,
first given + first surname → `syed.ali` (the manual `syedasad.ali` predated this rule).

**1) Zoho (admin = billing@thirstysprout.com).** Create user:
- First/Last name = Deel `first_name`/`last_name` (e.g. `Syed Asad` / `Ali`).
- Username = `<local-part>@thirstysprout.ai`.
- Password = auto-generate satisfying Zoho rules: ≥8 chars, ≥1 lower, ≥1 upper,
  ≥1 number, ≥1 special.
- Role = **User** (never Administrator for onboarding contractors).
- ✅ "Send credentials via email" → the contractor's **personal email** (the one
  used to sign the Deel contract, from Deel personal details, e.g.
  `syed.mob25@gmail.com`).
- ✅ "Force user to change password on first login".

**2) Slack (invite to "ThirstySprout – Hire Ai Talent" workspace).**
- Invite the contractor's **work email** (the Zoho address just created).
- Add to channels: `#announcements`, `#thirstysprout-projects-and-off-topic-stuff`
  (confirm full list with David).
- **Decision:** send the invite immediately, do **not** wait for the user to log in
  to Zoho — the invite then sits in their new inbox. (Open: how to highlight it so
  it isn't lost.) `Copy Invite Link` gives a 30-day link as an alternative.

**3) Harvest (admin = billing@thirstysprout.com, Team → Invite person).**
- **Seat constraint:** plan seats are capped (23/23 used). Each new hire needs a
  `+1` seat first. (Open: can automation raise the seat count via API, or must it
  just notify David? — needs API-docs check.)
- First/Last = Deel names; Work email = Zoho address; Employee ID = blank;
  Type = **Contractor**; Role optional; Capacity = 40h/wk default.
- **Default billable rate = REQUIRED** — consumed by the Invoicing automation; this
  is the value David is asked to provide at approval time. Cost rate (what we pay,
  = Deel `payment.rate`) optional but nice for margin.
- Permissions = **Member** (submit/edit own timesheets only).
- Project assignment is a separate step, often unknown at invite time → may need a
  manual/later follow-up (create the project if it doesn't exist).

**Final step (open):** where to send the onboarding guide — personal email, work
email, or Slack. To be decided with David.

### Data the store will need for provisioning (not captured in Phase 1 backfill)
`personal_email` (Zoho credential delivery), `billable_rate` (Harvest + invoicing),
`cost_rate`. Flagged for the approval/provisioning phase.

## Files

| File | Purpose |
|------|---------|
| `firestore_store.py` | `OnboardingStore`: `get` / `upsert` / `list_by_lifecycle` / `append_audit` |
| `naming.py` | `derive_email`, `name_confidence` (reuse `matcher.py`) |
| `test_naming.py` | unit tests — `python test_naming.py` or `pytest` |
| `main.py` | `backfill_onboarding` HTTP function + `run_backfill(dry_run=…)` |
| `onboarding_poll.py`, `onboarding_digest.py` | pre-existing candidate selection + Slack digest |

`deel_client.py` and `matcher.py` live in `Payroll/` (single source of truth) and
are **copied into this dir by cloudbuild at deploy** so `--source=Onboarding`
packages them. Locally they're imported from the sibling `Payroll/` dir.

## Run locally

```bash
# Unit tests (no network)
python test_naming.py

# Dry-run backfill against live Deel — reads only, writes nothing to Firestore
python main.py
```

`run_backfill(dry_run=True)` needs only `DEEL_API_KEY` (from `../.env`). A real run
(`dry_run=False`) additionally needs `google-cloud-firestore` and Firestore
credentials, so it runs in Cloud Functions, not locally.

## Deploy

Added as step 6 in `cloudbuild.yaml` (`backfill_onboarding`, secret `deel-api-key`)
plus a daily `onboarding-backfill` Cloud Scheduler job (07:00 Europe/Tbilisi) that
reconciles Firestore from Deel. Trigger a one-off preview with `?dry_run=1`.

**Prereqs:** Firestore in **Native mode** enabled in the project, and the function's
runtime service account granted `roles/datastore.user`.

## Decisions (confirmed by David)

- **Email username rule.** `firstname.lastname` = **first** given name + **first**
  (paternal) surname → `syed.ali`, `maria.garcia`. This is the current
  `naming.derive_email`. (The manual `syedasad.ali` predated this rule, superseded.)
- **Legal name source of truth.** Always Deel's **personal-details** `first_name` /
  `last_name`, never the free-text contract contractor name.
- **Provisioning trigger.** Fire **only when Deel onboarding is FULLY complete**
  (`hiring_status` == `active`) — never on contract create/sign — so we never set up
  someone who never finishes. While incomplete (`gated`), the contractor is
  **reminded they will not be paid** until onboarding is complete.
- **Harvest seats.** Automation **cannot** change seat count (billing). On a new hire
  it **notifies both the user and David** to add a paid `+1` seat. David also wants
  **availability management**: flag empty/unused seats so we can downgrade and not pay
  for empty seats. (So: monitor seat availability + notify; never auto-charge.)
- **Slack default channels.** `#announcements` and
  `#thirstysprout-projects-and-off-topic-stuff` — that's the full set for now.
- **Onboarding-guide delivery.** Send to **both** the work email and the personal email.
- **Billable rate + project = approval form (resolved).** Not pulled from Deel. At the
  approval/provisioning step the human fills in a form supplying the **billable rate**
  and the **Harvest project** the contractor should be added to. These two inputs feed
  the Harvest invite (billable rate → invoicing) and the project assignment.
- **Offboarding = in scope (resolved).** Build it. Triggered from the **dashboard**
  (a click): on trigger, revoke the contractor's access to **all four contractor tools**
  — Slack, Harvest, Deel, Zoho. Mirror of the provisioning flow, in reverse.

## Still open

- **`new_hiring_status`.** Deel `/people` also exposes a newer `new_hiring_status`
  alongside `hiring_status`; we gate on the proven `hiring_status`. Confirm which Deel
  considers canonical going forward.
- **Offboarding revoke mechanics (per tool).** Which API/action deactivates vs deletes
  in each of Slack / Harvest / Deel / Zoho, and whether Deel contract termination is
  in-scope or done manually. To be scoped when the offboarding phase is built.
