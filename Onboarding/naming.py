"""
Work-email derivation + name-confidence cross-check for onboarding.

Both functions reuse Payroll/matcher.py's UserMatcher.normalize_name() (lowercase,
strip accents, collapse whitespace) so naming behaves EXACTLY like the payroll path.

EMAIL RULE (David, confirmed):  username = <first given name>.<first surname>
    @thirstysprout.ai, derived from Deel's STRUCTURED first_name / last_name fields.
    - first given name = the FIRST token of first_name. Deel's first-name field
      sometimes holds two given names (e.g. "Syed Asad") -> take the first ("syed").
    - first surname    = the FIRST token of last_name. LATAM last-name fields hold
      two surnames (e.g. "Garcia Lopez") -> keep the paternal one ("garcia"),
      drop the maternal one.
    Normalization (lowercase, strip accents, alnum) reuses normalize_name.

    Worked examples (covered by test_naming.py):
        first="Tazeem"        last="Imran"        -> tazeem.imran
        first="Syed Asad"     last="Ali"          -> syed.ali   (two given names)
        first="Maria Fernanda" last="Garcia Lopez" -> maria.garcia (two surnames)

    Structured first_name/last_name come from /people. When Deel gives no structured
    last name we fall back to splitting a single full-name string (best effort;
    the result is always surfaced for human approval, never auto-created).

CONFIDENCE RULE (token-set of contract name vs personal-details name):
    one a subset of the other -> high
    partial token overlap     -> medium
    no shared tokens          -> low   (likely the wrong contract is attached —
                                        MUST be flagged, never auto-provisioned)
"""
import os
import re
import sys

# Reuse the SAME normalizer the payroll matcher uses. At deploy time cloudbuild
# copies matcher.py into this dir (single source of truth in Payroll/); locally
# we fall back to importing it from the sibling Payroll/ package.
try:
    from matcher import UserMatcher
except ImportError:  # local dev
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Payroll"))
    from matcher import UserMatcher

EMAIL_DOMAIN = "thirstysprout.ai"

_matcher = UserMatcher()


def _tokens(name: str):
    """Normalized whitespace tokens of a name (accents stripped, lowercased)."""
    if not name:
        return []
    return [t for t in _matcher.normalize_name(name).split() if t]


def _alnum(token: str) -> str:
    """Strip a token down to ascii alphanumerics (drops hyphens, apostrophes...)."""
    return re.sub(r"[^a-z0-9]", "", token)


def _first_token(name: str) -> str:
    """First normalized, alnum-only token of a name field ('' if none)."""
    for t in _tokens(name):
        a = _alnum(t)
        if a:
            return a
    return ""


def _email_from_fullname(full_name: str, domain: str):
    """
    Fallback split of a single full-name string (used only when Deel gives no
    structured last name). first + first-surname: drop middle names, and for
    4+ tokens keep the paternal (2nd-to-last) surname, dropping the maternal one.
    """
    tokens = [a for a in (_alnum(t) for t in _tokens(full_name)) if a]
    if not tokens:
        return None
    first = tokens[0]
    if len(tokens) >= 4:
        surname = tokens[-2]
    elif len(tokens) >= 2:
        surname = tokens[-1]
    else:
        surname = ""
    local = f"{first}.{surname}" if surname else first
    return f"{local}@{domain}"


def derive_email(first_name: str, last_name: str = None, domain: str = EMAIL_DOMAIN):
    """
    Derive the proposed work email (David's rule). Preferred call passes Deel's
    STRUCTURED fields: derive_email(first_name, last_name) -> first-given-name +
    "." + first-surname. Both fields may hold two names; we take the first token
    of each.

    Pass a single full-name string (last_name omitted) to fall back to a
    best-effort split of that string.

    Returns the email, or None if there are no usable tokens. This only PROPOSES
    the address — provisioning is a separate, human-approved step; we never
    auto-create a mailbox here.
    """
    if last_name is None:
        return _email_from_fullname(first_name, domain)

    first = _first_token(first_name)
    surname = _first_token(last_name)
    local = ".".join(p for p in (first, surname) if p)
    if not local:
        return None
    return f"{local}@{domain}"


def name_confidence(contract_name: str, personal_name: str) -> str:
    """
    Cross-check the contract name against the personal-details name.

    Returns "high" | "medium" | "low". "low" means the names share no tokens —
    likely the wrong contract is attached to this person; it MUST be flagged and
    never auto-provisioned.
    """
    a = set(_tokens(contract_name))
    b = set(_tokens(personal_name))

    if not a or not b:
        return "low"           # can't verify -> treat as low, force human review
    if a <= b or b <= a:
        return "high"          # one fully contained in the other
    if a & b:
        return "medium"        # partial overlap
    return "low"               # no shared tokens