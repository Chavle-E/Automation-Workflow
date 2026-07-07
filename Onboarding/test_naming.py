"""
Unit tests for naming.py. Runnable two ways:
    python test_naming.py      # plain asserts, prints a summary
    pytest test_naming.py      # standard collection
"""
from naming import derive_email, name_confidence


# ---- derive_email: David's rule on STRUCTURED first_name / last_name ---------

def test_email_structured_simple():
    assert derive_email("Tazeem", "Imran") == "tazeem.imran@thirstysprout.ai"


def test_email_structured_two_given_names():
    # David: first-name field holds two given names -> take the first.
    assert derive_email("Syed Asad", "Ali") == "syed.ali@thirstysprout.ai"


def test_email_structured_two_surnames():
    # LATAM last-name field holds two surnames -> keep the paternal (first) one.
    assert derive_email("Maria Fernanda", "Garcia Lopez") == "maria.garcia@thirstysprout.ai"


def test_email_structured_strips_accents():
    assert derive_email("José", "Peña") == "jose.pena@thirstysprout.ai"


def test_email_structured_missing_returns_none():
    assert derive_email("", "") is None
    assert derive_email("   ", "  ") is None


# ---- derive_email: single-string FALLBACK (no structured last name) ----------

def test_email_fullname_fallback_two_token():
    assert derive_email("Tazeem Imran") == "tazeem.imran@thirstysprout.ai"


def test_email_fullname_fallback_latam():
    # 4 tokens -> first . FIRST surname (drop middle + maternal surname)
    assert derive_email("Maria Fernanda Garcia Lopez") == "maria.garcia@thirstysprout.ai"


def test_email_fullname_fallback_three_token():
    # 3-token flat strings stay ambiguous; the consistent rule yields first+last.
    # Always surfaced for human approval (backfill never auto-creates a mailbox).
    assert derive_email("Syed Asad Ali") == "syed.ali@thirstysprout.ai"


def test_email_no_tokens_returns_none():
    assert derive_email("") is None
    assert derive_email("   ") is None


# ---- name_confidence --------------------------------------------------------

def test_confidence_high_subset():
    # contract tokens are a subset of personal-details tokens -> high
    assert name_confidence("Tazeem Imran", "Muhammad Tazeem Imran") == "high"


def test_confidence_latam_first_surname():
    # email check for the LATAM case alongside its (high) confidence
    assert derive_email("Maria Fernanda Garcia Lopez") == "maria.garcia@thirstysprout.ai"
    assert name_confidence("Maria Garcia", "Maria Fernanda Garcia Lopez") == "high"


def test_confidence_medium_partial_overlap():
    # share "tazeem" only -> medium
    assert name_confidence("Tazeem Imran", "Tazeem Khan") == "medium"


def test_confidence_low_no_overlap():
    # no shared tokens -> low (wrong contract likely attached)
    assert name_confidence("Tazeem Imran", "Carlos Mendoza") == "low"


def test_confidence_low_missing_data():
    assert name_confidence("", "Carlos Mendoza") == "low"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL  {t.__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    raise SystemExit(1 if failed else 0)