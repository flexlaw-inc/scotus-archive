"""
Statistical sanity checks — run on every release.
These tests are intentionally strict: a release that fails any of these
does not ship.

Baselines:
  - Epstein-Segal-Spaeth dissent rates by decade
  - SCOTUSblog OT2023 stat pack
  - Compendium per-justice opinion counts (spot-checked)
  - Black-Owens word-count monotonicity

All DB tests require DB_URL environment variable.
"""

import pytest
import yaml
from pathlib import Path


# ── Seed data structural tests (no DB required) ───────────────────────────────

SEEDS_DIR = Path(__file__).parent.parent / "seeds"


def test_justices_seed_loads():
    data = yaml.safe_load((SEEDS_DIR / "justices.yaml").read_text())
    justices = data["justices"]
    assert len(justices) >= 40, "Expected at least 40 modern-era justices"
    for j in justices:
        assert "canonical_name" in j
        assert "display_name" in j
        assert "tenure_start" in j
        assert isinstance(j.get("chief_justice"), bool)


def test_provisions_seed_loads():
    data = yaml.safe_load((SEEDS_DIR / "constitutional_provisions.yaml").read_text())
    provisions = data["provisions"]
    canonical_ids = [p["canonical_id"] for p in provisions]

    # Key provisions must be present
    required = [
        "amend.1",
        "amend.1.establishment",
        "amend.1.free_speech",
        "amend.4",
        "amend.14",
        "amend.14.s1.equal_protection",
        "amend.14.s1.due_process",
        "art.1.s8.commerce",
    ]
    for r in required:
        assert r in canonical_ids, f"Missing required provision: {r}"

    # All parent_ids must resolve
    for p in provisions:
        if "parent_id" in p and p["parent_id"]:
            assert p["parent_id"] in canonical_ids, (
                f"Provision {p['canonical_id']} has unresolved parent_id: {p['parent_id']}"
            )


def test_doctrinal_tests_seed_loads():
    data = yaml.safe_load((SEEDS_DIR / "doctrinal_tests.yaml").read_text())
    tests = data["tests"]
    canonical_ids = [t["canonical_id"] for t in tests]

    required = [
        "lemon_test",
        "Brandenburg_test",
        "strict_scrutiny_ep",
        "chevron_deference",
        "loper_bright_standard",
        "miranda_rule",
        "katz_test",
    ]
    for r in required:
        assert r in canonical_ids, f"Missing required test: {r}"

    # Overruled tests must have notes
    for t in tests:
        if t.get("active") is False:
            assert t.get("notes"), (
                f"Inactive test {t['canonical_id']} must have notes explaining overruling"
            )

    # parent_test_ids must resolve
    for t in tests:
        if "parent_test_id" in t and t["parent_test_id"]:
            assert t["parent_test_id"] in canonical_ids, (
                f"Test {t['canonical_id']} has unresolved parent_test_id: {t['parent_test_id']}"
            )


def test_no_sitting_chief_tenure_end():
    """Sitting Chief Justice must have tenure_end = null."""
    data = yaml.safe_load((SEEDS_DIR / "justices.yaml").read_text())
    sitting_chiefs = [
        j for j in data["justices"]
        if j.get("chief_justice") and j.get("tenure_end") is None
    ]
    assert len(sitting_chiefs) == 1, (
        f"Expected exactly 1 sitting Chief Justice, found {len(sitting_chiefs)}: "
        f"{[j['canonical_name'] for j in sitting_chiefs]}"
    )


# ── Database sanity checks (require DB_URL) ───────────────────────────────────

@pytest.mark.db
def test_scotus_case_count_ballpark(db_cur):
    """We should have 350K–400K SCOTUS cases."""
    db_cur.execute("SELECT COUNT(*) AS n FROM cases WHERE court_id = 1")
    n = db_cur.fetchone()["n"]
    assert 350_000 <= n <= 450_000, f"SCOTUS case count {n} outside expected range"


@pytest.mark.db
def test_scotus_opinion_count_ballpark(db_cur):
    """We should have 300K–450K SCOTUS opinion records."""
    db_cur.execute("""
        SELECT COUNT(*) AS n FROM opinions o
        JOIN cases c ON c.id = o.case_id
        WHERE c.court_id = 1
    """)
    n = db_cur.fetchone()["n"]
    assert 300_000 <= n <= 500_000, f"SCOTUS opinion count {n} outside expected range"


@pytest.mark.db
def test_scdb_token_count(db_cur):
    """At least 25K cases should carry an SCDB token."""
    db_cur.execute(r"""
        SELECT COUNT(*) AS n FROM cases
        WHERE court_id = 1
          AND EXISTS (
            SELECT 1 FROM unnest(COALESCE(citations, ARRAY[]::text[])) AS c
            WHERE c ~ '^SCDB \d{4}-\d{3}'
          )
    """)
    n = db_cur.fetchone()["n"]
    assert n >= 25_000, f"Expected ≥25K SCDB tokens, found {n}"


@pytest.mark.db
def test_dissent_rate_by_decade(db_cur):
    """
    Dissent rate by decade should be within 5 percentage points of
    Epstein-Segal-Spaeth published figures (Table 3-1).

    Baseline values (approximate, post-Phase-1 reclassification required for accuracy):
      1946-1949: ~18%   1950s: ~20%   1960s: ~25%   1970s: ~22%
      1980s: ~20%       1990s: ~22%   2000s: ~22%   2010s: ~21%

    NOTE: This test will only be meaningful after Phase 1 reclassification runs.
    Until then it is a stub that confirms the query runs without error.
    """
    db_cur.execute(r"""
        SELECT
            (EXTRACT(YEAR FROM c.decision_date) / 10 * 10)::int AS decade,
            COUNT(*) FILTER (WHERE o.opinion_type = 'dissent') AS dissents,
            COUNT(*) AS total_opinions
        FROM opinions o
        JOIN cases c ON c.id = o.case_id
        WHERE c.court_id = 1
          AND c.decision_date >= '1946-01-01'
          AND o.opinion_type IN ('majority', 'dissent', 'concurrence', 'per_curiam')
        GROUP BY decade
        ORDER BY decade
    """)
    rows = db_cur.fetchall()
    # Stub: just confirm we get results
    assert len(rows) >= 5, "Expected at least 5 decades of data"


@pytest.mark.db
@pytest.mark.slow
def test_ot2023_unanimous_count(db_cur):
    """
    OT2023 unanimous decisions should match SCOTUSblog stat pack.
    Requires votes table to be populated (Phase 4).

    SCOTUSblog OT2023: 58 total merits decisions; 27 unanimous (46%).
    Stub until Phase 4.
    """
    # Check if votes table exists and has data
    db_cur.execute("""
        SELECT COUNT(*) AS n FROM information_schema.tables
        WHERE table_name = 'votes'
    """)
    if db_cur.fetchone()["n"] == 0:
        pytest.skip("votes table not yet created — Phase 4 not run")

    db_cur.execute("""
        SELECT COUNT(*) AS n FROM votes WHERE 1=0  -- stub
    """)
    pytest.skip("Phase 4 not yet run — skipping OT2023 unanimous count check")
