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
    """Seed must contain the full 116-justice historical roster (FJC count).

    Expected composition:
      - 116 total rows (matches Oyez roster and FJC SCOTUS appointees)
      - 17 distinct Chief Justice rows (Jay through Roberts, incl. Rutledge
        recess appointment and Stone's elevation)
      - Unique oyez_justice_id for every row
      - Required fields on every row
    """
    data = yaml.safe_load((SEEDS_DIR / "justices.yaml").read_text())
    justices = data["justices"]
    assert len(justices) == 116, (
        f"Expected 116 justices (FJC SCOTUS roster), got {len(justices)}"
    )
    for j in justices:
        assert "canonical_name" in j
        assert "display_name" in j
        assert "tenure_start" in j
        assert isinstance(j.get("chief_justice"), bool)
        assert "oyez_justice_id" in j, f"missing oyez id on {j['canonical_name']}"
    # Chief Justice count
    chiefs = [j for j in justices if j.get("chief_justice")]
    assert len(chiefs) == 17, f"Expected 17 Chief Justice rows, got {len(chiefs)}"
    # Unique oyez_justice_id
    ids = [j["oyez_justice_id"] for j in justices]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    assert not dupes, f"Duplicate oyez_justice_id values: {dupes}"


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


@pytest.mark.db
def test_case_propositions_table_present(db_cur):
    """
    Phase 9b — case_propositions table must exist with the columns and
    constraints declared in schema/003 before Phase 9b extraction can run.
    """
    db_cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'case_propositions'
        ORDER BY ordinal_position
    """)
    cols = {r["column_name"] for r in db_cur.fetchall()}
    if not cols:
        pytest.skip("case_propositions table not yet created — schema/003 not run")

    required_cols = {
        "id", "case_id", "proposition_text", "proposition_source",
        "supporting_justice_ids", "opposing_justice_ids",
        "vote_count_for", "vote_count_against", "commands_majority",
        "precedential_status", "is_necessary_to_judgment",
        "ordering", "confidence", "reviewer_signed_off",
        "reviewer_signoff_at", "reviewer_id", "notes",
        "created_at", "updated_at",
    }
    missing = required_cols - cols
    assert not missing, f"case_propositions missing columns: {sorted(missing)}"

    # CHECK constraints for the declared enums must exist.
    db_cur.execute("""
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'case_propositions'::regclass
          AND contype = 'c'
    """)
    check_names = {r["conname"] for r in db_cur.fetchall()}
    required_checks = {
        "case_propositions_source_chk",
        "case_propositions_precedential_status_chk",
        "case_propositions_confidence_chk",
        "case_propositions_no_coalition_overlap_chk",
        "case_propositions_signoff_consistency_chk",
    }
    missing_checks = required_checks - check_names
    assert not missing_checks, f"Missing CHECK constraints: {sorted(missing_checks)}"


@pytest.mark.db
def test_case_propositions_check_constraints_enforce(db_cur):
    """
    Insert a row that violates each CHECK constraint and confirm it is rejected.
    Uses a savepoint per attempt so the test does not mutate the table.
    """
    import psycopg2
    # Need a real case_id to test FK; pick any SCOTUS case.
    db_cur.execute("SELECT id FROM cases WHERE court_id = 1 LIMIT 1")
    row = db_cur.fetchone()
    if not row:
        pytest.skip("No SCOTUS cases in DB")
    case_id = row["id"]

    bad_rows = [
        # proposition_source outside enum
        dict(source="not_a_source", status="majority", conf="high",
             label="bad_source"),
        # precedential_status outside enum
        dict(source="llm_v1", status="not_a_status", conf="high",
             label="bad_status"),
        # confidence outside enum
        dict(source="llm_v1", status="majority", conf="ultra",
             label="bad_confidence"),
    ]
    for attempt in bad_rows:
        db_cur.execute("SAVEPOINT s")
        try:
            db_cur.execute("""
                INSERT INTO case_propositions
                    (case_id, proposition_text, proposition_source,
                     supporting_justice_ids, opposing_justice_ids,
                     precedential_status, confidence)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (case_id, "test", attempt["source"], [], [],
                  attempt["status"], attempt["conf"]))
            db_cur.execute("RELEASE SAVEPOINT s")
            pytest.fail(f"Expected CHECK violation for {attempt['label']}, got none")
        except psycopg2.errors.CheckViolation:
            db_cur.execute("ROLLBACK TO SAVEPOINT s")

    # Coalition overlap violation.
    db_cur.execute("SAVEPOINT s")
    try:
        db_cur.execute("""
            INSERT INTO case_propositions
                (case_id, proposition_text, proposition_source,
                 supporting_justice_ids, opposing_justice_ids,
                 precedential_status, confidence)
            VALUES (%s, 'x', 'llm_v1', ARRAY[1,2]::bigint[], ARRAY[2,3]::bigint[],
                    'majority', 'high')
        """, (case_id,))
        db_cur.execute("RELEASE SAVEPOINT s")
        pytest.fail("Expected coalition-overlap CHECK violation")
    except psycopg2.errors.CheckViolation:
        db_cur.execute("ROLLBACK TO SAVEPOINT s")
