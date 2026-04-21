"""Load seed YAML files into the FLexlaw SCOTUS Archive database.

Handles three seed tables:
  - justices                  (from seeds/justices.yaml)
  - constitutional_provisions (from seeds/constitutional_provisions.yaml)
  - doctrinal_tests           (from seeds/doctrinal_tests.yaml)

For hierarchical tables (provisions, tests) uses a two-pass load so that
parent references — expressed in YAML as canonical_id strings — can resolve
to the integer FKs the schema actually uses.

Idempotent: every row is UPSERTed via ``ON CONFLICT ... DO UPDATE``. Re-runs
update rows in place without duplicating.

Connection: reads ``DB_URL`` from the environment
            (e.g. ``postgresql://john@localhost/legal_research``).

Usage::

    # production DB
    export DB_URL=postgresql://john@localhost/legal_research
    python3 -m pipeline.seed_loader

    # only one table
    python3 -m pipeline.seed_loader --table justices

    # preview without writing
    python3 -m pipeline.seed_loader --dry-run
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import yaml

LOG = logging.getLogger("seed_loader")

# Repo root resolved relative to this file (pipeline/seed_loader.py).
REPO_ROOT = Path(__file__).resolve().parent.parent
SEEDS_DIR = REPO_ROOT / "seeds"

TABLES = ("justices", "constitutional_provisions", "doctrinal_tests")


# ─────────────────────────────────────────────────────────────────────────────
# YAML loading helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_yaml(path: Path, top_key: str) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"seed file missing: {path}")
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict) or top_key not in data:
        raise ValueError(f"{path.name}: expected top-level key {top_key!r}")
    rows = data[top_key]
    if not isinstance(rows, list):
        raise ValueError(f"{path.name}: {top_key!r} must be a list")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# justices
# ─────────────────────────────────────────────────────────────────────────────

_JUSTICE_COLUMNS: Tuple[str, ...] = (
    "canonical_name", "display_name",
    "full_first", "full_middle", "full_last", "suffix",
    "born", "died", "nominated", "confirmed",
    "tenure_start", "tenure_end",
    "chief_justice", "chief_tenure_start", "chief_tenure_end",
    "appointing_president", "appointing_party",
    "prior_office", "law_school", "state_of_residence_at_appointment",
    "gender", "race_ethnicity", "religion",
    "succession_seat", "oyez_justice_id", "wikidata_qid",
)


def load_justices(cur, rows: Iterable[Dict[str, Any]], dry_run: bool) -> Tuple[int, int]:
    """Upsert justices. Returns (inserted, updated) counts."""
    columns = _JUSTICE_COLUMNS
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    # Conflict key: canonical_name (UNIQUE constraint in schema)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "canonical_name")

    sql = f"""
        INSERT INTO justices ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (canonical_name) DO UPDATE SET {updates}
        RETURNING (xmax = 0) AS inserted
    """

    inserted = updated = 0
    for row in rows:
        values = tuple(row.get(c) for c in columns)
        if dry_run:
            inserted += 1  # we can't know, call it a proposed insert
            continue
        cur.execute(sql, values)
        if cur.fetchone()[0]:
            inserted += 1
        else:
            updated += 1
    return inserted, updated


# ─────────────────────────────────────────────────────────────────────────────
# constitutional_provisions  (two-pass: parent_id is a canonical_id string)
# ─────────────────────────────────────────────────────────────────────────────

_PROVISION_COLUMNS_NO_PARENT: Tuple[str, ...] = (
    "canonical_id", "canonical_name", "short_name", "provision_type",
    "effective_from", "effective_to", "text_citation", "description", "sort_order",
)


def load_provisions(cur, rows: List[Dict[str, Any]], dry_run: bool) -> Tuple[int, int]:
    columns = _PROVISION_COLUMNS_NO_PARENT
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "canonical_id")
    upsert_sql = f"""
        INSERT INTO constitutional_provisions ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (canonical_id) DO UPDATE SET {updates}
        RETURNING (xmax = 0) AS inserted
    """

    # Pass 1: upsert every provision with parent_id left to the DB default (NULL).
    inserted = updated = 0
    for row in rows:
        values = tuple(row.get(c) for c in columns)
        if dry_run:
            inserted += 1
            continue
        cur.execute(upsert_sql, values)
        if cur.fetchone()[0]:
            inserted += 1
        else:
            updated += 1

    # Pass 2: resolve parent_id references.
    if dry_run:
        pending = sum(1 for r in rows if r.get("parent_id"))
        LOG.info("  (dry-run) would resolve %d provision parent refs", pending)
        return inserted, updated

    # Build canonical_id → id map once.
    cur.execute("SELECT canonical_id, id FROM constitutional_provisions")
    cid_to_id: Dict[str, int] = {cid: pid for cid, pid in cur.fetchall()}

    missing_refs: List[Tuple[str, str]] = []
    parent_updates = 0
    for row in rows:
        parent_ref = row.get("parent_id")
        child_cid = row["canonical_id"]
        if parent_ref is None:
            # If an existing DB row has a parent_id but the YAML has removed it,
            # clear the DB value to stay in sync.
            cur.execute(
                "UPDATE constitutional_provisions SET parent_id = NULL "
                "WHERE canonical_id = %s AND parent_id IS NOT NULL",
                (child_cid,),
            )
            continue
        parent_id = cid_to_id.get(parent_ref)
        if parent_id is None:
            missing_refs.append((child_cid, parent_ref))
            continue
        cur.execute(
            "UPDATE constitutional_provisions SET parent_id = %s "
            "WHERE canonical_id = %s AND (parent_id IS DISTINCT FROM %s)",
            (parent_id, child_cid, parent_id),
        )
        if cur.rowcount:
            parent_updates += 1

    if missing_refs:
        for child, parent in missing_refs:
            LOG.warning("provisions: %s references unknown parent %s", child, parent)
        raise ValueError(
            f"constitutional_provisions: {len(missing_refs)} unresolved parent refs"
        )

    LOG.info("  provisions: resolved %d parent links", parent_updates)
    return inserted, updated


# ─────────────────────────────────────────────────────────────────────────────
# doctrinal_tests  (two-pass: both provision ref and parent_test_id are strings)
# ─────────────────────────────────────────────────────────────────────────────

_TEST_COLUMNS_NO_REFS: Tuple[str, ...] = (
    "canonical_id", "name", "short_name",
    "established_in_year", "standard_of_review",
    "subject_area", "description",
    "active", "notes",
)


def load_tests(cur, rows: List[Dict[str, Any]], dry_run: bool) -> Tuple[int, int]:
    columns = _TEST_COLUMNS_NO_REFS
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "canonical_id")
    upsert_sql = f"""
        INSERT INTO doctrinal_tests ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (canonical_id) DO UPDATE SET {updates}
        RETURNING (xmax = 0) AS inserted
    """

    inserted = updated = 0
    for row in rows:
        # `active` defaults to TRUE in the schema; honor it if omitted.
        values = tuple(
            row.get(c) if c != "active" else row.get("active", True)
            for c in columns
        )
        if dry_run:
            inserted += 1
            continue
        cur.execute(upsert_sql, values)
        if cur.fetchone()[0]:
            inserted += 1
        else:
            updated += 1

    if dry_run:
        provision_refs = sum(1 for r in rows if r.get("constitutional_provision_id"))
        parent_refs    = sum(1 for r in rows if r.get("parent_test_id"))
        LOG.info(
            "  (dry-run) would resolve %d test→provision refs, %d test→parent refs",
            provision_refs, parent_refs,
        )
        return inserted, updated

    cur.execute("SELECT canonical_id, id FROM constitutional_provisions")
    prov_map: Dict[str, int] = {cid: pid for cid, pid in cur.fetchall()}
    cur.execute("SELECT canonical_id, id FROM doctrinal_tests")
    test_map: Dict[str, int] = {cid: tid for cid, tid in cur.fetchall()}

    missing: List[Tuple[str, str, str]] = []
    prov_updates = parent_updates = 0

    for row in rows:
        child_cid = row["canonical_id"]

        # Resolve constitutional_provision_id.
        prov_ref = row.get("constitutional_provision_id")
        if prov_ref is None:
            cur.execute(
                "UPDATE doctrinal_tests SET constitutional_provision_id = NULL "
                "WHERE canonical_id = %s AND constitutional_provision_id IS NOT NULL",
                (child_cid,),
            )
        else:
            prov_id = prov_map.get(prov_ref)
            if prov_id is None:
                missing.append((child_cid, "constitutional_provision_id", prov_ref))
            else:
                cur.execute(
                    "UPDATE doctrinal_tests SET constitutional_provision_id = %s "
                    "WHERE canonical_id = %s "
                    "AND (constitutional_provision_id IS DISTINCT FROM %s)",
                    (prov_id, child_cid, prov_id),
                )
                if cur.rowcount:
                    prov_updates += 1

        # Resolve parent_test_id.
        parent_ref = row.get("parent_test_id")
        if parent_ref is None:
            cur.execute(
                "UPDATE doctrinal_tests SET parent_test_id = NULL "
                "WHERE canonical_id = %s AND parent_test_id IS NOT NULL",
                (child_cid,),
            )
        else:
            parent_id = test_map.get(parent_ref)
            if parent_id is None:
                missing.append((child_cid, "parent_test_id", parent_ref))
            else:
                cur.execute(
                    "UPDATE doctrinal_tests SET parent_test_id = %s "
                    "WHERE canonical_id = %s "
                    "AND (parent_test_id IS DISTINCT FROM %s)",
                    (parent_id, child_cid, parent_id),
                )
                if cur.rowcount:
                    parent_updates += 1

    if missing:
        for child, field, ref in missing:
            LOG.warning("tests: %s.%s references unknown %s", child, field, ref)
        raise ValueError(f"doctrinal_tests: {len(missing)} unresolved refs")

    LOG.info(
        "  tests: resolved %d provision links, %d parent-test links",
        prov_updates, parent_updates,
    )

    # `established_by_case_id` is deliberately left unresolved — the cases
    # table is not yet populated for the scotus-archive scope. Once cases
    # are ingested, a separate loader pass can resolve these by Bluebook
    # citation or SCDB ID from the comments in the YAML.
    return inserted, updated


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

def _connect(db_url: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    return conn


def run(db_url: str, tables: Tuple[str, ...], dry_run: bool) -> int:
    conn = _connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                for table in tables:
                    LOG.info("loading %s ...", table)
                    if table == "justices":
                        rows = _load_yaml(SEEDS_DIR / "justices.yaml", "justices")
                        ins, upd = load_justices(cur, rows, dry_run)
                    elif table == "constitutional_provisions":
                        rows = _load_yaml(SEEDS_DIR / "constitutional_provisions.yaml", "provisions")
                        ins, upd = load_provisions(cur, rows, dry_run)
                    elif table == "doctrinal_tests":
                        rows = _load_yaml(SEEDS_DIR / "doctrinal_tests.yaml", "tests")
                        ins, upd = load_tests(cur, rows, dry_run)
                    else:
                        raise ValueError(f"unknown table: {table}")
                    LOG.info("  %s: %d inserted, %d updated (rows read: %d)",
                             table, ins, upd, len(rows))
            if dry_run:
                LOG.info("dry-run: rolling back")
                conn.rollback()
    finally:
        conn.close()
    return 0


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load seed YAML into the SCOTUS archive DB")
    p.add_argument(
        "--table",
        choices=("all",) + TABLES,
        default="all",
        help="Which seed table to load (default: all)",
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Run inside a transaction that rolls back at the end")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    db_url = os.environ.get("DB_URL")
    if not db_url:
        print("error: DB_URL environment variable is required", file=sys.stderr)
        return 2

    tables: Tuple[str, ...] = TABLES if args.table == "all" else (args.table,)
    return run(db_url, tables, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
