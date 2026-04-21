"""Opinion reclassifier runner (Phase 1 of v2.2 plan).

Reads opinions from the database, runs the ``signals.classify`` fuser, and
applies the result per the plan's rules:

- Never mutate ``opinion_type`` without first snapshotting the pre-existing
  value into ``opinion_type_original``.
- Every change — whether applied or merely proposed — writes a row to
  ``reclassification_log``. The log itself is a publishable artifact.
- Only ``high``-confidence verdicts are applied to ``opinion_type``
  automatically. ``medium`` tier gates on gold-set precision ≥ 0.98 and is
  skipped by default; pass ``--apply-medium`` to enable it. ``low`` and
  ``manual_required`` verdicts are logged only; they never mutate the row.

Schema dependency: requires migration ``schema/002_alter_existing_tables.sql``
to have been applied (for ``opinions.opinion_type_original`` /
``opinion_type_source`` / ``opinion_type_confidence`` columns and the
``reclassification_log`` table).

Connection: reads ``DB_URL`` from the environment.

Usage::

    # Dry run over the whole corpus (no writes).
    python3 -m pipeline.reclassifier.runner --dry-run

    # Apply only high-confidence reclassifications.
    python3 -m pipeline.reclassifier.runner

    # Apply high AND medium (only after gold-set gate).
    python3 -m pipeline.reclassifier.runner --apply-medium

    # Single opinion (for spot-checking).
    python3 -m pipeline.reclassifier.runner --opinion-id 1234567
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Iterable, List, Optional

import psycopg2
import psycopg2.extras

from .signals import (
    LANDMARK_CITE_THRESHOLD,
    VALID_LABELS,
    Verdict,
    classify,
)

LOG = logging.getLogger("reclassifier")

SOURCE_TAG = "reclassifier_v1"

# Tiers that are *safe* to apply automatically.
DEFAULT_APPLY_TIERS = {"high"}
MEDIUM_APPLY_TIERS  = {"high", "medium"}


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

_OPINIONS_SELECT_SQL = """
    SELECT
        o.id                    AS opinion_id,
        o.case_id               AS case_id,
        o.opinion_type          AS opinion_type,
        o.opinion_type_original AS opinion_type_original,
        o.opinion_type_source   AS opinion_type_source,
        o.author_original       AS author_original,
        o.plain_text            AS plain_text,
        NULL::text              AS cl_opinion_type,  -- placeholder (see note)
        coalesce(c.cite_count, 0) AS cite_count
    FROM opinions o
    LEFT JOIN cases c ON c.id = o.case_id
    {filter_clause}
    ORDER BY o.id
    {limit_clause}
"""

# NOTE on cl_opinion_type: the CourtListener type lives in a separate
# provenance table in the FLexlaw production schema, not a column on
# opinions. When that integration is stood up, replace the
# ``NULL::text AS cl_opinion_type`` line above with the appropriate join.


def _build_query(
    opinion_ids: Optional[List[int]],
    limit: Optional[int],
    only_undone: bool,
    court_id: Optional[int] = None,
) -> tuple:
    clauses: List[str] = []
    params: List = []

    if court_id is not None:
        clauses.append("c.court_id = %s")
        params.append(court_id)
    if opinion_ids:
        clauses.append("o.id = ANY(%s)")
        params.append(opinion_ids)
    if only_undone:
        # Skip opinions the reclassifier has already touched.
        clauses.append("o.opinion_type_source IS DISTINCT FROM %s")
        params.append(SOURCE_TAG)
    # Require a substantive opinion (plain_text OR author) to classify.
    clauses.append(
        "(o.plain_text IS NOT NULL OR o.author_original IS NOT NULL)"
    )

    filter_clause = "WHERE " + " AND ".join(clauses) if clauses else ""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    return (
        _OPINIONS_SELECT_SQL.format(
            filter_clause=filter_clause,
            limit_clause=limit_clause,
        ),
        params,
    )


_LOG_INSERT_SQL = """
    INSERT INTO reclassification_log (
        opinion_id,
        old_type,
        new_type,
        signal,
        signal_text,
        rule_id,
        confidence,
        pipeline_version,
        created_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
"""

_OPINION_UPDATE_SQL = """
    UPDATE opinions
    SET
        opinion_type            = %s,
        opinion_type_original   = COALESCE(opinion_type_original, opinion_type),
        opinion_type_source     = %s,
        opinion_type_confidence = %s
    WHERE id = %s
"""


def _evidence_json(v: Verdict) -> str:
    """Serialize Verdict signals to JSON for the reclassification_log row."""
    import json
    return json.dumps({
        "label": v.label,
        "confidence": v.confidence,
        "notes": v.notes,
        "disagreements": v.disagreements,
        "signals": [
            {
                "label": s.label,
                "source": s.source,
                "pattern_name": s.pattern_name,
                "evidence": s.evidence,
            }
            for s in v.signals
        ],
    })


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def run(
    db_url: str,
    *,
    dry_run: bool,
    apply_medium: bool,
    only_undone: bool,
    opinion_ids: Optional[List[int]],
    limit: Optional[int],
    court_id: Optional[int] = None,
    progress_every: int = 5000,
) -> int:
    apply_tiers = MEDIUM_APPLY_TIERS if apply_medium else DEFAULT_APPLY_TIERS
    LOG.info("apply tiers: %s", sorted(apply_tiers))
    if court_id is not None:
        LOG.info("filtering to court_id=%s", court_id)
    if dry_run:
        LOG.info("DRY RUN — no rows will be updated")

    sql, params = _build_query(opinion_ids, limit, only_undone, court_id=court_id)

    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor(name="reclassifier_cursor",
                             cursor_factory=psycopg2.extras.RealDictCursor) as scan:
                scan.itersize = 1000
                scan.execute(sql, params)

                # Writer cursor (unnamed, usable for UPDATE/INSERT).
                write = conn.cursor()

                counts = {
                    "seen": 0,
                    "no_change": 0,
                    "logged_only": 0,
                    "applied": 0,
                    "skipped_low_confidence": 0,
                }

                for row in scan:
                    counts["seen"] += 1
                    verdict = classify(
                        author_field=row["author_original"],
                        plain_text=row["plain_text"],
                        cl_opinion_type=row["cl_opinion_type"],
                        is_landmark=(row["cite_count"] or 0) > LANDMARK_CITE_THRESHOLD,
                    )
                    assert verdict.label in VALID_LABELS

                    if verdict.label == row["opinion_type"]:
                        counts["no_change"] += 1
                        continue

                    # Log every proposed change, whether or not applied.
                    # Schema-002 columns: signal (source of dominant signal),
                    # signal_text (JSON payload of full verdict), rule_id
                    # (pattern name), pipeline_version (source tag).
                    dominant = verdict.signals[0] if verdict.signals else None
                    write.execute(
                        _LOG_INSERT_SQL,
                        (
                            row["opinion_id"],
                            row["opinion_type"],
                            verdict.label,
                            dominant.source if dominant else "none",
                            _evidence_json(verdict),
                            dominant.pattern_name if dominant else None,
                            verdict.confidence,
                            SOURCE_TAG,
                        ),
                    )

                    if verdict.confidence in apply_tiers:
                        if dry_run:
                            counts["logged_only"] += 1
                        else:
                            write.execute(
                                _OPINION_UPDATE_SQL,
                                (
                                    verdict.label,
                                    SOURCE_TAG,
                                    verdict.confidence,
                                    row["opinion_id"],
                                ),
                            )
                            counts["applied"] += 1
                    else:
                        counts["logged_only"] += 1
                        if verdict.confidence in ("low", "manual_required"):
                            counts["skipped_low_confidence"] += 1

                    if counts["seen"] % progress_every == 0:
                        LOG.info("  progress: %s", counts)

                write.close()

            if dry_run:
                LOG.info("dry-run: rolling back")
                conn.rollback()

        LOG.info("final: %s", counts)
    finally:
        conn.close()

    return 0


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Reclassify SCOTUS opinions (Phase 1)")
    p.add_argument("--dry-run", action="store_true",
                   help="Process everything but roll back at the end.")
    p.add_argument("--apply-medium", action="store_true",
                   help="Also apply medium-confidence verdicts "
                        "(only after gold-set precision gate).")
    p.add_argument("--rerun", action="store_true",
                   help="Re-process opinions already touched by the reclassifier. "
                        "Default is to skip them.")
    p.add_argument("--opinion-id", type=int, action="append", dest="opinion_ids",
                   help="Classify only this opinion id (may be repeated).")
    p.add_argument("--limit", type=int, help="Cap the number of rows processed.")
    p.add_argument("--court-id", type=int, dest="court_id",
                   help="Restrict to opinions whose case is in this court (e.g. 1 = SCOTUS).")
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

    return run(
        db_url,
        dry_run=args.dry_run,
        apply_medium=args.apply_medium,
        only_undone=not args.rerun,
        opinion_ids=args.opinion_ids,
        limit=args.limit,
        court_id=args.court_id,
    )


if __name__ == "__main__":
    sys.exit(main())
