"""
Order-relocation pipeline.

Moves rows out of the `opinions` table and into the `court_orders` sibling
table for the manual_required `majority -> other` cohort that the triage memo
(docs/triage-memo-2026-04-21.md) identified as procedural orders, not
opinions.

Cohort scope:
    reclassification_log rows where
        old_type    = 'majority'
        new_type    = 'other'
        confidence  = 'manual_required'
    joined to opinions joined to cases (court_id = 1, SCOTUS).

Decision logic (per opinion):
    1.  If opinions.author IS NOT NULL  -> SKIP. Authored content stays in
        `opinions`. (Defensive — reclassifier author_field signal should have
        excluded these from the cohort, but we double-check.)
    2.  If LENGTH(plain_text) >= 1000   -> SKIP, classify as
        'concatenated_candidate'. These are bug-2 rows (CAP collapsed the
        full U.S. Reports entry into one row) and need the splitter, not
        the relocator.
    3.  If empty or pattern matches     -> RELOCATE. Insert into
        court_orders, then DELETE from opinions, log the relocation.
    4.  If short (<1000 chars) and no pattern matches -> RELOCATE as
        order_type='unclassified_short'. The triage memo found these are
        overwhelmingly abbreviated cert denials; cleanup batch can re-
        classify later, but they don't belong in `opinions`.

Defaults to --dry-run. Use --apply to actually mutate the database.

Usage:
    python -m pipeline.orders.relocate_orders --dry-run                 # report only
    python -m pipeline.orders.relocate_orders --dry-run --limit 5000    # smaller test
    python -m pipeline.orders.relocate_orders --apply                   # full relocation
    python -m pipeline.orders.relocate_orders --apply --limit 1000      # small live batch

Audit trail:
    - Every relocated row keeps a forensic link via court_orders.original_opinion_id.
    - Every relocation writes a row to reclassification_log:
          old_type='majority',
          new_type='__moved_to_court_orders__',
          signal='orders_relocator',
          signal_text=<order_type>,
          rule_id=<classifier_rule_id>,
          confidence='auto',
          pipeline_version='orders_relocator_v1'.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from collections import Counter

import psycopg2
import psycopg2.extras

DB_URL = os.environ.get("DB_URL", "postgresql:///legal_research")
PIPELINE_VERSION = "orders_relocator_v1"
ORDER_TEXT_MAX = 8000          # cap court_orders.order_text length (orders are tiny anyway)
LONG_TEXT_THRESHOLD = 1000     # at-or-above this length, defer to splitter
HEAD_SLICE = 1500              # how many chars of opener we feed to the classifier

# Match-order matters — first match wins. Keep the most specific patterns first.
PATTERNS: list[tuple[str, str, re.Pattern]] = [
    ("cert_dismissed",       "opener_cert_dismissed_v1",
        re.compile(r"(?:petition.{0,40}certiorari|writ of certiorari)[^.\n]{0,160}(?:improvidently granted|dismissed)", re.I)),
    ("cert_granted",         "opener_cert_granted_v1",
        re.compile(r"(?:petition.{0,40}(?:for[a-z ]*writ[a-z ]*of)?[a-z ]*certiorari|writ of certiorari)[^.\n]{0,120}(?:is )?granted", re.I)),
    ("cert_denied",          "opener_cert_denied_v1",
        re.compile(r"(?:petition.{0,40}(?:for[a-z ]*writ[a-z ]*of)?[a-z ]*certiorari|writ of certiorari)[^.\n]{0,120}(?:is )?(?:denied|den-?\s*ied)", re.I)),
    ("rehearing_denied",     "opener_rehearing_denied_v1",
        re.compile(r"(?:petition )?for rehearing[^.\n]{0,80}denied", re.I)),
    ("rehearing_granted",    "opener_rehearing_granted_v1",
        re.compile(r"(?:petition )?for rehearing[^.\n]{0,80}granted", re.I)),
    ("stay_action",          "opener_stay_v1",
        re.compile(r"\bapplication for[^.\n]{0,80}stay[^.\n]{0,120}(?:granted|denied)", re.I)),
    ("injunction_action",    "opener_injunction_v1",
        re.compile(r"\bapplication for[^.\n]{0,80}injunction[^.\n]{0,120}(?:granted|denied)", re.I)),
    ("mandamus_action",      "opener_mandamus_v1",
        re.compile(r"\b(?:petition|writ) (?:for )?[a-z]*\s*mandamus[^.\n]{0,120}(?:granted|denied)", re.I)),
    ("habeas_action",        "opener_habeas_v1",
        re.compile(r"\b(?:petition|writ) (?:for )?[a-z]*\s*habeas[^.\n]{0,120}(?:granted|denied)", re.I)),
    ("disbarment",           "opener_disbarment_v1",
        re.compile(r"\b(?:disbarred|suspension from the (?:bar|practice)|disbarment|suspended from the practice of law|show cause why .* should not be disbarred)\b", re.I)),
    ("reinstatement",        "opener_reinstatement_v1",
        re.compile(r"\breinstat(?:ed|ement)\b[^.\n]{0,80}(?:bar|practice of law)", re.I)),
    ("leave_to_file",        "opener_leave_to_file_v1",
        re.compile(r"\bleave to file[^.\n]{0,120}(?:granted|denied)", re.I)),
    ("appeal_dismissed",     "opener_appeal_dismissed_v1",
        re.compile(r"\bappeal[^.\n]{0,40}(?:is )?dismissed\b", re.I)),
    ("motion_action",        "opener_motion_v1",
        re.compile(r"^(?:\s*[^\n]{0,80}\n){0,2}\s*(?:the )?motion[^.\n]{0,160}(?:is )?(?:granted|denied)", re.I)),
]


def classify(text: str) -> tuple[str, str]:
    """Return (order_type, classifier_rule_id) for the opening text."""
    if not text or not text.strip():
        return ("empty", "empty_v1")
    head = text[:HEAD_SLICE]
    for order_type, rule_id, rx in PATTERNS:
        if rx.search(head):
            return (order_type, rule_id)
    return ("unclassified_short", "unclassified_short_v1")


SELECT_COHORT_SQL = """
    SELECT
        o.id              AS opinion_id,
        o.case_id,
        o.author,
        o.plain_text,
        o.source          AS opinion_source,
        c.decision_date,
        c.cap_case_id
    FROM reclassification_log rl
    JOIN opinions o ON o.id = rl.opinion_id
    JOIN cases    c ON c.id = o.case_id
    WHERE rl.old_type    = 'majority'
      AND rl.new_type    = 'other'
      AND rl.confidence  = 'manual_required'
      AND c.court_id     = %s
      -- Don't try to relocate a row that's already gone.
      AND o.id NOT IN (SELECT original_opinion_id FROM court_orders WHERE original_opinion_id IS NOT NULL)
    ORDER BY o.id
"""

INSERT_ORDER_SQL = """
    INSERT INTO court_orders (
        case_id, order_type, order_text, decision_date,
        author, author_original, source, original_opinion_id,
        classifier_rule_id, cap_case_id
    ) VALUES (
        %(case_id)s, %(order_type)s, %(order_text)s, %(decision_date)s,
        NULL, NULL, %(source)s, %(original_opinion_id)s,
        %(classifier_rule_id)s, %(cap_case_id)s
    )
    RETURNING id
"""

DELETE_OPINION_SQL = "DELETE FROM opinions WHERE id = %s"

LOG_RELOCATION_SQL = """
    INSERT INTO reclassification_log (
        opinion_id, old_type, new_type,
        signal, signal_text, rule_id,
        confidence, pipeline_version
    ) VALUES (
        %(opinion_id)s, 'majority', '__moved_to_court_orders__',
        'orders_relocator', %(order_type)s, %(rule_id)s,
        'auto', %(pipeline_version)s
    )
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Actually mutate the database. Default is dry-run.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Force dry-run (default if --apply absent).")
    parser.add_argument("--court-id", type=int, default=1,
                        help="Court to operate on. Default: 1 (SCOTUS).")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process at most N rows. Useful for staged batches.")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Commit every N relocations. Default: 500.")
    args = parser.parse_args(argv)

    apply = args.apply and not args.dry_run
    mode = "APPLY" if apply else "DRY-RUN"
    print(f"[relocate] mode={mode} court_id={args.court_id} limit={args.limit}")

    counters: Counter[str] = Counter()
    skipped_authored = 0
    deferred_concatenated = 0
    relocated = 0
    seen = 0
    start = time.time()

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False

    # Use a server-side cursor on a separate connection to avoid the
    # rollback-clears-cursor problem when we abort mid-batch.
    read_conn = psycopg2.connect(DB_URL)
    read_conn.autocommit = True
    read_cur = read_conn.cursor("relocate_scan", cursor_factory=psycopg2.extras.RealDictCursor)
    read_cur.itersize = 500
    read_cur.execute(SELECT_COHORT_SQL, (args.court_id,))

    write_cur = conn.cursor()

    try:
        for row in read_cur:
            seen += 1
            if args.limit is not None and seen > args.limit:
                seen -= 1
                break

            # Defensive: never relocate authored content.
            if row["author"]:
                skipped_authored += 1
                continue

            text = row["plain_text"] or ""
            if len(text) >= LONG_TEXT_THRESHOLD:
                deferred_concatenated += 1
                continue

            order_type, rule_id = classify(text)
            counters[order_type] += 1

            if not apply:
                relocated += 1
                if seen % 5000 == 0:
                    elapsed = time.time() - start
                    rate = seen / elapsed if elapsed else 0
                    print(f"[relocate] seen={seen}  would_relocate={relocated}  "
                          f"deferred={deferred_concatenated}  authored_skipped={skipped_authored}  "
                          f"rate={rate:.0f}/s")
                continue

            # APPLY path.
            write_cur.execute(INSERT_ORDER_SQL, {
                "case_id":             row["case_id"],
                "order_type":          order_type,
                "order_text":          text[:ORDER_TEXT_MAX] if text else None,
                "decision_date":       row["decision_date"],
                "source":              row["opinion_source"],
                "original_opinion_id": row["opinion_id"],
                "classifier_rule_id":  rule_id,
                "cap_case_id":         row["cap_case_id"],
            })
            new_order_id = write_cur.fetchone()[0]

            write_cur.execute(LOG_RELOCATION_SQL, {
                "opinion_id":       row["opinion_id"],
                "order_type":       order_type,
                "rule_id":          rule_id,
                "pipeline_version": PIPELINE_VERSION,
            })

            write_cur.execute(DELETE_OPINION_SQL, (row["opinion_id"],))
            relocated += 1

            if relocated % args.batch_size == 0:
                conn.commit()
                elapsed = time.time() - start
                rate = relocated / elapsed if elapsed else 0
                print(f"[relocate] applied={relocated}  deferred={deferred_concatenated}  "
                      f"authored_skipped={skipped_authored}  rate={rate:.0f}/s  "
                      f"new_court_orders_id_last={new_order_id}")

        # Final commit (apply mode).
        if apply:
            conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        read_cur.close()
        read_conn.close()
        write_cur.close()
        conn.close()

    elapsed = time.time() - start
    print()
    print(f"=== {mode} summary ===")
    print(f"seen                  : {seen}")
    print(f"relocated             : {relocated}")
    print(f"deferred_concatenated : {deferred_concatenated}  (>= {LONG_TEXT_THRESHOLD} chars; for splitter)")
    print(f"authored_skipped      : {skipped_authored}  (defensive — opinions.author NOT NULL)")
    print(f"elapsed               : {elapsed:.1f}s")
    print(f"rate                  : {relocated / elapsed if elapsed else 0:.0f}/s")
    print()
    print("=== order_type distribution ===")
    for ot, n in counters.most_common():
        print(f"  {ot:30s}  {n:>8d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
