"""Backfill opinions.author_original for SCOTUS from best available source.

Tier 1 (authoritative, free): copy opinions.author → opinions.author_original
        for every SCOTUS opinion where author is non-empty.

Tier 2 (regex from plain_text): parse first ~300 chars of plain_text for the
        opening attribution line. Preserves the ROLE cue (dissenting,
        concurring, delivered the opinion, Per Curiam) so the reclassifier's
        author_signal can fire at high confidence.

Never overwrites an already-populated author_original. Idempotent —
re-running after partial completion picks up where it left off.
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from typing import Optional, Tuple

import psycopg2
import psycopg2.extras

LOG = logging.getLogger("backfill_author")

SCOTUS_COURT_ID = 1

# ── Regex patterns (ordered: first match wins) ──────────────────────────────

# Per Curiam (with or without trailing punctuation).
_RE_PER_CURIAM = re.compile(r"^\s*Per\s*Curiam\b[\.:]?", re.IGNORECASE)

# "Mr. Justice X, with whom Justice Y joins, dissenting."  /  "Justice Scalia, dissenting."
# Captures the full attribution line up through the role cue — we store it
# verbatim so author_signal sees the cue.
# Role cues (order matters — longer patterns first so we prefer the mixed
# "concurring in part and dissenting in part" over plain "dissenting").
_ROLE_CUES = (
    r"concurring\s+in\s+part\s+and\s+dissenting\s+in\s+part"
    r"|dissenting\s+in\s+part\s+and\s+concurring\s+in\s+part"
    r"|concurring\s+in\s+the\s+judgment\s+in\s+part\s+and\s+dissenting\s+in\s+part"
    r"|concurring\s+in\s+the\s+judgment"
    r"|concurring\s+in\s+the\s+result"
    r"|dissenting\s+from\s+the\s+denial\s+of\s+certiorari"
    r"|dissenting\s+from\s+the\s+grant\s+of\s+certiorari"
    r"|delivered\s+the\s+opinion\s+of\s+the\s+Court"
    r"|delivered\s+the\s+judgment\s+of\s+the\s+Court"
    r"|announced\s+the\s+judgment\s+of\s+the\s+Court"
    r"|took\s+no\s+part\s+in"
    r"|concurring"
    r"|dissenting"
)

# "Mr. Justice X [with whom...joins,]? <role>"
# Deliberately allows OCR garbage in the name (e.g., "Me. Justice Beennan")
# because the classifier only needs the ROLE cue to attribute the opinion.
_RE_JUSTICE_ATTR = re.compile(
    r"^\s*"
    r"(?:Mr\.\s+|Mrs\.\s+|Ms\.\s+|Me\.\s+)?"          # honorific (incl. OCR 'Me.')
    r"(?:Chief\s+)?"
    r"Justices?\s+"                                    # Justice / Justices
    r"[A-Za-z][A-Za-z\.\-'\s]{1,80}?"                  # name(s)
    r"(?:\s*,?\s*with\s+whom\s+[^\.]{0,200}?joins?,?)?" # "with whom X joins"
    r"\s*,?\s+"
    r"(?:" + _ROLE_CUES + r")"
    r"[^\.]{0,30}\.",                                   # up through first period
    re.IGNORECASE,
)

# Short form: just "Justice X" or "Mr. Justice X" alone (no role cue visible
# in first 300 chars). Lower confidence but still useful — author_signal will
# tag it as 'justice_name_only' → LABEL_MAJORITY.
_RE_JUSTICE_BARE = re.compile(
    r"^\s*"
    r"(?:Mr\.\s+|Mrs\.\s+|Ms\.\s+|Me\.\s+)?"
    r"(?:Chief\s+)?"
    r"Justices?\s+"
    r"[A-Za-z][A-Za-z\.\-']{2,30}"
    r"(?:\s+(?:Jr|Sr|II|III|IV)\.?)?"
)


def extract_author_from_opener(plain_text: Optional[str]) -> Tuple[Optional[str], str]:
    """Return (author_text, source_tag) or (None, 'no_match').

    source_tag labels which pattern hit, for auditing.
    """
    if not plain_text:
        return None, "no_text"

    # Look at only the first ~400 chars — attribution is always at the top.
    head = plain_text[:400].lstrip()
    if not head:
        return None, "empty_head"

    # Per Curiam trumps everything.
    m = _RE_PER_CURIAM.match(head)
    if m:
        return "Per Curiam", "per_curiam"

    # Full attribution with role cue.
    m = _RE_JUSTICE_ATTR.match(head)
    if m:
        # Trim trailing period and whitespace for storage.
        attr = m.group(0).rstrip(". \t\n")
        # Collapse whitespace.
        attr = re.sub(r"\s+", " ", attr)
        return attr, "justice_with_role"

    # Bare justice name (no role visible in opener).
    m = _RE_JUSTICE_BARE.match(head)
    if m:
        attr = re.sub(r"\s+", " ", m.group(0).strip())
        return attr, "justice_bare"

    return None, "no_match"


# ── DB passes ───────────────────────────────────────────────────────────────

def tier1_copy_from_author(conn, dry_run: bool) -> dict:
    """Copy opinions.author → opinions.author_original where populated."""
    sql = """
        UPDATE opinions o
        SET author_original = o.author
        FROM cases c
        WHERE o.case_id = c.id
          AND c.court_id = %s
          AND o.author_original IS NULL
          AND o.author IS NOT NULL
          AND length(trim(o.author)) > 0
    """
    with conn.cursor() as cur:
        if dry_run:
            cur.execute(
                """SELECT count(*) FROM opinions o JOIN cases c ON c.id=o.case_id
                   WHERE c.court_id=%s AND o.author_original IS NULL
                     AND o.author IS NOT NULL AND length(trim(o.author))>0""",
                (SCOTUS_COURT_ID,),
            )
            n = cur.fetchone()[0]
            LOG.info("tier1 (dry-run): would copy %d rows", n)
            return {"tier1_candidates": n}
        cur.execute(sql, (SCOTUS_COURT_ID,))
        n = cur.rowcount
        conn.commit()
        LOG.info("tier1: copied author→author_original for %d rows", n)
        return {"tier1_copied": n}


def tier2_regex_parse(conn, dry_run: bool, batch_size: int = 10000) -> dict:
    """Parse plain_text opener for remaining NULL author_original rows.

    Uses TWO connections: ``conn`` (passed in) for the streaming scan's
    named cursor — never committed on — and a second connection for
    periodic batched UPDATEs so commits don't invalidate the scan cursor.
    """
    counts = {
        "scanned": 0, "per_curiam": 0, "justice_with_role": 0,
        "justice_bare": 0, "no_match": 0, "empty_head": 0, "no_text": 0,
        "updated": 0,
    }

    # Streaming fetch of candidates — named cursor on the read connection.
    # Must NEVER be committed on, or the server-side cursor dies.
    scan = conn.cursor(name="backfill_scan",
                       cursor_factory=psycopg2.extras.RealDictCursor)
    scan.itersize = batch_size
    scan.execute("""
        SELECT o.id AS opinion_id, o.plain_text
        FROM opinions o
        JOIN cases c ON c.id = o.case_id
        WHERE c.court_id = %s
          AND o.author_original IS NULL
          AND o.plain_text IS NOT NULL
        ORDER BY o.id
    """, (SCOTUS_COURT_ID,))

    # Separate write connection so its commits don't nuke the scan cursor.
    write_conn = psycopg2.connect("postgresql:///legal_research")
    write_conn.autocommit = False
    write = write_conn.cursor()

    pending: list[Tuple[str, int]] = []   # (author_text, opinion_id)
    FLUSH_EVERY = 5000

    def flush():
        if not pending:
            return
        psycopg2.extras.execute_values(
            write,
            "UPDATE opinions SET author_original = v.auth "
            "FROM (VALUES %s) AS v(auth, oid) WHERE opinions.id = v.oid",
            pending, template="(%s, %s)"
        )
        counts["updated"] += len(pending)
        write_conn.commit()
        pending.clear()

    try:
        for row in scan:
            counts["scanned"] += 1
            author, tag = extract_author_from_opener(row["plain_text"])
            counts[tag] = counts.get(tag, 0) + 1
            if author and not dry_run:
                pending.append((author, row["opinion_id"]))
                if len(pending) >= FLUSH_EVERY:
                    flush()
                    LOG.info("  progress: %s",
                             {k: v for k, v in counts.items() if v})
            if counts["scanned"] % 25000 == 0:
                LOG.info("  scanned %d", counts["scanned"])
        if not dry_run:
            flush()
    finally:
        write.close()
        write_conn.close()
        scan.close()
    LOG.info("tier2 done: %s", counts)
    return counts


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Backfill opinions.author_original for SCOTUS")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--skip-tier1", action="store_true")
    p.add_argument("--skip-tier2", action="store_true")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    conn = psycopg2.connect("postgresql:///legal_research")
    try:
        results = {}
        if not args.skip_tier1:
            results["tier1"] = tier1_copy_from_author(conn, args.dry_run)
        if not args.skip_tier2:
            results["tier2"] = tier2_regex_parse(conn, args.dry_run)
        LOG.info("final: %s", results)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
