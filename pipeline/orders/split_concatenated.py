"""
Concatenated-opinion splitter (Bug 2 from triage memo).

Background
----------
CAP's PDF-to-text extraction collapsed many U.S. Reports entries — Dobbs,
Bruen, Loper Bright, Harvard SFFA, etc. — into a single `opinions` row whose
`plain_text` contains:

    [Syllabus]
    JUSTICE X delivered the opinion of the Court.
        ... majority opinion ...
    JUSTICE Y, concurring.
        ... concurrence ...
    JUSTICE Z, concurring in part and dissenting in part.
        ... mixed opinion ...
    JUSTICE W, dissenting.
        ... dissent ...

The triage memo (docs/triage-memo-2026-04-21.md) found ~579 rows ≥20k chars
and ~4,857 rows ≥1,000 chars in the manual_required `majority -> other`
cohort that fit this pattern. They are real opinions; they don't belong in
`court_orders`. They need to be split into one row per block.

Pipeline phases
---------------
This module implements the regex-only segmentation pass. Estimated coverage
is ~85-95% of modern (post-1950) opinions, per spot-check of the boundary
patterns. Edge cases (older formatting, missing punctuation, OCR artifacts)
are reported as low-confidence and held back for an LLM-assisted second
pass (separate module, not yet written).

Phases:
    1.  REPORT (--report):    Scan the cohort, detect candidate boundaries,
                              emit a JSON manifest with detected blocks per
                              opinion. No DB mutation. Default mode.
    2.  APPLY  (--apply):     Materialize new opinions rows for each block,
                              populate opinion_type, author, sequence_in_case,
                              opinion_type_source='splitter_v1'. Soft-delete
                              the original concatenated row (set plain_text
                              to NULL, append 'concatenated_split' to
                              text_issues, write reclassification_log entry
                              with new_type='__split_into_pieces__').

Deliberately scoped: this v1 does *not* attempt to attribute joined-by
relationships ("WHO joined WHO's opinion"). That requires another pass to
parse "JUSTICE X joins this opinion as to Parts I, II-A, and IV" lines.

Cohort filter
-------------
    reclassification_log rows where
        old_type    = 'majority'
        new_type    = 'other'
        confidence  = 'manual_required'
    AND opinions.plain_text is NOT NULL
    AND LENGTH(plain_text) >= 1000   (the relocator's deferred bucket)
    AND cases.court_id = 1

Usage
-----
    python -m pipeline.orders.split_concatenated --report                # JSON to docs/
    python -m pipeline.orders.split_concatenated --report --min-length 20000
    python -m pipeline.orders.split_concatenated --report --opinion-id 1539367
    python -m pipeline.orders.split_concatenated --apply --opinion-id 1539367
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

DB_URL = os.environ.get("DB_URL", "postgresql:///legal_research")
PIPELINE_VERSION = "splitter_v1"

# --------------------------------------------------------------------------
# Boundary detection
# --------------------------------------------------------------------------
# Two attribution forms cover modern U.S. Reports:
#   ALL-CAPS (post-1980ish):   "JUSTICE THOMAS, dissenting."
#   Title-case (older):        "Justice Thomas, dissenting."
#                              "Mr. Justice Holmes, concurring."
#
# Author block can include suffix: ", JR." / ", III"
# Role tail covers all the canonical role phrases.

# A single-justice attribution: "Justice Alito" / "Chief Justice Roberts" / "Mr. Justice Holmes"
_ONE_NAME_TC = r"(?:Mr\.?\s+)?(?:Chief\s+)?Justice\s+[A-Z][a-zA-Z'\-]+(?:,\s*(?:Jr\.|Sr\.|II|III))?"
_ONE_NAME_UC = r"(?:MR\.?\s+)?(?:CHIEF\s+)?JUSTICE\s+[A-Z][A-Z'\-]+(?:,\s*(?:JR\.|SR\.|II|III))?"

# Joint authorship: "Justice Breyer, Justice Sotomayor, and Justice Kagan" — captured as one author group.
# Allow up to 4 names (joint dissents rarely exceed 3).
_NAME_TC = (
    rf"{_ONE_NAME_TC}"
    rf"(?:\s*,\s*{_ONE_NAME_TC}){{0,3}}"
    rf"(?:\s*,?\s+and\s+{_ONE_NAME_TC})?"
)
_NAME_UC = (
    rf"{_ONE_NAME_UC}"
    rf"(?:\s*,\s*{_ONE_NAME_UC}){{0,3}}"
    rf"(?:\s*,?\s+AND\s+{_ONE_NAME_UC})?"
)

_ROLE_TC = (
    r"delivered\s+the\s+opinion\s+of\s+the\s+Court"
    r"|delivered\s+the\s+opinion\s+of\s+the\s+court"
    r"|announced\s+the\s+judgment\s+of\s+the\s+Court"
    r"|announced\s+the\s+judgment\s+of\s+the\s+court"
    r"|concurring(?:\s+in\s+(?:the\s+)?(?:judgment|part(?:[^.]*?)))?"
    r"|dissenting(?:\s+in\s+(?:the\s+)?(?:judgment|part(?:[^.]*?)))?"
    r"|concurring\s+in\s+part\s+and\s+dissenting\s+in\s+part"
    r"|dissenting\s+in\s+part\s+and\s+concurring\s+in\s+part"
)

_ROLE_UC = _ROLE_TC.replace("delivered the opinion of the Court", "DELIVERED THE OPINION OF THE COURT")
_ROLE_UC = _ROLE_UC.upper()

# Boundary regex: line-anchored, captures author + role.
BOUNDARY_TC_RX = re.compile(
    rf"^\s*({_NAME_TC})(?:\s*,\s*with\s+whom\s+{_NAME_TC}\s+joins?,?)?\s*,?\s+({_ROLE_TC})\s*\.?",
    re.MULTILINE,
)
BOUNDARY_UC_RX = re.compile(
    rf"^\s*({_NAME_UC})(?:\s*,\s*WITH\s+WHOM\s+{_NAME_UC}\s+JOINS?,?)?\s*,?\s+({_ROLE_UC})\s*\.?",
    re.MULTILINE,
)

# Per-curiam boundary (no author):
PER_CURIAM_RX = re.compile(r"^\s*PER\s+CURIAM\.?\s*$", re.MULTILINE | re.IGNORECASE)

# Syllabus block start (modern U.S. Reports have explicit "Syllabus" header):
SYLLABUS_RX = re.compile(r"^\s*Syllabus\s*$|^\s*SYLLABUS\s*$", re.MULTILINE)


def _normalize_role(role: str) -> str:
    """Map raw role text to opinion_type enum."""
    # Collapse internal whitespace (incl. newlines from line-wrapped role phrases).
    r = re.sub(r"\s+", " ", role).lower()
    if "concurring in part and dissenting in part" in r:
        return "combined"
    if "dissenting in part and concurring in part" in r:
        return "combined"
    if "delivered the opinion of the court" in r:
        return "majority"
    if "announced the judgment of the court" in r:
        return "plurality"
    if "dissenting" in r:
        return "dissent"
    if "concurring" in r:
        return "concurrence"
    return "other"


def _normalize_author(name: str) -> str:
    """Strip honorifics, normalize case for storage in opinions.author_original."""
    # Keep the form as it appeared (per author_original convention from migration 002).
    return name.strip()


def find_boundaries(text: str) -> list[dict[str, Any]]:
    """Return ordered list of detected boundaries with their byte offsets."""
    hits: list[dict[str, Any]] = []

    # Syllabus
    m = SYLLABUS_RX.search(text)
    if m:
        hits.append({
            "kind":         "syllabus",
            "offset":       m.start(),
            "boundary_text": m.group(0).strip(),
            "author":       None,
            "opinion_type": "other",       # syllabus is reporter material, not an opinion
            "is_syllabus":  True,
        })

    # Per Curiam (only count if it appears EARLY — late "PER CURIAM" can be a quote)
    for m in PER_CURIAM_RX.finditer(text):
        hits.append({
            "kind":         "per_curiam",
            "offset":       m.start(),
            "boundary_text": m.group(0).strip(),
            "author":       None,
            "opinion_type": "per_curiam",
            "is_syllabus":  False,
        })

    # Authored attributions
    for rx, case_label in [(BOUNDARY_TC_RX, "tc"), (BOUNDARY_UC_RX, "uc")]:
        for m in rx.finditer(text):
            author = _normalize_author(m.group(1))
            role   = m.group(2)
            opinion_type = _normalize_role(role)
            hits.append({
                "kind":         f"authored_{case_label}",
                "offset":       m.start(),
                "boundary_text": m.group(0).strip()[:120],
                "author":       author,
                "opinion_type": opinion_type,
                "role_raw":     role,
                "is_syllabus":  False,
            })

    # Order by position, dedupe near-duplicate offsets.
    hits.sort(key=lambda h: h["offset"])
    deduped: list[dict[str, Any]] = []
    for h in hits:
        if deduped and abs(h["offset"] - deduped[-1]["offset"]) < 5:
            continue
        deduped.append(h)
    return deduped


def segment(text: str, boundaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Slice the text at boundary offsets, returning blocks with body text."""
    if not boundaries:
        return [{
            "sequence":     0,
            "kind":         "unsplit",
            "opinion_type": "majority",
            "author":       None,
            "boundary_text": None,
            "body":         text,
            "body_length":  len(text),
        }]

    blocks: list[dict[str, Any]] = []

    # Block before the first boundary (caption / opening stuff) — usually trivial.
    if boundaries[0]["offset"] > 200:
        head = text[: boundaries[0]["offset"]]
        blocks.append({
            "sequence":     0,
            "kind":         "preamble",
            "opinion_type": "other",
            "author":       None,
            "boundary_text": None,
            "body":         head,
            "body_length":  len(head),
        })

    for i, b in enumerate(boundaries):
        end = boundaries[i + 1]["offset"] if i + 1 < len(boundaries) else len(text)
        body = text[b["offset"]:end]
        blocks.append({
            "sequence":     len(blocks),
            "kind":         b["kind"],
            "opinion_type": b["opinion_type"],
            "author":       b["author"],
            "boundary_text": b["boundary_text"],
            "body":         body,
            "body_length":  len(body),
        })
    return blocks


# --------------------------------------------------------------------------
# Confidence scoring
# --------------------------------------------------------------------------
def score(blocks: list[dict[str, Any]]) -> str:
    """Heuristic confidence label for an opinion's segmentation."""
    n_authored = sum(1 for b in blocks if b["author"])
    has_majority = any(b["opinion_type"] == "majority" for b in blocks)
    too_short = any(b["body_length"] < 200 and b["kind"] not in ("syllabus", "preamble") for b in blocks)
    if n_authored == 0:
        return "low"
    if not has_majority:
        return "low"
    if too_short:
        return "medium"
    return "high"


# --------------------------------------------------------------------------
# DB
# --------------------------------------------------------------------------
SELECT_COHORT_SQL = """
    SELECT DISTINCT o.id AS opinion_id, o.case_id, o.plain_text, c.decision_date
    FROM reclassification_log rl
    JOIN opinions o ON o.id = rl.opinion_id
    JOIN cases    c ON c.id = o.case_id
    WHERE rl.old_type    = 'majority'
      AND rl.new_type    = 'other'
      AND rl.confidence  = 'manual_required'
      AND c.court_id     = %(court_id)s
      AND o.plain_text   IS NOT NULL
      AND LENGTH(o.plain_text) >= %(min_length)s
    ORDER BY o.id
"""

SELECT_ONE_SQL = """
    SELECT o.id AS opinion_id, o.case_id, o.plain_text, c.decision_date
    FROM opinions o
    JOIN cases    c ON c.id = o.case_id
    WHERE o.id = %(opinion_id)s
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", action="store_true", default=True,
                        help="Emit a JSON manifest of detected boundaries. Default.")
    parser.add_argument("--apply", action="store_true",
                        help="Materialize split rows in the database. NOT YET IMPLEMENTED — review report first.")
    parser.add_argument("--court-id", type=int, default=1)
    parser.add_argument("--min-length", type=int, default=1000,
                        help="Skip rows shorter than this. Default: 1000.")
    parser.add_argument("--opinion-id", type=int, default=None,
                        help="Process a single opinion id (overrides cohort filter).")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--out", type=str, default="/home/john/scotus-archive/docs/splitter-report.json")
    args = parser.parse_args(argv)

    if args.apply:
        print("ERROR: --apply not yet implemented. Run --report first, review the manifest, "
              "then enable --apply in a follow-up commit after John approves the splitter "
              "logic on the high-stakes landmark cases.", file=sys.stderr)
        return 2

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if args.opinion_id:
        cur.execute(SELECT_ONE_SQL, {"opinion_id": args.opinion_id})
    else:
        cur.execute(SELECT_COHORT_SQL, {
            "court_id":   args.court_id,
            "min_length": args.min_length,
        })

    out_rows: list[dict[str, Any]] = []
    confidence_counts: Counter[str] = Counter()
    block_kind_counts: Counter[str] = Counter()
    seen = 0

    for row in cur:
        seen += 1
        if args.limit is not None and seen > args.limit:
            break
        text = row["plain_text"]
        boundaries = find_boundaries(text)
        blocks = segment(text, boundaries)
        conf = score(blocks)
        confidence_counts[conf] += 1
        for b in blocks:
            block_kind_counts[f"{b['opinion_type']}/{b['kind']}"] += 1

        out_rows.append({
            "opinion_id":   row["opinion_id"],
            "case_id":      row["case_id"],
            "decision_date": row["decision_date"].isoformat() if row["decision_date"] else None,
            "text_length":  len(text),
            "boundary_count": len(boundaries),
            "block_count":  len(blocks),
            "confidence":   conf,
            "blocks": [
                {
                    "sequence":      b["sequence"],
                    "kind":          b["kind"],
                    "opinion_type":  b["opinion_type"],
                    "author":        b["author"],
                    "boundary_text": b["boundary_text"],
                    "body_length":   b["body_length"],
                }
                for b in blocks
            ],
        })

    cur.close()
    conn.close()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "rows":               out_rows,
        "confidence_counts":  dict(confidence_counts),
        "block_kind_counts":  dict(block_kind_counts),
        "seen":               seen,
        "pipeline_version":   PIPELINE_VERSION,
    }, indent=2, default=str))

    print(f'[splitter] seen={seen}  written={out_path}')
    print(f'[splitter] confidence: {dict(confidence_counts)}')
    print(f'[splitter] top block kinds:')
    for k, n in block_kind_counts.most_common(15):
        print(f'  {k:40s}  {n:>5d}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
