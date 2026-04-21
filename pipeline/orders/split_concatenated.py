"""Concatenated-opinion splitter v4.

Same as v3 but with:
  - Stray OCR punctuation between "Justice" and surname, either order
    (handles 'Justice .NELSON', 'Justice. DANIEL', "Justice' Harlan").
  - Unicode fancy apostrophes U+2018/U+2019 in surnames ("M\u2018Lean").
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path

import psycopg2
import psycopg2.extras

DB_URL = os.environ.get("DB_URL", "postgresql:///legal_research")
PIPELINE_VERSION = "splitter_v1"

_ONE_NAME_TC = r"(?:(?:Mr|Mb|Ml)\.?\s+)?(?:(?:Chief|Chtee|Cbief)\s+)?Justice[\s.,'\u2018\u2019]+[A-Z][a-zA-Z'\u2018\u2019\-]+(?:,\s*(?:Jr\.|Sr\.|II|III))?"
_ONE_NAME_UC = r"(?:MR\.?\s+)?(?:CHIEF\s+)?JUSTICE[\s.,'\u2018\u2019]+[A-Z][A-Z'\u2018\u2019\-]+(?:,\s*(?:JR\.|SR\.|II|III))?"

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

_WSP = r"[\s.,\-']+"

_ROLE_TC = (
    rf"delivered{_WSP}the{_WSP}(?:following{_WSP})?opinion(?:{_WSP}of{_WSP}the{_WSP}(?:Court|court))?(?:{_WSP}as{_WSP}follows(?:{_WSP}viz\.?)?)?"
    rf"|announced{_WSP}the{_WSP}judgment{_WSP}of{_WSP}the{_WSP}(?:Court|court)"
    rf"|announced{_WSP}the{_WSP}conclusions{_WSP}reached{_WSP}by{_WSP}the{_WSP}(?:Court|court)"
    r"|concurring(?:\s+in\s+(?:the\s+)?(?:judgment|part(?:[^.]*?)))?"
    r"|dissenting(?:\s+in\s+(?:the\s+)?(?:judgment|part(?:[^.]*?)))?"
    r"|concurring\s+in\s+part\s+and\s+dissenting\s+in\s+part"
    r"|dissenting\s+in\s+part\s+and\s+concurring\s+in\s+part"
)

_ROLE_UC = _ROLE_TC.replace("delivered the opinion of the Court", "DELIVERED THE OPINION OF THE COURT")
_ROLE_UC = _ROLE_UC.upper()

_PINCITE_PREFIX = r"(?:\[\*\d+\]\s+)?"

BOUNDARY_TC_RX = re.compile(
    rf"^\s*{_PINCITE_PREFIX}({_NAME_TC})(?:\s*,\s*with\s+whom\s+{_NAME_TC}\s+joins?,?)?\s*,?\s+({_ROLE_TC})\s*\.?",
    re.MULTILINE,
)
BOUNDARY_UC_RX = re.compile(
    rf"^\s*{_PINCITE_PREFIX}({_NAME_UC})(?:\s*,\s*WITH\s+WHOM\s+{_NAME_UC}\s+JOINS?,?)?\s*,?\s+({_ROLE_UC})\s*\.?",
    re.MULTILINE,
)

_SURNAME = r"[A-Z][a-zA-Z'\u2018\u2019\-]{2,18}"
BOUNDARY_SURNAME_RX = re.compile(
    rf"^(?P<name>{_SURNAME}),\s*"
    rf"(?:Ch\.?|Cbief|Chtee|Chief)?\s*J\.\s*$\n"
    rf"(?:\s*\n)*"
    rf"\s*(?P<role>{_ROLE_TC})\s*\.?",
    re.MULTILINE,
)

BOUNDARY_INVERTED_RX = re.compile(
    rf"^\s*{_PINCITE_PREFIX}(?:The|THE){_WSP}(?:opinion|OPINION){_WSP}of{_WSP}(?:the|THE){_WSP}(?:court|Court|COURT){_WSP}(?:was|WAS){_WSP}(?:delivered|DELIVERED){_WSP}(?:by|BY)\s*[:.]?\s*$\n"
    rf"(?:\s*\n)*"
    rf"\s*{_PINCITE_PREFIX}(?P<name>{_ONE_NAME_TC}|{_ONE_NAME_UC})\s*\.?",
    re.MULTILINE,
)

PER_CURIAM_RX = re.compile(r"^\s*PER\s+CURIAM\.?\s*$", re.MULTILINE | re.IGNORECASE)
SYLLABUS_RX = re.compile(r"^\s*Syllabus\s*$|^\s*SYLLABUS\s*$", re.MULTILINE)


def _normalize_role(role):
    r = re.sub(r"[^a-z0-9]+", " ", role.lower()).strip()
    r = r.replace("the following opinion", "the opinion")
    if "concurring in part and dissenting in part" in r:
        return "combined"
    if "dissenting in part and concurring in part" in r:
        return "combined"
    if "delivered the opinion of the court" in r:
        return "majority"
    if "delivered the opinion" in r:
        return "majority"
    if "announced the judgment of the court" in r:
        return "plurality"
    if "announced the conclusions reached by the court" in r:
        return "majority"
    if "dissenting" in r:
        return "dissent"
    if "concurring" in r:
        return "concurrence"
    return "other"


def _normalize_author(name):
    return name.strip()


def _ocr_normalize_for_boundary(text):
    t = re.sub(r"(?<=\W)Mb\.", "Mr.", text)
    t = re.sub(r"^Mb\.", "Mr.", t, flags=re.MULTILINE)
    t = t.replace("Chtee", "Chief")
    t = re.sub(r"\btha(?=\s+[Cc]ourt\b)", "the", t)
    return t


def find_boundaries(text):
    nt = _ocr_normalize_for_boundary(text)
    hits = []

    m = SYLLABUS_RX.search(nt)
    if m:
        hits.append({
            "kind":          "syllabus",
            "offset":        m.start(),
            "boundary_text": m.group(0).strip(),
            "author":        None,
            "opinion_type":  "other",
            "is_syllabus":   True,
        })

    for m in PER_CURIAM_RX.finditer(nt):
        hits.append({
            "kind":          "per_curiam",
            "offset":        m.start(),
            "boundary_text": m.group(0).strip(),
            "author":        None,
            "opinion_type":  "per_curiam",
            "is_syllabus":   False,
        })

    for rx, case_label in [(BOUNDARY_TC_RX, "tc"), (BOUNDARY_UC_RX, "uc")]:
        for m in rx.finditer(nt):
            author = _normalize_author(m.group(1))
            role = m.group(2)
            hits.append({
                "kind":          f"authored_{case_label}",
                "offset":        m.start(),
                "boundary_text": m.group(0).strip()[:120],
                "author":        author,
                "opinion_type":  _normalize_role(role),
                "role_raw":      role,
                "is_syllabus":   False,
            })

    for m in BOUNDARY_SURNAME_RX.finditer(nt):
        author = m.group("name").strip()
        role = m.group("role")
        hits.append({
            "kind":          "authored_surname",
            "offset":        m.start(),
            "boundary_text": m.group(0).strip()[:120],
            "author":        author,
            "opinion_type":  _normalize_role(role),
            "role_raw":      role,
            "is_syllabus":   False,
        })

    for m in BOUNDARY_INVERTED_RX.finditer(nt):
        author = _normalize_author(m.group("name"))
        hits.append({
            "kind":          "authored_inverted",
            "offset":        m.start(),
            "boundary_text": m.group(0).strip()[:120],
            "author":        author,
            "opinion_type":  "majority",
            "role_raw":      "delivered the opinion of the court",
            "is_syllabus":   False,
        })

    hits.sort(key=lambda h: h["offset"])
    deduped = []
    for h in hits:
        if deduped and abs(h["offset"] - deduped[-1]["offset"]) < 5:
            continue
        deduped.append(h)
    return deduped


def segment(text, boundaries):
    if not boundaries:
        return [{
            "sequence":      0,
            "kind":          "unsplit",
            "opinion_type": "majority",
            "author":        None,
            "boundary_text": None,
            "body":          text,
            "body_length":   len(text),
        }]

    blocks = []

    if boundaries[0]["offset"] > 200:
        head = text[: boundaries[0]["offset"]]
        blocks.append({
            "sequence":      0,
            "kind":          "preamble",
            "opinion_type":  "other",
            "author":        None,
            "boundary_text": None,
            "body":          head,
            "body_length":   len(head),
        })

    for i, b in enumerate(boundaries):
        end = boundaries[i + 1]["offset"] if i + 1 < len(boundaries) else len(text)
        body = text[b["offset"]:end]
        blocks.append({
            "sequence":      len(blocks),
            "kind":          b["kind"],
            "opinion_type":  b["opinion_type"],
            "author":        b["author"],
            "boundary_text": b["boundary_text"],
            "body":          body,
            "body_length":   len(body),
        })
    return blocks


def score(blocks):
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


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", action="store_true", default=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--court-id", type=int, default=1)
    parser.add_argument("--min-length", type=int, default=1000)
    parser.add_argument("--opinion-id", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--out", type=str, default="/home/john/scotus-archive/docs/splitter-report.json")
    args = parser.parse_args(argv)

    if args.apply:
        print("ERROR: --apply not yet implemented.", file=sys.stderr)
        return 2

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if args.opinion_id:
        cur.execute(SELECT_ONE_SQL, {"opinion_id": args.opinion_id})
    else:
        cur.execute(SELECT_COHORT_SQL, {"court_id": args.court_id, "min_length": args.min_length})

    out_rows = []
    confidence_counts = Counter()
    block_kind_counts = Counter()
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
            "opinion_id":     row["opinion_id"],
            "case_id":        row["case_id"],
            "decision_date":  row["decision_date"].isoformat() if row["decision_date"] else None,
            "text_length":    len(text),
            "boundary_count": len(boundaries),
            "block_count":    len(blocks),
            "confidence":     conf,
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
