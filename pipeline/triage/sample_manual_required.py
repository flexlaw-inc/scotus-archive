"""
Stream 1 triage: sample the 337,739 manual_required majority->other proposals and
classify opener patterns (cert denied, motion granted/denied, disbarment,
in-chambers, rehearing, unclassified) to inform an order-vs-opinion schema
decision.
"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

import psycopg2
import psycopg2.extras

DB_URL = "postgresql:///legal_research"
OUT_PATH = Path(__file__).with_name("triage_majority_to_other.json")
SAMPLE_SIZE = 1000

PATTERNS = [
    ("cert_denied",        re.compile(r"(?:petition.{0,40}(?:for[a-z ]*writ[a-z ]*of)?[a-z ]*certiorari|writ of certiorari)[^.\n]{0,120}(?:is )?denied", re.I)),
    ("cert_granted",       re.compile(r"(?:petition.{0,40}(?:for[a-z ]*writ[a-z ]*of)?[a-z ]*certiorari|writ of certiorari)[^.\n]{0,120}(?:is )?granted", re.I)),
    ("cert_dismissed",     re.compile(r"(?:petition.{0,40}certiorari|writ of certiorari)[^.\n]{0,160}(?:improvidently granted|dismissed)", re.I)),
    ("rehearing_denied",   re.compile(r"(?:petition )?for rehearing[^.\n]{0,80}denied", re.I)),
    ("stay_action",        re.compile(r"\bapplication for[^.\n]{0,80}stay[^.\n]{0,120}(?:granted|denied)", re.I)),
    ("injunction_action",  re.compile(r"\bapplication for[^.\n]{0,80}injunction[^.\n]{0,120}(?:granted|denied)", re.I)),
    ("mandamus_action",    re.compile(r"\b(?:petition|writ) (?:for )?[a-z]*\s*mandamus[^.\n]{0,120}(?:granted|denied)", re.I)),
    ("habeas_action",      re.compile(r"\b(?:petition|writ) (?:for )?[a-z]*\s*habeas[^.\n]{0,120}(?:granted|denied)", re.I)),
    ("motion_action",      re.compile(r"^(?:\s*[^\n]{0,80}\n){0,2}\s*(?:the )?motion[^.\n]{0,160}(?:is )?(?:granted|denied)", re.I)),
    ("disbarment",         re.compile(r"\b(?:disbarred|suspension from the (?:bar|practice)|disbarment|suspended from the practice of law|show cause why .* should not be disbarred)\b", re.I)),
    ("reinstatement",      re.compile(r"\breinstat(?:ed|ement)\b[^.\n]{0,80}(?:bar|practice of law)", re.I)),
    ("recusal",            re.compile(r"\btook no part in the (?:consideration|decision)\b", re.I)),
    ("in_chambers",        re.compile(r"\bin chambers\b|\bopinion in chambers\b", re.I)),
    ("summary_affirmance", re.compile(r"\bjudgment (?:is )?affirmed\b[^.\n]{0,40}\Z", re.I)),
    ("appeal_dismissed",   re.compile(r"\bappeal[^.\n]{0,40}(?:is )?dismissed\b", re.I)),
    ("bar_admission",      re.compile(r"\bpermit (?:the )?respondent to resign\b|\bmotion for (?:admission|leave) to (?:the bar|practice)", re.I)),
    ("leave_to_file_action", re.compile(r"\bleave to file[^.\n]{0,120}(?:granted|denied)", re.I)),
]


def normalize_opener(txt: str) -> str:
    if not txt:
        return ""
    opener = re.sub(r"\s+", " ", txt.strip())
    return opener[:240]


def classify(text: str) -> str:
    if not text or len(text.strip()) < 4:
        return "empty_or_tiny"
    head = text[:1500]
    for name, pat in PATTERNS:
        if pat.search(head):
            return name
    if len(text.strip()) < 400:
        return "short_unclassified"
    return "unclassified"


def main() -> None:
    with psycopg2.connect(DB_URL) as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(
            """
            SELECT r.opinion_id, o.plain_text, o.author, c.case_name, c.decision_date
            FROM reclassification_log r
            JOIN opinions o ON o.id = r.opinion_id
            JOIN cases c ON c.id = o.case_id
            WHERE r.old_type = 'majority'
              AND r.new_type = 'other'
              AND r.confidence = 'manual_required'
              AND c.court_id = 1
            ORDER BY random()
            LIMIT %s
            """,
            (SAMPLE_SIZE,),
        )
        rows = cur.fetchall()

    counter = Counter()
    examples: dict[str, list[dict]] = {}
    for row in rows:
        bucket = classify(row["plain_text"] or "")
        counter[bucket] += 1
        examples.setdefault(bucket, [])
        if len(examples[bucket]) < 3:
            examples[bucket].append({
                "opinion_id": row["opinion_id"],
                "case_name": row["case_name"],
                "decision_date": str(row["decision_date"]) if row["decision_date"] else None,
                "author": row["author"],
                "opener": normalize_opener(row["plain_text"] or ""),
            })

    total = sum(counter.values())
    report = {
        "sample_size": total,
        "total_population": 337_739,
        "buckets": sorted(
            [
                {
                    "name": name,
                    "count": n,
                    "pct": round(100.0 * n / total, 2),
                    "examples": examples.get(name, []),
                }
                for name, n in counter.items()
            ],
            key=lambda d: -d["count"],
        ),
    }
    OUT_PATH.write_text(json.dumps(report, indent=2, default=str))
    print(f"Wrote {OUT_PATH}")
    print(f"Sample size: {total}")
    for b in report["buckets"]:
        name = b["name"]
        count = b["count"]
        pct = b["pct"]
        print(f"  {name:<24} {count:>4}  ({pct:>5.2f}%)")


if __name__ == "__main__":
    main()
