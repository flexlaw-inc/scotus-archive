"""
Phase 7a scanner v2 — uses single alternation regex per taxonomy for speed.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import psycopg2
import psycopg2.extras

from pipeline.phase7a_regex.patterns import load_compiled

DB_URL = "postgresql:///legal_research"
DOCS_DIR = Path("/home/john/scotus-archive/docs")
SOURCE_TAG = "regex_v1"
DEFAULT_CONFIDENCE = "medium"


def era_bucket(year: int | None) -> str:
    if year is None:
        return "unknown"
    if year < 1866:
        return "pre-Reconstruction (<1866)"
    if year < 1900:
        return "Reconstruction-Gilded (1866-1899)"
    if year < 1937:
        return "Lochner (1900-1936)"
    if year < 1953:
        return "New Deal-early civil rights (1937-1952)"
    if year < 1969:
        return "Warren Court (1953-1968)"
    if year < 1986:
        return "Burger Court (1969-1985)"
    if year < 2005:
        return "Rehnquist Court (1986-2004)"
    return "Roberts Court (2005-)"


def resolve_taxonomy_ids(conn) -> tuple[dict[str, int], dict[str, int]]:
    with conn.cursor() as cur:
        cur.execute("SELECT canonical_id, id FROM constitutional_provisions")
        prov_ids = {cid: pid for cid, pid in cur.fetchall()}
        cur.execute("SELECT canonical_id, id FROM doctrinal_tests")
        test_ids = {cid: tid for cid, tid in cur.fetchall()}
    return prov_ids, test_ids


def scan_one(text: str, mega: re.Pattern, meta: list) -> dict[str, list[str]]:
    """Run one alternation regex across the text; return canonical_id -> labels."""
    hits: dict[str, list[str]] = defaultdict(list)
    for m in mega.finditer(text):
        # Find which group matched (only one is non-None per match)
        li = m.lastindex
        if li is None:
            continue
        cid, label = meta[li - 1]  # groups are 1-indexed
        if label not in hits[cid]:
            hits[cid].append(label)
    return hits


def run(apply_writes: bool, audit: bool, limit: int | None, min_year: int | None) -> None:
    (prov_mega, prov_meta), (doc_mega, doc_meta) = load_compiled()
    print(f"[patterns] prov_groups={prov_mega.groups} doc_groups={doc_mega.groups}")

    read_conn = psycopg2.connect(DB_URL)
    read_conn.set_session(readonly=True)
    write_conn = psycopg2.connect(DB_URL) if apply_writes else None

    prov_id_map, test_id_map = resolve_taxonomy_ids(read_conn)
    print(f"[taxonomy] provisions={len(prov_id_map)} doctrines={len(test_id_map)}")

    scan = read_conn.cursor(name="phase7a_scan", cursor_factory=psycopg2.extras.DictCursor)
    scan.itersize = 500
    sql = """
        SELECT o.id AS opinion_id, o.plain_text, c.decision_date, c.case_name,
               length(o.plain_text) AS ln
        FROM opinions o
        JOIN cases c ON c.id = o.case_id
        WHERE c.court_id = 1
          AND o.plain_text IS NOT NULL
          AND length(o.plain_text) >= 400
    """
    if min_year:
        sql += f" AND c.decision_date >= '{int(min_year)}-01-01'"
    sql += " "
    if limit:
        sql += f" LIMIT {int(limit)}"
    scan.execute(sql)

    scanned = 0
    any_prov = 0
    any_doc = 0
    any_match = 0
    prov_ctr: Counter = Counter()
    doc_ctr: Counter = Counter()
    era_gap: Counter = Counter()
    era_total: Counter = Counter()
    gap_samples: dict[str, list[dict]] = defaultdict(list)

    prov_rows: list[tuple] = []
    doc_rows: list[tuple] = []
    BUF = 2000

    t0 = time.time()
    while True:
        batch = scan.fetchmany(500)
        if not batch:
            break
        for row in batch:
            scanned += 1
            text = (row["plain_text"] or "")[:50000]
            year = row["decision_date"].year if row["decision_date"] else None
            era = era_bucket(year)
            era_total[era] += 1

            ph = scan_one(text, prov_mega, prov_meta)
            dh = scan_one(text, doc_mega, doc_meta)
            if ph:
                any_prov += 1
            if dh:
                any_doc += 1
            if ph or dh:
                any_match += 1
            else:
                era_gap[era] += 1
                if len(gap_samples[era]) < 30:
                    gap_samples[era].append({
                        "opinion_id": row["opinion_id"],
                        "case_name": row["case_name"],
                        "decision_date": str(row["decision_date"]) if row["decision_date"] else "",
                        "length": row["ln"],
                        "opener": re.sub(r"\s+", " ", text[:300]).strip(),
                    })

            for cid, labels in ph.items():
                prov_ctr[cid] += 1
                if apply_writes and cid in prov_id_map:
                    prov_rows.append((row["opinion_id"], prov_id_map[cid], "primary",
                                      "|".join(labels[:3])[:200], SOURCE_TAG, DEFAULT_CONFIDENCE))
            for cid in dh:
                doc_ctr[cid] += 1
                if apply_writes and cid in test_id_map:
                    doc_rows.append((row["opinion_id"], test_id_map[cid], "applies",
                                     SOURCE_TAG, DEFAULT_CONFIDENCE))

            if apply_writes and (len(prov_rows) >= BUF or len(doc_rows) >= BUF):
                _flush(write_conn, prov_rows, doc_rows)
                prov_rows, doc_rows = [], []

        if scanned % 50 == 0:
            rate = scanned / (time.time() - t0 + 1e-9)
            print(f"[scan] {scanned:>7}  ({rate:.0f}/s)  any_match_so_far={any_match}  gap={sum(era_gap.values())}", flush=True)

    if apply_writes:
        _flush(write_conn, prov_rows, doc_rows)
        write_conn.close()
    read_conn.close()

    elapsed = time.time() - t0
    print(f"\n[done] scanned={scanned} in {elapsed:.1f}s ({scanned/max(1,elapsed):.0f}/s)")
    print(f"       with_any_provision_match = {any_prov:>6}  ({_pct(any_prov, scanned)}%)")
    print(f"       with_any_doctrine_match  = {any_doc:>6}  ({_pct(any_doc, scanned)}%)")
    print(f"       with_any_match           = {any_match:>6}  ({_pct(any_match, scanned)}%)")
    print(f"       zero_match               = {scanned - any_match:>6}  ({_pct(scanned - any_match, scanned)}%)")

    print("\n[top provision hits]")
    for cid, n in prov_ctr.most_common(20):
        print(f"  {cid:<44} {n}")
    print("\n[top doctrine hits]")
    for cid, n in doc_ctr.most_common(20):
        print(f"  {cid:<32} {n}")
    print("\n[gap by era]")
    for era in sorted(era_total, key=_era_key):
        print(f"  {era:<46} total={era_total[era]:>6} gap={era_gap[era]:>6} ({_pct(era_gap[era], era_total[era])}%)")

    if audit:
        _write_audit(era_total, era_gap, gap_samples, prov_ctr, doc_ctr, scanned, any_match)


def _era_key(era: str) -> int:
    m = re.search(r"(\d{4})", era)
    if m:
        return int(m.group(1))
    if "pre-Reconstruction" in era:
        return 0
    return 9999


def _flush(write_conn, prov_rows, doc_rows) -> None:
    if not prov_rows and not doc_rows:
        return
    with write_conn.cursor() as cur:
        if prov_rows:
            psycopg2.extras.execute_values(cur,
                """INSERT INTO opinion_provisions (opinion_id, provision_id, role, citation_form_seen, source, confidence)
                   VALUES %s ON CONFLICT (opinion_id, provision_id, role) DO NOTHING""",
                prov_rows)
        if doc_rows:
            psycopg2.extras.execute_values(cur,
                """INSERT INTO opinion_doctrines (opinion_id, test_id, role, source, confidence)
                   VALUES %s ON CONFLICT (opinion_id, test_id, role) DO NOTHING""",
                doc_rows)
    write_conn.commit()


def _pct(n: int, d: int) -> str:
    return f"{100.0 * n / d:.2f}" if d else "0.00"


def _write_audit(era_total, era_gap, gap_samples, prov_ctr, doc_ctr, scanned, any_match) -> None:
    DOCS_DIR.mkdir(exist_ok=True)
    csv_path = DOCS_DIR / "phase7a-coverage-gap.csv"
    summary_path = DOCS_DIR / "phase7a-coverage-summary.md"

    with csv_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["era", "opinion_id", "case_name", "decision_date", "length", "opener_200c"])
        for era in sorted(era_total, key=_era_key):
            for s in gap_samples.get(era, []):
                w.writerow([era, s["opinion_id"], s["case_name"], s["decision_date"], s["length"], s["opener"][:200]])

    lines = [
        "# Phase 7a Coverage-Gap Audit",
        f"Generated: {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}",
        "",
        f"- Scanned: **{scanned:,}** SCOTUS opinions (length >= 400 chars)",
        f"- At least one provision or doctrine match: **{any_match:,}** ({_pct(any_match, scanned)}%)",
        f"- Zero matches: **{scanned - any_match:,}** ({_pct(scanned - any_match, scanned)}%)",
        "",
        "## Gap by era",
        "",
        "| Era | Total | Zero-match | Gap % |",
        "|---|---:|---:|---:|",
    ]
    for era in sorted(era_total, key=_era_key):
        t, g = era_total[era], era_gap[era]
        lines.append(f"| {era} | {t:,} | {g:,} | {_pct(g, t)}% |")
    lines += ["", "## Top 25 provision matches", "", "| canonical_id | hits |", "|---|---:|"]
    for cid, n in prov_ctr.most_common(25):
        lines.append(f"| `{cid}` | {n:,} |")
    lines += ["", "## Top 25 doctrine matches", "", "| canonical_id | hits |", "|---|---:|"]
    for cid, n in doc_ctr.most_common(25):
        lines.append(f"| `{cid}` | {n:,} |")
    lines += ["", "## Sample gap opinions (first 3 per era)", "", "Full list in `phase7a-coverage-gap.csv`.", ""]
    for era in sorted(era_total, key=_era_key):
        samples = gap_samples.get(era, [])[:3]
        if not samples:
            continue
        lines.append(f"### {era}")
        lines.append("")
        for s in samples:
            lines.append(f"- **{s['case_name']}** ({s['decision_date']}, {s['length']:,} chars): {s['opener'][:200]!r}")
        lines.append("")

    summary_path.write_text("\n".join(lines))
    print(f"[audit] wrote {csv_path}")
    print(f"[audit] wrote {summary_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--audit", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--min-year", type=int, default=None)
    args = ap.parse_args()
    if not args.apply and not args.dry_run:
        sys.exit("specify --apply or --dry-run")
    run(apply_writes=args.apply, audit=args.audit, limit=args.limit, min_year=args.min_year)
