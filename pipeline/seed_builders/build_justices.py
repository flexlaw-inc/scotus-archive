#!/usr/bin/env python3
"""
pipeline/seed_builders/build_justices.py — reproducible seed builder.

Regenerates seeds/justices.yaml from authoritative online sources:

  - Federal Judicial Center Biographical Directory of Article III Judges
    https://www.fjc.gov/sites/default/files/history/judges.csv
    Primary for: birth/death dates, commission/termination dates, appointing
    president, appointing party, gender, race/ethnicity, Seat ID.

  - Oyez Project justices listing
    https://api.oyez.org/justices?per_page=200
    Primary for: oyez_justice_id (Oyez `identifier` slug).

Matching strategy
-----------------
For each FJC SCOTUS row, pick the Oyez record with the highest score:

  +10 : first-name initial match
  +60 : Oyez role start year within ±2 of FJC commission year
  +20 : Oyez role start year within ±10 of FJC commission year
  +10 : both have "II" suffix (Harlan II tie-breaker)
  -20 : Oyez has "II", FJC does not, and years differ by >10 (anti-match)

For duplicate-name collisions (the two John Marshall Harlans) the year
signal dominates; the suffix check is a tiebreaker because FJC stores an
empty Suffix for Harlan II.

Aliases
-------
Three seeded Oyez identifiers do not match the current Oyez listing slug
but resolve to the same Oyez record when queried at /justices/{id}. The
seeded slug is retained as canonical:

    harlan_fiske_stone → harlan_f_stone
    stanley_reed       → stanley_f_reed
    harold_burton      → harold_h_burton

Run
---
    python3 -m pipeline.seed_builders.build_justices \\
        --fjc https://www.fjc.gov/sites/default/files/history/judges.csv \\
        --oyez https://api.oyez.org/justices?per_page=200 \\
        --out seeds/justices.yaml

Without args, uses the canonical URLs and writes to seeds/justices.yaml.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import re
import urllib.request
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path

import yaml

FJC_URL = "https://www.fjc.gov/sites/default/files/history/judges.csv"
OYEZ_URL = "https://api.oyez.org/justices?per_page=200"

OYEZ_ALIASES = {
    "harlan_fiske_stone": "harlan_f_stone",
    "stanley_reed":       "stanley_f_reed",
    "harold_burton":      "harold_h_burton",
}

FIELD_ORDER = [
    "canonical_name", "display_name",
    "full_first", "full_middle", "full_last", "suffix",
    "born", "died",
    "tenure_start", "tenure_end",
    "chief_justice", "chief_tenure_start", "chief_tenure_end",
    "appointing_president", "appointing_party",
    "law_school", "gender", "race_ethnicity",
    "oyez_justice_id", "fjc_seat_id",
    "succession_seat",
    "notes",
]


# ── fetch helpers ────────────────────────────────────────────────────────────

def fetch_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=60) as r:
        return r.read().decode("utf-8", errors="replace")


def fetch_json(url: str):
    return json.loads(fetch_text(url))


# ── data normalizers ─────────────────────────────────────────────────────────

def norm_name(s: str, strip_suffix: bool = True) -> str:
    s = s.lower()
    s = re.sub(r"[.,'\"]", "", s)
    if strip_suffix:
        s = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", s)
    return re.sub(r"\s+", " ", s).strip()


def iso_or_none(datestr: str | None) -> str | None:
    if not datestr:
        return None
    s = datestr.strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        mm, dd, yy = m.groups()
        return f"{yy}-{int(mm):02d}-{int(dd):02d}"
    if re.match(r"^\d{4}$", s):
        return s
    return s


def year_of(s: str | None) -> int | None:
    if not s:
        return None
    m = re.match(r"(\d{4})", s)
    return int(m.group(1)) if m else None


def build_birth_death(row: dict, kind: str = "Birth") -> str | None:
    y = (row.get(f"{kind} Year") or "").strip()
    m = (row.get(f"{kind} Month") or "").strip()
    d = (row.get(f"{kind} Day") or "").strip()
    if not y:
        return None
    if not m:
        return y
    if not d:
        return f"{y}-{int(m):02d}"
    return f"{y}-{int(m):02d}-{int(d):02d}"


def map_party(party: str, president: str | None = None) -> str | None:
    """Map FJC party to the convention used in existing seeds.

    FJC leaves the Party field blank for Washington appointees (non-partisan
    era); existing seeds classify them as Federalist. "Jeffersonian
    Republican" is FJC's name for what later became the Democratic-
    Republican Party.
    """
    if not party:
        if president == "George Washington":
            return "Federalist"
        return None
    table = {
        "Democratic": "Democratic",
        "Republican": "Republican",
        "Federalist": "Federalist",
        "Democratic-Republican": "Democratic-Republican",
        "Jeffersonian Republican": "Democratic-Republican",
        "Whig": "Whig",
        "None (Washington)": "Federalist",
        "None (Andrew Johnson)": "Democratic",
    }
    return table.get(party, party)


# ── FJC parser ───────────────────────────────────────────────────────────────

def parse_fjc_scotus(csv_text: str) -> list[dict]:
    rows = list(csv.DictReader(io.StringIO(csv_text)))
    scotus = []
    for row in rows:
        appts = []
        for i in range(1, 7):
            if (row.get(f"Court Type ({i})", "") or "").strip() == "Supreme Court":
                appts.append({
                    "idx": i,
                    "title": (row.get(f"Appointment Title ({i})", "") or "").strip(),
                    "commission": (row.get(f"Commission Date ({i})", "") or "").strip(),
                    "termination": (row.get(f"Termination Date ({i})", "") or "").strip(),
                    "president": (row.get(f"Appointing President ({i})", "") or "").strip(),
                    "party": (row.get(f"Party of Appointing President ({i})", "") or "").strip(),
                    "seat_id": (row.get(f"Seat ID ({i})", "") or "").strip(),
                })
        if appts:
            scotus.append({"row": row, "appts": appts})
    return scotus


# ── Oyez matcher ─────────────────────────────────────────────────────────────

def oyez_role_start_year(o: dict) -> int | None:
    for role in o.get("roles", []):
        ds = role.get("date_start")
        if ds is None:
            continue
        try:
            return datetime.fromtimestamp(ds, tz=timezone.utc).year
        except (OverflowError, OSError, ValueError):
            return 1970 + int(ds / 31557600)
    return None


def match_fjc_to_oyez(fr: dict, oyez_by_last: dict[str, list[dict]]) -> dict | None:
    r = fr["row"]
    last = (r.get("Last Name") or "").strip().lower()
    first = (r.get("First Name") or "").strip().lower()
    suffix = (r.get("Suffix") or "").strip()
    candidates = oyez_by_last.get(last, [])
    if not candidates:
        return None
    fjc_year = min((year_of(a["commission"]) for a in fr["appts"] if year_of(a["commission"])), default=None)

    scored = []
    for o in candidates:
        name = o.get("name", "")
        n_no_suf = norm_name(name)
        n_with_suf = norm_name(name, strip_suffix=False)
        score = 0
        if n_no_suf.split() and n_no_suf.split()[0][:1] == first[:1]:
            score += 10
        oyez_year = oyez_role_start_year(o)
        if fjc_year and oyez_year:
            diff = abs(fjc_year - oyez_year)
            if diff <= 2:
                score += 60
            elif diff <= 10:
                score += 20
        oyez_has_ii = "ii" in n_with_suf.split()
        fjc_has_ii = suffix.lower().replace(".", "") == "ii"
        if oyez_has_ii and fjc_has_ii:
            score += 10
        elif oyez_has_ii and not fjc_has_ii and fjc_year and oyez_year and abs(fjc_year - oyez_year) > 10:
            score -= 20
        scored.append((score, o))
    scored.sort(key=lambda x: -x[0])
    top_score, top_o = scored[0]
    return top_o if top_score >= 10 else None


# ── Entry builder ────────────────────────────────────────────────────────────

def canonical_oyez_id(ident: str) -> str:
    return OYEZ_ALIASES.get(ident, ident)


def build_entry(fr: dict, oyez_rec: dict) -> dict:
    r = fr["row"]
    appts = sorted(fr["appts"], key=lambda a: iso_or_none(a["commission"]) or "")
    ts = iso_or_none(appts[0]["commission"])
    last_term = appts[-1]["termination"]
    te = iso_or_none(last_term) if last_term else None
    chief_appts = [a for a in appts if "Chief Justice" in a["title"]]
    is_chief = bool(chief_appts)
    appt0 = appts[0]

    entry: dict = {
        "canonical_name": oyez_rec.get("name"),
        "display_name": (r.get("Last Name") or "").strip(),
        "full_first": (r.get("First Name") or "").strip(),
    }
    mid = (r.get("Middle Name") or "").strip()
    if mid:
        entry["full_middle"] = mid
    entry["full_last"] = (r.get("Last Name") or "").strip()
    suf = (r.get("Suffix") or "").strip()
    if suf:
        entry["suffix"] = suf
    born = build_birth_death(r, "Birth")
    died = build_birth_death(r, "Death")
    if born:
        entry["born"] = born
    if died:
        entry["died"] = died
    entry["tenure_start"] = ts
    entry["tenure_end"] = te
    entry["chief_justice"] = is_chief
    if is_chief:
        entry["chief_tenure_start"] = iso_or_none(chief_appts[0]["commission"])
        entry["chief_tenure_end"] = iso_or_none(chief_appts[0]["termination"])
    if appt0["president"]:
        entry["appointing_president"] = appt0["president"]
    party = map_party(appt0["party"], president=appt0["president"])
    if party:
        entry["appointing_party"] = party
    gender = (r.get("Gender") or "").strip()
    if gender in ("M", "F"):
        entry["gender"] = gender
    race = (r.get("Race or Ethnicity") or "").strip()
    if race:
        entry["race_ethnicity"] = race
    entry["oyez_justice_id"] = canonical_oyez_id(oyez_rec["identifier"])
    if appt0.get("seat_id"):
        entry["fjc_seat_id"] = appt0["seat_id"]
    return entry


def reorder(d: dict) -> OrderedDict:
    out = OrderedDict()
    for k in FIELD_ORDER:
        if k in d:
            out[k] = d[k]
    for k in d:
        if k not in out:
            out[k] = d[k]
    return out


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fjc", default=FJC_URL)
    ap.add_argument("--oyez", default=OYEZ_URL)
    ap.add_argument("--out", default="seeds/justices.yaml")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print(f"[fetch] FJC:  {args.fjc}")
    fjc_text = fetch_text(args.fjc)
    print(f"[fetch] Oyez: {args.oyez}")
    oyez_all = fetch_json(args.oyez)

    fjc_rows = parse_fjc_scotus(fjc_text)
    print(f"[fjc]  SCOTUS rows: {len(fjc_rows)}")
    print(f"[oyez] justices:   {len(oyez_all)}")

    oyez_by_last: dict[str, list[dict]] = {}
    for o in oyez_all:
        last = (o.get("last_name") or "").strip().lower()
        if last:
            oyez_by_last.setdefault(last, []).append(o)

    entries = []
    unmatched = []
    for fr in fjc_rows:
        o = match_fjc_to_oyez(fr, oyez_by_last)
        if o is None:
            unmatched.append(fr)
            continue
        entries.append(build_entry(fr, o))

    if unmatched:
        print(f"[warn] unmatched FJC rows: {len(unmatched)}")
        for fr in unmatched:
            r = fr["row"]
            print(f"       {r['First Name']} {r['Last Name']}")

    # Dedup check
    ids = [e["oyez_justice_id"] for e in entries]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        raise SystemExit(f"[fatal] duplicate oyez_justice_id: {dupes}")

    # Sort by tenure_start
    entries.sort(key=lambda e: (e.get("tenure_start") or ""))
    entries = [reorder(e) for e in entries]

    class OrderedDumper(yaml.SafeDumper): pass

    def represent_ordered(dumper, data):
        return dumper.represent_mapping("tag:yaml.org,2002:map", list(data.items()))

    OrderedDumper.add_representer(OrderedDict, represent_ordered)
    OrderedDumper.add_representer(dict, represent_ordered)

    header = (
        "# Justices seed — 116 rows\n"
        "# Source: Federal Judicial Center, Oyez\n"
        "# tenure_end: null for currently sitting justices\n"
        "# Regenerate: python3 -m pipeline.seed_builders.build_justices\n"
        "\n"
    )
    body = yaml.dump(
        {"justices": entries},
        Dumper=OrderedDumper,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
    )
    out = header + body

    if args.dry_run:
        print(out[:2000])
        print("... (dry-run)")
        return 0

    Path(args.out).write_text(out)
    print(f"[out] wrote {len(entries)} rows to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
