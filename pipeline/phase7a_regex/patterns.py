"""
Phase 7a pattern derivation v2 — returns both per-pattern list (for debugging)
and a single alternation regex per taxonomy with named groups for fast scanning.

Named group format: g<N>__<slug>  — slug is a sanitized canonical_id so we can
map back to the taxonomy row.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import yaml

REPO = Path("/home/john/scotus-archive")
SEEDS = REPO / "seeds"

AMEND_NUM_MAP = {
    1: ("I", "First"), 2: ("II", "Second"), 3: ("III", "Third"), 4: ("IV", "Fourth"),
    5: ("V", "Fifth"), 6: ("VI", "Sixth"), 7: ("VII", "Seventh"), 8: ("VIII", "Eighth"),
    9: ("IX", "Ninth"), 10: ("X", "Tenth"), 11: ("XI", "Eleventh"), 12: ("XII", "Twelfth"),
    13: ("XIII", "Thirteenth"), 14: ("XIV", "Fourteenth"), 15: ("XV", "Fifteenth"),
    16: ("XVI", "Sixteenth"), 17: ("XVII", "Seventeenth"), 18: ("XVIII", "Eighteenth"),
    19: ("XIX", "Nineteenth"), 20: ("XX", "Twentieth"), 21: ("XXI", "Twenty-First"),
    22: ("XXII", "Twenty-Second"), 23: ("XXIII", "Twenty-Third"), 24: ("XXIV", "Twenty-Fourth"),
    25: ("XXV", "Twenty-Fifth"), 26: ("XXVI", "Twenty-Sixth"), 27: ("XXVII", "Twenty-Seventh"),
}
ART_NUM_MAP = {1: "I", 2: "II", 3: "III", 4: "IV", 5: "V", 6: "VI", 7: "VII"}


@dataclass
class Pattern:
    canonical_id: str
    pattern_label: str
    pattern_str: str  # raw regex source (we compile into alternation, not individually)


def _escape_short(name: str) -> str:
    return r"\b" + re.escape(name) + r"\b"


def _provisions_patterns(p: dict) -> list[Pattern]:
    cid = p["canonical_id"]
    short = p.get("short_name") or p.get("canonical_name") or cid
    out: list[Pattern] = [Pattern(cid, f"short:{short}", _escape_short(short))]

    if re.fullmatch(r"amend\.\d+", cid):
        n = int(cid.split(".")[1])
        if n in AMEND_NUM_MAP:
            roman, ordinal = AMEND_NUM_MAP[n]
            out.append(Pattern(cid, f"amend_roman:{roman}", rf"\bAmendment\s+{roman}\b"))
            out.append(Pattern(cid, f"amend_ordinal:{ordinal}", rf"\b{ordinal}\s+Amendment\b"))
            out.append(Pattern(cid, "amend_cite", rf"\bU\.?S\.?\s*Const\.?\s*amend\.?\s*{roman}\b"))
    if re.fullmatch(r"art\.\d+", cid):
        n = int(cid.split(".")[1])
        if n in ART_NUM_MAP:
            roman = ART_NUM_MAP[n]
            out.append(Pattern(cid, f"art_roman:{roman}", rf"\bArticle\s+{roman}\b"))
            out.append(Pattern(cid, "art_cite", rf"\bU\.?S\.?\s*Const\.?\s*art\.?\s*{roman}\b"))
    return out


def _doctrine_patterns(t: dict) -> list[Pattern]:
    cid = t["canonical_id"]
    name = t.get("name") or cid
    out: list[Pattern] = [Pattern(cid, f"name:{name}", _escape_short(name))]
    core = re.sub(r"\s*(?:Test|Rule|Doctrine|Standard|Deference|Scrutiny|Balance|Balancing)\s*$", "", name, flags=re.I).strip()
    if core and core != name and len(core) >= 4:
        out.append(Pattern(cid, f"core:{core}", _escape_short(core)))
        out.append(Pattern(cid, f"core_test:{core}", rf"\b{re.escape(core)}\s+(?:test|rule|doctrine|standard|deference)\b"))
    return out


def _slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s)


def build_mega_regex(patterns: list[Pattern]) -> tuple[re.Pattern, list[tuple[str, str]]]:
    """
    Returns (compiled_regex, group_index_to_(canonical_id, label)).
    The group_index list is ordered; group N in a match corresponds to entry[N].
    """
    parts: list[str] = []
    meta: list[tuple[str, str]] = []
    for i, p in enumerate(patterns):
        gname = f"g{i}_{_slug(p.canonical_id)[:40]}"
        parts.append(f"(?P<{gname}>{p.pattern_str})")
        meta.append((p.canonical_id, p.pattern_label))
    mega = re.compile("|".join(parts), re.IGNORECASE)
    return mega, meta


def load_patterns() -> tuple[list[Pattern], list[Pattern]]:
    provs = yaml.safe_load((SEEDS / "constitutional_provisions.yaml").read_text())["provisions"]
    tests = yaml.safe_load((SEEDS / "doctrinal_tests.yaml").read_text())["tests"]
    prov_pats: list[Pattern] = []
    for p in provs:
        prov_pats.extend(_provisions_patterns(p))
    doc_pats: list[Pattern] = []
    for t in tests:
        doc_pats.extend(_doctrine_patterns(t))
    return prov_pats, doc_pats


def load_compiled() -> tuple[tuple[re.Pattern, list], tuple[re.Pattern, list]]:
    prov_pats, doc_pats = load_patterns()
    prov_mega = build_mega_regex(prov_pats)
    doc_mega = build_mega_regex(doc_pats)
    return prov_mega, doc_mega


if __name__ == "__main__":
    pp, dp = load_patterns()
    print(f"Provision patterns: {len(pp)}")
    print(f"Doctrine patterns:  {len(dp)}")
    prov_mega, doc_mega = load_compiled()
    print(f"Mega regex prov groups: {prov_mega[0].groups}")
    print(f"Mega regex doc groups:  {doc_mega[0].groups}")
