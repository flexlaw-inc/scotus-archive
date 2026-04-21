"""Opinion reclassifier signal detectors (Phase 1 of v2.2 plan).

Pure-Python pattern classifiers: each function takes text input and returns
a ``Signal`` (or None). No database, no side effects — this module is
unit-testable in isolation.

Label vocabulary:

    majority      — delivered the opinion of the Court
    plurality     — announced the judgment of the Court (no majority)
    concurrence   — concurring (separately)
    dissent       — dissenting
    mixed         — concurring in part and dissenting in part
    per_curiam    — Per Curiam
    other         — substantive but unclassifiable (e.g. statement
                    respecting denial of cert, "took no part" non-opinion)

The four-signal hierarchy from v2.2 §5 Phase 1 (most reliable first):

    1. author_field       — patterns on ``opinions.author``
    2. opening_text       — patterns on LEFT(plain_text, 800)
    3. body_text          — fallback pattern on full body text
    4. courtlistener      — cross-validation against CL's opinion_type

The ``classify`` function fuses these signals into a single ``Verdict``
with a confidence tier per the plan:

    high              — author-field match AND opening-text match agree
    medium            — one matches, the other is absent (not contradictory)
    low               — body-text only, OR contradictory top-two signals,
                        OR CL disagrees
    manual_required   — mixed/plurality candidates; landmark cases
                        (cite_count > 500); or disagreements the runner
                        shouldn't resolve unattended.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional

# ─────────────────────────────────────────────────────────────────────────────
# Labels
# ─────────────────────────────────────────────────────────────────────────────

LABEL_MAJORITY     = "majority"
LABEL_PLURALITY    = "plurality"
LABEL_CONCURRENCE  = "concurrence"
LABEL_DISSENT      = "dissent"
LABEL_MIXED        = "mixed"
LABEL_PER_CURIAM   = "per_curiam"
LABEL_OTHER        = "other"

VALID_LABELS = {
    LABEL_MAJORITY, LABEL_PLURALITY, LABEL_CONCURRENCE, LABEL_DISSENT,
    LABEL_MIXED, LABEL_PER_CURIAM, LABEL_OTHER,
}

# "Main three" — the labels the plan's F1 ≥ 0.98 exit criterion covers.
MAIN_LABELS = {LABEL_MAJORITY, LABEL_DISSENT, LABEL_CONCURRENCE}


# ─────────────────────────────────────────────────────────────────────────────
# Data types
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Signal:
    """A single signal fired by one of the four classifiers."""
    label: str
    source: str            # 'author_field' | 'opening_text' | 'body_text' | 'courtlistener'
    pattern_name: str      # e.g. 'per_curiam_exact' — for reproducibility
    evidence: str          # the matched snippet, truncated to 200 chars


@dataclass
class Verdict:
    """Fused output: label + confidence tier + audit trail."""
    label: str
    confidence: str                  # 'high' | 'medium' | 'low' | 'manual_required'
    signals: List[Signal] = field(default_factory=list)
    disagreements: List[str] = field(default_factory=list)
    notes: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# 1. author_field classifier
# ─────────────────────────────────────────────────────────────────────────────
#
# Order matters: more-specific patterns first. "concurring in part and
# dissenting in part" must be checked before plain "dissenting" or
# "concurring", otherwise it would be misclassified as one of those.

_AUTHOR_PATTERNS: List[tuple] = [
    # (label, pattern_name, regex)
    (LABEL_PER_CURIAM,  "per_curiam_exact",         re.compile(r"^\s*per\s+curiam[\.\:]?\s*$", re.I)),
    (LABEL_MIXED,       "concur_and_dissent",       re.compile(r"concurring in part and dissenting in part", re.I)),
    (LABEL_MIXED,       "dissent_and_concur",       re.compile(r"dissenting in part and concurring in part", re.I)),
    (LABEL_DISSENT,     "dissent_from_denial",      re.compile(r"dissent(?:ing)?\s+from\s+the\s+denial", re.I)),
    (LABEL_DISSENT,     "dissent_generic",          re.compile(r"\bdissent(?:ing)?\b", re.I)),
    (LABEL_CONCURRENCE, "concur_in_judgment",       re.compile(r"concurring in (?:the )?judgment", re.I)),
    (LABEL_CONCURRENCE, "concur_generic",           re.compile(r"\bconcur(?:ring|rence)?\b", re.I)),
    (LABEL_PLURALITY,   "plurality_label",          re.compile(r"\bplurality\b", re.I)),
    (LABEL_OTHER,       "took_no_part",             re.compile(r"took\s+no\s+part", re.I)),
    # Justice-name-only authorship: "JUSTICE KAGAN" / "Justice Kagan" / "Mr. Justice Marshall"
    (LABEL_MAJORITY,    "justice_name_only",        re.compile(r"^\s*(?:Mr\.\s+)?(?:Chief\s+)?Justice\s+[A-Z][A-Za-z\-]+(?:,\s*J\.?)?\s*$")),
    (LABEL_MAJORITY,    "justice_allcaps",          re.compile(r"^\s*(?:MR\.\s+)?(?:CHIEF\s+)?JUSTICE\s+[A-Z][A-Z\-]+\s*$")),
]


def author_signal(author_field: Optional[str]) -> Optional[Signal]:
    """Classify from the opinions.author column."""
    if not author_field:
        return None
    text = author_field.strip()
    if not text:
        return None
    for label, name, pat in _AUTHOR_PATTERNS:
        m = pat.search(text)
        if m:
            return Signal(
                label=label, source="author_field", pattern_name=name,
                evidence=text[:200],
            )
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 2. opening_text classifier — first ~800 characters of plain_text
# ─────────────────────────────────────────────────────────────────────────────
#
# Per v2.2 §5 Phase 1. Keyed on the stylized opening line of each opinion.

_OPENING_WINDOW = 800

_OPENING_PATTERNS: List[tuple] = [
    (LABEL_PER_CURIAM,  "per_curiam_opening",
        re.compile(r"\bper\s+curiam\b", re.I)),
    (LABEL_MIXED,       "concur_and_dissent",
        re.compile(r"concurring in part and dissenting in part", re.I)),
    (LABEL_MIXED,       "dissent_and_concur",
        re.compile(r"dissenting in part and concurring in part", re.I)),
    (LABEL_DISSENT,     "named_dissenting",
        re.compile(
            r"(?:Mr\.\s+)?(?:Chief\s+)?Justice\s+[A-Z][A-Za-z\-]+"
            r"(?:\s*,\s*with\s+whom[^,]+(?:joins?|joined)[^,]*)?"
            r"\s*,?\s*dissenting", re.I)),
    (LABEL_CONCURRENCE, "concur_in_judgment",
        re.compile(r"concurring in (?:the )?judgment", re.I)),
    (LABEL_CONCURRENCE, "named_concurring",
        re.compile(
            r"(?:Mr\.\s+)?(?:Chief\s+)?Justice\s+[A-Z][A-Za-z\-]+"
            r"(?:\s*,\s*with\s+whom[^,]+(?:joins?|joined)[^,]*)?"
            r"\s*,?\s*concurring", re.I)),
    (LABEL_MAJORITY,    "delivered_opinion",
        re.compile(r"delivered the opinion of the Court", re.I)),
    (LABEL_PLURALITY,   "announced_judgment",
        re.compile(r"announced the judgment of the Court", re.I)),
]


def opening_text_signal(plain_text: Optional[str]) -> Optional[Signal]:
    """Classify from the first ~800 chars of plain_text."""
    if not plain_text:
        return None
    window = plain_text[:_OPENING_WINDOW]
    for label, name, pat in _OPENING_PATTERNS:
        m = pat.search(window)
        if m:
            return Signal(
                label=label, source="opening_text", pattern_name=name,
                evidence=m.group(0)[:200],
            )
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 3. body_text classifier (tertiary — low confidence)
# ─────────────────────────────────────────────────────────────────────────────

_BODY_DISSENT   = re.compile(r"\bI\s+(?:respectfully\s+)?dissent\b", re.I)
_BODY_MAJ_CUES  = re.compile(r"delivered the opinion of the Court", re.I)


def body_text_signal(plain_text: Optional[str]) -> Optional[Signal]:
    """Fallback: body text cues ('I respectfully dissent') when openers fail."""
    if not plain_text:
        return None
    # Only fires as a dissent cue if no majority opener is present.
    if _BODY_MAJ_CUES.search(plain_text):
        return None
    m = _BODY_DISSENT.search(plain_text)
    if m:
        return Signal(
            label=LABEL_DISSENT, source="body_text",
            pattern_name="respectfully_dissent",
            evidence=plain_text[max(0, m.start() - 30):m.end() + 30][:200],
        )
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 4. CourtListener cross-validation
# ─────────────────────────────────────────────────────────────────────────────
#
# CL's opinion_type uses a different vocabulary. Normalize to ours.

_CL_TYPE_MAP = {
    "010combined":      LABEL_MAJORITY,      # lead opinion
    "020lead":          LABEL_MAJORITY,
    "025plurality":     LABEL_PLURALITY,
    "030concurrence":   LABEL_CONCURRENCE,
    "035concurrenceinpart": LABEL_MIXED,
    "040dissent":       LABEL_DISSENT,
    "050addendum":      LABEL_OTHER,
    "060remittitur":    LABEL_OTHER,
    "070rehearing":     LABEL_OTHER,
    "080onthemerits":   LABEL_MAJORITY,
    "090onmotiontostrike": LABEL_OTHER,
    "100trialcourt":    LABEL_OTHER,
    "015unamimous":     LABEL_MAJORITY,  # [sic] — CL's spelling
    "015unanimous":     LABEL_MAJORITY,
    # CourtListener sometimes ships human-readable strings too:
    "Majority Opinion": LABEL_MAJORITY,
    "Concurrence":      LABEL_CONCURRENCE,
    "Dissent":          LABEL_DISSENT,
    "Per Curiam":       LABEL_PER_CURIAM,
    "Plurality":        LABEL_PLURALITY,
}


def courtlistener_signal(cl_opinion_type: Optional[str]) -> Optional[Signal]:
    """Map a CourtListener opinion_type code/string into our vocabulary."""
    if not cl_opinion_type:
        return None
    key = cl_opinion_type.strip()
    label = _CL_TYPE_MAP.get(key) or _CL_TYPE_MAP.get(key.lower())
    if label is None:
        return None
    return Signal(
        label=label, source="courtlistener",
        pattern_name="cl_type_map", evidence=key[:200],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Fusion — turn a set of signals into a single Verdict + confidence tier
# ─────────────────────────────────────────────────────────────────────────────

# Landmark flag: per §5 Phase 1, cite_count > 500 forces manual review.
LANDMARK_CITE_THRESHOLD = 500


def classify(
    *,
    author_field: Optional[str] = None,
    plain_text: Optional[str] = None,
    cl_opinion_type: Optional[str] = None,
    is_landmark: bool = False,
) -> Verdict:
    """Fuse the four signal classifiers into a single Verdict.

    Parameters are keyword-only to force explicit call sites at the runner.

    ``is_landmark`` forces ``manual_required`` per the plan's rule that any
    case with citation count >500 routes through human review regardless of
    signal agreement.
    """
    s_author  = author_signal(author_field)
    s_opening = opening_text_signal(plain_text)
    s_body    = body_text_signal(plain_text)
    s_cl      = courtlistener_signal(cl_opinion_type)

    signals = [s for s in (s_author, s_opening, s_body, s_cl) if s is not None]

    # No signal fired at all — caller must decide.
    if not signals:
        return Verdict(
            label=LABEL_OTHER, confidence="manual_required",
            signals=[], notes="no signal fired",
        )

    # --- label selection ---------------------------------------------------
    # Prefer author+opening agreement. Otherwise take the most-reliable
    # non-None signal in priority order: author > opening > body > CL.
    primary = s_author or s_opening or s_body or s_cl
    assert primary is not None  # for mypy / readability
    label = primary.label

    # --- confidence tier ---------------------------------------------------
    disagreements: List[str] = []
    conf: str

    # Landmark short-circuit.
    if is_landmark:
        conf = "manual_required"
        notes = "landmark (cite_count > threshold)"
    elif label in {LABEL_MIXED, LABEL_PLURALITY}:
        # Always manual for these two — plan calls out that they're hard.
        conf = "manual_required"
        notes = f"{label} requires human review"
    elif s_author is not None and s_opening is not None:
        if s_author.label == s_opening.label:
            conf = "high"
            notes = "author+opening agree"
        else:
            # Contradictory top-two signals — prefer author but drop confidence.
            conf = "low"
            disagreements.append(f"author={s_author.label} opening={s_opening.label}")
            notes = "author and opening disagree; preferring author"
    elif s_author is not None and s_opening is None:
        conf = "medium"
        notes = "author-only (opening absent/unclear)"
    elif s_author is None and s_opening is not None:
        conf = "medium"
        notes = "opening-only (author absent)"
    elif s_body is not None:
        conf = "low"
        notes = "body-text cue only"
    elif s_cl is not None:
        conf = "low"
        notes = "CourtListener-only"
    else:  # defensive
        conf = "manual_required"
        notes = "no confident signal"

    # CourtListener disagreement downgrade (if we haven't already bottomed out).
    if s_cl is not None and s_cl.label != label and conf in {"high", "medium"}:
        conf = "low"
        disagreements.append(f"CL={s_cl.label}")
        notes = (notes or "") + "; CL disagrees"

    return Verdict(
        label=label, confidence=conf,
        signals=signals, disagreements=disagreements, notes=notes,
    )
