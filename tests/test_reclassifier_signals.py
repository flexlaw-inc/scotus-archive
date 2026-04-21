"""Unit tests for pipeline.reclassifier.signals.

All tests are pure-Python (no DB). They cover the four signal detectors
individually plus the fusion rules in ``classify``.
"""
from __future__ import annotations

import pytest

from pipeline.reclassifier import signals as sig


# ── author_signal ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("text,expected_label,expected_pattern", [
    ("Per Curiam",                                  sig.LABEL_PER_CURIAM,   "per_curiam_exact"),
    ("PER CURIAM.",                                 sig.LABEL_PER_CURIAM,   "per_curiam_exact"),
    ("Justice Scalia, concurring in part and dissenting in part",
                                                    sig.LABEL_MIXED,        "concur_and_dissent"),
    ("Justice Ginsburg, dissenting",                sig.LABEL_DISSENT,      "dissent_generic"),
    ("Justice Thomas, dissenting from the denial of certiorari",
                                                    sig.LABEL_DISSENT,      "dissent_from_denial"),
    ("Justice Kagan, concurring in the judgment",   sig.LABEL_CONCURRENCE,  "concur_in_judgment"),
    ("Justice Sotomayor, concurring",               sig.LABEL_CONCURRENCE,  "concur_generic"),
    ("Justice Alito announced the judgment of the Court and delivered a plurality opinion",
                                                    sig.LABEL_PLURALITY,    "plurality_label"),
    ("Justice Roberts took no part in the consideration or decision",
                                                    sig.LABEL_OTHER,        "took_no_part"),
    ("JUSTICE KAGAN",                               sig.LABEL_MAJORITY,     "justice_allcaps"),
    ("Mr. Justice Brennan",                         sig.LABEL_MAJORITY,     "justice_name_only"),
    ("Justice Kagan",                               sig.LABEL_MAJORITY,     "justice_name_only"),
])
def test_author_signal_matches(text, expected_label, expected_pattern):
    s = sig.author_signal(text)
    assert s is not None, f"no signal for {text!r}"
    assert s.label == expected_label, f"{text!r} → {s.label}, expected {expected_label}"
    assert s.pattern_name == expected_pattern
    assert s.source == "author_field"


@pytest.mark.parametrize("text", ["", None, "   "])
def test_author_signal_empty_input(text):
    assert sig.author_signal(text) is None


# ── opening_text_signal ─────────────────────────────────────────────────────

def test_opening_majority():
    text = "Justice Kagan delivered the opinion of the Court."
    s = sig.opening_text_signal(text)
    assert s is not None and s.label == sig.LABEL_MAJORITY
    assert s.pattern_name == "delivered_opinion"


def test_opening_named_dissent():
    text = "Justice Thomas, with whom Justice Alito joins, dissenting."
    s = sig.opening_text_signal(text)
    assert s is not None and s.label == sig.LABEL_DISSENT


def test_opening_concur_in_judgment():
    text = "Justice Gorsuch, concurring in the judgment."
    s = sig.opening_text_signal(text)
    assert s is not None and s.label == sig.LABEL_CONCURRENCE
    assert s.pattern_name == "concur_in_judgment"


def test_opening_plurality():
    text = "Justice Alito announced the judgment of the Court and delivered an opinion."
    s = sig.opening_text_signal(text)
    assert s is not None and s.label == sig.LABEL_PLURALITY


def test_opening_mixed_beats_plain_dissent():
    """'concurring in part and dissenting in part' must NOT match plain dissent."""
    text = "Justice Souter, concurring in part and dissenting in part."
    s = sig.opening_text_signal(text)
    assert s is not None and s.label == sig.LABEL_MIXED


def test_opening_window_respected():
    """Pattern outside the 800-char window does not fire."""
    prefix = " " * 1000
    text = prefix + "Justice Kagan delivered the opinion of the Court."
    assert sig.opening_text_signal(text) is None


# ── body_text_signal ────────────────────────────────────────────────────────

def test_body_fires_on_respectfully_dissent():
    text = ("The question presented is whether... [lots of analysis]. "
            "For these reasons, I respectfully dissent.")
    s = sig.body_text_signal(text)
    assert s is not None and s.label == sig.LABEL_DISSENT


def test_body_suppressed_if_majority_opener():
    """Don't flip a majority opinion to dissent on a stray 'I dissent'."""
    text = ("Justice Kagan delivered the opinion of the Court. "
            "The dissent argues... we say I dissent with that reasoning.")
    assert sig.body_text_signal(text) is None


# ── courtlistener_signal ────────────────────────────────────────────────────

@pytest.mark.parametrize("cl_input,expected_label", [
    ("040dissent",         sig.LABEL_DISSENT),
    ("030concurrence",     sig.LABEL_CONCURRENCE),
    ("020lead",            sig.LABEL_MAJORITY),
    ("025plurality",       sig.LABEL_PLURALITY),
    ("035concurrenceinpart", sig.LABEL_MIXED),
    ("015unanimous",       sig.LABEL_MAJORITY),
    ("015unamimous",       sig.LABEL_MAJORITY),   # CL's [sic] spelling
    ("Majority Opinion",   sig.LABEL_MAJORITY),
    ("Per Curiam",         sig.LABEL_PER_CURIAM),
])
def test_cl_signal_maps(cl_input, expected_label):
    s = sig.courtlistener_signal(cl_input)
    assert s is not None and s.label == expected_label


def test_cl_signal_unknown():
    assert sig.courtlistener_signal("999nonsense") is None
    assert sig.courtlistener_signal(None) is None


# ── classify fusion ─────────────────────────────────────────────────────────

def test_classify_high_confidence_agreement():
    v = sig.classify(
        author_field="Justice Kagan",
        plain_text="Justice Kagan delivered the opinion of the Court.",
    )
    assert v.label == sig.LABEL_MAJORITY
    assert v.confidence == "high"
    assert len(v.signals) == 2


def test_classify_medium_author_only():
    v = sig.classify(author_field="Justice Thomas, dissenting", plain_text=None)
    assert v.label == sig.LABEL_DISSENT
    assert v.confidence == "medium"


def test_classify_medium_opening_only():
    v = sig.classify(
        author_field=None,
        plain_text="Justice Kagan delivered the opinion of the Court. ...",
    )
    assert v.label == sig.LABEL_MAJORITY
    assert v.confidence == "medium"


def test_classify_low_on_disagreement():
    """Author says concur, opening says dissent — low confidence, prefer author."""
    v = sig.classify(
        author_field="Justice Alito, concurring",
        plain_text="Justice Alito, dissenting. [full opinion follows...]",
    )
    assert v.label == sig.LABEL_CONCURRENCE
    assert v.confidence == "low"
    assert any("opening=dissent" in d for d in v.disagreements)


def test_classify_low_on_body_only():
    v = sig.classify(
        author_field=None, plain_text=None,
        cl_opinion_type=None,
    )
    # Nothing fires → other/manual
    assert v.confidence == "manual_required"
    assert v.label == sig.LABEL_OTHER


def test_classify_cl_disagreement_downgrades():
    v = sig.classify(
        author_field="Justice Ginsburg, dissenting",
        plain_text="Justice Ginsburg, dissenting.",
        cl_opinion_type="030concurrence",
    )
    # author+opening both say dissent: would be high
    assert v.label == sig.LABEL_DISSENT
    # CL disagrees → downgrade to low
    assert v.confidence == "low"


def test_classify_landmark_forces_manual():
    v = sig.classify(
        author_field="Justice Kennedy",
        plain_text="Justice Kennedy delivered the opinion of the Court.",
        is_landmark=True,
    )
    assert v.label == sig.LABEL_MAJORITY
    assert v.confidence == "manual_required"


def test_classify_mixed_forces_manual():
    v = sig.classify(
        author_field="Justice Souter, concurring in part and dissenting in part",
        plain_text="Justice Souter, concurring in part and dissenting in part.",
    )
    assert v.label == sig.LABEL_MIXED
    assert v.confidence == "manual_required"


def test_classify_plurality_forces_manual():
    v = sig.classify(
        author_field="Justice Alito, plurality",
        plain_text="Justice Alito announced the judgment of the Court.",
    )
    assert v.label == sig.LABEL_PLURALITY
    assert v.confidence == "manual_required"
