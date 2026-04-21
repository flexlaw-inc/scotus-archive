# SCOTUS Reclassifier — Triage Memo
**Date:** 2026-04-21
**Subject:** The 337,739 `manual_required` `majority → other` proposals aren't a single population. Schema decision needs to account for two distinct structural bugs.

## Sampling method

Stratified random sample of 1,000 rows from `reclassification_log` where `old_type='majority'`, `new_type='other'`, `confidence='manual_required'`, joined to `cases` filtered to `court_id=1`. No rule_id fired on any of these rows (the `rule_id` column is NULL for the whole cohort), so the reclassifier had no evidence either way and flagged them for human review.

## Length distribution of the full 337,739-row cohort

| Length of `opinions.plain_text` | Rows | Share |
|---|---:|---:|
| 0 (NULL or empty) | 45,794 | 13.6% |
| 1–99 chars | 241,536 | 71.5% |
| 100–399 chars | 45,552 | 13.5% |
| 400–999 chars | 2,358 | 0.7% |
| 1,000–4,999 chars | 1,031 | 0.3% |
| 5,000–19,999 chars | 889 | 0.3% |
| ≥20,000 chars | 579 | 0.2% |

**98.6% of the cohort is under 400 characters.** Those are orders (judgment lines, cert denials, motion rulings). The remaining 1.4% is substantive — and that's where the second bug lives.

## Opener-pattern classification (1,000-row sample)

Applied after trimming to the first 1,500 chars of `plain_text`, first-match-wins. Bucket counts are rough but directionally sound.

| Bucket | n | % |
|---|---:|---:|
| short_unclassified (<400 chars, no pattern match) | 706 | 70.6% |
| empty_or_tiny | 134 | 13.4% |
| cert_denied | 66 | 6.6% |
| rehearing_denied | 17 | 1.7% |
| disbarment | 15 | 1.5% |
| recusal (took no part) | 13 | 1.3% |
| leave_to_file_action | 12 | 1.2% |
| mandamus_action | 8 | 0.8% |
| cert_granted | 8 | 0.8% |
| motion_action | 7 | 0.7% |
| unclassified (≥400 chars, no pattern) | 6 | 0.6% |
| stay_action, habeas_action, cert_dismissed | 8 | 0.8% |

Examples from `short_unclassified` (all real rows):
- `Carrizales v. United States` (2004-11-01): *"C. A. 9th Cir. Certiorari denied."*
- `Rickert v. United States` (2013-03-18): *"C. A. 8th Cir. Certiorari denied."*
- `Marsh v. United States` (2012-11-26): *"C. A. 4th Cir. Cer-tiorari denied."*

These miss the `cert_denied` regex purely because the opener is abbreviated/hyphenated. The true cert-denied share is far higher than 6.6% once you account for `short_unclassified` — a more lenient classifier would push it above 80%.

## The surprise: the long-text tail

Spot check of the 579 rows with ≥20,000 chars of `plain_text`:

| opinion_id | case | length |
|---|---|---:|
| 1539430 | Students for Fair Admissions v. Harvard (2023) | 505,996 |
| 1539367 | Dobbs v. Jackson Women's Health (2022) | 451,977 |
| 1539371 | NYSRPA v. Bruen (2022) | 291,033 |
| 1539445 | Haaland v. Brackeen (2023) | 273,355 |
| 1539562 | United States v. Skrmetti (2025) | 247,372 |
| 1539486 | Trump v. United States (2024) | 246,433 |
| 1539544 | Trump v. CASA (2025) | 245,191 |
| 1539489 | Loper Bright Enterprises v. Raimondo (2024) | 243,355 |
| 1539312 | Fulton v. Philadelphia (2021) | 233,350 |
| 438218 | Ogden v. Saunders (1827) | 231,579 |

These are **not orders**. Every one of them is a landmark SCOTUS decision. The opener on each of the modern ones is literally *"Syllabus"* followed by the syllabus text, then the majority opinion, then each concurrence, then each dissent — all concatenated into a single row under `opinion_type='majority'` with `author=NULL`. CAP's import apparently collapsed the entire `U.S. Reports` entry for the case into one `opinions` row.

The reclassifier correctly declined to mutate these — no author_field signal, opening is "Syllabus" not a per-curiam or attribution line, body is too large for the body classifier to produce a single verdict. So they got parked in `manual_required`.

## Implications: two distinct bugs

**Bug 1 — Orders mis-tagged as opinions (~332,882 rows, 98.6% of cohort).**
These are genuine orders: cert denials, motion rulings, disbarments, stay grants, rehearing denials, summary dispositions. CAP's ingest treated every filing as an "opinion" because that's how CAP's schema models it. FlexLaw needs to distinguish decisional opinions from orders of the Court.

**Bug 2 — Concatenated-opinion rows (~2,500–4,857 rows, 0.7%–1.4% of cohort).**
These are real opinions that got bundled together by CAP's PDF-to-text extraction. One `opinions` row per `cases` row, but the row's `plain_text` contains the entire *U.S. Reports* entry — syllabus, majority, every concurrence, every dissent. These need to be *split* into constituent opinions (one per justice/authoring block), not relabeled. This is a text-segmentation job, not a reclassifier job.

## Schema proposal

**Bug 1 — order-vs-opinion.** Two viable approaches:

Approach A — Add `'order'` to the `opinion_type` enum. Lowest schema churn. Keeps everything in `opinions`. Downside: conflates two different kinds of artifacts. Every opinion-level analytic must filter `opinion_type != 'order'`.

Approach B — Sibling `court_orders` table. Cleaner semantics. Matches SCDB's handling (SCDB excludes orders entirely). Downside: FK targets that currently point at `opinions.id` for orders would need relocation, and the citator needs an update.

**Recommendation: Approach B.** ~99% of the cohort has no author, no opinion structure, and is never cited as precedent — they're procedural artifacts, not opinions. SCDB already draws this line. Keeping them in `opinions` permanently contaminates any "opinions per term" / "opinions per justice" statistic. Row counts make it manageable — the migration moves ~333k rows once.

### Carve-up

- `court_orders` = **unsigned** procedural dispositions only. `author` / `author_original` columns included (nullable) for the rare signed-order edge case.
- Authored separate writings (dissents from denial, concurrences from denial, statements respecting denial, in-chambers opinions) stay in `opinions`. They are reasoned, citable, opinion-like. New `opinion_type` enum values added.
- New `opinions.related_order_id` FK lets an authored separate writing point at the order it attaches to.

### Revised DDL

```sql
CREATE TABLE court_orders (
    id                  BIGSERIAL PRIMARY KEY,
    case_id             BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
    order_type          TEXT NOT NULL,   -- cert_denied|cert_granted|cert_dismissed|rehearing_denied|
                                         -- stay|injunction|mandamus|habeas|motion|disbarment|
                                         -- reinstatement|leave_to_file|summary_affirmance|
                                         -- appeal_dismissed|other
    order_text          TEXT,            -- the short disposition text
    decision_date       DATE,
    author              TEXT,            -- usually NULL; populated for signed orders
    author_original     TEXT,            -- audit snapshot (mirrors opinions.author_original pattern)
    source              TEXT,            -- 'cap' | 'courtlistener' | 'scotus_orders_list' | ...
    original_opinion_id BIGINT,          -- pre-migration opinions.id, for audit joinability
    classifier_rule_id  TEXT,            -- which regex bucket classified this
    cap_case_id         BIGINT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE opinions
    ADD COLUMN related_order_id BIGINT REFERENCES court_orders(id) ON DELETE SET NULL;

ALTER TYPE opinion_type_enum ADD VALUE IF NOT EXISTS 'in_chambers';
ALTER TYPE opinion_type_enum ADD VALUE IF NOT EXISTS 'dissent_from_denial';
ALTER TYPE opinion_type_enum ADD VALUE IF NOT EXISTS 'concurrence_from_denial';
ALTER TYPE opinion_type_enum ADD VALUE IF NOT EXISTS 'statement_respecting_denial';
```

### Relocation

Insert-then-delete from `opinions` for the ~333k manual_required unsigned-order rows plus the 1,238 `dissent→other` and 460 `concurrence→other` rows in the log whose text is <400 chars. Authored rows (dissents from denial etc.) stay in `opinions` — they just get a new `opinion_type` value and optional `related_order_id` link. Audit-preserving: `original_opinion_id` on `court_orders` lets any future forensic join reconstruct the pre-migration `opinions.id`.

**Bug 2 — concatenated opini