# Changelog

All notable changes to the FLexlaw SCOTUS Archive are documented here.

Format: [Semantic Versioning](https://semver.org/). Breaking schema changes bump major.
Additive changes (new columns, new tables) bump minor. Data corrections bump patch.

---

## [Unreleased]

### Added
- `schema/003_case_propositions.sql` — Phase 9b proposition-coalition table. One row per discrete proposition decided, with supporting/opposing justice coalitions, generated coalition-size columns, `precedential_status` enum, Phase 7 reviewer signoff fields, and CHECK constraints on all enum columns. Idempotent. GIN indexes on coalition arrays for "which cases did Justice X support proposition Y in?" queries.
- Two `test_sanity` cases: presence check (required columns and CHECK constraints) and constraint-enforcement check (attempts four deliberate violations under savepoints).
- `seeds/justices.yaml` — backfilled remaining 63 justices from the FJC
  Biographical Directory (judges.csv) and the Oyez Project justices API.
  File now contains the full 116-row historical SCOTUS roster (17 Chiefs
  + 99 Associates; John Rutledge counted once as Associate with a second
  recess appointment as Chief). Backfill rows are matched FJC ↔ Oyez by
  last name with year-of-commission as the primary disambiguator (needed
  for the two John Marshall Harlans, since FJC does not populate Suffix
  for Harlan II). Aliases are used for three legacy seeded identifiers
  whose Oyez listing slug differs (`harlan_f_stone`, `stanley_f_reed`,
  `harold_h_burton`) — both slugs resolve on /justices/{id}.
- `pipeline/seed_builders/build_justices.py` — reproducible seed builder.
  Regenerates the full roster from the FJC CSV + Oyez listing. Supports
  `--dry-run` and writes to `seeds/justices.yaml` by default. Invoke:
  `python3 -m pipeline.seed_builders.build_justices`.
- `tests/test_sanity.py::test_justices_seed_loads` tightened from
  `len >= 40` to `len == 116` plus 17-Chief-Justice count and
  unique-oyez-id assertions.
- `pipeline/seed_loader.py` — idempotent UPSERT loader for the three seed
  YAMLs. Populates `justices`, `constitutional_provisions`, and
  `doctrinal_tests` against the connection in `DB_URL`. Two-pass load for
  the hierarchical tables: pass 1 inserts every row with parent FKs
  NULL; pass 2 resolves the YAML's string `canonical_id` references into
  integer FKs. Supports `--table <name>` and `--dry-run` (rolls back at
  the end). Re-runs are clean: all 116/42/34 rows become UPDATEs, no
  duplicates. Invoke: `python3 -m pipeline.seed_loader`.
- `seeds/justices.yaml` date-precision fixes: the FJC CSV stores
  year-only birth dates for three pre-1840 justices. Updated to
  day-precision values from Wikidata where available (John Blair Jr. =
  1732-04-17 per Q778866; John Catron = 1786-01-07 per Q1699569). James
  M. Wayne remains null — both FJC and Wikidata Q1250079 have year 1790
  only; day/month unknown. YAML is now ISO-8601 or null, nothing
  intermediate, so `DATE` column loads succeed.
- `schema/001_new_tables.sql` — replaced invalid
  `CREATE TYPE IF NOT EXISTS vote_value AS ENUM (...)` with a
  `DO $$ ... $$` block checking `pg_type`. PostgreSQL does not accept
  `IF NOT EXISTS` on `CREATE TYPE`. Migration is now re-runnable.
- `pipeline/reclassifier/` — Phase 1 opinion reclassifier.
    - `signals.py` (pure-Python, DB-free): the four pattern classifiers
      from v2.2 §5 Phase 1 — ``author_signal``, ``opening_text_signal``
      (first 800 chars), ``body_text_signal``, ``courtlistener_signal``
      — plus a ``classify`` fuser that enforces the plan's confidence
      hierarchy: ``high`` (author+opening agree), ``medium`` (one fires,
      the other absent), ``low`` (body-only, or contradictory top two,
      or CL disagrees), ``manual_required`` (mixed/plurality/landmark).
      Landmark threshold is cite_count > 500 per the plan.
    - `runner.py`: orchestrator over the ``opinions`` table. Never
      mutates ``opinion_type`` without snapshotting the pre-existing
      value into ``opinion_type_original`` and writing a
      ``reclassification_log`` row. Only the ``high`` tier applies
      automatically; ``--apply-medium`` enables the medium tier (gated
      on the plan's gold-set precision requirement); low and
      manual_required are logged but never written. Supports
      ``--dry-run``, ``--opinion-id``, ``--limit``, ``--rerun``.
      Requires migration 002 on the target DB.
- `tests/test_reclassifier_signals.py` — 42 unit tests covering each
  signal detector individually plus the fusion rules. Verifies that
  the dangerous overlaps are resolved correctly (concurring-in-part-
  and-dissenting-in-part must not match plain dissent; body-text
  dissent cue must be suppressed when the opinion also opens with
  "delivered the opinion of the Court").
- `pytest.ini` — adds repo root to ``pythonpath`` so tests can import
  the ``pipeline`` package.
- Initial schema migrations (001, 002).
- Seed files: justices (115-row target), constitutional_provisions, doctrinal_tests.
- Gold-set directory structure (60-case v1.0.0 scope).
- Statistical sanity-check harness (pytest stubs).
- CI workflow.

### Audit findings (from builder-vs-seed diff; deferred)

- David Souter (`david_h_souter`): FJC reports death date 2025-05-08;
  current seed has `died: null` and `tenure_end: 2009-06-29`. Seed is
  correct on tenure_end (he retired) but missing his death. Deferred.
- Charles Evans Hughes (`charles_e_hughes`): current seed captures only
  his 1930-1941 Chief Justice stint; FJC also records his 1910-1916
  Associate Justice appointment under Taft. A dual-tenure representation
  is not yet in the schema; deferred to a later revision.
- Commission-date vs swearing-in-date: existing seeds use the date each
  justice took the judicial oath; the builder uses FJC's Commission Date
  (date the commission was signed). Both are defensible anchors. Seed
  values retained; builder output is a conservative alternative.
