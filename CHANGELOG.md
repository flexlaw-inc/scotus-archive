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
