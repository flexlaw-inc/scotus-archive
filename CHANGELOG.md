# Changelog

All notable changes to the FLexlaw SCOTUS Archive are documented here.

Format: [Semantic Versioning](https://semver.org/). Breaking schema changes bump major.
Additive changes (new columns, new tables) bump minor. Data corrections bump patch.

---

## [Unreleased]

### Added
- `schema/003_case_propositions.sql` — Phase 9b proposition-coalition table. One row per discrete proposition decided, with supporting/opposing justice coalitions, generated coalition-size columns, `precedential_status` enum, Phase 7 reviewer signoff fields, and CHECK constraints on all enum columns. Idempotent. GIN indexes on coalition arrays for "which cases did Justice X support proposition Y in?" queries.
- Two `test_sanity` cases: presence check (required columns and CHECK constraints) and constraint-enforcement check (attempts four deliberate violations under savepoints).
- Initial schema migrations (001, 002).
- Seed files: justices (115-row target), constitutional_provisions, doctrinal_tests.
- Gold-set directory structure (60-case v1.0.0 scope).
- Statistical sanity-check harness (pytest stubs).
- CI workflow.
