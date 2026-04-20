# Changelog

All notable changes to the FLexlaw SCOTUS Archive are documented here.

Format: [Semantic Versioning](https://semver.org/). Breaking schema changes bump major.
Additive changes (new columns, new tables) bump minor. Data corrections bump patch.

---

## [Unreleased]

### Added
- Initial schema migrations (001, 002)
- Seed files: justices (115 rows), constitutional_provisions (~80 rows), doctrinal_tests (~60 rows)
- Gold-set directory structure (60-case v1.0.0 scope)
- Statistical sanity-check harness (pytest stubs)
- CI workflow

---

<!-- Template for future releases:

## [v1.0.0] - YYYY-MM-DD

### Summary
First public release.

### Dataset statistics
- Cases: N
- Opinions: N
- Votes: N
- Citation edges (opinion-level): N
- Gold-set F1 (majority/dissent/concurrence): 0.XX

### Pipelines run
| Phase | Script | Model | Version | Cost |
|-------|--------|-------|---------|------|
| 1 | reclassifier.py | regex | v1.0 | $0 |
| ...  | ...    | ...   | ...     | ... |

### Known issues
- [describe any known data gaps or caveats]

### Validation
- Gold-set precision/recall report: gold/validation_v1.0.0.md
- Statistical sanity checks: all passing (see CI run SHA)
- Cross-source agreement: SCDB 98.X%, Oyez 99.X%

-->
