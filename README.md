# FLexlaw SCOTUS Archive

A research-grade dataset covering all Supreme Court opinions from 1791 to present.

**Status:** Pre-release (Phase 0 — scaffolding)
**Target release:** v1.0.0 (estimated ~19 weeks from Phase 0 start)

---

## What this is

The FLexlaw SCOTUS Archive is designed for empirical legal scholars, political scientists, and legal historians — not as a product UI. The unit of success is adoption by researchers who need to cite a dataset.

Every row has provenance. Original source text is immutable. Derived data is versioned. Everything joins to SCDB.

See the [full plan](https://flexlaw.co) for the complete 13-phase pipeline.

---

## Repository structure

```
schema/          SQL migrations (run in order: 001, 002, ...)
seeds/           YAML seed data loaded into DB at Phase 0
  justices.yaml              115 justices, FJC + Oyez + Wikidata
  constitutional_provisions.yaml   ~80 provisions, hierarchical
  doctrinal_tests.yaml             ~60 doctrinal tests with lineage
gold/            Hand-coded ground truth (60 cases, v1.0.0 scope)
  cases/         One YAML file per gold-set case
pipeline/        Python pipeline scripts (one per phase)
tests/           pytest suite — seed validation + statistical sanity checks
.github/         CI configuration
```

---

## Data layers

```
L0  Raw extracts (CAP, CL, Oyez, SCDB)          — immutable
L1  Normalized core tables                      — cleaned, deduplicated
L2  Structured metadata (votes, justices, ...)  — derived from L1
L3  LLM enrichment (briefs, tags, summaries)    — derived from L1 + L2
L4  Embeddings                                  — derived from L1 text + L3
L5  Cross-source crosswalks (SCDB, Oyez, MQ)   — joins to external datasets
L6  Published releases                          — tagged, versioned, DOI'd
```

---

## License

- **LLM-generated enrichment and derived structured metadata**: [CC BY 4.0](LICENSE)
- **Harvard CAP opinion text**: Public domain
- **CourtListener opinion text**: CC0
- **SCDB data**: Washington University terms
- **Martin-Quinn / Bailey scores**: Publicly available from source institutions
- **Oyez transcripts**: Subject to separate terms (pending confirmation)

---

## Citation

```
FLexlaw SCOTUS Archive (v1.0.0). FLexlaw, 2026. Zenodo. DOI: 10.5281/zenodo.XXXXXXX
```

---

## Novel contributions

1. **Opinion-level citation graph** — "Scalia's dissent in Heller cites Stevens's concurrence in Marshall" is a structurally different data point from "Heller cites Marshall." No existing public dataset provides this.
2. **Vote table with SCDB cross-validation** — joining justice-level votes to full opinion text and ideology scores.
3. **Constitutional-provision and doctrinal-test taxonomies** — hierarchical, reviewer-signed, linked to every opinion.
4. **Full SCDB / Oyez / Martin-Quinn / Bailey crosswalks** in one queryable dataset.

---

## Setup

```bash
# Apply schema migrations
psql -d your_db -f schema/001_new_tables.sql
psql -d your_db -f schema/002_alter_existing_tables.sql

# Run tests (no DB required for seed validation)
pip install pytest pyyaml
pytest tests/ -m "not db"

# Run sanity checks (requires DB)
DB_URL="postgresql://user@localhost/legal_research" pytest tests/ -m db -v
```

---

## Phases

| Phase | Description | Status |
|-------|-------------|--------|
| 0 | Foundations: schema, gold set, seeds, CI | In progress |
| 1 | Opinion reclassification | Pending |
| 2 | Missing opinion ingestion | Pending |
| 3 | Brief enrichment (opinion-level) | Pending |
| 4 | Vote extraction | Pending |
| 5 | Justice table + ideology scores | Pending |
| 6 | SCDB crosswalk | Pending |
| 7 | Constitutional provision + doctrine tagging | Pending |
| 8 | Opinion-level citation graph | Pending |
| 9 | Plurality + Marks analysis | Pending |
| 10 | Embeddings | Pending |
| 11 | Text metrics | Pending |
| 12 | Oyez integration | Pending (licensing pending) |
| 13 | Cert proceedings | Pending |
