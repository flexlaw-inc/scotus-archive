-- FLexlaw SCOTUS Archive — Migration 001
-- New tables: justices, justice_term_scores, votes, constitutional_provisions,
--             opinion_provisions, doctrinal_tests, opinion_doctrines,
--             opinion_citations, oral_arguments, oral_argument_speakers,
--             cert_proceedings, dataset_releases, opinion_text_versions
--
-- Apply against: legal_research (production) or scotus_archive (standalone)
-- Idempotent: all CREATE TABLE statements use IF NOT EXISTS

BEGIN;

-- ── Justices ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS justices (
    id                              BIGSERIAL PRIMARY KEY,
    canonical_name                  TEXT NOT NULL,         -- 'John Marshall Harlan II'
    display_name                    TEXT NOT NULL,         -- 'Harlan'
    full_first                      TEXT,
    full_middle                     TEXT,
    full_last                       TEXT,
    suffix                          TEXT,                  -- 'Jr.', 'II'
    born                            DATE,
    died                            DATE,
    nominated                       DATE,
    confirmed                       DATE,
    tenure_start                    DATE,
    tenure_end                      DATE,                  -- NULL for sitting justices
    chief_justice                   BOOLEAN NOT NULL DEFAULT FALSE,
    chief_tenure_start              DATE,
    chief_tenure_end                DATE,
    appointing_president            TEXT,
    appointing_party                TEXT,
    prior_office                    TEXT,
    law_school                      TEXT,
    state_of_residence_at_appointment TEXT,
    gender                          TEXT,
    race_ethnicity                  TEXT,
    religion                        TEXT,
    succession_seat                 INTEGER,               -- 1-9
    oyez_justice_id                 TEXT,
    wikidata_qid                    TEXT,
    UNIQUE (canonical_name)
);

CREATE INDEX IF NOT EXISTS idx_justices_tenure
    ON justices (tenure_start, tenure_end);

-- ── Justice ideology scores by term ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS justice_term_scores (
    justice_id          BIGINT  NOT NULL REFERENCES justices(id),
    term                INTEGER NOT NULL,   -- OT year, e.g. 2022
    martin_quinn        REAL,
    martin_quinn_post_sd REAL,
    bailey              REAL,
    bailey_ci_low       REAL,
    bailey_ci_high      REAL,
    segal_cover         REAL,               -- time-invariant at appointment
    PRIMARY KEY (justice_id, term)
);

-- ── Votes ─────────────────────────────────────────────────────────────────────

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'vote_value') THEN
        CREATE TYPE vote_value AS ENUM (
            'majority', 'concurrence', 'dissent', 'mixed',
            'no_participation', 'equally_divided'
        );
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS votes (
    id                  BIGSERIAL PRIMARY KEY,
    case_id             BIGINT NOT NULL REFERENCES cases(id),
    justice_id          BIGINT NOT NULL REFERENCES justices(id),
    vote                vote_value NOT NULL,
    vote_subtype        TEXT,               -- 'concur_in_judgment_only', etc.
    authored_opinion_id BIGINT REFERENCES opinions(id),
    joined_opinion_ids  BIGINT[],
    joined_in_part      JSONB,              -- {opinion_id: ['II', 'III-A']}
    direction           TEXT,               -- SCDB: 'liberal'|'conservative'|null
    source              TEXT NOT NULL,      -- 'scdb'|'text_parse_v1'|'human'
    confidence          TEXT NOT NULL,      -- 'high'|'medium'|'low'
    UNIQUE (case_id, justice_id)
);

CREATE INDEX IF NOT EXISTS idx_votes_case ON votes(case_id);
CREATE INDEX IF NOT EXISTS idx_votes_justice ON votes(justice_id);

-- ── Constitutional provisions taxonomy ────────────────────────────────────────

CREATE TABLE IF NOT EXISTS constitutional_provisions (
    id              SERIAL PRIMARY KEY,
    canonical_id    TEXT UNIQUE NOT NULL,   -- 'amend.14.s1.equal_protection'
    canonical_name  TEXT NOT NULL,          -- 'Fourteenth Amendment, Section 1, Equal Protection Clause'
    short_name      TEXT,                   -- 'Equal Protection (14th Am.)'
    provision_type  TEXT NOT NULL,          -- 'amendment'|'article'|'section'|'clause'
    parent_id       INTEGER REFERENCES constitutional_provisions(id),
    effective_from  DATE,
    effective_to    DATE,
    text_citation   TEXT,                   -- 'U.S. Const. amend. XIV § 1'
    description     TEXT,
    sort_order      INTEGER
);

CREATE TABLE IF NOT EXISTS opinion_provisions (
    opinion_id          BIGINT NOT NULL REFERENCES opinions(id),
    provision_id        INTEGER NOT NULL REFERENCES constitutional_provisions(id),
    role                TEXT NOT NULL,      -- 'primary'|'secondary'|'background'
    citation_form_seen  TEXT,
    source              TEXT NOT NULL,      -- 'regex_v1'|'llm_v1'|'human'
    confidence          TEXT NOT NULL,
    PRIMARY KEY (opinion_id, provision_id, role)
);

CREATE INDEX IF NOT EXISTS idx_opinion_provisions_opinion ON opinion_provisions(opinion_id);
CREATE INDEX IF NOT EXISTS idx_opinion_provisions_provision ON opinion_provisions(provision_id);

-- ── Doctrinal tests taxonomy ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS doctrinal_tests (
    id                          SERIAL PRIMARY KEY,
    canonical_id                TEXT UNIQUE NOT NULL,   -- 'lemon_test'
    name                        TEXT NOT NULL,          -- 'Lemon Test'
    short_name                  TEXT,
    established_by_case_id      BIGINT REFERENCES cases(id),
    established_in_year         INTEGER,
    standard_of_review          TEXT,
    constitutional_provision_id INTEGER REFERENCES constitutional_provisions(id),
    subject_area                TEXT,
    description                 TEXT,
    parent_test_id              INTEGER REFERENCES doctrinal_tests(id),
    active                      BOOLEAN NOT NULL DEFAULT TRUE,
    notes                       TEXT                    -- e.g. 'overruled by Kennedy v. Bremerton (2022)'
);

CREATE TABLE IF NOT EXISTS opinion_doctrines (
    opinion_id  BIGINT  NOT NULL REFERENCES opinions(id),
    test_id     INTEGER NOT NULL REFERENCES doctrinal_tests(id),
    role        TEXT    NOT NULL,   -- 'establishes'|'applies'|'modifies'|'limits'|'overrules'|'reaffirms'|'distinguishes'
    source      TEXT    NOT NULL,
    confidence  TEXT    NOT NULL,
    PRIMARY KEY (opinion_id, test_id, role)
);

CREATE INDEX IF NOT EXISTS idx_opinion_doctrines_opinion ON opinion_doctrines(opinion_id);
CREATE INDEX IF NOT EXISTS idx_opinion_doctrines_test ON opinion_doctrines(test_id);

-- ── Opinion-level citation graph ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS opinion_citations (
    id                      BIGSERIAL PRIMARY KEY,
    citing_opinion_id       BIGINT NOT NULL REFERENCES opinions(id),
    cited_case_id           BIGINT REFERENCES cases(id),
    cited_opinion_id        BIGINT REFERENCES opinions(id),
    cited_pinpoint          TEXT,           -- '501 U.S. 452, 461'
    citation_string         TEXT NOT NULL,
    char_offset_in_citing   INTEGER,
    parenthetical           TEXT,
    signal                  TEXT,           -- 'see'|'see also'|'but see'|'cf.'|'contra'|null
    treatment               TEXT,           -- 'positive'|'negative'|'cited'|null
    resolution_degraded     BOOLEAN NOT NULL DEFAULT FALSE,  -- true = fell back to case-level
    source                  TEXT NOT NULL,
    confidence              TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_opinion_citations_citing ON opinion_citations(citing_opinion_id);
CREATE INDEX IF NOT EXISTS idx_opinion_citations_cited_case ON opinion_citations(cited_case_id);
CREATE INDEX IF NOT EXISTS idx_opinion_citations_cited_opinion ON opinion_citations(cited_opinion_id);

-- ── Oral arguments ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS oral_arguments (
    id                  BIGSERIAL PRIMARY KEY,
    case_id             BIGINT NOT NULL REFERENCES cases(id),
    argued_date         DATE,
    duration_seconds    INTEGER,
    oyez_media_url      TEXT,
    transcript_text     TEXT,
    transcript_source   TEXT,               -- 'oyez'|'supreme_court_gov'
    UNIQUE (case_id, argued_date)
);

CREATE TABLE IF NOT EXISTS oral_argument_speakers (
    id                      BIGSERIAL PRIMARY KEY,
    oral_argument_id        BIGINT NOT NULL REFERENCES oral_arguments(id),
    speaker_name            TEXT,
    speaker_role            TEXT,           -- 'petitioner'|'respondent'|'amicus'|'justice'|'chief'
    justice_id              BIGINT REFERENCES justices(id),
    bar_admission_year      INTEGER,
    total_speaking_seconds  INTEGER,
    interruption_count      INTEGER
);

-- ── Cert proceedings ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cert_proceedings (
    id                                      BIGSERIAL PRIMARY KEY,
    case_id                                 BIGINT NOT NULL REFERENCES cases(id),
    docket_number                           TEXT,
    cert_filed_date                         DATE,
    cert_disposed_date                      DATE,
    cert_outcome                            TEXT,  -- 'granted'|'denied'|'gvr'|'dismissed'
    dissent_from_denial_opinion_id          BIGINT REFERENCES opinions(id),
    statement_respecting_denial_opinion_id  BIGINT REFERENCES opinions(id)
);

-- ── Opinion text versions (immutable append-only) ────────────────────────────

CREATE TABLE IF NOT EXISTS opinion_text_versions (
    id                      BIGSERIAL PRIMARY KEY,
    opinion_id              BIGINT NOT NULL REFERENCES opinions(id),
    version_tag             TEXT NOT NULL,  -- 'cap_original'|'normalized_v1'|'reocr_v1'
    text                    TEXT NOT NULL,
    source                  TEXT NOT NULL,  -- 'harvard_cap'|'courtlistener'|'gemini_ocr'
    source_extracted_at     TIMESTAMPTZ,
    pipeline_version        TEXT,           -- git SHA
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (opinion_id, version_tag)
);

-- ── Dataset releases ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dataset_releases (
    id                  SERIAL PRIMARY KEY,
    version             TEXT UNIQUE NOT NULL,   -- 'v1.0.0'
    released_at         TIMESTAMPTZ NOT NULL,
    pg_dump_path        TEXT,
    parquet_path        TEXT,
    zenodo_doi          TEXT,
    git_sha             TEXT NOT NULL,
    changelog           TEXT,
    case_count          INTEGER,
    opinion_count       INTEGER,
    vote_count          INTEGER,
    gold_set_precision  REAL,
    gold_set_recall     REAL,
    gold_set_f1         REAL
);

COMMIT;
