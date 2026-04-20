-- FLexlaw SCOTUS Archive — Migration 002
-- Alter existing cases and opinions tables to add SCOTUS-archive columns.
--
-- Safe to run multiple times: all ALTER TABLE ... ADD COLUMN IF NOT EXISTS
-- Requires PostgreSQL 9.6+

BEGIN;

-- ── cases additions ───────────────────────────────────────────────────────────

ALTER TABLE cases
    ADD COLUMN IF NOT EXISTS scdb_case_id           TEXT,
    ADD COLUMN IF NOT EXISTS oyez_case_id           TEXT,
    ADD COLUMN IF NOT EXISTS is_plurality           BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_per_curiam          BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_dig                 BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_summary_affirmance  BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS disposition            TEXT,
    ADD COLUMN IF NOT EXISTS majority_size          SMALLINT,
    ADD COLUMN IF NOT EXISTS marks_controlling_opinion_id BIGINT REFERENCES opinions(id),
    ADD COLUMN IF NOT EXISTS judicially_argued      BOOLEAN,
    ADD COLUMN IF NOT EXISTS chief_justice_era      TEXT;

CREATE INDEX IF NOT EXISTS idx_cases_scdb_case_id ON cases(scdb_case_id) WHERE scdb_case_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cases_oyez_case_id ON cases(oyez_case_id) WHERE oyez_case_id IS NOT NULL;

-- ── opinions additions ────────────────────────────────────────────────────────

ALTER TABLE opinions
    ADD COLUMN IF NOT EXISTS opinion_type_original      TEXT,
    ADD COLUMN IF NOT EXISTS opinion_type_source        TEXT,
    ADD COLUMN IF NOT EXISTS opinion_type_confidence    TEXT,
    ADD COLUMN IF NOT EXISTS justice_id                 BIGINT REFERENCES justices(id),
    ADD COLUMN IF NOT EXISTS justice_match_confidence   TEXT,
    ADD COLUMN IF NOT EXISTS author_original            TEXT,
    ADD COLUMN IF NOT EXISTS joined_justice_ids         BIGINT[],
    ADD COLUMN IF NOT EXISTS partial_joiners            JSONB,
    ADD COLUMN IF NOT EXISTS sequence_in_case           SMALLINT,
    ADD COLUMN IF NOT EXISTS word_count                 INTEGER,
    ADD COLUMN IF NOT EXISTS fk_grade_level             REAL,
    ADD COLUMN IF NOT EXISTS flesch_reading_ease        REAL,
    ADD COLUMN IF NOT EXISTS type_token_ratio           REAL,
    ADD COLUMN IF NOT EXISTS avg_sentence_length        REAL,
    ADD COLUMN IF NOT EXISTS avg_word_length            REAL,
    ADD COLUMN IF NOT EXISTS citation_count_outbound    INTEGER,
    ADD COLUMN IF NOT EXISTS footnote_count             INTEGER,
    ADD COLUMN IF NOT EXISTS unique_cases_cited         INTEGER,
    ADD COLUMN IF NOT EXISTS self_citation_count        INTEGER,
    ADD COLUMN IF NOT EXISTS page_boundaries            JSONB,
    ADD COLUMN IF NOT EXISTS provenance_source          TEXT;

CREATE INDEX IF NOT EXISTS idx_opinions_justice_id ON opinions(justice_id) WHERE justice_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_opinions_type_source ON opinions(opinion_type_source) WHERE opinion_type_source IS NOT NULL;

-- ── Reclassification log (Phase 1 audit artifact) ────────────────────────────

CREATE TABLE IF NOT EXISTS reclassification_log (
    id                  BIGSERIAL PRIMARY KEY,
    opinion_id          BIGINT NOT NULL REFERENCES opinions(id),
    old_type            TEXT NOT NULL,
    new_type            TEXT NOT NULL,
    signal              TEXT NOT NULL,      -- 'author_field'|'opening_text'|'body_text'|'courtlistener'
    signal_text         TEXT,               -- excerpt that triggered the rule
    rule_id             TEXT,               -- e.g. 'author_dissenting_v1'
    confidence          TEXT NOT NULL,
    pipeline_version    TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reclassification_log_opinion ON reclassification_log(opinion_id);

-- ── Marks provisional assignments (Phase 9) ──────────────────────────────────

CREATE TABLE IF NOT EXISTS marks_provisional (
    id                          BIGSERIAL PRIMARY KEY,
    case_id                     BIGINT NOT NULL REFERENCES cases(id),
    candidate_opinion_id        BIGINT NOT NULL REFERENCES opinions(id),
    llm_rationale               TEXT,
    expert_review_needed        BOOLEAN NOT NULL DEFAULT TRUE,
    expert_reviewed_by          TEXT,
    expert_reviewed_at          TIMESTAMPTZ,
    accepted                    BOOLEAN,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (case_id)
);

COMMIT;
