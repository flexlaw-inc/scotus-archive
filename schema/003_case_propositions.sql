-- FLexlaw SCOTUS Archive — Migration 003
-- New table: case_propositions (Phase 9b — proposition coalitions / issue voting)
--
-- Models precedential content at the micro-holding level: one row per discrete
-- legal proposition necessary to the judgment, with the coalition of justices
-- who endorsed and opposed it. Complements (does not replace) the Phase 9a
-- Marks-candidate pointer on cases.marks_controlling_opinion_id.
--
-- See plan v2.2 §5 Phase 9b for rationale. Canonical example: NFIB v. Sebelius
-- has four discrete propositions — three-way coalitions on Commerce Clause,
-- taxing power, Medicaid coercion, and Anti-Injunction Act — none of which is
-- cleanly recoverable from a single Marks pointer.
--
-- Apply against: legal_research (production) or scotus_archive (standalone)
-- Idempotent: CREATE TABLE IF NOT EXISTS + DO-block for constraints

BEGIN;

-- ── case_propositions ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS case_propositions (
    id                              BIGSERIAL PRIMARY KEY,
    case_id                         BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,

    -- The proposition itself.
    proposition_text                TEXT NOT NULL,
        -- e.g., "the individual mandate is a valid exercise of the taxing power"

    -- Provenance of the row.
    proposition_source              TEXT NOT NULL,
        -- 'llm_v1' | 'scdb_inferred' | 'human' | 'reviewer_edit'

    -- Justice coalitions. Both required (may be empty arrays).
    supporting_justice_ids          BIGINT[] NOT NULL DEFAULT '{}',
    opposing_justice_ids            BIGINT[] NOT NULL DEFAULT '{}',

    -- Opinion-level attribution.
    supporting_opinion_ids          BIGINT[],
        -- opinions expressing the proposition (may be >1 across majority/concurrences)
    opposing_opinion_ids            BIGINT[],
        -- opinions rejecting or contradicting it (typically dissents)

    -- Generated coalition sizes for fast filtering.
    vote_count_for                  SMALLINT GENERATED ALWAYS AS (cardinality(supporting_justice_ids)) STORED,
    vote_count_against              SMALLINT GENERATED ALWAYS AS (cardinality(opposing_justice_ids)) STORED,
    commands_majority               BOOLEAN  GENERATED ALWAYS AS (cardinality(supporting_justice_ids) >= 5) STORED,

    -- Precedential character.
    precedential_status             TEXT NOT NULL,
        -- 'majority'                   — 5+ votes and at least one majority-typed
        --                                 (or joined-by-majority) supporting opinion
        -- 'plurality_marks_candidate'  — 5+ votes via Marks narrowest-ground application
        -- 'contested'                  — 5 votes but Marks application is contested
        --                                 in the literature (Rapanos-type)
        -- 'fractured'                  — no single proposition reaches 5 votes despite
        --                                 the judgment commanding 5 on the disposition
        -- 'dicta'                      — fewer than 5 votes, or not load-bearing

    -- Judgment-necessity test (dicta separation).
    is_necessary_to_judgment        BOOLEAN,
        -- TRUE = removing this proposition would change the judgment
        -- FALSE = supporting discussion, not load-bearing

    -- Ordering within the case (1-based; display order as extracted).
    ordering                        SMALLINT,

    -- Confidence in the extracted row.
    confidence                      TEXT NOT NULL,
        -- 'high' | 'medium' | 'low' | 'provisional'

    -- Phase 7 reviewer signoff (required before release for plurality cases).
    reviewer_signed_off             BOOLEAN NOT NULL DEFAULT FALSE,
    reviewer_signoff_at             TIMESTAMPTZ,
    reviewer_id                     TEXT,
        -- free-form reviewer identifier (email or handle); not an FK

    -- Reviewer annotations — Marks dispute history, SCDB cross-ref, etc.
    notes                           TEXT,

    created_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ
);

-- ── Constraints ──────────────────────────────────────────────────────────────

-- Wrapped in DO blocks so the migration stays idempotent — re-running it must
-- not error when the constraint already exists.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'case_propositions_source_chk') THEN
        ALTER TABLE case_propositions
            ADD CONSTRAINT case_propositions_source_chk
            CHECK (proposition_source IN ('llm_v1', 'scdb_inferred', 'human', 'reviewer_edit'));
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'case_propositions_precedential_status_chk') THEN
        ALTER TABLE case_propositions
            ADD CONSTRAINT case_propositions_precedential_status_chk
            CHECK (precedential_status IN (
                'majority', 'plurality_marks_candidate',
                'contested', 'fractured', 'dicta'
            ));
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'case_propositions_confidence_chk') THEN
        ALTER TABLE case_propositions
            ADD CONSTRAINT case_propositions_confidence_chk
            CHECK (confidence IN ('high', 'medium', 'low', 'provisional'));
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'case_propositions_no_coalition_overlap_chk') THEN
        ALTER TABLE case_propositions
            ADD CONSTRAINT case_propositions_no_coalition_overlap_chk
            CHECK (NOT (supporting_justice_ids && opposing_justice_ids));
            -- a justice cannot simultaneously support and oppose the same proposition
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'case_propositions_signoff_consistency_chk') THEN
        ALTER TABLE case_propositions
            ADD CONSTRAINT case_propositions_signoff_consistency_chk
            CHECK (
                (reviewer_signed_off = FALSE AND reviewer_signoff_at IS NULL AND reviewer_id IS NULL)
                OR
                (reviewer_signed_off = TRUE  AND reviewer_signoff_at IS NOT NULL AND reviewer_id IS NOT NULL)
            );
    END IF;
END$$;

-- ── Indexes ──────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_case_propositions_case_id
    ON case_propositions (case_id);

CREATE INDEX IF NOT EXISTS idx_case_propositions_precedential_status
    ON case_propositions (precedential_status);

CREATE INDEX IF NOT EXISTS idx_case_propositions_commands_majority
    ON case_propositions (commands_majority)
    WHERE commands_majority = TRUE;

CREATE INDEX IF NOT EXISTS idx_case_propositions_reviewer_signed_off
    ON case_propositions (reviewer_signed_off)
    WHERE reviewer_signed_off = FALSE;
    -- partial index; the default case at ingest. Flip once reviewer signs off.

-- GIN indexes for coalition-membership queries
-- ("every case where Justice Kennedy was in the supporting coalition").
CREATE INDEX IF NOT EXISTS idx_case_propositions_supporting_gin
    ON case_propositions USING GIN (supporting_justice_ids);

CREATE INDEX IF NOT EXISTS idx_case_propositions_opposing_gin
    ON case_propositions USING GIN (opposing_justice_ids);

-- ── updated_at trigger ───────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION case_propositions_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_case_propositions_updated_at ON case_propositions;
CREATE TRIGGER trg_case_propositions_updated_at
    BEFORE UPDATE ON case_propositions
    FOR EACH ROW EXECUTE FUNCTION case_propositions_set_updated_at();

COMMIT;
