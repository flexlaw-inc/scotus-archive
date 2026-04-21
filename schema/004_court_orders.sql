-- FLexlaw SCOTUS Archive — Migration 004
-- court_orders sibling table + opinion_type enum expansion.
--
-- Background: triage of the 337,739 manual_required `majority → other`
-- reclassifier proposals (see docs/triage-memo-2026-04-21.md) showed the
-- cohort bifurcates into two structurally distinct populations:
--
--   (1) Bug 1 — orders mis-tagged as opinions (~333k rows, 98.6% of cohort).
--       CAP's import treats every filing as an "opinion." Cert denials,
--       motion rulings, disbarments, stays, rehearing denials are not
--       opinions in any meaningful sense — no author, no reasoning, never
--       cited as precedent. SCDB excludes them entirely. This migration
--       creates a sibling `court_orders` table to receive these rows.
--
--   (2) Bug 2 — concatenated full-opinion rows (~2.5–4.8k rows). Real
--       opinions where CAP collapsed the entire U.S. Reports entry
--       (syllabus + majority + each concurrence + each dissent) into a
--       single `opinions` row. Not a schema problem — needs a text-
--       segmentation pipeline (separate work item, no migration here).
--
-- This migration also adds new opinion_type enum values to accommodate
-- authored separate writings *attached to* orders (dissents from denial,
-- concurrences from denial, statements respecting denial, in-chambers
-- opinions). Those rows STAY in `opinions` — they're reasoned, citable,
-- opinion-like — they just need the right type label and an FK pointer
-- back to the order they attach to.
--
-- Apply against: legal_research (production) or scotus_archive (standalone)
-- Idempotent: CREATE TABLE IF NOT EXISTS, ADD COLUMN IF NOT EXISTS,
-- ADD VALUE IF NOT EXISTS for enums.

BEGIN;

-- ── opinion_type enum expansion ─────────────────────────────────────────────
-- ADD VALUE IF NOT EXISTS requires PG 9.6+. Cannot be wrapped in DO block
-- with EXCEPTION handling (ADD VALUE is not transactional in older PGs);
-- but in PG 12+ it works inside transactions.

ALTER TYPE opinion_type ADD VALUE IF NOT EXISTS 'in_chambers';
ALTER TYPE opinion_type ADD VALUE IF NOT EXISTS 'dissent_from_denial';
ALTER TYPE opinion_type ADD VALUE IF NOT EXISTS 'concurrence_from_denial';
ALTER TYPE opinion_type ADD VALUE IF NOT EXISTS 'statement_respecting_denial';

-- ── court_orders ────────────────────────────────────────────────────────────
-- One row per discrete order of the Court (or single justice in chambers).
-- Most rows here are unsigned procedural artifacts; a small minority are
-- signed (e.g., in-chambers stay rulings before transfer to the full Court).

CREATE TABLE IF NOT EXISTS court_orders (
    id                  BIGSERIAL PRIMARY KEY,
    case_id             BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,

    -- Classification of the order. Free-text TEXT (not enum) so the regex
    -- classifier can grow new buckets without migrations.
    order_type          TEXT NOT NULL,
        -- Known values produced by pipeline.triage / pipeline.orders classifier:
        --   cert_denied | cert_granted | cert_dismissed | rehearing_denied |
        --   stay | injunction | mandamus | habeas | motion | disbarment |
        --   reinstatement | leave_to_file | summary_affirmance |
        --   appeal_dismissed | recusal | misc_administrative | other

    -- The disposition text itself. Usually one to a few lines.
    order_text          TEXT,

    decision_date       DATE,

    -- Author. Usually NULL (most orders are per curiam / unsigned). Populated
    -- for signed orders — primarily in-chambers rulings and the rare signed
    -- order list entry.
    author              TEXT,
    author_original     TEXT,
        -- audit snapshot of author as it appeared at ingest time, mirroring
        -- the opinions.author_original convention from migration 002.

    -- Provenance.
    source              TEXT,
        -- 'cap' | 'courtlistener' | 'scotus_orders_list' | 'manual'
    original_opinion_id BIGINT,
        -- pre-migration opinions.id, so post-relocation forensic joins still
        -- work and reclassification_log entries remain join-reachable.
    classifier_rule_id  TEXT,
        -- which regex bucket fired (e.g., 'opener_cert_denied_v1')
    cap_case_id         BIGINT,
        -- propagated from cases.cap_case_id at relocation time, for upstream
        -- cross-reference into the Caselaw Access Project corpus.

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_court_orders_case_id
    ON court_orders(case_id);
CREATE INDEX IF NOT EXISTS idx_court_orders_type_date
    ON court_orders(order_type, decision_date);
CREATE INDEX IF NOT EXISTS idx_court_orders_original_opinion_id
    ON court_orders(original_opinion_id)
    WHERE original_opinion_id IS NOT NULL;

-- ── opinions.related_order_id ───────────────────────────────────────────────
-- Lets an authored separate writing (e.g., a Sotomayor dissent from denial of
-- cert) point at the order it attaches to. Nullable; only populated for the
-- new authored-separate-writing-from-denial enum values.

ALTER TABLE opinions
    ADD COLUMN IF NOT EXISTS related_order_id BIGINT
        REFERENCES court_orders(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_opinions_related_order_id
    ON opinions(related_order_id)
    WHERE related_order_id IS NOT NULL;

-- ── reclassification_log: extend to accommodate order relocations ──────────
-- The relocation pipeline writes one log row per relocated opinion. The
-- existing schema already accommodates this — we record old_type='majority'
-- (or whatever the row had), new_type='__moved_to_court_orders__', signal
-- 'orders_relocator', rule_id matches court_orders.classifier_rule_id.
-- No DDL needed; this comment exists for documentation only.

COMMIT;

-- ── post-migration verification ─────────────────────────────────────────────
-- Run after applying:
--   SELECT enumlabel FROM pg_enum
--    WHERE enumtypid = 'opinion_type'::regtype ORDER BY enumsortorder;
--   \d court_orders
--   \d+ opinions  -- verify related_order_id is present
