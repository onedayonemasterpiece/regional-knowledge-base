-- DESIGN LEAF ONLY / UNACTIVATED / NOT A PRODUCTION MIGRATION.
-- Do not move this file into sql/kaggle, assign 013/014/015, execute it,
-- change DOMAIN_REVISIONS or advertise the tool before the runtime owner
-- approves ordered migration, exact handler revisions and PG acceptance.
--
-- Proposed additive schema AFTER current source/Story and Case migrations.
-- Reuses SQL006 source_research_frontier, SQL007 source assertions and
-- Story/Variant, SQL010 Case. No UPDATE of accepted originals, assertion
-- versions, user-written variants, human contributions or source-pass status.
--
-- The source-pass frontier STATE=complete means pages were reviewed. The
-- cross-book frontier below tracks independently whether comparisons ran.

CREATE TABLE rkb_v4.story_reconciliation_cursor (
    source_id uuid PRIMARY KEY REFERENCES rkb_v4.sources(source_id),
    source_revision bigint NOT NULL CHECK (source_revision >= 1),
    source_frontier_id uuid NOT NULL UNIQUE
        REFERENCES rkb_v4.source_research_frontier(frontier_id),
    reconciliation_state text NOT NULL DEFAULT 'open'
        CHECK (reconciliation_state IN ('open','complete')),
    comparison_cursor text,
    progress_version bigint NOT NULL DEFAULT 1 CHECK (progress_version > 0),
    last_applied_sequence bigint CHECK (last_applied_sequence > 0)
);

-- This cursor is NOT automatically marked complete when a book's
-- SourceResearchInput.state becomes 'complete'.
-- Source revision, eligibility, withdrawal, and source access must be
-- rechecked under the same native read/command fences before every output
-- and every state change, rather than trusted from a previous MCP response.
CREATE TABLE rkb_v4.story_reconciliation_intents (
    operation_key text PRIMARY KEY
        CHECK (length(operation_key) BETWEEN 1 AND 160),
    actor_id uuid NOT NULL,
    source_id uuid NOT NULL REFERENCES rkb_v4.sources(source_id),
    source_revision bigint NOT NULL CHECK (source_revision >= 1),
    source_eligibility_revision bigint NOT NULL CHECK (source_eligibility_revision >= 1),
    source_frontier_id uuid NOT NULL
        REFERENCES rkb_v4.source_research_frontier(frontier_id),
    peer_source_id uuid NOT NULL REFERENCES rkb_v4.sources(source_id),
    peer_source_revision bigint NOT NULL CHECK (peer_source_revision >= 1),
    peer_eligibility_revision bigint NOT NULL CHECK (peer_eligibility_revision >= 1),
    assertion_id uuid NOT NULL,
    peer_assertion_id uuid NOT NULL,
    comparison_key text NOT NULL,
    proposal_fingerprint text NOT NULL CHECK (proposal_fingerprint ~ '^[0-9a-f]{64}$'),
    request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    immutable_request jsonb NOT NULL CHECK (jsonb_typeof(immutable_request)='object'),
    current_step integer NOT NULL DEFAULT 0 CHECK (current_step >= 0),
    attempted boolean NOT NULL DEFAULT false,
    case_id uuid REFERENCES rkb_v4.disagreement_cases(case_id),
    done boolean NOT NULL DEFAULT false,
    rejected boolean NOT NULL DEFAULT false,
    rejected_step integer CHECK (rejected_step >= 0),
    progress_version bigint NOT NULL DEFAULT 1 CHECK (progress_version > 0),
    CHECK (source_id <> peer_source_id),
    CHECK (NOT (done AND rejected)),
    CHECK ((rejected AND rejected_step=current_step)
        OR (NOT rejected AND rejected_step IS NULL))
);
CREATE INDEX story_reconciliation_actor_cursor
    ON rkb_v4.story_reconciliation_intents(actor_id,source_id,operation_key);

-- Future native read/command bindings (NOT IMPLEMENTED HERE):
-- 1) bounded next: one actor/visibility-scoped page of current source
--    frontier + exact source assertions, including source-pass complete;
-- 2) stage: actor-scoped operation_key + exact immutable_request hash,
--    source/peer revisions, quote-region checks, one atomic INSERT/duplicate
--    CAS; a reused key with changed bytes FAILS;
-- 3) begin/advance/finish: expected progress_version compare-and-swap,
--    authorized current source/withdrawal/eligibility, no blind local ACK;
-- 4) Case effect: existing disagreement_edit(create/add_evidence), followed
--    by status operation_key reconciliation. Never derive Case identity from
--    proposal_fingerprint or assume pending == SAVED;
-- 5) completion covers ONE model-authored comparison, not all possible
--    cross-book counterparts. The calling model may resume next later.
--
-- The acceptance chain must independently pin SQL function SHA256,
-- source history cursor, Dataset coverage and native PG readback. No
-- migration number, live RKB function, registry, executor or tool publication
-- is authorized by this draft.
