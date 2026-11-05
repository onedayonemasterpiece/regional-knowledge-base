# Catalog correction and service-only retrieval

Active target2026-10-20. Source pages/chunks/evidence/hash/revision are immutable after acceptance; [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md) governs storage/recovery.

The long corrective-stage rule is exposed once in `book_ingest` tool description; the
MCP initialize instructions retain only a brief ordinary-continuation reminder.

During an ongoing native `phase=staging`, a confirmed omission on an already
accepted physical page may be supplied by a new `book_ingest(stage)` for that
same ingestion and issued page receipt. This is **not** blind continuation
replay: existing `region_key`/`region_id` transcriptions and hashes and
existing `chunk_key`/`passage_id` bodies stay unchanged. Add the missing
readable text under a **new `region_key` and new `chunk_key`** with matching
source evidence; never rename the old region. SQL026
`source_batch_conflicts_v1` rejects changes to existing region/passage
identities. A rejected semantic-only enrichment after TEXT APPLIED can instead
restage identical text with corrected candidates. No source-text correction
is allowed after finalize; the older accepted commands are not replayed.

Native stage intake returns `unavailable` with reason
`source_stage_not_accepted_retry_same_stage` when SQLite reports native
`SQLITE_BUSY` (code 5) in the local spool stage transaction, which rolls back
before acceptance. Retry the same stage payload and ingestion. Errors after
that transaction, including the pending-progress update, and other SQLite
errors retain their existing reconciliation behavior; this reason does not
describe an accepted or unknown write.

Use catalog.update with typed descriptive whitelist, expected_revision and source-owner personal authority. Cached-schema clients may use existing book_ingest(stage, metadata.catalog_update) on finalized owned checkpoint; same implementation/validation applies. Do not combine catalog correction with file/source/story/entity payloads. Explicit null publication_year clears an unverified year; it does not invent a date. Article registration uses verified parent/region boundaries without changing original bytes.

Header/footer/page-number-only passages remain preserved and explicitly fetchable but are excluded from ordinary serving projection and continuation neighbors. Mixed passages, semantic headings, footnotes/captions/tables/marginalia and genuine illustration-bearing material stay searchable. Incomplete provenance fails to a visible review state, not silent deletion. PG query eligibility is before the branch limit; ANN filtering still needs bounded refill/iterative or exact narrow-scope handling. No lexical prefilter of dense and no remote Supabase path.

Source activation commits the compact active pointer and research frontier together after source/vector gates. Facts/metadata do not depend on fake Story; saved narrative availability is separate from source/index/research/publication state. Old active revision is retained until replacement is actually ready.

Provider diagnostics contain HTTP status and allowlisted error code, never arbitrary body/description/tokens. Invalid credentials require supported credential repair, not blind retry or impersonation; use existing dedicated same-host integration identities. Direct Kaggle source reads use genuine independent sessions configured for implementation.

Cold Kaggle BGE returns a bounded typed pending/unavailable status and any honestly labelled eligible lexical evidence. Ordinary read_query uses a RAM permit and request_id for this attempt, without durable intake/ACK, fsync, seal, RESUME or resultSaved; its late/cancelled generation is discarded. Do not interpret its ID as a saved-result handle or automatically repeat it after readiness. A read stage inside an accepted durable question instead retains that question's exact dependency/checkpoint and established job identity; retry does not create a second question or accepted effect. No local BGE/E5 fallback or long synchronous search. Current revision/withdrawal, authority epochs and min_data_seq remain enforced; a preceding UNKNOWN write of the same dialog yields explicit prior_write_pending with known freshness, never false latest/not-found. Warm latency and mass quality are separate gates. The typed read/durable distinction is defined in [MCP](../mcp.md); the real-client Mira/Dot comparison follows [handoff §14.4](../prompts/rkb-scalable-kaggle-implementation-20261019.md), not an assumed gain from a route or language change.

Reading completion does not discard pages whose TEXT packet was applied but
whose SEMANTIC packet was rejected. Normal status/finalize returns
`continue_pages` at the first such physical page, using the existing APPLIED
journal receipts rather than the latest compact batch counters. The Launcher
activity projection likewise keeps this work active. The same ingestion and
ordinary model-authored stage may supply identical accepted text and corrected
semantic input for those pages, including a historically reading-completed
packet import. Other finalized corrections remain sealed; exact-text SQL guards,
old commands, receipts and original reading-completion provenance are unchanged.
No separate repair queue, mode or manual caption edit is involved.
