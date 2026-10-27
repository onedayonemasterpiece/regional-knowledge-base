# Catalog correction and service-only retrieval

Active target2026-10-20. Source pages/chunks/evidence/hash/revision are immutable after acceptance; [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md) governs storage/recovery.

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
