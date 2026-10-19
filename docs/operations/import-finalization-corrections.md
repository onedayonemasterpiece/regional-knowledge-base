# Accepted-source correction and service passages

## Target adapter and retained correction contract — 2026-10-19

The [implementation handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md) governs the PostgreSQL/pgvector+BGE target. Preserve the typed catalog correction allowlist, expected revision, verified write identity and immutable accepted source/page/chunk provenance described below. Public accepted-text search is a read-policy decision and never authorizes catalog mutation.

Target activation commits the accepted revision and resumable research enqueue together. Source facts, dates/observations, entities, optional story episodes and disagreements share that research pass. Accepted-source correction/repair must use the same facts-first model without compulsory Story creation, silently replacing human edits or replaying an accepted model call.

Service-only retrieval exclusions remain scoped to layout-only content; semantic headings, footnotes, tables, marginalia, illustrations and explicitly uncertain legacy provenance retain the safeguards below. Port filters and article scopes to PG and measure their interaction with bounded ANN candidates; remove E5 from the target while preserving independent complete-input BGE token validation. Provider diagnostics continue to redact credentials and arbitrary response bodies. The original implementation checks below are baseline evidence, not target deployment acceptance.

## Preserved implementation and historical record


After acceptance, source pages, chunks, source hash and revision remain immutable.
Use catalog.update for descriptive corrections, with expected_revision and source-owner authorization.

For clients with a cached catalog schema, the same typed correction is available
through existing book_ingest(command="stage", ingestion_id=..., metadata={
"catalog_update": {"expected_revision": 1, ...}}). Only a finalized owned checkpoint
is eligible. Do not combine this operation with file, source, story or entity payloads.
The adapter invokes the same catalog update implementation, whitelist, revision guard
and owner authorization; it is not a direct database or access-control workaround.
Explicit null publication_year clears an unverified year. Article components continue
to use book_ingest(start) with exact parent/region boundaries and no new source file.

## Service-only retrieval

Header, footer and page-number-only chunks remain preserved and explicitly fetchable,
but are excluded from FTS, validated dense results and continuation neighbors.
Mixed passages, semantic headings, footnotes (including footnote_region_ids), captions,
tables, marginalia, illustration-bearing and incomplete legacy provenance remain visible.
The FTS/neighbor filter precedes LIMIT. Dense filtering follows the remote bounded
candidate window: this does not guarantee remote service vectors cannot occupy slots
inside that window. No source or embedding is deleted, and vector space is unchanged.

## Archive OAuth diagnostics

Refresh failures log only HTTP status and an allowlisted OAuth error code.
Unknown error values and arbitrary response bodies/descriptions are never logged.
This diagnoses the existing grant; it does not create/rotate credentials or prove
the cause of an earlier HTTP400. An invalid grant still requires the supported
owner-consent flow; normal retries cannot repair revoked/expired authorization.

## Cold Kaggle queries

Cold BGE worker startup is distinct from MCP response latency. Existing search
returns a pending actor-bound job and degraded evidence rather than holding the tool
for notebook startup. Reuse the same job after readiness. Do not restore local BGE
or advertise a cold-start measurement as warm p95. Mass-ingestion readiness remains
subject to docs/mass-ingestion-readiness.md; these corrections alone do not pass it.
