# Accepted-source correction and service passages

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
