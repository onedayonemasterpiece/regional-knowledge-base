# Accepted import repair: release and live retest

Observed 2026-10-17, UTC. This report contains no private source text, document IDs or credentials.

## Released

- [PR #127](https://github.com/onedayonemasterpiece/regional-knowledge-base/pull/127), merged at 16:27:32.
- Candidate: `ddfc4acdf3c0cc0c9641d6e24fe30451123f722d`.
- Main and served release: `3f52bd15270b1c29e7d8636ef3a16aa004908fb7`.
- Full local suite with the declared PyMuPDF ingest dependency: **454 passed, 29 skipped**. The earlier two missing-fitz failures were resolved by installing the dependency in an isolated ignored test cache, not by skipping assertions.
- [CI run 39049335798](https://github.com/onedayonemasterpiece/regional-knowledge-base/actions/runs/39049335798): Python 3.12 and 3.13 both successful.
- Existing release script deployed only RKB MCP and its indexing worker at 16:28:48. Local/public health and both processes' exact release keys passed. A consistent 82,911,232-byte SQLite backup passed integrity and foreign-key checks before restart. No rollback was needed; a backup is not proof of provider archival.
- A live tool traceback additionally confirmed the actual MCP route loaded this release, not merely the health endpoint.

## What changed

Preflight and activation now share semantic-region coverage rules while retaining header/footer/page-number source data. Accepted story checkpoint repair normalizes UUID IDs and commits its checkpoint together with story activation. Catalog source correction is owner- and active-revision-guarded; existing article components reuse verified source regions without reimport or replacing chunks/vectors. Entity graph staging remains a separate transaction.

## Live retest

At 16:31:32 a corrected metadata-only replay succeeded: **10 saved, 0 staged, 0 needing review**. Two repaired histories retained their identities and advanced to revision 3. The checks distinguished a court observation count, a mortality count and a population decrease rather than conflating them. Evidence remained bound to the original accepted source revision. No source pages/chunks were restaged and no duplicate stories were created.

An initial post-release stage call accidentally omitted its story payload in the caller. The server correctly rejected that empty finalized-stage request; this was not a reproduced backend regression. The corrected call above is the relevant acceptance result.

## Remaining product gaps

1. The current connector still advertises the old catalog command enum. It rejects `update` locally before sending MCP, although the deployed server supports the new typed command. Therefore source-card correction and article-component acceptance remain pending; the physical source must not yet be claimed corrected. Refresh the connector schema or use its supported current tool surface, not direct database writes or broadened credentials.
2. Original-source archival remains pending. Runtime logs prove the archive worker actually attempts the document; this is not proven queue starvation. Its provider token endpoint repeatedly returns HTTP 400, followed by `source_archive_retry` with `HTTPStatusError`. The precise OAuth error body is not logged. Do not infer `invalid_grant`, revoked credentials or a required credential replacement without further evidence. No grant, token or permission was changed, and no new archive idempotency key was submitted.
3. BGE document vectors remain 24/24. Scoped semantic story retrieval recovered before release; a post-restart query readiness warm-up is separate from vector completeness. Do not infer permanent semantic failure from a transient starting/unavailable state. BGE inference remains Kaggle-only; E5 and tokenizer assets were untouched.

This is a bounded import-repair release and partial live acceptance, not full source-archive, catalog, reconciliation or mass-ingestion readiness acceptance. Continue the original import goal through the remaining checks.
