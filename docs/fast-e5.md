# Local E5 fast tier

The fast encoder runs as a separate user systemd service. The MCP process imports
only the local HTTP client, never ONNX/tokenizer/numpy or model weights. Explicit
`RKB_FAST_E5_ENABLED=1` selects the fixed loopback `http://127.0.0.1:8767` endpoint.
Errors degrade to the existing lexical search; there is no external-provider
retry. Shared provider keys do not enable embeddings. Legacy explicitly configured
providers remain a separate opt-in configuration, not the fast-tier fallback.

Pinned assets are Xenova/multilingual-e5-small revision
`761b726dd34fb83930e26aab4e9ac3899aa1fa78`: INT8 ONNX SHA
`f80102d3f2a1229f387d3c81909990d8945513e347b0eab049f7de3c6f98c193`,
tokenizer SHA `0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39`.
Both are checked before readiness. Space:
`e5-small-int8:761b726:model-f80102d3:tok-0b44a9d7:mean-l2-512:q1-d4:v1`.
Queries use `query: ` at batch1; passages use `passage: ` at deterministic
per-document text_start/id batch4, with a final shorter batch when necessary.
Tokenizer BOS/EOS, right padding ID1, attention-mask mean including special tokens,
512-token cap and float32 L2 normalization match PR #26. E5 is batch sensitive:
batch groups and source hashes are persisted, and affected groups must be refreshed
when grouping/input changes. Do not silently use batch1 documents with batch4
quality expectations. The current incomplete source projection remains incomplete.

The same isolated service also exposes a bounded loopback-only `/token-count`
operation for ingestion validation. It uses an untruncated copy of the pinned E5
tokenizer and a separately provisioned BGE tokenizer from the exact BGE revision.
The BGE tokenizer file is SHA-256 checked before use and lives outside Git at
`fast-e5/bge-m3-tokenizer.json` by default (or `RKB_BGE_TOKENIZER_PATH`).
The MCP process still imports no tokenizer/model package. If the exact BGE
tokenizer is unavailable or mismatched, validation of a new revision fails closed
instead of allowing silent 512-token truncation.

One process, one native inference thread, one async FIFO queue of capacity10.
The service binds only loopback, accepts at most64KiB JSON/8000 characters per
text, query batch1 or passage batch≤4, and fixed model/space/roles only. Queue
full returns429 `encoder_queue_full`; expired queue work returns503. HTTP/read
and client timeouts are bounded. The client never starts another process.
Systemd limits are CPUQuota100%, CPUAffinity0, MemoryMax1GiB, MemorySwapMax0.
MCP restarts do not restart this independent service.

`sql/010_fast_e5.sql` adds typed `vector(384)` storage with source hash/revision and
batch fingerprint, an HNSW cosine index, RLS and an explicit-space RPC. The existing
768-d column/RPC remain unchanged. Future BGE1024 can use another typed table/RPC
without destructive E5 changes; no BGE implementation is included. Stale vectors
are excluded. SQL fusion uses the existing simple/websearch AND lexical branch
and RRF60; candidate depth is5×match_count with a minimum20. Normal MCP search
keeps8 results; bounded regression can ask20 internally.

Apply only after the isolated PostgreSQL/pgvector migration verifier passes and
its SQL hash matches. `scripts/production/apply_fast_e5_migration.py` checks that
receipt and legacy identity. `backfill_e5.py` reads active authorized source
projections, checks exact hashes, serializes operator runs with a host lock, writes
idempotently per group, rechecks active revision/source in the write transaction,
and verifies full valid active coverage. Rerunning unchanged input skips inference
and inserts nothing. Newly finalized chunks keep legacy embedding NULL. With production
`RKB_AUTO_INDEX_ENABLED=1`, activation automatically wakes the index reconciler;
`automatic_indexing_pending` clears from live status when both spaces are complete.
Normal imports need no operator backfill. See [automatic indexing](operations/automatic-indexing.md).
The MCP ingestion process never stores a query vector in legacy passage storage.

Operations: `systemctl --user status/restart regional-knowledge-e5.service`;
loopback `/health` gives readiness/queue/busy/space, with no source text or model
filesystem paths. MCP `/fast-tier/health` exposes only configured/ready/mode.
Operator-only `scripts/production/fast_e5_status.py` uses service DB credentials
and adds active vector counts, never private document details. Retrieval results
retain compatibility `mode` and add `retrieval_mode=fast_e5|lexical_only`.
Stage timings are internal and excluded from model-visible result serialization;
structured boundary logs contain mode/count/times, never text or credentials.

Rollback: first set `RKB_FAST_E5_ENABLED=0` (ensure the dedicated external quartet
is absent), restore the previous immutable MCP release/unit and restart MCP.
The extra table can safely remain dormant. Stop E5 if desired. Only after all E5
callers are disabled, `sql/010_fast_e5.rollback.sql` removes its own RPC/table/index;
legacy vectors/chunks remain. Test rollback in an isolated database; never run the
synthetic verifier against production. The verifier guards its local fixture DB.
Model/runtime symlinks reuse the retained pinned lab cache without another large
copy; its retention protects these active runtime dependencies. Client inference
and Kaggle/BGE remain deferred. Actual deployed SHA and measured acceptance are
recorded in the dated report, not inferred from the historical isolated benchmark.