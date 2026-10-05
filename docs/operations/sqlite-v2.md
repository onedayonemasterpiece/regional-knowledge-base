# SQLite corpus and Supabase vector plane

The v2 release uses `RKB_SQLITE_CORPUS_PATH` for persistent corpus, exact text,
FTS, catalog, ACL/rights, ingestion, graph/discovery, POI outbox and publication
state. Supabase keeps minimal immutable anchors, both original embedding spaces
and vector search. The local application actor bridge authorizes access; remote
RPC scope is server-computed from local authorization. No bearer forwarding or
remote document admission is required. Offline vectors leave local fetch/catalog
and truthful lexical-only search usable.

## Migration, retirement and recovery

The snapshot uses explicit metadata columns, excluding embeddings everywhere,
and preserves accepted IDs/text/hashes. Verified native SQLite backup/restore
parity precedes retirement. The observed legacy snapshot has 24,917 rows,
10 roots, 1,167 pages, 2,626 chunks across retained revisions, and 14,479 exact
fragments with zero unmapped text. All 553 legacy active citations were compared;
existing corpus was neither recognized again nor embedded again.

Quiesce legacy corpus writers for cutover. Do not rerun initial snapshot migration
over new local imports/catalog edits. `retire_remote_corpus.py` compares every
legacy row with both restored backup and current local authority under remote
table locks, checks external FKs, restores pure vector scope, and drops only
reviewed corpus tables/functions with RESTRICT. It preserves all vector rows and
revisions; any unresolved dependency aborts atomically. No VACUUM FULL is used.

```bash
.venv/bin/python scripts/production/retire_remote_corpus.py \
  --database /home/dev/.local/state/regional-knowledge-base/corpus.sqlite3 \
  --backup "$RKB_EVIDENCE/full-authority-migration/corpus-backup.sqlite3" \
  --migration-report "$RKB_EVIDENCE/full-authority-migration/migration.json" \
  --evidence "$RKB_EVIDENCE/retirement" --apply
```

Evidence must be private managed retained artifacts, outside Git. Restore using
SQLite backup API and verify its digest before selecting it. After retirement,
rollback must use a SQLite-compatible release; the old remote-corpus runtime is
not a valid rollback. Historical SQL migrations plus retained exact detail
snapshot and function definitions provide recovery material if remote restoration
is explicitly needed. Never delete vectors to restore local corpus state.

## Publication and read behavior

Finalization persists exact staged local chunks and a pending source/revision/hash
manifest. Existing E5/BGE workers publish vectors without holding SQLite write
locks. Only exact acknowledgments in both unchanged spaces activate the revision;
an unfinished replacement leaves the previous revision selected. Replays are
idempotent. Graph/POI durable rows remain local and private outbox events require
existing recipient authorization.

UUID fetch uses `(table_name,row_key)`. Vector candidate filtering uses indexed
metadata only. Indexed source positions select bounded previous/current/next
chunks, with semantic article boundaries and the original continuation gate.
Ordinary readers use WAL without a writer transaction. Worker mutations are short
serialized transactions; SQLite busy waits run outside the event loop.

## Originals and proof

Originals default to 512 MiB and four idle hours, configurable through
`RKB_ORIGINAL_CACHE_MAX_BYTES` / `RKB_ORIGINAL_CACHE_IDLE_HOURS`. Actual original
access renews idle time. An authorized warm proof renews an existing original without downloading an
absent one. Incoming reservations evict eligible LRU entries;
cross-process leases protect live copies. Existing indexing maintenance performs
periodic idle/partial cleanup even without requests. Proof cache is 64 MiB.

Native PDF word/quads are checked first; same-page multi-region and bounded
page-by-page multi-page quotes are supported. DjVu/image-only proof uses cheap
`gemini-2.5-flash-lite` with at most two calls/page and independent crop reading.
Server code validates visible text/geometry and draws yellow stripes on the real
source. Provider refusal, repeated phrase or mismatch yields an honest refusal.
`RKB_SCAN_PROOF_ENABLED` and source ownership are rechecked for warm model hits. Both scan readers receive
only the mapped region union; boxes are transformed to physical coordinates and
rejected if they escape that union, including gaps between disjoint regions.
No page archive, spool, new topic or whole-book readiness gate exists.

## Reproducible acceptance

```bash
.venv/bin/pytest -o addopts='' -q --basetemp="$RKB_EVIDENCE/tests"
RKB_FROZEN_CASES="$RKB_EVIDENCE/hard6-inputs.json" \
RKB_FROZEN_BASELINE="$RKB_EVIDENCE/hard6.json" \
RKB_ACCEPTANCE_DIR="$RKB_EVIDENCE/postrollout-hard6" \
  .venv/bin/python scripts/production/accept_frozen_hard6.py
.venv/bin/python scripts/production/verify_product_mcp.py \
  --evidence "$RKB_EVIDENCE/product-mcp" --verify-only
.venv/bin/python scripts/production/verify_product_proofs.py \
  --evidence "$RKB_EVIDENCE/product-mcp"
.venv/bin/python scripts/production/measure_sqlite_capacity.py \
  --evidence "$RKB_EVIDENCE/capacity"
```

Capacity uses separate temporary remote tables, varied source lengths and both
original dimensions/HNSW operators, never a large production benchmark. The
100-material fixture has 1,940 pages/3,825 chunks; measured vector-plane allocation
is about 68.6 MB, SQLite about 12.8 MB. This is an example capacity envelope, not a
promise for any 100 books. Quality is checked separately against all six exact
frozen complete-answer cases and their target evidence IDs. Private receipts hold
individual D01–D10 results, source hashes, actual sizes and cold/warm timings.
