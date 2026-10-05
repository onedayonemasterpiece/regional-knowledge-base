# Automatic active-revision indexing

Normal ChatGPT imports use book_ingest start/pages/stage/validate/finalize. Once
activation succeeds, source evidence is authoritative immediately. Indexing runs
in the background; the user never needs to run either backfill script.

`RKB_AUTO_INDEX_ENABLED=1` requires local E5. Production enables
`regional-knowledge-indexing.service`, with an immutable release working directory
and the ordinary service environment. Model libraries stay in the accepted E5
sidecar/Kaggle CPU worker. No paid provider fallback exists in this mode.

Migration014 adds a payload-free activation notification, actor-RLS readiness
counts and SQL-snapshot semantic coverage guards. No durable queue/table is added.
The worker owns one PostgreSQL advisory session mutex and LISTEN connection; a
five-second periodic pass recovers a missed wakeup, process restart or completed
BGE job. The DB pool has two connections (listener plus ordinary bounded work).

A pass rotates over at most four missing documents. Each document processes at
most two ordered E5 batch4 groups and sixteen BGE rows. Ready chunks are skipped;
a missing E5 member is encoded in its original document batch, while unchanged
rows are not rewritten. Document payload bounds match the existing 40000-character
stage chunk bound; query limits, model/space, token cap, prefixes, pooling and
batch sizes are unchanged.

Source bytes are fetched only after ordinary owner/account RLS authorization,
then checked against original SHA256. Writes recheck owner/account/active revision,
source hash and exact space. Inactive revisions and archived controls are ignored;
old vectors are never deleted. BGE jobs retain the existing actor/space/chunk/
revision/hash idempotency keys. Completed jobs can be installed after restart
without submitting or encoding them again. At most64 unfinished document jobs
are produced; the existing queue claims interactive queries ahead of documents.
An E5 outage does not block BGE enqueue, source activation or lexical retrieval.

Readiness appears in finalized book_ingest status, search/Live evidence results
and the narrow regular-MCP indexing_status tool (optional document scope): active
chunks, ready/missing E5/BGE counts, indexing state, worker state and effective
retrieval mode. All corpus counts use caller RLS. Foreign document scope is denied;
zero authorized chunks remains zero, regardless of another owner's inventory.
No titles, text, worker credentials or global queue count are exposed.

Search selects complete active **BGE semantic retrieval first** when available.
The measured default warm mode is `bge`; `bge_lexical`, `e5_bge` and
`e5_bge_lexical` remain explicit modes for bounded experiments/compatibility,
not automatic equal-weight defaults. When BGE is incomplete, search falls back to
complete ready E5 plus bounded lexical, otherwise lexical. SQL guards use the same
active/RLS snapshot as ranking, so activation cannot silently mix stale/partial
semantic vectors. Missing BGE reports pending rather than main ready. An actor with
complete BGE may use main even if E5 still needs repair.

Interactive BGE query wait is bounded by `RKB_BGE_QUERY_WAIT_SECONDS` (0.8 s by
default, hard-clamped to 3 s). A missed deadline returns the fast fallback with
`main_state=pending` and the resumable BGE job ID. General FTS work is bounded by
`RKB_LEXICAL_BUDGET_MS` (100 ms by default); it is omitted entirely from modes
without a lexical branch. Exact aliases remain separately bounded phrase signals.
Readiness is a snapshot, not a guarantee that an external worker will complete a
new query within its wait budget.

The remote vector call uses the locally authorized document/revision set as its
compact RLS scope. It no longer serializes every active chunk UUID on each query.
Only the bounded returned vector candidates are matched back to local
chunk/revision/text/search-material hashes before they can be exposed.

The private runtime `RKB_INDEXING_HEALTH_PATH` is an atomic coarse heartbeat file,
not recovery state. Logs record activation wakeups, vector writes, enqueue counts,
retry error types and correlation IDs; they contain no source text or credentials.
Missing vectors remain recoverable after any transient failure. Persistent source
hash mismatch, disabled owner, DB/provider outage or disk/queue failure requires
operator diagnosis; degraded readiness must not be treated as completed indexing.
The health file becomes unavailable after120 seconds without a pass.

Deployment: prove/apply migration014 with verify_indexing_migration.py and
apply_indexing_migration.py before enabling the flag/unit. Keep BGE SQLite state,
credentials, native E5 runtime and model files across releases. A unit restart
resumes the missing vectors; do not reset queues. Rollback stops/disables this
unit and removes the flag while retaining vectors/jobs and SQL safety guards.
Manual backfill scripts remain exceptional operator recovery tools.
## Retrieval quality release gate

Do not infer semantic quality from vector readiness counts. Before changing the
default retrieval mode or mass-rechunking accepted books, run the private frozen
retrieval gate:

```bash
.venv/bin/python scripts/production/verify_retrieval_release_gate.py \
  --cases "$RKB_EVIDENCE/retrieval-gate/cases.json" \
  --output "$RKB_EVIDENCE/retrieval-gate/result.json"
```

The private fixture supplies source-grounded target evidence IDs, explicit
thresholds and query/source language labels. It must include Russian queries over
German sources. The gate measures `bge`, `e5`, `lexical` and `e5_bge` independently;
a failed mode cannot be hidden by another branch. Fixture text and evidence IDs
remain outside Git.
