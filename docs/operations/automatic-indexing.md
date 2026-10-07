# Automatic active-revision indexing

Normal ChatGPT imports use book_ingest start/pages/stage/validate/finalize. Once
activation succeeds, source evidence is authoritative immediately. Indexing runs
in the background; the user never needs to run either backfill script.

`RKB_AUTO_INDEX_ENABLED=1` enables background vector publication. The measured
production gate is BGE-only by default: E5 is an optional diagnostic/indexing
space and is not allowed to block book activation. Production enables
`regional-knowledge-indexing.service`, with an immutable release working directory
and the ordinary service environment. BGE document inference remains on the
bounded worker path; no paid provider fallback exists.

Migration014 adds a payload-free activation notification, actor-RLS readiness
counts and SQL-snapshot semantic coverage guards. No durable queue/table is added.
The worker owns one PostgreSQL advisory session mutex and LISTEN connection; a
five-second periodic pass recovers a missed wakeup, process restart or completed
BGE job. The DB pool has two connections (listener plus ordinary bounded work).

A pass rotates over at most four documents missing an enabled indexing space.
BGE is always attempted first and keeps a bounded window of up to 64 unfinished
document jobs. Interactive query jobs still have strict claim priority. Completed
BGE results are installed in a bounded batch so one book does not pay one remote
vector-plane transaction per chunk. E5 batch4 work runs only when explicitly
enabled as a required or diagnostic space. Ready chunks are skipped; unchanged
rows are not rewritten. Document payload bounds match the existing
40000-character stage chunk bound; query limits, model/space, token cap, prefixes,
pooling and model batch sizes are unchanged.

Indexing reads the exact owner-authorized chunk payload directly from the local
corpus authority and verifies its hashes. It does not call the public evidence
hydration path, which also expands page/region/illustration metadata and is not
part of embedding. Writes recheck owner/account/selected revision, source hash and
exact space. Inactive revisions and archived controls are ignored;
old vectors are never deleted. BGE jobs retain the existing actor/space/chunk/
revision/hash idempotency keys. Completed jobs can be installed after restart
without submitting or encoding them again. At most 64 unfinished document jobs
are produced; the existing queue claims interactive queries ahead of documents.
A completed document job is retained only until its vector has been durably
installed and acknowledged, then retired from the queue. Completed interactive
query results are retained for 24 hours for bounded replay and then removed.
Completed benchmark/audit document results also expire after 24 hours; they must
not turn the production recovery queue into a persistent benchmark vector cache.
The queue is recovery state, not a permanent second vector store.
An E5 outage does not block BGE enqueue, source activation or lexical retrieval.
The default publication/readiness gate is BGE; set `RKB_REQUIRED_VECTOR_SPACES=e5,bge`
only for an explicit strict dual-space experiment. `RKB_INDEX_E5_DIAGNOSTIC=1`
may populate E5 without making it a publication requirement.
If a newer replacement revision is finalized while an older replacement is still
waiting for vectors, the older pending publication is marked `superseded` and
its ingestion becomes an explicit failed/superseded job. The indexer then selects
only the newest pending revision; any already-written older vectors remain inert
and cannot activate that superseded revision.

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

The private runtime `RKB_INDEXING_HEALTH_PATH` is an atomic heartbeat and
progress view, not recovery state. It records pending-document count, last
publication progress and per-phase timings. A pending publication with no progress
for five minutes becomes explicit `indexing_stalled` / degraded health instead of
looking healthy. Logs record activation wakeups, vector writes, enqueue counts,
retry error types and correlation IDs; they contain no source text or credentials.
While required vector publication is pending, unrelated archive/mirror and storage
GC work is deferred so external maintenance cannot hold the import critical path.
Missing required vectors remain recoverable after any transient failure. Missing
optional E5 vectors are diagnostic debt, not a publication blocker. Persistent
source hash mismatch, disabled owner, DB/provider outage or disk/queue failure
requires operator diagnosis; degraded readiness must not be treated as completed
indexing. The health file becomes unavailable after 120 seconds without a pass.

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