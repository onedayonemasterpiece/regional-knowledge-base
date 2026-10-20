# Kaggle PostgreSQL passage-search slice

This is an undeployed implementation slice for accepted shared book text.
It adds no controller, model service, queue, identity authority or readiness gate.
The target contract read was the 2026-10-20 handoff §§4.1–4.4 at baseline
`3d464ca`; installed skill read:
`/home/dev/.local/share/devcoveer/auth/codex/skills/kaggle-warm-debugging/SKILL.md`
with its RKB profile and migrations reference.

`sql/kaggle/001_search.sql` is an immutable transactional DDL payload for the
existing ordered migration executor. That executor must bind its hash,
predecessor, expected/resulting revision, compatibility and database generation;
this file creates no second migration ledger and deliberately fails on reuse.
PostgreSQL 17 and preinstalled `public.vector` pgvector 0.8.2 are prerequisites.
Application grants, extension installation and live activation are separate.
Functions use invoker rights and schema-qualified objects.

Source and passage UUIDs are supplied unchanged by the accepted command;
revision is an integer >=1. Source identity and revision/passage payloads cannot be updated. Bibliography
metadata corrections do not alter or reembed immutable passages. Mutable source acceptance/withdrawal and one per-source
active-revision pointer determine serving eligibility. Revision activation must
occur under existing writer authority only after the complete projection and
research frontier are ready; this slice does not implement that domain command.
Source-bound locators are text references, never scan/full-original grants.
Bounded deletion/GC remains the controller's responsibility.

All lexical, dense and hydration SQL joins accepted, nonwithdrawn sources to
the active revision. Accepted book text has no per-owner filter. This does not
implement private drafts/cases or grant private media access. FTS stores explicit
RU/DE/simple regconfig, uses a fixed three-config query set, GIN and bounded
candidates. Dense uses independent `ORDER BY embedding <=> query LIMIT` with
HNSW cosine over FP32 vector(1024); source/revision/ordinal has a unique B-tree.
Python performs bounded RRF and one hydration for <=5 passages (default 3). Hydration returns full schema-bounded
body (<=16000 characters) and bibliography (<=8000 characters). A future wire
adapter must explicitly label excerpts if it introduces truncation.

`fused_search` takes an exclusively checked-out idle async psycopg connection
with tuple rows and an injected asynchronous encoder. It checks the connection
is open, transaction status IDLE and row factory exactly psycopg tuple_row;
exclusive checkout and asynchronous connection ownership are caller obligations. Execute it beside the
resident Kaggle BGE process. The caller must enforce verified authority,
pinned tokenizer/model/input policy, actual model concurrency and final output
freshness. The callable starts encode before lexical SQL, then runs independent
dense SQL and batch hydration. It does not load any model or send vectors back
to DevCoveer. The shared <=2-second coroutine deadline spans all branches;
SQL timeout is refreshed from the remaining monotonic budget before each
search/setting SQL operation; the outer absolute deadline also bounds the first
read-only SET and timeout-setting calls. Transaction-local settings reset on exit. An encoder adapter must discard a cancelled consumer safely
without releasing a shared forward's permit prematurely. The wrapper does not
await a cancellation-resistant encoder after timeout. A narrow process-local
task set holds a strong reference until completion and a callback retrieves any
late exception; it has no dispatch/retry/job semantics. The model adapter owns
actual inference completion and permit release. Connection rollback
and cancellation must complete before pool reuse. No per-request durable write
is performed. Inputs are limited to 1000 characters/4096 UTF-8 bytes. Budgets
require integer types (booleans rejected); encoder output passes the existing
BGE finite/dimension/L2 contract after rejecting boolean/string elements.

All responses explicitly report incomplete ANN recall: a full candidate list
cannot establish recall. Timeout raises instead of returning an empty success.
The fixture does not establish production role ACL, grants/revocation freshness,
read-your-writes cursor, alias/entity retrieval, source-scope exact fallback,
prepared-plan behavior at scale, BGE quality, capacity, performance, deployed
routing, or product acceptance.

## Reproduce the disposable fixture check

Use only the explicitly named pre-existing isolated container, with synthetic
rows. The test executes DDL and queries inside one transaction and rolls it back.
It neither starts nor stops PostgreSQL and has no production DSN support.
Set `TMPDIR`, pytest `--basetemp`, and optionally `RKB_PG_SEARCH_EVIDENCE` to a
managed central artifact task directory.

```sh
RKB_PG_SEARCH_FIXTURE_CONTAINER=rkb-v4-fixture-20261020 \
PYTHONPATH=src python -m pytest tests/test_kaggle_pg_search.py -p no:cacheprovider
```

Verified 2026-10-20 on PostgreSQL 17.10 / pgvector 0.8.2 with four synthetic
1024-dimensional passage vectors. Actual SQL verifies active/accepted/withdrawn
filtering, zero-lexical-hit dense retrieval, hydration after withdrawal, pointer
switch, immutable passage updates, full >4000-character text/bibliography
hydration and metadata correction without projection changes. Focused unit
checks cover deadline/caller cancellation, transaction cleanup, resistant
encoder late failure observation and input/connection bounds. The sixth prepared execution has actual
EXPLAIN ANALYZE BUFFERS output. The natural tiny-table plan uses a sequential
scan; a separate diagnostic with `enable_seqscan=off` proves HNSW indexability.
This forced diagnostic is not serving-performance evidence. Both execution
plans and the transaction rollback are recorded in the managed task's
`pg/search-sql-reviewed.txt`. No BGE inference, corpus import, real GEO/Saved writes or
Kaggle runtime action was performed.
