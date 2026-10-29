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

## Actual asynchronous driver verification

`tests/test_kaggle_pg_driver.py` adds three actual psycopg asynchronous driver
checks. Local execution requires `RKB_PG_DRIVER_FIXTURE_SOCKET` to name
`<managed RKB task>/pg/socket` below `/home/dev/artifacts/regional-knowledge-base`.
The task's bounded `.artifact.json` must match the project and actual task path.
Before DDL, the fixture checks the named Docker container's `rkb.data_class`
label is `test_fixture`, its network is `none`, it is running and its socket
mount matches that exact directory. It independently checks the connected
database/user and PostgreSQL/pgvector versions. The long artifact path exceeds
Linux's Unix socket address length, so the fixture opens only that directory
and connects through its short `/proc/self/fd/<directory descriptor>` path;
the descriptor stays open until all test connections close.

CI may instead set `RKB_KAGGLE_TEST_DSN` for the existing graph-postgres service.
This path requires `GITHUB_ACTIONS=true` and exact repository
`onedayonemasterpiece/regional-knowledge-base`. Parsed connection parameters
must exactly match the existing synthetic service: loopback `127.0.0.1`, port
`54329`, database `rkb_graph_test`, user `postgres` and its declared fixture-only
password. Extra connection parameters and arbitrary DSNs are rejected. The
actual database/user and PG17/pgvector 0.8.2 are checked again before schema
creation. This adds no container or service. CI execution/results must still be
reported separately from local checks.

Run these tests serially with the SQL fixture tests. Each driver test requires
`rkb_v4` to be absent, creates exactly that schema from the migration payload,
uses synthetic rows and drops only its own schema afterward. Setup errors roll
back instead of deleting a preexisting schema. There is no production DSN,
model inference, corpus data or product configuration involved.

Verified locally on 2026-10-20 using actual psycopg 3.3.6 and the isolated PG17.10 /
pgvector 0.8.2 fixture: injected encode → lexical/dense SQL → batch hydration
returned all 5017 body and 5013 bibliography characters. Two additional tests
held a real exclusive table lock through an independent connection, observed
the serving backend in `pg_stat_activity` waiting on `Lock`, and exercised
respectively the search deadline and caller cancellation. Both returned the
proper exception, left the serving connection IDLE, restored transaction-local
statement/HNSW settings, allowed a subsequent `SELECT 42`, and successfully
searched again after releasing the lock. The combined SQL/unit/driver run had
11 passing tests. Structured local evidence is in the managed task directory's
`pg/driver-tests.txt`. No remote GitHub Actions result is implied.

## Server/client deadline race

CI run `39415796227` at source `58792b1` exposed the server's integer-millisecond
`statement_timeout` expiring before the asyncio absolute timer on Python 3.13.
The search boundary now translates that exact PostgreSQL statement-timeout
diagnostic into the promised `TimeoutError` after transaction rollback, only
when the error arrived within the one-millisecond rounding window of this
request's deadline. It records the receipt time before rollback and matches the
same exception afterward. Early statement cancellations, PostgreSQL's explicit
user-request cancellation diagnostic and other SQL errors retain their types;
caller task cancellation remains `CancelledError`.

A deterministic focused regression makes the synthetic server response win
before the event loop can run its timer callback, and checks both early and
near-deadline unrelated cancellations remain unchanged. The subsequent local
SQL/unit/real-driver run passed 15 tests, including the real locked-query
rollback/settings/reuse checks. Evidence: `pg/timeout-race-tests.txt` in the
managed task directory. This local Python 3.14 check does not imply a new remote
Python 3.12/3.13 CI result.

## Bounded source filters (compatible expansion)

`fused_search(..., filters=QueryFilters(...))` now accepts 1–32 distinct UUID
source IDs and the sole implemented kind `source`. Other kinds, invalid UUIDs,
empty/oversized source lists and duplicate UUID identities fail before inference
or SQL. This adapter validates UUID syntax in addition to the wire contract's
identifier shape. An omitted filter, empty `QueryFilters`, or `kinds=('source',)`
without source IDs retains the existing unscoped SQL path.

Immutable migration `sql/kaggle/003_search_scope.sql` adds distinct invoker-rights
functions; it does not change `001_search.sql` or existing function identities.
Apply it through the ordered migration authority after `002`, before routing
scoped requests to this executor. Missing scoped functions raise an execution
error; the executor never silently retries without the requested scope.
Lexical, dense and hydration SQL each independently constrain the selected
source IDs and recheck accepted/non-withdrawn/active-revision eligibility.
Only the bounded caller-provided source list crosses the SQL boundary; the
application never enumerates corpus scope.

Scoped dense retrieval materializes the eligible source subset, then computes
exact cosine distance there. The source/revision index supports subset selection.
The plain distance expression remains the ordering criterion; no distance
threshold or score transform replaces it. Large selected books can still exceed
the single existing total deadline (at most two seconds), producing a timeout.
Unscoped retrieval keeps its existing HNSW path. Scoped results remain explicitly
incomplete with `bounded_candidate_fusion_unverified`: exact scoped dense top-N
does not establish full fused-search recall from bounded branch candidates.
Unscoped results retain `bounded_ann_recall_unverified`.

Validation on 2026-10-20: `tests/test_kaggle_search_scope.py` passed 12 tests using
the existing guarded disposable PG17/pgvector fixture, including synthetic mixed
accepted, withdrawn, unaccepted, inactive and out-of-scope sources; independent
dense hits; current hydration withdrawal/acceptance/revision checks; and the real
Python executor. Existing search tests passed separately with fixture opt-ins
skipped. No model/provider call, corpus import or deployment was performed.
This slice adds filtering only: public QUERY wiring, canonical person resolution,
shared fair admission and the delayed-output authority gate remain separate work.

## Normal Query statement guard — 2026-10-29

[Product approval](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1793287432139079)
at15:23:52 UTC permits `statement_timeout=60000` for each executing normal-search
SQL statement. It does not limit queue wait, tokenization, model execution or
total request time, and does not change exact/acceptance/health deadlines.
The existing transaction sets the guard before isolation/read-only setup;
the existing per-statement settings retain it before each frontier, lexical,
dense, hydration and dependency query. This adds one setup SQL roundtrip to
normal Query, not a timer, retry or parallel execution path.

Only a real PostgreSQL statement-timeout cancellation produces
`unavailable/native_query_stalled`. Its fixed log contains request ID and SQLSTATE,
never SQL, query text or arbitrary exception text. Transaction rollback and the
pool lease finish before the existing WSS resource-release path makes the slot
available. The existing Slack error renderer asks the person to retry; the
system does not automatically repeat the query. Several individual statements
can cumulatively take longer than60s. A stalled model or connection acquisition
is outside this SQL-only guard.

The distinguishing disposable-PG test scales only its selected SQL statement
to20ms and executes `pg_sleep(1)` to obtain an actual57014 cancellation without
waiting60s. It verifies rollback-to-IDLE, resource release and a successful next
query on the same WSS session and model load. Synthetic verification is not a
live SQL-stall diagnosis or deployment acceptance.
