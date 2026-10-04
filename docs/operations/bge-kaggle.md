# BGE main tier

The E5 encoder remains an independent loopback service. MCP imports neither Torch
nor Transformers. BGE inference runs in one private Kaggle **CPU** script. The
worker only receives a run-bound broker credential, never database/object-store
credentials. No paid inference fallback is configured.

## Contract and installation

Apply `sql/011_bge_rankings.sql` only after `verify_bge_migration.py` passes on the
isolated PostgreSQL fixture. `apply_bge_migration.py` requires its exact SHA/proof
and checks that legacy/E5 rows survive. Rollback removes only BGE resources.

Space: `bge-m3:5617a9f:t211-tr5161:cls-l2-512:q1-d1:v1`; model `BAAI/bge-m3`,
revision `5617a9f61b028005a4858fdac845db406aefb181`; 1024 dimensions. Kaggle CPU
uses Torch 2.11.0+cpu, Transformers 5.16.1, Tokenizers 0.23.1 and
Huggingface Hub 1.29.0. The worker refuses an incompatible runtime. Identical
query/document preprocessing: no prefix, tokenizer truncation at 512 tokens,
FP32 CLS pooling and L2 normalization, batch 1, four intra-op/one inter-op threads.
Long chunks can lose tail information; this contract does not claim the model's
full 8192-token context. Source SHA256 and active revision must match at ranking.

Normal imports automatically enqueue/install BGE document vectors after activation.
The [automatic indexing reconciler](automatic-indexing.md) uses active missing
vectors and this existing durable queue; no user/operator backfill is required.
`backfill_bge.py` remains an explicit maintenance tool, not an import step.
Owner RLS, source-byte hash, active revision, model revision and exact space are
rechecked before installation. Counts describe indexed chunks, not source-book
completeness.

## Queue and services

`regional-knowledge-bge-controller.service` runs the system Python Kaggle SDK and
`bge_kaggle_controller.py`; the shared `/home/dev/.env` supplies only Kaggle's
username/key. Do not duplicate credentials in project environment files.

`RKB_BGE_QUEUE_PATH` names the durable private SQLite database outside Kaggle.
Parent directories are 0700; queue and generated private launch material are
0600. `RKB_BGE_BROKER_URL` must be HTTPS. The consuming MCP process and controller
use the same database. Keep it across deployments; do not replace it on restart.
Run credentials/private launch sources stay in runtime state, not Git or reports.

`RKB_BGE_ENABLED=1` enables main retrieval; `RKB_BGE_WARM_MODE` selects the accepted
ranking (`bge_lexical`, `e5_bge_lexical`, `bge` or `e5_bge`). Vectors remain in
separate 384/1024-d spaces. Fusion uses RRF with k=60 and independent branch depth
100; no cross-model cosine arithmetic. Disable the flag to serve E5 immediately
while investigating BGE. Missing vectors are reconciled automatically; search
uses E5/lexical until the authorized active BGE coverage is complete.

On cold demand, search returns normal E5 evidence plus `main_state=starting` and
an actor-bound `main_job_id`. Reissue the same query with that ID to obtain its
main result. Both full `search` and Live `knowledge_search` accept it. Completed
jobs can be replayed without inference. A different query or actor cannot reuse
the job. Warm calls wait at most 10 seconds for BGE, then return E5 with pending
state. Queue capacity 256; document producers hold at most 64 pending jobs;
interactive queries are claimed before document jobs. Fast and main evidence
both undergo ordinary ACL checks and fetch authorization. Revocation remains
effective even if a vector job completed earlier.

## Lifecycle and degraded modes

SQLite transactions atomically create one current run. Dispatch is claimed once,
with an immutable notebook slug derived from the run ID. After a lost response,
the controller probes that notebook and never repeats an unknown save/version.
Provider failure preserves jobs; failed starts have a 60-second cooldown.
An ambiguous launch that cannot be reconciled expires after 15 minutes rather
than risking a duplicate launch.

Useful requests/claims/completions renew a 30-minute lease. Heartbeat every 20
seconds only reports liveness/resources and never renews that lease. A ready
worker missing for 120 seconds is fenced. Claim leases last 180 seconds; unfinished
jobs return to pending. A result must match run, claim nonce, space and normalized
1024-d vector; duplicates are idempotent and stale results cannot overwrite data.

At 10h45m, one successor warms while the current worker serves. On readiness,
the old worker drains only its claimed job. At 11h, run credentials expire
unconditionally. At most one serving and one warming run exist; the old process
may take up to one heartbeat interval to exit after being fenced. Provider
compute may briefly overlap during drain, but a fenced worker cannot claim jobs.
Idle expiry likewise revokes the credential. Stopping the controller alone does
not stop a serving worker; disabling the main tier preserves E5 independently.

The narrow `/bge-worker/heartbeat`, `/claim`, `/complete` routes accept only
run-bound credentials, fixed contract and bounded payloads. They offer no arbitrary
execution or general user tool access. Do not expose worker credentials to MCP
clients. Queue status is safe to inspect locally; job texts/results are private.

Acceptance tooling: `multilingual_benchmark.py` for parallel query encodings and
seven ablations; `accept_bge.py` for actual 1/5/10 search/fetch concurrency. Write
all private outputs to a managed retained directory. Public reports contain
aggregates and evidence checksums, not texts, source IDs or vectors.

`smoke_bge_http.py` verifies public OAuth MCP search/fetch and revokes its temporary
operator test token family. `verify_bge_lifecycle.py --execute` performs controlled
tests on this project's real queue/notebooks: timestamp acceleration for idle/
lifetime boundaries, claimed-job fencing and temporary controller suspension for
provider/start-failure injection. Run only with a ready empty queue during an
operator acceptance window; it intentionally rotates the current worker. It
always restores the controller. These are injected failures, not measurements of
a natural provider outage or an eleven-hour wall-clock soak.
