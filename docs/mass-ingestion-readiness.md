# Mass-ingestion readiness

Updated: 2026-10-19. This is the human-readable companion to [.devcoveer/requirements.json](../.devcoveer/requirements.json). The nine protected requirement IDs and numeric gates are preserved. The historical ID `CORE-SUPABASE-CAPACITY` now means measured storage headroom; it does not require Supabase.

The target is one PostgreSQL/pgvector working corpus and BGE in Kaggle, a bounded durable DevCoveer control journal, direct authenticated bidirectional WSS through the existing broker, fresh MCP book imports and a single persisted research workflow. E5 is removed from target runtime/storage/fallback/tokenizer requirements. Text and bibliography across active accepted books have no per-book owner ACL; current withdrawal/revision and pixel/original/private scopes remain enforced.

## Applicable release plan

Use the [single coding-agent handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md) and [new acceptance matrix](testing/rkb-scalable-kaggle-acceptance-20261019.md). The [import/retrieval test plan](testing.md) retains its meaningful source-grounded cases. Record actual dated results in the [testing journal](testing-journal.md). Documentation, ready-vector counts and an installed PostgreSQL instance do not establish readiness.

## Protected gates

| Gate | Required result |
|---|---|
| Scale | ≥100 book-equivalent sources and ≥100,000 active chunks, or a larger real corpus; engineering readiness benchmark, not product limit. |
| Storage | ≤80% steady and ≤90% largest-import/snapshot/restore/cutover peak of each applicable budget, including heap/vectors/indexes/TOAST/WAL/temp/system and backup overlap; separately bounded control/recovery/cache. |
| Dense quality | ≥30 held-out source-grounded fact families; BGE-only Hit@5≥85%, Hit@10≥90%; critical RU-query→DE-source Hit@10≥85%. |
| Ablations | BGE-only, lexical-only and configured fusion reported separately; lexical does not prefilter dense candidates. E5 history is not a target release gate. |
| Latency | Warm backend p95≤1000ms; public MCP p95≤1500ms and hard evidence deadline2000ms, with all attempts/errors/degraded outcomes accounted for. |
| Chunk fidelity | Semantic passages around256 pinned BGE tokens; exact final augmented input including prefix≤512, no silent truncation. Printed text, captions, notes and provenance preserved. |
| Per-book | Natural/paraphrased and relevant cross-language evidence queries on the exact accepted revision. Finalization/counts alone are insufficient. |
| Operational reliability | Durable input and exact accepted model/human result, WSS warm reconnect/ACK, one queue, fenced replay, verified snapshot+tail and explicit failure-domain/RPO evidence. |
| Story and feature preservation | Facts/dates/observations/entities/geography survive zero-story input; coherent episodes and evidence-linked variants, no invented narrative; real multiuser role/request/voice contracts preserved. |

## Full protected contract

### CORE-MASS-BOOK-INGESTION

Regional Knowledge Base is intended for routine mass ingestion of books, not isolated successful demos. The system is mass-ingestion-ready only when production-like capacity, retrieval quality, latency, import-quality and operational-reliability gates all pass.

- The minimum engineering scale benchmark covers at least 100 book-equivalent sources and 100,000 active chunks, or a larger real corpus when available; this is a readiness benchmark, not a product limit.
- Routine imports are resumable and idempotent, use bounded queues/backpressure, do not require keeping a ChatGPT turn open, and one failed/superseded revision cannot block unrelated books.
- A book is not considered accepted merely because text or vectors were written; its active revision must pass the required import and retrieval acceptance.
- Accepted exact source-analysis payloads, human contributions and typed resolved mutations are durable before application; restart/replay cannot silently replace a previously accepted model result by a new inference or lose a confirmed effect. Snapshot-plus-tail recovery and its stated failure domains are verified before production cutover.

### CORE-SUPABASE-CAPACITY

Mass ingestion must remain within measured working-storage capacity with explicit headroom. The target architecture uses one PostgreSQL/pgvector working corpus in Kaggle and a bounded durable control journal on DevCoveer; this stable requirement ID retains its historical name but does not require Supabase.

- Full source binaries, scans and BGE weights remain outside the control SQLite database. Account for PostgreSQL heap, vectors, indexes, TOAST, WAL, system overhead, temp files, staged imports, restore/snapshot overlap, and separately bounded control journal, recovery objects and caches.
- At the production-like scale benchmark, measured steady-state usage is at most 80% of each applicable storage budget and the largest-source reimport, snapshot/restore and cutover peak is at most 90%; document the actual provider limits and reserved headroom.
- Capacity is established from measured database, volume and object-storage usage/growth, not row-count or raw-vector arithmetic alone.
- Corpus completeness, evidence provenance and retrieval quality are not silently reduced merely to fit quota; if the gate fails, storage/index representation is optimized and remeasured.
- Compaction preserves every accepted command/payload still needed by the oldest retained verified recovery baseline or unfinished consumer; resource pressure applies bounded backpressure rather than deleting unprotected accepted work.

### CORE-DENSE-RETRIEVAL-QUALITY

Dense vector retrieval must independently provide high-quality evidence discovery; lexical/FTS retrieval is a bounded complementary signal and must not mask weak vector search.

- The held-out source-grounded retrieval gate contains at least 30 distinct fact families and reports known-evidence Hit@5, Hit@10 and language slices without using lexical retrieval to prefilter dense candidates.
- BGE-only known-evidence Hit@5 is at least 85% and Hit@10 at least 90% on the accepted held-out benchmark.
- Every critical language slice that is part of the product, including Russian queries over German sources while German books are in scope, reaches at least 85% known-evidence Hit@10.
- Release acceptance always reports BGE-only, lexical-only and configured-fusion ablations separately; one branch cannot hide another branch's degradation. E5 is removed from the target runtime, storage, fallback and required gates; old E5 measurements remain historical evidence only.
- Retrieval metrics use source-grounded evidence and held-out fact families and are not mislabeled as generated-answer accuracy.

### CORE-AGENT-SEARCH-LATENCY

Knowledge search must have predictable low latency suitable for repeated agent tool calls, with bounded degradation rather than hidden long waits.

- On a production-like corpus and representative concurrency, warm backend ranked-candidate retrieval has p95 <= 1000 ms.
- Public MCP search has p95 <= 1500 ms and a hard interaction deadline of 2000 ms for the evidence-search path.
- No normal retrieval branch introduces an implicit multi-second or 10-second wait; unavailable slow branches either complete inside the same budget or return an explicit degraded state.
- Latency acceptance reports percentiles, not only averages or best-case single queries.

### CORE-BOOK-IMPORT-QUALITY

Each imported book must preserve source fidelity and be chunked for retrieval rather than for page layout or transport convenience.

- Scanner/service material that is not part of the book is excluded while printed book material and document->page->region provenance are preserved.
- Retrieval chunks are coherent semantic passages around the measured approximately 256-token operating target; page boundaries are provenance and are not default embedding boundaries.
- The exact final embedding input, including any prefix, is counted with the pinned deployed BGE tokenizer; exceeding its current 512-token hard limit blocks acceptance rather than silently truncating. Increasing this limit requires a measured retrieval/import-quality change gate; an E5 tokenizer is not a target dependency.
- Before a newly imported or reprocessed book is considered accepted, source-grounded natural-query retrieval checks run against that exact active revision, including paraphrased queries and cross-language queries when relevant.
- Old book corpus/chunks/vectors/import receipts are not migrated into the target. Books are freshly imported from verified originals; activation atomically schedules persisted research that creates or updates disagreement cases, without waiting for future expert consensus.
- Source-bound factual assertions, exact evidence, people and roles, organizations, event dates and calendar precision, quantitative observations and units, historical toponyms/geography and disagreement candidates are retained even when a fragment yields no narrative story. Import and accepted-source re-extraction share this contract; filtering weak stories must not drop factual knowledge.
- Story extraction separates facts, coherent episodes, editorial dossiers and publication variants in the existing registry. It joins episodes across page/window boundaries, permits zero stories and invents no facts, motives, dialogue or causality. A small source-grounded blind comparison verifies narrative usefulness, false acceptance/refusal and preservation before release; no story quota, mandatory length or second model judge is required.

### CORE-RETRIEVAL-CHANGE-GATE

Changes that can alter search quality, scale or latency must rerun the relevant mass-ingestion gates before production rollout.

- Changes to chunking, embedding model/space, vector indexes/search, fusion, lexical query formulation, authorization scope or storage placement rerun the applicable capacity, dense-quality and latency acceptance.
- A green unit test suite or vector-readiness count alone is never sufficient evidence of mass-ingestion readiness.
- Acceptance results identify the exact corpus/revision/model/search configuration so improvements cannot be inferred from incomparable benchmarks.

### CORE-REQUIREMENTS-GUARD

These mass-ingestion, capacity, retrieval-quality and latency requirements are protected product requirements and must not be silently weakened or reinterpreted by implementation work.

- DevCoveer exposes this manifest automatically to implementation and review tasks.
- If an implementation conflicts with a critical requirement, the conflict is surfaced instead of normalized into a weaker target.
- Intentional requirement changes use requirements_update with an explicit owner authorization reference.

### CORE-UNIFIED-EXPERT-ACCESS

Experts use the existing Regional Knowledge Base MCP resource directly from any compatible AI client or through Slack with dot as a replaceable executor. Both paths resolve the same canonical person and enforce current capabilities and applicable publication, case and artifact permissions server-side; no separate expert MCP service, endpoint, corpus or identity registry is required.

- Personal resource-bound OAuth identifies the actual expert, never an owner invite or a client-supplied actor. Supported clients are verified individually; naming ChatGPT or Grok is not a compatibility result.
- Slack resolves the verified provider account to the same existing RKB person UUID. A dedicated executor service uses the same MCP endpoint and a durable request/route/generation-bound authority; it never inherits dot's or the owner's unrelated credentials.
- Effective permissions are the intersection of the person's current capabilities, case/artifact scope and delivery audience; service execution is additionally limited by its service grant and request fence. Text search and bibliography across active accepted books have no per-book owner ACL; current revision/withdrawal and private pixels/original grants still apply. Import and participant administration remain owner-only and unavailable through delegated expert requests.
- Concurrent actors, cross-route access, lease/link/grant revocation, delayed results and direct calls to forbidden tools are checked on the server, regardless of catalog visibility.
- Acceptance demonstrates direct personal research and Slack-delegated research on the same authorized case/revision, including a real non-owner, denial and revocation; documentation and green component tests alone do not establish live support.
- The redesign preserves import, story editing/search/history/variants, dates/calendar, people and organizations, observations/comparison, georeferencing and historical layers, disagreement cases/contributions/conclusions, and private drafts with current role and audience rules. Fresh book reimport is not authorization to delete irreplaceable manual work or rebuild another product's canonical POI/map catalogue.
- Mira in Projects Hub remains the preferred voice client through the existing shared live-interaction and typed RKB adapter. Real delegated identity, progressive research capabilities, durable pending/result/receipt continuation and screen evidence are implemented and verified with more than one real user; a search-only adapter or emulator success is not full integration acceptance.

### CORE-REQUEST-LOAD-MANAGEMENT

Regional Knowledge Base uses one durable user-request lifecycle across clients, preserving per-dialog context and order, equal-person fairness, measured resource admission and honest arithmetic ETA. Ordinary load queues accepted questions rather than discarding them; imports retain capacity for at least one supported search.

- Slack and supported direct/Mira clients use the existing request authority and MCP resource. Verified canonical people share fairness across clients; client-supplied actors or dialog IDs cannot bypass access checks.
- Accepted requests survive restart and retain a final result/delivery outcome in the original dialog. Later dialog turns use completed prior context and cannot overtake an undelivered or uncertain predecessor; other dialogs continue.
- Fair selection and measured stage permits apply atomically to every claim/resume/wake path, including an explicit request ID. Control resources, external Kaggle PostgreSQL/BGE, proof/download and vector dependencies are distinguished; capacity is calibrated and adapts to measured resource increases/decreases. E5 is not a target stage.
- Forecasts use measured durations and the actual fair order, not an LLM or invented concurrency limits. ETA at most ten seconds causes no waiting message; longer ETA and material increases have durable coalesced notices only for the person's nearest request. Ready final answers take precedence.
- Ordinary capacity waits do not consume outage retry budgets, expire accepted inputs or ask users to resend. Proven external unavailability produces one honest terminal response; unknown provider effects reconcile without blind resend and poison stages remain recoverable.
- Import stages cooperate at existing safe durable boundaries and cannot consume the measured search reserve. Required BGE publication, source fidelity, prior active revision and Kaggle-only inference remain protected; no process kill or unsafe transaction pause.
- The existing evidence-search deadline and latency/quality gates remain unchanged. Report queue wait and time to final answer separately; do not hide a long HTTP search behind the durable question queue.
- Acceptance includes cross-client/dialog fairness, restart and stale-generation boundaries, notification thresholds, real resource calibration/resize and import+search concurrency, actual second-client result/ACK readback and held-out ETA error. A green unit suite or pending response alone does not prove product acceptance.
- Kaggle maintains direct authenticated outbound bidirectional persistent communication with the existing DevCoveer broker. One durable queue, request/claim IDs, writer fence and result hashes govern both directions. ACK follows durable acceptance; reconnect preserves the loaded worker; stale claims do not kill it. Duplicate connections cannot create another dispatcher. Verify provider reachability, reconnect and mixed load.

## Historical evidence boundary

The text below preserves the dated 2026-10-08 evidence and its then-current conclusions. Its references to Supabase, optional E5, compact migration and “remaining proof” describe that checkpoint. They are not the target deployment plan or a claim about today's production. Historical metric values have not been rewritten. The new PostgreSQL/WSS/story pipeline requires its own applicable acceptance, while these reports remain useful evidence of known retrieval risks.

## Historical production stress: dense-quality gate FAIL (2026-10-08)

The live RLS-v5 retrieval stress on the **actual accepted source revisions**
found all 64 original proof passages in active chunks, but the production
BGE-only held-out known-proof Hit@5/Hit@10 was only
**25/32 = 78.13% / 27/32 = 84.38%** with frozen FP32 query vectors
(versus **28/32 = 87.50% / 29/32 = 90.63%** for the *experimental*
t256 vector matrix). The same 32 cases using the local INT8 query encoder
on the active passages gave 78.13% / 87.50%; FP32 alone does not recover
the required recall. All 384 source-grounded query variants were
re-evaluated against the exact production v5 release, with no lexical
boost, aliases or neighbor expansion. Adding/excluding the third real
book changed **zero** of the 384 target evidence ranks. Long instruction
wrappers and unrelated book titles still cause substantial retrieval loss.

Sustained live encoder tests also recorded seven HTTP failures and one
connection timeout in the first 128 attempted requests at concurrency 2
under high shared-host load; the run deliberately aborted and was **not**
counted as a completed successful benchmark. A separate 384-query search
using precomputed vectors completed, confirming distinct encoding versus
retrieval concerns. The 20 experimental boundary-pair cases recovered
both adjoining evidence chunks within Top-5 only 40% of the time.

**Release decision from this test: mass book filling is not approved.**
No later passing production acceptance is established by this document.
Documentation cleanup, a newer code SHA or ready vector counts do not replace
a dated benchmark on identified active revisions. A
non-destructive staged, semantically coherent ~256-token re-chunk/re-embed
trial and BGE query-sidecar resilience tests are the next retrieval gates.
The existing 100k compact-halfvec capacity numbers are synthetic and do
not indicate a deployed compact index: on 2026-10-08 the Supabase BGE column was
`vector(1024)` with ordinary HNSW. The actual Supabase DB was 75.8 MB
against the project's 500 MB *planning* quota on 2026-10-08. Preserve
the full-scale 100k/source headroom and latency requirements unchanged.

See [full dated stress report](reports/expanded-retrieval-stress-20261008.md).
This latest live result supersedes any inference of **production acceptance**
from the earlier frozen t256 experiment; the historical experiment itself
remains useful design evidence.

## Historical experimental evidence and remaining proof

The frozen two-book t256 experiment summarized by the 2026-10-08 stress report
supports the small-passage BGE design decision; it is not the active-production result above.
On the frozen held-out original questions, t256+BGE reaches Hit@5 **87.5%** and
Hit@10 **90.625%**. The held-out Russian-query -> German-source slice reaches
Hit@5/Hit@10 **91.67% / 91.67%**. E5 is materially weaker on the same corpus and
is therefore no longer a default publication requirement.

The same stress deliberately exposes remaining weaknesses rather than hiding
them. Verbose instruction wrappers reduce retrieval, and adding an unrelated
other-book title can poison the query embedding badly. A single cosine threshold
also does not reliably separate answerable from out-of-corpus questions.
Retrieval clients should therefore send the semantic question cleanly, use
document scope separately from query text, and verify returned evidence.

Compact pgvector storage has also been measured, not merely estimated. At
100,000 synthetic rows backed by real BGE vectors, halfvec(1024) plus a
binary-quantized HNSW candidate index and halfvec rerank projects the migrated
database to about **345 MB** steady state and **352 MB** with a 2,000-row pending
revision, against the 500 MB planning quota used for that experiment. Candidate+rerank p95 is
about **35-42 ms** in that temporary-table stress. This passes the storage
headroom gate for the benchmark shape, but still requires production migration
acceptance.

That does **not** yet mean the project is mass-ingestion-ready. The remaining
critical proof includes:

- production migration/acceptance of the compact BGE vector plane;
- end-to-end p95 MCP/backend latency, including query embedding, at production-like
  scale and concurrency;
- repeatable per-book active-revision retrieval acceptance on real imports,
  including any newly activated replacement revision;
- continued quality after corpus growth beyond the historical two-book design corpus.

Until these gates pass together, treat the system as retrieval-hardening /
mass-ingestion-preparation rather than approved for unattended bulk filling.

