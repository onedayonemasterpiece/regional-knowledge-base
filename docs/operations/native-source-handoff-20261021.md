# Native source continuation — source-only handoff, 2026-10-21

Base: a120c9cf37007eef99f165a68cb2f03869640ba6.
Working handoff: src_057176926f8423be3d5c39a0. This supersedes its initial 14-file phase1 snapshot.
No deployment, real-book download, provider mutation, or live Kaggle operation was performed.

## Implemented slice
Explicit schema14 native PG batch/seal/materialize/activate using the existing acceptance/outcome journal, writer ownership and resident BGE binding. Legacy SQL001–012 and <=32-page v4 codec are unchanged.
Selected page-image provenance is verified and model transcription stays distinguished from native text.
Repeated entity mentions and normalized alias proofs survive across batches; corrected transcription refreshes dependent graph projection while accepted batch bytes remain immutable.
Fresh materialized notebook imports compiler/bindings from its own packaged tree; rights dependency now included. Unknown nested semantic fields fail closed.

## Evidence and limits
- Initial two continuation regressions failed before the fix: op_f2b71c50aff6288f7f89bc8f.
- Focused source tests: 102 PASS, 1 catalog fixture skip; op_ea7894d18b68b90f623fd68b. Includes fresh-notebook packaging and three graph continuation regressions.
- Final broad source suite: 205 PASS, 17 subtests PASS, 1 catalog opt-in skip, exit0; job_ae00935d069e061763cf2e43. Includes valid nonempty illustration refusal; focused 11 compiler tests also PASS: op_c080a453889cbcbbaf89fb58.
- Real disposable PG17.10/pgvector0.8.2: 61 PASS, 1 catalog skip, job_8a164ce6753ad85cfbee31eb. Includes five SQL013 and seven SQL014 tests, 174 synthetic pages, exact replay, atomic rollback/retry, source text correction, public activation/search and multi-mention/alias evidence. Vectors are synthetic, not live BGE.
- Separate exact bootstrap-role catalog fixture: 49 PASS, no skip, job_1854c93cd6870ff488f4c51c, explicit RKB_CATALOG_FIXTURE_TIMEOUT_SECONDS=10. Production and test-default timeout remain 2 seconds.
- IMPORTANT: default 2-second catalog readiness probe failed with database_probe_deadline on the 0.5-CPU fixture (job_c6ca6e07ab54a74e12cd2f4f). Structural PASS is NOT production readiness latency PASS. Recheck default-budget readiness on target/normal CI resources before rollout; do not substitute this for public-search latency or mass-ingestion gates.
- A prior catalog fixture failed during temporary init readiness; no product result inferred. All fixture containers stopped/removed (exit0); each used network=none, memory256MiB/no swap, 0.5CPU, private socket, one pytest; MemAvailable was >3,200,000KiB. Own transient scripts removed.
- Real Kaggle resident model, actual original fidelity, Telegram runtime client/lease, persistence/Dataset and complete-book acceptance: NOT RUN. No private source locator read.

## Not complete: illustration/media activation
Valid nonempty illustration input explicitly raises native_illustration_activation_unavailable, before tokenizer/model; it is not silently discarded. Media/article linkage unsupported by this slice likewise fails closed. A 174-page text fixture is not evidence of complete illustrated-book parity.

Next bounded slice: read existing ArchiveStage contract; preserve existing issued-image/archive receipt authority; extend versioned image manifest/compiler for illustration metadata, source bbox, display rotation, printed caption versus model observation, passage/POI linkage; additive SQL projection+origin verification+atomic activation+existing gated public read/withdrawal. Add forgery/replay/correction/partial-activation tests. Do not invent transport or change frozen accepted v4 bytes. Illustration-only chunks need an explicit supported contract; articles remain separately unsupported.

## Integration
Apply the final captured full patch against exact base, not just git staged state (initial staging contains only phase1). Review/rebase onto operator main separately; do not overwrite operator WIP.
Operator owns final deployed binding and live gates. Current source deliverable is a tested bounded slice, not an all-product/all-checks PASS.
