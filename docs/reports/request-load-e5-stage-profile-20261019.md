# E5 stage characterization — 19 October 2026

The measured passage encoder can occupy the single native worker for roughly
1–2 seconds per legal batch4. Query priority cannot interrupt that active batch.
This is a concrete latency risk for the protected evidence-search budget.
The measurements do not establish a safe request concurrency/resource envelope.

All calls used the already-running pinned E5 service. No runtime configuration,
model assets, vector publication or corpus changed; no second encoder or BGE
inference was started. Query/token-count inputs were synthetic. HTTP vector
responses were checked for expected space, dimension384, finiteness and L2 norm;
no retrieval relevance/quality claim follows from those checks.

The live source file's `PinnedEncoder.encode` and `token_counts` AST hashes match
candidate3948cd9. The process's loaded module was not introspected. Its queue is
the older deployed implementation: these are native-stage measurements, not a
live acceptance of the candidate's query-first/token-count serialization. The
cgroup reports CPU quota100000/100000 and MemoryMax2GiB. Host load was not isolated.

| Input class | Successful samples | Median wall time | Maximum wall time |
| --- | ---: | ---: | ---: |
| Short query,24 characters/11 untruncated E5 tokens | 20 | 11.9ms | 53.9ms |
| Long query,6600 characters/1656 untruncated tokens, query cap512 | 20 | 348.4ms | 745.0ms |
| Passage batch4,419 tokens per passage | 20 | 1199.5ms | 1714.1ms |
| Untruncated token-count batch32,4400 characters per item | 20 | 96.3ms | 214.6ms |

An earlier passage4 diagnostic used a2-second client timeout. Its **first call
timed out; zero successful samples**. The native journal separately recorded
completion in2.00964 seconds. The same service remained ready/idle afterward;
there was no observed restart, OOM or outage. That negative result remains retained.

The later20+20 passage/count characterization used the actual existing
`LocalE5Embedder`3.2-second client timeout, for the distinct purpose of measuring
the supported native contract. It does not replace the earlier2-second failure
or prove the protected search deadline. Final health showed the same process,
one encoder, ready, idle and no queued work. Maximum observed post-call RSS was
1,235,132,416 bytes; final cgroup memory.current was1,269,170,176 bytes. These are
different measurements and must not be added together. Earlier query sampling
recorded RSS every10ms; later import/count RSS was sampled at call boundaries.

The [aggregate JSON](request-load-e5-stage-profile-20261019.json) records input and
private-evidence hashes, exact model space, resource limits and sample counts.
Raw timing/resource snapshots, diagnostic failure and narrow native journal
extract remain in the managed retained implementation artifacts. No private
source text, credentials, model assets or human request metadata are published.

Next acceptance needs mixed search/import runs on the exact candidate, useful
authorized evidence, SQLite/vector/writer and other import-stage measurements,
whole-request/final-delivery samples, resource resize and held-out ETA checks.
The import grouping/pins must remain unchanged: E5 is batch sensitive, so
splitting batch4 into batch1 would require a separate quality/compatibility
decision and cannot be treated as a scheduling-only fix. No quality threshold,
latency gate or protected requirement was changed; LOAD-CALIBRATION remains open.
