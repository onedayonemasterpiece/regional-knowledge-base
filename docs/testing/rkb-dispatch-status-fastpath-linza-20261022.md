# M7 — WSS dispatch status fastpath on PR239 proof epoch

**Status: implemented source-only on exact PR239 HEAD; pending integration review.**
Target is `integration/rkb-native-import-20261022`, never main. Opus
alone merges/installs/tests live. The source branch has an explicit
dependency on PR239's READY proof-epoch fix
`910a3aa26999ff6945f679fdc64645f3962feab5`.
PR239's `promote` evaluation/identity fencing is unchanged.

Required product contracts: pinned v4.1 implementation at bd138b2,
owner-alignment and complete acceptance. Full PDF/DjVu pixels, figures,
facts, Story.body, continuation, Dataset Saved, remote intent, permissions
and unknown-effect reconciliation remain mandatory. An M7 WSS
component fix does NOT accept the product or permit live operations.

## Evidence and causal claim

Opus's previously recorded sanitized v107 GEO-only trace
`2ce3d3b1…` (1362ms) separates auth111ms, binding49ms,
dispatch_gate56ms, admission226ms, dispatch-to-result535ms
(including native199ms), finish_gate220ms and result-to-terminal375ms.
Windows overlap and **must never be added**. This is a single
empty-corpus observation, not a representative book-search p95.

The actual channel dispatcher previously called
`await status()` → `asyncio.to_thread(queue.channel_status)` /
SQLite read **on every dispatch iteration even after `promoted=True`**.
On the already promoted shared worker, the following
`promote(observed)` was a no-op. The new code leaves full status checks
in place when ANY of these is true: not yet promoted, legacy (no
shared admission), or outstanding fresh RESOURCE_PROBE. It skips only
the redundant promoted/shared/probe-clear read:

```python
# channel_route.dispatch, after PR239's proof-epoch source:
while True:
    if not promoted or shared is None or probe_nonce is not None:
        observed = await status()
        await promote(observed)
    if shared is not None:
        ...
```

This change removes a demonstrably redundant read-only operation.
**No live millisecond or p95 improvement is asserted.** The existing
sender **independently** rechecks fresh authenticated status
`await status()` just before `shared.mark_work_sent` and socket send.
The actual `SharedReadAdmission.next_channel_work` calls
`_snapshot` (run/owner/generation) and a current
`ReadAuthority.require` before selecting the read. Receiver still
validates every input frame/generation. Stale lease/current-run
revocation remains fail-closed. SourceReadGate and final
ReadIssuanceMiddleware synchronous issuance, ACL, durable intent,
UNKNOWN and Saved are completely unchanged.

## Actual-class test and negative acceptance

All tests use real BgeQueue, configured SharedReadAdmission,
synthetic existing ReadAuthority and Starlette TestClient; no PG,
resident BGE, Kaggle provider or new credentials.

- `tests/test_bge_dispatch_status_fastpath_linza.py` executes the
  actual post-PR239 patched source and in-memory negative-control
  reconstruction of the exact PR239 source *without M7*.
  Its instrumentation ONLY counts current queue.status callsites and
  still delegates to the real method; it never fakes promotion,
  admission or issuance. Both variants issue actual WSS ATTACH,
  READY, QUERY/QUERY_RESULT, resource release with a real typed
  authorized read. Baseline performs ≥1 promoted dispatch read,
  M7 none with no unresolved probe; both retain sender/receiver
  fresh status and actual request/result identity. A separate
  source-diff test confirms the only logical hunk and **PR239 proof
  epoch check still present**.
- `tests/test_bge_dispatch_fastpath_negative_linza.py` proves
  a live promoted WSS rejects an expired/retired run lease;
  a current public_search revocation prevents issuing new
  shared-admission work; stale previous connection generation
  is refused on reconnect; a genuine outstanding
  `RESOURCE_PROBE` blocks all QUERY dispatch until a fresh,
  exact owned nonce arrives (not a stale probe or heartbeat).
  No blind sleeps, retries or draining responses until green.
- `tests/test_bge_reconnect_grace_linza.py` is copied unchanged
  from PR232 Git blob `1347c8db7def7a2450381358e468e5b65961abb1`:
  fake-clock 80s outage resumes same pending result and
  incarnation with one forward; after grace 600s expires, no
  extra READY. This is not a second reconnection protocol.

Each CI receipt is source-only. Causal benefit must be assessed
with exact owner-authorized same-corpus public MCP before/after
queries, same frozen human-reviewed q01…q08, all failures/timeouts
included and success-only latency quantiles shown separately.
The three mandatory pinned docs and acceptance remain unaffected.

## PR/ownership mechanics

Branch base is **PR239 head** `910a3aa…`, not old unprotected
integration source. Therefore its PR-to-integration diff naturally
contains the exact still-open PR239 prerequisite as well as M7;
merge PR239 first, then merge this one after resolving base
against the then-current integration. NEVER cherry-pick the WSS
file without epoch fencing, force overwrite a changed shared file,
or merge to main directly.

Review of the next latency candidate `finish_gate/result_to_terminal`
must not change `read_issuance.py` while RKB-Риф owns final image
issuance, nor `kaggle_public_backend.py` while it is integrated
with other source/media changes. Any additional source change
requires a non-overlapping owner-scoped hunk and causal
instrumentation; otherwise report exact dependency.
