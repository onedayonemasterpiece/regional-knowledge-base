# RKB Dev dispatch admission — redundant promoted-status read

**2026-10-22: INERT PATCH / test-only diagnostic, not applied to production.**
Opus owns shared `bge_channel.py` in PR239; any runtime hunk belongs to him.

## Actual one-trace evidence and precise scope

Sanitized v107 GEO-only trace `2ce3d3b1…`: auth 111ms, binding 49ms,
dispatch_gate 56ms, **admission 226ms**, dispatch→result 535ms
(including native199), finish_gate 220ms, result→terminal 375ms,
total1362ms. Phases partially overlap: **do not sum**. This is one
empty-corpus query, NOT a corpus-backed p95 or product quality result.

`channel_route.dispatch()` currently awaits `status()` on **every**
poll before `promote(observed)`, even when `promoted=True`. In that case
`promote` is by definition a no-op (`if ready and not promoted`). This
is an extra SQLite `queue.channel_status` read via `asyncio.to_thread`.

The actual worker send is **still fenced**:
`SharedReadAdmission.next_channel_work` reads current owner/generation
via `_snapshot`, independently rechecks `ReadAuthority.require` and
reserves bounded admission; `sender` independently performs
`await status()` **immediately before** `mark_work_sent` and socket
send; `receiver` validates current generation on each incoming frame.
No authorization, lifecycle, final output or durability check changes.

## Exact inert review hunk — owner applies, not this PR

```diff
diff --git a/src/regional_knowledge/bge_channel.py b/src/regional_knowledge/bge_channel.py
--- a/src/regional_knowledge/bge_channel.py
+++ b/src/regional_knowledge/bge_channel.py
@@
             async def dispatch():
                 nonlocal last_work, cancelled_work
                 resume_pending = True
                 while True:
-                    observed = await status()
-                    await promote(observed)
+                    if not promoted or shared is None or probe_nonce is not None:
+                        observed = await status()
+                        await promote(observed)
                     if shared is not None:
```

**When not yet promoted, in legacy nonshared mode, or while a fresh resource probe is outstanding**, the periodic `status()` validation is unchanged; the duplicate check is removed only for already-promoted shared reads whose loop actually calls fenced `next_channel_work`.
On promotion/state change the receiver and existing channel generation
fences remain. Opus must review against **his current PR239 HEAD**
and preserve its proof-epoch race correction. Do not force/cherry-pick
against changed source without reviewing the exact hunk.

## Executable bounded before/after

`tests/test_bge_dispatch_status_fastpath_linza.py` uses the real
`BgeQueue` and established `shared_channel_run` OAuth fixture.
It compiles exact current source into two **disposable in-memory
modules** with test-only status callsite counters; candidate differs
only in the inert hunk above. Runs the actual Starlette/TestClient WSS
ATTACH, real shared resource baseline/READY, authorized read admission,
on-wire QUERY, QUERY_RESULT and RELEASE, with the same current sender
check. In the original, ≥1 promoted-dispatch `status()`; in the
candidate 0, but sender+receiver still call and enforce true current
status. Any unexpected source hunk drift fails closed.

The test measures begin_read→QUERY wire elapsed time only as a
**synthetic sanity metric, not live p95**. A lower status-call count is
proof of redundant I/O removal, not proof of a repeatable E2E speed
percentage. Review must keep successful hybrid answer quality,
revocation/generation/lease rechecks, current final ASGI issuance,
UNKNOWN and remote Saved; there is **no** new release gate.

Opus's later same-corpus public MCP before/after is mandatory for
quantifying any latency gain. No new Kaggle provider job or DevPG
was launched by this diagnostic.
