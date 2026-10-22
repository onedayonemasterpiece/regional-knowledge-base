# RKB — Slack E2E acceptance checklist (not executed)

Date: 2026-10-22. Owner: **RKB-Кварц**, test-only source handoff.
Status: **NOT RUN / NO LIVE SLOT**. This document authorizes no provider
mutation, Slack test post, Kaggle launch, PostgreSQL access, publication or
credential inspection. Opus is sole live/runtime and integration owner.

Normative requirements, all at immutable base
`bd138b2449f68cabb2cbfa3df552df8471f40c78`:

- [Implementation v4.1](../prompts/rkb-scalable-kaggle-implementation-20261019.md), especially §§6, 9, 10, 11, 14.
- [Owner alignment](../designs/rkb-owner-alignment-20261021.md): Dataset primary; no false Saved or new provider effect before verified remotely durable intent.
- [Acceptance](rkb-scalable-kaggle-acceptance-20261019.md): CHN-1, ID-1, REC-4/5/6/9, QUE-1/2, DBG-13 and source/security gates.

Full PDF/DjVu import, actual source illustrations/pixels, evidence-backed facts,
saved genuine `Story.body`, and continuation remain product requirements. Neither
a green synthetic test nor a Slack-only run waives those requirements.

## 1. Gates before obtaining a live slot

1. **Opus** publishes the one canonical integration branch, exact base/HEAD,
   merged incoming source inventory and per-file ownership. Include the
   reviewed PR215 grouped checkpoint source, Users/Slack frozen source, and
   the corrected clean-writer test. Never overlay an old whole-file
   `server.py` on current `main`.
2. Opus separately confirms v106-or-later exact runtime incarnation,
   source/code/schema hashes, active attached provider, a real signed ingress
   route (not merely registered locally), original writer binding and
   Dataset version/cursor readback. Treat a cold/pending or
   `snapshot_manifest_invalid` state as **not ready**.
3. Current authority maps the *same canonical person* across Slack and MCP;
   current team/app/channel grants and approved test actor are documented.
   Confirm permitted bounded test destination and cleanup protocol, without
   creating an alternate bot, client, registry or auth path.
4. Opus/dot issues **explicit time-bounded live E2E slot** with concrete
   conversation/thread ID, permitted effect count, baseline event/request
   IDs, resource boundaries, readback path and stop/compensation rules.
   Until then **no real Slack events or provider mutations**.

## 2. Non-secret evidence collected per one approved run

| Check | Exercise and expected proof | Failing signal |
|---|---|---|
| Signature | Actual Slack Events API raw bytes; HMAC v0, timestamp ±300 s, app/team/actor/channel | Unsigned/old/wrong-scope event produces a request |
| Intake retry | One signed event, delivered twice before remote coverage | Premature HTTP 200, duplicate request ID or lost original event |
| Grouped checkpoint | Existing control writer accepts exact intent; 30 s quiet window; successful CAS, native version verification and exact captured cursor | Only local `received` yields 200; unverified Dataset/Saved claim |
| Canonical identity | Same person and capability seen via Slack + MCP; current source/Case/grants rechecked at final output | Owner fallback, stale link, private draft or source leak |
| Late child | New message in same thread after capture but before verified version | Late row incorrectly promoted to previous version, or duplicate claim |
| Claims/fairness | Original accepted dialog head before its child; unrelated person/dialog proceeds under UNKNOWN | Head overtaken or global deadlock |
| Result/send | Remotely verified result intent and send intent before provider action; exactly one `client_msg_id`/claim | New send without verified intent, mismatch or double post |
| Lost post ACK | Actual native readback of original marker; UNKNOWN stays UNKNOWN until verified | Automatic resend, new marker, or false delivery receipt |
| Recovery | Crash/restart and exact version/pointer after lost finalize/verify, retired writer/current-run mismatch | Forged new Saved, epoch revival or previous writer rebinding |
| Source preservation | Approved real book/evidence (PDF and DjVu, pixels, figures/captions, facts, Story.body, continuation) | Text-only/synthetic subset misreported as full import |

For each case record the exact (redacted) route, request ID, event ID/hash,
writer generation, version, checkpoint sequence/coverage proof, final
server-observed state and result marker, and corresponding non-secret
native/provider receipt. Separate source, CI, E2E and live results; never
log signature, signing secret, actor token, private text or credentials.

## 3. Synthetic tests and limitations

Test-only branch `chatgpt/rkb-quartz-signed-ingress-tests-20261022`
adds `tests/test_expert_slack_signed_replay_checkpoint.py` covering real
`ExpertSlackIngress`, HMAC verification, `TargetExpertRequestControl`,
`RecoveryJournal`, `TargetExpertDeliveryControl`, request-order fairness,
and the persisted one-shot UNKNOWN fence. It uses Starlette ASGITransport,
synthetic control-object storage and a synthetic source-read gate, **not**
Slack/PG/Dataset/native network. Its journal seal is **not** a Dataset
checkpoint or product Saved proof.

`tests/test_expert_dataset_checkpoint_acceptance.py` from the separate
pre-existing 16-file diagnostic is not imported here, since its prior
12 PASS used a forbidden per-instance S3→Dataset
`writer._store_identity=None` reset. A valid independent candidate must
use a **new actual** `QueueWriterAuthority` immediately bound to its
synthetic Dataset genesis, with old S3 writer unchanged, and check actual
`SourceDatasetCheckpoints.refresh` before claiming coverage. The
required fixture correction, actual joined test and exact receipt must
be imported onto Opus's canonical source candidate; absence remains RED/
NOT RUN, never inferred PASS.

No user-facing Slack publication is part of these local tests. All live
checks remain NOT RUN until the owner authorizes the explicit slot.
