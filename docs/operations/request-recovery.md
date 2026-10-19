# Retained request dependency recovery

This source candidate adds an audited local owner action for requests in
`needs_recovery` after a missing/changed BGE dependency. It is not deployed.
It does not implement general poison-stage recovery, provider outage retries,
model execution or delivery reconciliation.

Use the existing protected RKB service environment, with the actual
`RKB_SQLITE_CORPUS_PATH`, `RKB_BGE_QUEUE_PATH`, owner subject and request runtime
configuration. Both paths must already exist. Do not copy credentials into the
checkout, reports or command arguments. This CLI uses the same local owner
authority as `regional-knowledge-expert-admin`; it is not an MCP tool or a new
administrative service.

```sh
regional-knowledge-request-recovery list --limit 20
regional-knowledge-request-recovery list --after-id PREVIOUS_ID --limit 20
regional-knowledge-request-recovery recheck \
  --request-id REQUEST_ID --generation EXPECTED_GENERATION \
  --checkpoint-hash EXPECTED_CHECKPOINT_HASH \
  --operation-key SAVED_OPERATOR_KEY --reason 'Description of verified repair'
```

`list` exposes only opaque request/job IDs, generation, checkpoint hash, state
and last reason code. It omits question text, continuation, evidence and tokens.
Keep its output private. It is scoped to the configured app/team.

Before rechecking, investigate the retained job/queue evidence and repair the
actual missing or changed dependency through the applicable recovery procedure.
Restore the original job from verified retained data where available. The CLI
accepts no replacement job ID, query, human identity or vector. A completed query
is not itself evidence that the research answer is semantically correct.

`recheck` checks the owner's current authority, expected request generation and
immutable checkpoint, then reads the existing BGE consumer reference outside the
corpus writer transaction. The job must still be the same query, literal-body
hash, actor and active parent reference. Authority, source/case access and request
fences are checked again before the transition commits.

- A restored ready dependency returns the same request to `pending`.
- A restored pending/claimed dependency returns it to `dependency_wait` with no
  execution lease. The existing bounded lifecycle observes its eventual result.
- A missing/changed/released dependency remains `needs_recovery`. The unresolved
  observation is audited; it does not create a run, enqueue or model call.

The subsequent executor must obtain a new claim through the existing fair
person/dialog selection. The question, continuation, observed resources and
checkpoint are retained. Old execution tokens stay fenced. Owner administration
does not grant the expert access to a revoked source or case.

Save the operation key before invoking `recheck`. After a lost response, repeat
the exact request/generation/hash/key/reason: the immutable receipt is returned,
even if the request subsequently advanced. A different payload under that key
is a conflict. After an unresolved receipt and a later deliberate repair, use a
new operator key for the new observation. Concurrent exact replays record one
transition. Every outcome is stored in the existing corpus authority with an
owner-attributed request event; concise logs contain only correlation/state.

`blocked`, active execution and final/delivery states are excluded. In particular,
this action cannot reopen an answer or resend `unknown` Slack delivery. Use its
existing native receipt/readback reconciliation. A conflict or permission denial
does not change the original request or grant rights.

Unit/CLI tests cover repair, exact backup restoration, unresolved receipts,
concurrent replay, stale generation/hash, current capability revocation including
a race during the BGE read, wrong actor/body/kind/closed parent, app isolation and
unknown-delivery refusal. These synthetic tests do not prove live operator repair
or complete load-management acceptance.
