# CAS-F10: expert-request control slice (source-only, 2026-10-21)

**Status: SOURCE-ONLY / NOT DEPLOYED / NOT PRODUCT-ACCEPTED.** This is a
small, reviewable partial implementation of [CAS-F10](../testing/rkb-migration-parity-20261021.md)
and the [owner alignment](../designs/rkb-owner-alignment-20261021.md). It
does **not** enable an expert request in the target MCP server or send
anything to Slack. It neither changes the active Kaggle notebook nor
replaces source/book storage. Both personal-MCP and actual Slack user
end-to-end acceptance remain NOT RUN.

## What was reused from PR #151

- `request_order.py`: the existing per-dialog head barrier and per-person
  fair selection. Only its import of the old book-corpus serializer was
  replaced by equivalent JSON serialization of bounded control primitives.
- `expert_request_control.py`: the tested native Slack HMAC/replay
  boundary; verified external identity and grants via the *existing*
  Participants; request dedup by event/message; bounded local request/order
  projection; existing RecoveryJournal exact-command acceptance readback
  before a request becomes eligible for claim. Verify the complete original
  `ExactCommand.encode()` (including operation key, revisions and logical
  time), not just its source payload. Service epoch and per-request
  execution-token/generation fences. A local `received` row
  is explicitly **not** `saved`, `accepted` or an authorisation to execute.
- `expert_delivery_control.py`: result-intent and send-intent are separate
  exact externally verifiable commands in the same **existing** recovery
  journal. No new recovery publisher or writer was introduced. A deliverable
  result and the one-shot sender permit both require their exact accepted
  remote intent. An unknown send ACK stays `unknown` across controller
  reconstruction and forbids retransmission. A real native Slack
  provider message with the exact client message ID, app/bot and destination
  fields allows a receipt to advance the dialog head. An expired pre-send
  lease can be reclaimed with a new execution generation and token, while
  the original result_generation and accepted remote result/send intents
  remain immutable. `sending`/`unknown` cannot be reclaimed for automatic
  resend. The current Slack channel allowlist is rechecked at claim,
  context and final send, not only at initial event intake. Results may not
  be issued on failed current request/identity/source gate.

This intentionally **does not import** the legacy
`ExpertRequests.__init__`, `DisagreementJournal`, `PersonalDrafts`,
`SQLiteBackend`, or `ExpertExecutor`: those would accidentally re-create
a local book/Case authority. The small SQLite tables here are for bounded
DevCoveer *control* only, using the existing Participants authority.
The existing fixture constructs a SQLiteCorpus; this does not establish that
the eventual target deployment already uses a separately bounded control
file. Confirm actual placement and capacity before enabling this module.

## Required integration (NOT DONE)

1. Wire the **existing singleton writer/ControlSpool/recovery authority** to
   stage and seal the typed `expert.request.v1`, `expert.result.v1` and
   `expert.slack.send.v1` exact commands. Synthetic tests call
   `RecoveryJournal.seal` only to provide a known accepted proof; the API
   is not permission to call it directly from a live MCP request.
   Preserve the existing UNKNOWN/ordering/fencing and remote tail/RPO
   invariants. A result/local delivery receipt does not imply a PG
   `Saved` mutation or a versioned Dataset checkpoint.
2. Replace the test `source_gate` callable with a trusted, fast and
   bounded current-issuance witness over the real Kaggle PG source evidence.
   It must check current case/source rights, audience and request generation.
   Any expensive/async PG read must occur **outside** the SQLite writer
   transaction. The synchronous gate called inside this first source slice
   is only suitable for a prevalidated/current local witness, not an
   unbounded network operation.
3. Implement a trusted Slack transport adapter using the previously tested
   native Socket/HTTP signature and provider echo/readback receipts, not a
   new Slack service. The `verified_native_slack_receipt` constructor
   accepts **only data passed by that authenticated adapter**; it is not
   an endpoint receiving untrusted caller JSON. Native readback-miss
   cannot prove send failure; keep `unknown`, never blind-resend.
4. Integrate into TargetServiceStartup and existing MCP `server.py`
   **after** the independently owned A cold-demand/shared-server changes.
   Reuse the current `ReadAuthority.resolve_adapter` and
   `KaggleSourceBackend.search_adapter`; never create an owner bearer.
   Add a *request lease/identity fence* to final ReadIssuanceMiddleware
   output, in addition to the existing source gate. Until then the personal
   MCP/delegated search path is not safe to activate.
5. Record real source dependencies and readback eligibility before
   result/delivery, retaining PR151 per-dialog ACK, history withholding,
   source withdrawal and current identity/grant semantics. This source slice
   has no public tools/HTTP routes and is not a full question executor.
   Load tests must check request queue fairness, latency and bounded
   control storage alongside four-user reads.

## State semantics

| State | Meaning | Allowed external effect |
|---|---|---|
| `received` | Locally stored signed request; exact remote intent not yet proved | None |
| `pending` | Request intent exists in the **same** remote acceptance journal | Claim only after current identity/grants/admission |
| `leased` | One fenced execution generation and token; on pre-send expiry, a new lease preserves original result_generation | Trusted PG lookup only with current destination/source permissions |
| `result_unsealed` | Bounded result prepared locally | None |
| `result_ready` | Exact result intent externally accepted | Prepare send intent, **no Slack send** |
| `send_intent_ready` | Exact provider intent accepted; original generation/marker remain immutable across pre-send reclaim | One permitted native send only after final current-source and Slack destination policy check |
| `sending` / `unknown` | Effect may already have happened | No duplicate send; provider readback/reconcile only |
| `delivered` | Exact native provider receipt correlated to sent intent | Advance original dialog head; **not** a claim of durable PG mutation |

All statuses remain internal to this source-only controller; no user-facing
Saved is exposed by these classes. The final provider receipt does not relax
future source/user revocation checks. No startup job, notebook Save or
Telegram action is performed by this code.

## Focused evidence

- `tests/test_expert_request_control.py`: signed event/retry/identity
  reject; current authority; remote exact acceptance before claim;
  fair dialog ordering; no second admission.
- `tests/test_expert_delivery_control.py`: result/send remote proof;
  no duplicate after lost ACK and process restart; unknown first dialog
  does not block unrelated dialogs; exact native provider receipt;
  revocation/source denial at send; malicious receipt rejection.
- Run with `PYTHONPATH=src pytest -q tests/test_expert_request_control.py
  tests/test_expert_delivery_control.py`. These use synthetic/isolated
  in-memory provider receipts, not a real Slack account or Kaggle PG.

Remaining release gates: authenticated Slack transport E2E, actual
request-bound source/Case access through native PG, MCP final issuance
revocation tests, recovery after loss of Dev local disk and Kaggle Dataset
coverage, and successful real-user expert workflow. Do not mark CAS-F10
PASS or merge a full cutover solely from green fixture tests.
