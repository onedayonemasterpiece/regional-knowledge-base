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

## Opt-in trusted expert evidence read (subsequent source slice)

`src/regional_knowledge/expert_evidence_receiver.py` adds an optional
`POST /internal/rkb/expert-evidence` endpoint using exactly the existing
`ReadAuthority.resolve_adapter → KaggleSourceBackend.search_adapter →
ReadIssuanceMiddleware` chain. It is registered by `build_server` **only**
when an explicit `ExpertEvidenceReceiver` instance is passed, with exact
identical target startup queue/Participants/spool ownership. Default service
configuration does **not** activate this endpoint.

Only the existing canonical internal executor service may call over
loopback. Required `Authorization: Bearer <provisioned-executor-token>`,
`X-RKB-Expert-Service` and bounded JSON
`{codec:"rkb.expert-evidence.v1",request_id,generation,execution_token,
min_data_seq,deadline_monotonic}`. **No caller-controlled query, actor
subject, source whitelist or Slack channel.** A current claimed request and
its original signed Slack origin provide the query and human identity.
The audience uses the actual stored Slack DM/group destination and the
existing source gate (group cannot upgrade to personal/private evidence).

The ASGI response is bound to one retained native search result. Before
the final HTTP body, `ReadIssuanceMiddleware` refreshes source/participant
permissions and a bounded read-only SQLite `issuance_current` check for
request state, execution generation/token, service credential and current
Slack destination policy **plus a fresh verified Participants external link,
original person/link revision and current public_search grant using the
same bounded read-only connection**. This second identity check prevents an unlink or
grant revocation committed after the asynchronous SourceReadGate snapshot but
before the final synchronous send. The same original absolute `deadline_monotonic` is
rechecked during final issuance **after** the route's async timeout scope;
a delayed middleware refresh cannot issue stale evidence. A revoked account, superseded lease, removed
channel or unavailable source yields empty evidence; there is no
unprotected secondary serialization path. The control DB final check has
a 2-ms SQLite busy timeout and fails closed.

The endpoint is NOT a Slack delivery/ACK executor, does not call
`chat.postMessage`, cannot fabricate owner bearer tokens and writes
no PG/domain data. Its output still requires subsequent result-intent,
send-intent and exact native receipt transitions via the existing singleton
remote writer. Existing `ProjectsHubReader` is unchanged. The runtime
cannot enable this option until the same trusted Slack transport and
operator-configured service credential used by CAS-F10 are actually
integrated. Actual source gate/PG authority, real Slack provider delivery,
Dataset durability and end-user E2E remain NOT RUN.

The new ASGI tests use real synthetic OAuth/Participants, actual target
queue read admission, middleware and same HTTP response path. They cover
retraction at final issuance, stale execution generation, forged inputs,
loopback boundary, and explicit-only registration. They are not an
installed Kaggle test.

Remaining release gates: authenticated Slack transport E2E, actual
request-bound source/Case access through native PG, MCP final issuance
revocation tests on the installed product, recovery after loss of Dev
local disk and Kaggle Dataset coverage, and successful real-user expert
workflow. Do not mark CAS-F10 PASS or merge a full cutover solely from
green fixture tests.

## Opt-in signed Slack Events API (source-only)

`src/regional_knowledge/expert_slack_ingress.py` defines `POST /internal/rkb/expert-slack-events`.
The route exists only when `build_server(expert_slack_ingress=...)` receives an instance
that owns the **same** configured target Participants and ControlSpool; default deployments
do not enable it. The exact original Slack HTTP body is bounded to 256 KiB and checked
against the `v0` signing secret and 300-second timestamp window before JSON decoding.
Signed URL challenges are returned without creating requests. Signed bot/self events
are ignored. Human events use the existing verified Slack account, channel allowlist,
current grants and idempotent event identity in `TargetExpertRequestControl`.

**Slack 200 is not a local SQLite receipt.** For a first event, local `received`
must return **HTTP 503 + Retry-After** until the original exact request has been
sealed remotely through the **existing single RecoveryJournal writer**. A replay
after verified external acceptance returns 200 without duplicate work. No request
ID, user text or secret is returned. A malformed/forged/unverified event fails closed.
Signed URL challenge is a separate stateless handshake, not an accepted request.

**Do not activate this route yet:** Slack retries are finite and the source-only
endpoint does not itself schedule, seal, send or reconcile work. The already-existing
ControlSpool writer must be wired to durably accept pending event intents promptly
before provider retries end; inventing a second writer or sealing directly inside
the HTTP callback would violate the existing recovery/UNKNOWN contract. Real Slack
delivery/echo, PG evidence and product acceptance are still NOT RUN.

`tests/test_expert_slack_ingress.py` exercises signed HTTP challenges, initial 503,
remote-accepted retry 200, duplicate/tampered events, revocation, forged HMAC,
wrong app/team and disabled-by-default route, all on synthetic fixtures.


## 2026-10-24: bounded read-only Slack search reply (source-only, OFF)

Per Opus 1792800345, **ordinary read-only search must not pass through
expert-agent executor or three Dataset result/send/receipt checkpoints**.
They remain mandatory for their own accepted provider-effect workflows.
This opt-in path keeps the *existing* signed Slack ingress and verified
Participants/source gate, but uses its existing unique event_id/request row
only for local at-most-once Slack delivery. This is not a new service or queue.

- Set existing RKB_EXPERT_SLACK_INGRESS_CONFIG only after Shkiper verifies
  the real Slack app, signing secret, team/bot IDs and explicit allowed_channels.
  Configure Slack Events app_mention/DM subscription to route through the
  existing external ingress to /internal/rkb/expert-slack-events. As of the
  source handoff, live ingress is OFF and the external callback URL is unknown.
- Optional RKB_EXPERT_SLACK_REPLY_CONFIG points to a private regular file:
  {"codec":"rkb.expert-slack-search-reply.v1",
   "bot_token_file":"/absolute/owner-private/bot-token"}
  The bot_token_file must have owner-only permissions, contain a bot xoxb-
  token, and remain unchanged while in use. **Only Shkiper/owner** can install
  config/token/grants/Slack app settings; no secrets are shipped in source.
- Each signed non-bot event is checked for replay, exact app/team/channel,
  current verified Slack human link and public_search grant. In reply mode
  the existing ingress record is locally inserted once by event_id and
  acknowledged (HTTP 200), with no Dataset checkpoint needed for a read-only
  message. A duplicate event never acquires a second send permit.
- The reply uses the already-composed ExpertEvidenceReceiver and its current
  canonical ReadAuthority/SourceReadGate under **the original human**, with
  group-public output only. It quotes actual returned source title, citation,
  page locator (unknown if absent) and excerpt. No LLM, owner impersonation,
  unsupported web link or original-scan proof is introduced.
- The existing event row makes exactly one transition searching→sending before
  chat.postMessage. The bot token is checked against Slack auth.test for the
  original workspace and bot user; client_msg_id is the exact Slack event_id,
  thread_ts is the originating signed thread. A verified matching native
  provider echo records delivered. Any uncertain send becomes UNKNOWN and
  must **never** trigger an automatic second post. Source/person/channel
  eligibility is rechecked before sending.
- This does not install a callback, do a live Slack E2E, impersonate a nonowner,
  guarantee recovery after process crash during an acknowledged read, or grant
  any new account permission. Tests are synthetic. Run live owner/nonowner
  acceptance separately after authorized install with Shkiper; Opus reviews
  and merges source only.

## 2026-10-24: Socket Mode as a second ingress (source-only)

Kaliningrad Base app A0C9VJE0DHD and bot U0C9VJE31QX were observed
by the sole live operator using Slack Socket Mode, without an HTTP Events URL.
This source change uses the official Slack SDK async SocketModeClient over
the existing websockets dependency, inside the current RKB API lifespan.
No new service, LLM, external Request URL, Dataset delivery stages or grants.

**Shkiper/owner-only installation conditions (not executed here):**

- The existing private RKB_EXPERT_SLACK_INGRESS_CONFIG path may select
  codec=rkb.expert-slack-socket.v1 with exactly app_id, team_id,
  bot_user_id and nonempty allowed_channels, using the verified
  installation identities. No HTTP signing secret is required for that
  authenticated Socket Mode connection. The previous HTTP codec and
  HMAC-verified endpoint are unchanged and cannot use the socket-only mode.
- The already installed environment must provide existing SLACK_APP_TOKEN
  (xapp, for SDK connections.open) and SLACK_BOT_TOKEN (xoxb, for
  existing SlackChatSender). No new secret file, token, permission,
  manifest or application install is created by source. Do not enable
  the separate HTTP reply config simultaneously in socket-only mode.
- Inside the same target startup lifespan the authenticated SDK listener
  accepts only events_api/app_mention. Its original Events payload passes
  the existing _verified_slack_observation app/team/bot/subtype check and
  the SAME TargetExpertRequestControl.accept with verified human link,
  current public_search grant and channel allowlist. The socket envelope
  is ACKed only after the existing event_id row is locally committed
  as received; unrelated/bot events ACK without recording a question.
- From there PR383's existing ExpertSlackSearchReply owns the native
  search, source withdrawal/ACL and group-public read gate, source/page/
  quote formatting, one bot chat.postMessage in the original thread with
  client_msg_id=event_id and UNKNOWN-no-resend CAS. The socket envelope_id
  never substitutes for the original event_id. There is no agent executor.

Actual owner/nonowner live acceptance remains NOT RUN. The external Slack
Socket Mode connection, runtime configuration and E2E replay require
separate operator authorization; only Opus may review and merge this source.


## 2026-10-25: source-only RKB MCP Events → dot → Kaliningrad Base

This narrowly implements the [owner-approved parallel search/wake contract](../design/rkb-kaliningrad-base-collaboration-20261018.md#2-один-rkb-backend-и-существующий-durable-request-protocol). **Not enabled, not deployed, and not a live E2E result.** No numeric startup estimate is added; the approved PR462 message remains unchanged.

**Existing boundaries, no new service or queue.** The current Slack HTTP/Socket ingress verifies the original human, app/team, event and channel before storing one request in \`rkb_expert_requests\`. With an explicitly bound \`ExpertDotEvents\` executor, source search and the RKB MCP Events webhook wake start independently. The old source-only, at-most-once three-card Slack reply is disabled *only for this opted-in route*. The callback carries immutable \`request_id\` and Slack \`event_id\`, never message text or actor/owner authority. The original request row stores the event intent in \`trigger_event_id\` *before* the HTTP effect. A webhook HTTP 2xx is **not** terminal: until the original claim/result, a restart or subscription renewal can replay the same immutable event ID. An active wake also keeps retrying transient Dataset coverage/network failures with capped backoff until the subscription expires, stopping once the original request is claimed or finished. For fresh live messages, search and wake start in parallel while the number of speculative reads remains within the already-used Slack bound (16). The 17th and later accepted question **still gets its own wake**, and dot performs its one native lookup on claim; no acknowledged question is discarded by the prefetch cap. Restart recovery wakes every saved question **without speculative native reads** (65+ pending questions do not cause 65 additional background searches). Each issued prefetch retains the **existing** source issuance ticket for at most 20 seconds while dot receives the event and refreshes that same gate before providing evidence. A later dot read reissues a new current search rather than serving private, stale cached bytes. The callback connects to one DNS-validated public IP at a time, failing over after \`ConnectError\` or \`ConnectTimeout\` (both before HTTP POST) to another validated IP with the **same event ID**, while retaining the original Host and TLS SNI. There is no independent outbox, polling service or second search authorization.

**Minimal runtime configuration, Shkiper only.** Continue to compose one installed \`TargetServiceStartup\`, \`ExpertSlackIngress\`, \`ExpertEvidenceReceiver\` and normal Slack \`ExpertSlackSearchReply\` with the actual bot/app tokens. Owner-provision a **restricted, non-owner** \`ExecutorCredentials\` under the existing \`TargetExpertRequestControl.provision_executor\`; use a real \`TargetExpertDeliveryControl\` with the deployment-owned current source access gate (never an allow-all fixture). Supply exactly this context to the existing \`server.main(expert_executor_context=...)\` composition, adding \`mcp_subject\` and \`mcp_client_id\` for the separate dot MCP OAuth connection and \`mcp_callback_host\` for the **exact operator-approved ChatGPT webhook host**. This source change neither creates those identities nor mints a secret. The active deployment must provide this context; the default \`main()\` stays in its existing source-only mode. RKB checks the actual OAuth token's subject+client before tool/event use and never accepts an owner token on the executor route. External ChatGPT/MCP uses its existing authorized OAuth transport; same-host executor controls remain existing loopback-bound service methods and shared ControlSpool. No public loopback endpoint.

**Protocol once installed.** Refresh MCP metadata and subscribe as dot to \`rkb.expert.question.created\` with an empty filter. ChatGPT supplies the signed HTTPS callback and signing secret at subscribe time; the callback is challenge-verified, and the signing secret is saved in the **existing encrypted OAuth state** only, revocable by \`events/unsubscribe\`. The MCP Event payload contains only \`request_id,event_id\`. Call \`expert_request_get(request_id,event_id)\`, retaining the returned \`generation,execution_token\` as a scoped lease. On \`state=pending\`, retry the *same* request (a cold/unavailable read is not “no supporting evidence”); a delayed re-read after \`leased\`, \`sending\`, \`unknown\`, \`delivered\` or \`blocked\` returns that original state without running another search. Use the original-language question and current public-scoped source evidence to write one coherent answer, with matching sources and honest abstention. Call \`expert_result_record(request_id,event_id,generation,execution_token,text)\`; if persistence returns \`result_pending\` or \`send_intent_pending\`, repeat **that same exact call**, not a new answer or Slack send. The existing Dataset records must cover result and send intent before the provider sender claims its one permit; Slack's verified native receipt is staged back to the existing Dataset. An uncertain provider effect stays \`unknown\` and is reconciled by the existing recovery/receipt path, not automatically reposted. The original Slack sender checks its configured bot identity once via \`auth.test\` and the current local credential, **before** invoking the existing atomic \`claim_send_once\` from that same send call. The claim transaction is the sole current-source/destination check. A rejected bot identity, local credential or source therefore leaves the original send intent unconsumed (not \`unknown\`), and dot can retry that same saved result. Only after a permit has been issued does the sender attempt \`chat.postMessage\`; a possibly completed provider POST remains \`unknown\` without automatic resend.

**Acceptance after Opus merge (Shkiper + dot):** verified original owner and non-owner Slack questions; subscription present in RKB event discovery and dot acknowledgment while idle; original actor-scoped search in parallel with wake; one coherent source-grounded reply in the **original** thread, correct Kaliningrad Base sender and matching receipt; same event replay and callback timeout without second post; revoked source/actor/service denies a pending reply; process restart resumes an undelivered event from the existing row. These are **operator E2E requirements, not claims from synthetic CI**. No import or runtime/provider mutation belongs to this PR.
