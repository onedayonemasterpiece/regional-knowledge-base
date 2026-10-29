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
  group-public output only. Without an actual external expert executor,
  one bounded fallback message begins «Найдено в источниках:» and gives
  one line per distinct book (known title/author/year/page and the first
  source sentence, capped at 200 characters). It is **not** a model answer.
  Verified zero hits and technical unavailability remain distinct. No raw
  three-card quote list, UUID, internal LLM, owner impersonation or unsupported
  proof link is introduced.
- The existing event row makes exactly one transition searching→sending before
  chat.postMessage, after Slack auth.test and the current source-ticket check.
  A refusal before this claim leaves the original request searching with no
  send attempt; its existing task retries with 2–60 second backoff and fresh
  authorization/evidence. The bot token is checked against Slack auth.test for the
  original workspace and bot user; client_msg_id is the exact Slack event_id,
  thread_ts is the originating signed thread. A verified matching native
  provider echo records delivered. Any uncertain send becomes UNKNOWN and
  must **never** trigger an automatic second post. Source/person/channel
  eligibility is rechecked before sending.
- Socket startup and completion wake existing dialog heads through the same
  `notify`; later questions do not run searches before becoming the head.
  UNKNOWN is read back with bounded, authenticated Slack thread pagination,
  using the event ID in `rkb_slack_reply` message metadata (Slack may omit
  `client_msg_id` from history). A matching bot reply records `delivered`.
  A complete scan without that marker records `abandoned_no_resend`, not
  successful delivery; incomplete or denied reads retain UNKNOWN. This also
  handles old unmarked sends without claiming that an unmarked reply was absent.
- This does not install a callback, do a live Slack E2E, impersonate a nonowner,
  or grant any new account permission. Recovery tests are synthetic; native
  metadata readback alone is not full request E2E. Run live owner/nonowner
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
  search, source withdrawal/ACL and group-public read gate, limited answer/source
  formatting, one bot chat.postMessage in the original thread with
  client_msg_id=event_id and UNKNOWN-no-resend CAS. The socket envelope_id
  never substitutes for the original event_id. There is no agent executor.

Actual owner/nonowner live acceptance remains NOT RUN. The external Slack
Socket Mode connection, runtime configuration and E2E replay require
separate operator authorization; only Opus may review and merge this source.

### Invalid Slack search input

An empty question or a question longer than 1000 characters after removing the bot mention receives one explicit length-validation reply through the ordinary authenticated send permit. It performs no search and completes the request so later questions in that dialog can proceed. Duplicate delivery and an unknown POST retain the existing no-resend rules.

The search adapter also excludes a trailing Slack connector attribution,
`*Sent using* <@APP_USER>`, from the query before that length check. The original
accepted event text remains unchanged; ordinary mentions and attribution text
inside a question are retained.


## S1, 29.10.2026 — Runbook подключения dot (NOT INSTALLED)

**Target from owner, 25.10:** verified Slack question → original durable
`rkb_expert_requests` / Dataset-covered intake → MCP Events webhook
`rkb.expert.question.created` → restricted dot claims original question
and reads guarded RKB evidence → correlated model result → original
`TargetExpertDeliveryControl` one-shot native Slack message/receipt.
Read-first search and event wake should run in parallel. An HTTP 2xx webhook
ACK **does not** imply model execution or Slack delivery.

**Current source code and critical missing seams:**

| Boundary | Already implemented | Missing for dot |
| --- | --- | --- |
| Signed Slack intake and dedup | `expert_slack_ingress.py`, `expert_slack_socket_mode.py`, `expert_request_control.py:accept` | Socket ACK currently permits local-only `received`; durable remote intake must be covered before a model wake |
| Original request/lease/order | `TargetExpertRequestControl.stage_received_dataset/confirm_dataset_coverage/claim/claim_context`; `request_order.py` | No MCP event publisher/subscription bound to a covered request |
| Source read | `ExpertEvidenceReceiver` (original human, target SourceReadGate) | No dot-facing restricted MCP tool for the exact request; same-host HTTP handler checks client `127.0.0.1` |
| Result | `/internal/rkb/expert-executor-claim`, `/internal/rkb/expert-executor-result` (Dataset result and send-intent) | Both handlers are **loopback service routes**; dot cannot invoke them through public MCP `knowledge_search` |
| Native send | `TargetExpertDeliveryControl.claim_send_once/stage_dataset_native_receipt/confirm_dataset_native_receipt`; `SlackChatSender` | No installed binding from accepted model result to native bot sender; `send_intent_ready` is explicitly `provider_sent=false` |
| Legacy fallback | `ExpertSlackSearchReply` with the original `searching→sending` CAS and UNKNOWN/no-resend | In configured short-path the bot immediately claims and sends; it does **not** wait for dot. No executor timeout arbitration exists |

**MCP Events contract to implement before enabling:** MCP protocol
`2026-07-28`; advertise `events` in `server/discover` and implement
authenticated `events/list`, `events/subscribe`, `events/unsubscribe`
on the **same RKB MCP endpoint**. Event name:
`rkb.expert.question.created`; no free-text/user-private data in the
payload. `data={request_id, event_id}` identifies the stored signed
Slack request; `eventId` stays stable on callback retries. The
`events/subscribe` request supplies a ChatGPT HTTPS callback URL and
`whsec_` signing secret; persist scoped subscription, expiration and
original authenticated *restricted executor* grant across restart, verify
signed callback challenge, reject DNS/private-address rebinding, use
Standard Webhooks headers and bounded retry on the **same event ID**.
Recheck current grant before each delivery. Neither `subscriptions/listen`
(core MCP streaming notifications) nor a `tools/list_changed`
notification is the supported ChatGPT MCP Events **webhook**
subscription. Avoid a second outbox: pending, replay and correlation
must use the existing remotely accepted expert request; do not mark a
local-only record as covered. There is **no** implemented MCP Events
subscription in this PR/repository; the archived, unmerged PR #472 was
closed for a 2,314-line obsolete integration and cannot be installed.

**Dot claimant / result wire contracts (already existing, same-host only):**

- `POST /internal/rkb/expert-executor-claim`: require verified loopback
  `127.0.0.1`, `Authorization: Bearer <provisioned service token>`,
  `X-RKB-Expert-Service: <service_id>`. Exact JSON
  `{"codec":"rkb.expert-executor-claim.v1","request_id":"<stored UUID or null>","lease_seconds":120}`.
  `leased` returns original `request_id`, positive `generation`,
  ephemeral `execution_token`; `empty/deferred` is **not** a result.
  `claim_context` rechecks original person/link/destination before a
  trusted client reads `input_text`.
- `POST /internal/rkb/expert-evidence` on the same loopback service
  identity: exact
  `{"codec":"rkb.expert-evidence.v1","request_id":"<UUID>","generation":1,"execution_token":"<lease token>","min_data_seq":0,"deadline_monotonic":<same-host current monotonic+≤2s>,"top_k":3}`.
  No submitted actor, query, ACL or destination. The handler derives them
  from the original request and final SourceReadGate; a remote dot cannot
  manufacture the same-host `deadline_monotonic`.
- `POST /internal/rkb/expert-executor-result`: same service headers,
  `{"codec":"rkb.expert-executor-result.v1","request_id":"<same UUID>","generation":1,"execution_token":"<same token>","text":"<dot's answer>"}`.
  A verified model answer uses 1–4 meaningful sentences in the question's
  language, then `Источники:` with actual author/book/year/page where
  available; for German, answer in German **followed immediately by a
  Russian translation**. No borrowed UUID/URN, arbitrary citation or
  invented value on irrelevant top-K. Repeat the **same** payload while
  result/send intent is `result_pending/send_intent_pending`; only
  `send_intent_ready` proves an original remote send intent, **not**
  Slack delivery.
- Original native sender must next execute
  `claim_send_once(credentials, request_id, generation, token)`, which
  atomically verifies the original generation, current scope and send
  intent and returns the **only** provider permit. Send once using the
  configured Kaliningrad Base `xoxb` principal to exactly the returned
  channel/thread and `delivery_id` as `client_msg_id`, then build
  `verified_native_slack_receipt` from the authenticated Slack echo or
  exact provider readback. `stage_dataset_native_receipt` and
  `confirm_dataset_native_receipt` must verify remote coverage before
  advancing the dialog head. Lost provider ACK → `unknown`; reconcile
  original thread by its marker and **never blind-resend**.

**Shkiper / owner installation steps (still required, no source-only
permission to perform them):** (1) verify the existing Kaliningrad Base
Slack app/team/bot identity and original nonowner participant link and
`public_search` grant; (2) owner-route `provision_executor` with
`ExecutorCredentials(service_id, token)` and bind
`ExpertExecutorContext` to real `TargetExpertDeliveryControl` with a
current source gate (no permissive fake); (3) provision a distinct
restricted RKB OAuth client for dot, never an owner bearer, and a
trusted gateway to existing loopback claim/evidence/result that
preserves original request identity and delegated ACL; (4) install
the actual MCP Events webhook publisher/subscription, have dot subscribe
and verify callback; (5) connect actual model result to the **existing**
one-shot bot sender/receipt controls; (6) only then enable the model-first
route and run owner/nonowner, incorrect-author, no-support, revoked grant,
restart, webhook 2xx-without-claim, and lost-Slack-ACK E2E. No part of
this runbook authorizes deployment or secret disclosure.

**Timing:** Slack HTTP transport ACK is subject to the 2.5-second
ingress timeout. Existing executor claim accepts a lease of 1–300
seconds (120 default); internal evidence read has a ~2-second bound;
internal result HTTP operation has a 2.5-second timeout; native Slack
client has an 8-second HTTP timeout. PH's observed dot responses took
43–82 seconds and are **not** an RKB latency guarantee. A separate
model-first timeout/fallback must atomically arbitrate with the original
`pending/leased/send_intent_ready/sending/unknown` states before
sending; no hardcoded 7-second answer promise or unconditional fallback
after an already leased or sent result.
