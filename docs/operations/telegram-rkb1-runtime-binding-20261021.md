# RKB1 native Telegram client and exclusive lease — source-only handoff

Implementation base: `c423e3360bccb6c6b9b6e140a008fcf9b9309e00` (merged PR201).
Separate frozen importer: `src_77a3c13365306167dd696555` (base
`a120c9cf37007eef99f165a68cb2f03869640ba6`). Integrate by
three-way review, not by replacing any controller/launcher/channel files.
This helper is **NOT** a deployed credential/frame contract or live acceptance.

## One slot and two deliberately separate processes

`Rkb1Lease(queue,archive_chat_id=exact_configured_chat,topic_id=2)` runs
**only in the existing Dev controller**, using `BgeQueue.connect()` and
`rkb_writer_control` in the existing private SQLite. It adds a single
nonsecret slot `rkb_telegram_session_lease` keyed by `TELEGRAM_RKB1`
(not by topic, not by request, not by connection generation). On
`.claim(run_id,runtime_incarnation)` it requires current run, active
writer, authenticated WSS incarnation, healthy channel and pinned
provider version. `.current(run_id,incarnation)` fails closed on lost
heartbeat/channel or writer authority, but **never deletes the slot**.

A new run/incarnation cannot claim the occupied slot until
`BgeQueue.retire_provider_reservation` has saved the previous run's
actual exact-execution `bge_provider_terminal_receipts` under its
validated provider identity. TTL expiry, status ERROR, Stop/cancel,
WSS disconnect, notebook version COMPLETE, or locally inferred terminal
are not sufficient. Same-incarnation WSS reconnect is idempotent and
keeps the session owner even when socket generation changes.

Only original DOCUMENT topic **2** is permitted in this initial source
slice. The slot represents the entire RKB1 session, **not a separate
session per topic**. A later approved illustration scope for topic 4
must explicitly widen the allowlist while reusing this SAME session
owner/client; do not make another Telegram session lock or silently
widen topic 2 from the locator.

## Exact native seam — no credential persistence

After the existing authenticated WSS HELLO and Dev lease issuance,
an independently authorized **ephemeral WSS secret response** is
delivered directly to the owning Kaggle runtime. Its producer,
receipt/ACK, non-logging dispatch, and effective credential permission
are **outside this helper** and require Shkiper's coordinated review
before live use. Do not add credential fields to run configs, retries,
durable jobs, SourceHistory, Dataset, Git, source artifacts, diagnostics,
exception bodies or notebook output.

`build_rkb1_downloader(...)` receives only explicitly injected runtime
values: `source_name="TELEGRAM_RKB1"`, base64-JSON `bundle_b64`, root
`TELEGRAM_API_ID`/`TELEGRAM_API_HASH` values, run+incarnation, exact
archive chat/topic2, trusted `authenticated_lease_current` and
`dependency_current` callbacks, `StringSession`/Telethon client
factories and `NativeTelegramOriginalDownloader` factory. No environment
search, fallback provider, E2E identity or alternative transport.

Bundle fieldset is EXACT:
`session,app_version,device_model,lang_code,system_lang_code,system_version`.
API id/hash are separately injected from their **RKB** root config,
never extracted from the bundle and never sourced from Vibe/S22/E2E.
All five Telegram device/language attributes reach the constructed
client unchanged. Constructor has `auto_reconnect=False` and zero
native request/connection retries; existing owner lifecycle controls
connection and retry. The helper never connects, logs in, sends,
downloads or disconnects a client. It does not retain or expose a
plaintext session as a result or durable dataclass.

The owner is responsible for passing a callback that verifies the
CURRENT authenticated WSS/Dev lease for the exact `(run_id,incarnation)`
for the duration of the stream; an arbitrary boolean or client-supplied
predicate is NOT an authorization grant. The callback can fail closed
while disconnected and resume with the same owner after reconnect.

Actual native readiness, **after separate credential-use authorization**:
1. Verify an EXACT pinned CP313-compatible Telethon installation in
   Stage0 and the capability to create the configured client.
2. Prove the injected session is user-authorized under the owned
   connection (`check_rkb1_authorized`), not merely a present root key.
3. Using the approved private scalar locator, check exact configured
   chat/topic2/message/document and proceed through the existing
   `NativeTelegramOriginalDownloader` bounded stream.
4. Existing original-import/page/stage/PG/BGE/Dataset gates still apply.
   No `Saved` or successful search is implied by a socket, client or
   Telegram metadata match.

No production `TELEGRAM_RKB1` value was read or copied for this patch.
An earlier public archive entry is NOT substituted for the current
operator-verified legacy source identity. No private grant/proof,
source scan or actual message/document ID is committed here.

## Verification and limits

`PYTHONPATH=src pytest -q tests/test_telegram_runtime_binding.py`
uses only fake device/session data and an isolated `BgeQueue` SQLite;
no provider, Kaggle, PostgreSQL, BGE weights, native client or secret
frame is contacted. Tests cover writer binding, idempotent reconnect,
unexpired and expired owners, exact native terminal handoff, changed
incarnation/scope, redacted bundle validation, preserved device metadata,
client construction without connection and synthetic authorization.

The helper does **not** modify shared `kaggle_target_runtime.py`,
`kaggle_target_launch.py`, `bge_channel.py`, `bge_wss_transport.py`,
`bge_queue.py`, `queue_writer.py` or any frozen import/media module.
Shkiper alone integrates this source seam with authenticated private
WSS secret-frame exclusion and true Stage0/native provider readiness.
No permission to transmit new credentials is inferred from source ACK.
