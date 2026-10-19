# Durable archive read continuation

## Target durable read continuation (2026-10-19)

**Target contract, not deployment evidence.** Follow the [single handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md). Processing moves to Kaggle; callers retain one durable request lifecycle without a second archive workflow.

Persist request/attempt identity, exact source/entry/hash, actor/audience/pixel scope and dependency revisions before I/O. Return bounded pending and resume the same work from a new chat/process. Status reads do not create imports, semantic requests or publications.

Preserve keyed provider receipts where supported. Direct Telegram reads do not automatically inherit Vibe keyed-get guarantees: restarted transfer may repeat bounded chunks while one logical result is accepted. Do not claim exactly-once network transfer or invent provider operation IDs.

Separate byte verification, render readiness, locator acceptance, artifact readiness, result acceptance and external delivery. Save exact accepted payloads/IDs before durable success; retry/failover restores them. Stale new submissions are fenced, already accepted commands replay.

Recheck current source/pixel permissions on cache/callback/delivery; history is not a permanent grant. Partial successes remain available without exposing denied inventory. Retain preview/render dependencies until locator callbacks are terminal; image ACK alone is not GC.

Pending/unknown outcomes reconcile the same attempt. Confirmed transient failures use persisted bounded retry/cooldown; FloodWait releases capacity until retry. Auth/digest failures are not repaired by new request keys. Verified asset expiry follows its exact source continuation, not a new send.

Existing Vibe guarded terminal blocked recovery for telegram_transport_unhealthy/provider_read_deadline uses a persisted next key and15-minute cooldown; verified-asset HTTP404/410 is a separate expiry path. Preserve the [released fix](../reports/illustration-audit-remediation-20261019.md). Attempt/receipt identity survives ordinary cache cleanup.

## Retained keyed-Vibe baseline

Following sections describe the prior keyed-get implementation. Its blanket terminal-failure language is qualified by the released exceptions above. Existing tests do not prove direct Telegram or Kaggle-reset acceptance.


Deploy the matching VibePublish opt-in `media_store get` idempotency implementation
before this RKB change. Old Vibe deployments ignore get request keys and do not
provide the required lost-response guarantee. This changes retrieval only, never
source publication or archive grants.

The existing private OriginalCache SQLite stores one opaque scope, random request
key and operation ID per owner/issuer/archive entry. Admission identity is committed
before provider I/O. Repeating an invocation replays the SAME keyed get, which
rechecks current Vibe ACL/binding, and resumes the same durable operation. An
operation-ID conflict fails closed. No credentials or source text are stored in
this metadata.

The synchronous polling budget remains bounded. A provider still running returns
`archive_source_pending` with its operation ID, not a missing-original verdict.
Retrying the same source resumes the attempt. Lost admission responses, process
restarts, network failures, authorization failures and terminal provider failures
do not silently generate fresh requests.

Verified receipts remain recorded even after payload return: a caller may crash
before installing the original in the byte cache. Only a definitive HTTP404/410
from that verified receipt's asset retires the attempt, allowing the next call to
make a new retrieval with a new key. This is not a new publication. Other HTTP
failures retain the receipt. Compare-and-delete cannot remove a concurrent newer
attempt.

Attempt metadata is bounded by the archive entries requested and is deliberately
not evicted by the original-byte cache's idle/size cleanup. Evicting an unresolved
admission would discard the information needed for safe replay. Terminal provider
failures remain available for diagnosis rather than being retried indefinitely.

Regression coverage includes slow provider completion, restart, lost response,
concurrent admission, crash after payload return, owner/entry isolation, current
authorization denial, expired assets, unknown failures and receipt conflicts.
Unit tests model the paired Vibe contract; release acceptance must additionally
exercise the actual deployed keyed-get endpoint and current source-byte hashes.
