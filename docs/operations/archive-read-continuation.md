# Durable archive read continuation

Updated 2026-10-21. Target contract: [single implementation handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md), corrected by [owner alignment](../designs/rkb-owner-alignment-20261021.md). This document distinguishes the **historical legacy VibePublish read path** from the **target direct Kaggle↔Telethon archive path**. Legacy keyed-GET, provider-digest and SHA-readback clauses below remain historical evidence/legacy compatibility only; they are not requirements for target Telegram media, whose success is verified by the presence of the expected accessible photo/document at its registered locator. It does not claim the new deployment is complete.

This durable lifecycle applies to accepted resumable archive/render work, not ordinary ephemeral `read_query`. Text search uses the same admission authority without fsync, remote seal, durable RESULT_ACK/RESUME or archive preflight. Its bounded asynchronous telemetry does not write to the queue SQLite database or contend for its write locks. A read stage inside an accepted durable question retains that question's existing context and recovery contract without a second intake.

## Admission and identities

Persist the logical request, attempt key, exact source edition identity and native media locator (message/chat/topic/type), artifact purpose, actor/audience/scope and dependency versions before provider I/O. Input source hashes and PG/code recovery manifests belong to their own contracts; do not require a Telegram-media checksum or native-byte roundtrip to prove provider upload success. Commit admission before returning an operation identity. A bounded `pending` response is not a missing-original verdict. A new chat/process resumes the same work; status reads never create a fresh import, semantic request or publication.

**Historical VibePublish compatibility only:** the Vibe keyed-get path stores the opaque owner/issuer/archive-entry scope, random request key and provider operation ID. Repeating the request rechecks current provider authorization/binding and resumes the same operation. An operation-ID conflict fails closed. Lost replies, process restarts and unknown provider outcomes do not authorize a fresh key. Use the existing dedicated same-host service identity, not a user's OAuth or owner bearer.

**Target direct Telethon path:** Kaggle reads and uploads media using exact registered native locators and genuine separately authorized sessions. Verify that the expected accessible photo/document exists at the exact message/chat/topic locator; do not require downloading it again for custom SHA comparison. Restart may re-read bounded bytes when an actual requested page/proof needs them; preserve the same logical work ID and do not claim exactly-once network transfer or invent Vibe provider IDs. Unknown send outcomes require lookup/reconciliation before another send, not an automatic duplicate.

Retain the owner's cap: one exclusive genuine Telegram session lease per authorized Kaggle run; splitting text/archive/proof readiness does not authorize additional runs. A ready text worker remains usable during archive reconnect, FloodWait or a pending render. Release the affected archive/proof permit while waiting without disabling SQL/BGE search.

## Read, render and result are different states

Distinguish Telegram-media presence at an authorized locator, source-edition identity, actual render readiness, accepted localization, artifact readiness, accepted domain result and external delivery. Telegram-media presence does not certify dataset snapshot durability or code integrity. Persist exact accepted payloads and IDs through the existing fenced controller; a local Kaggle commit or an image send alone is not durable acceptance. Current attempts reject stale new writes while replaying already accepted commands.

A locator is bound to source SHA/revision, page, render hash/dimensions, crop/rotation/transform, quote/span and pixel scope. Retain the exact preview/render or an exactly reproducible descriptor until locator callbacks are terminal. Image delivery ACK is not permission to evict outstanding localization input. If the same preview cannot be recovered, start an explicit new render/localization generation and reject old callbacks.

Multi-page or multi-item operations return allowed completed results with per-item `ready`, `pending`, `unavailable`, `denied` or `failed`. Do not discard a verified page because another page is slow, or reveal inaccessible inventory in an error. Deliver real MCP ImageContent with actual MIME and explicit source/preview/provider rendition identities. Telegram photos can be recompressed; source/crop/artifact integrity fields must not be interpreted as a compulsory provider-media SHA equality/readback check.

## Current access at every boundary

Apply the [artifact access policy](../rights.md): accepted illustration renditions are public; quote stripes/page/crop scans and localization previews require owner or the existing `scan_proof_read` grant for demo/expert/local_historian; full originals retain separate private access. Verified category assignment creates the audited capability grant atomically under [rights.md](../rights.md); `claimed` does not. Proof rendering checks the current grant/scope; category removal/demo expiry and manual revocation remain effective, with no grant recreation on reads. A proof renderer uses purpose-scoped internal source access after caller authorization, never owner impersonation.

Check withdrawal, source revision, current policy, grants and audience on warm cache, retry, callback and final delivery. An old receipt or signed asset URL is not a permanent grant. Shared Slack `public_only` can receive public illustrations, not the union of members' private proof grants. Output geometry must actually fit its permitted artifact class/scope.

Current access checks do not require retransferring or fully rehashing an unchanged cached object. Concurrent requested page/proof operations may share one bounded in-flight download and leased cache keyed by registered source identity. For target Telegram, check the accessible expected native media locator, expected type, revision/authority and required file/container bounds; do **not** introduce mandatory native-provider media digest/download/readback. In-memory/transient cache integrity, pinned preview transforms, source-edition comparison and PG/snapshot verification remain separate where actually required. Changed identity, revoked rights or invalid media require a fresh authorized lookup; do not turn a cache miss into a new publication. Render/cache refs bind page/transform/generation, artifact class and scope; public illustrations never reuse restricted surrounding pixels as a public rendition. Actual ImageContent/MIME and the final per-audience authorization remain mandatory.

## Bounded recovery

| Observed outcome | Continuation |
| --- | --- |
| Pending, timeout, lost admission reply or unknown result | Reconcile the same key/operation; return bounded pending |
| Historical Vibe verified receipt, process crashed before installing bytes | Legacy Vibe-only: reuse the receipt/exact-byte contract. Target Telethon: check the existing registered media locator and acquire bytes only when requested processing needs them; no compulsory provider checksum roundtrip. |
| Historical Vibe verified asset gives definitive HTTP 404/410 | Legacy Vibe-only: retire only that read attempt with compare-and-delete and admit a new read. Target Telegram: if locator/message/media is unavailable, report unavailable and reconcile original identity; never upload a duplicate source automatically. |
| Exact terminal blocked code `telegram_transport_unhealthy` or `provider_read_deadline` in the Vibe path | Persist next read key and 15-minute cooldown atomically; concurrent callers share the transition |
| Authorization failure, unknown terminal outcome or receipt conflict; historical Vibe digest mismatch | Preserve evidence and report the specific failure; no blind fresh-key retry. Digest mismatch is a legacy Vibe-only check, not an owner-approved Telegram acceptance gate. |
| FloodWait or confirmed transient dependency failure | Persist provider retry-after/cooldown, release capacity and resume fairly |

The guarded terminal recovery, keyed first verification GET, cooldown/fairness and missing-digest failure behavior were fixed for the **historical Vibe** path in the [19 October remediation](../reports/illustration-audit-remediation-20261019.md). Preserve historical evidence and compatible legacy runtime while it serves users, but **do not port a mandatory Vibe provider SHA gate or keyed GET to target Telethon**. Direct Telegram/new-session/reset behavior requires its own presence/locator/UNKNOWN target acceptance.

## Retention and verification

Byte-cache expiry does not erase unresolved attempt identity or a still-live locator dependency. Keep live operation state and compact receipts until terminal reconciliation/checkpoint requirements are satisfied; they are domain recovery state, not arbitrary copies of the entire hot journal. Completed payloads/receipts follow the bounded retention in the handoff. Older historical backups do not pin an unlimited local replay tail.

The versioned native-text continuation optimization in [ingestion](../ingestion.md) reuses an exact page/render reference only inside the same live visual context after actual image delivery. New/missing context or changed render requires real pixels again. It does not let a locator outlive its pinned preview, replace a required independent strip read, or turn a reference into visual-review evidence.

Verify slow completion, lost response, restart, concurrent admission, crash after payload return, current permission denial, deleted/inaccessible Telegram media, stale locator, partial result and UNKNOWN receipt conflict. Keep missing/mismatching digest checks **only** in historical Vibe and independent PG/code/snapshot integrity suites. Mock keyed Vibe reads do not prove target direct Telethon; acceptance verifies the actual registered Telegram photo/document exists and is accessible to the authorized session, without compulsory native-byte SHA/download readback. Test a recompressed photo as a positive case and an absent locator as a negative case.
