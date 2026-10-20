# Durable archive read continuation

Updated 2026-10-20. Target contract: [single implementation handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md). This document describes one resumable read lifecycle across the DevCoveer control journal and Kaggle processing; it does not claim the new deployment is complete.

This durable lifecycle applies to accepted resumable archive/render work, not ordinary ephemeral `read_query`. Text search uses the same admission authority without fsync, remote seal, durable RESULT_ACK/RESUME or archive preflight. Its bounded asynchronous telemetry does not write to the queue SQLite database or contend for its write locks. A read stage inside an accepted durable question retains that question's existing context and recovery contract without a second intake.

## Admission and identities

Persist the logical request, attempt key, exact source/entry/hash, artifact purpose, actor/audience/scope and dependency versions before provider I/O. Commit admission before returning an operation identity. A bounded `pending` response is not a missing-original verdict. A new chat/process resumes the same work; status reads never create a fresh import, semantic request or publication.

The Vibe keyed-get path stores the opaque owner/issuer/archive-entry scope, random request key and provider operation ID. Repeating the request rechecks current provider authorization/binding and resumes the same operation. An operation-ID conflict fails closed. Lost replies, process restarts and unknown provider outcomes do not authorize a fresh key. Use the existing dedicated same-host service identity, not a user's OAuth or owner bearer.

Direct Kaggle Telegram reads use exact registered native locators and genuine separately authorized sessions. They do not automatically acquire Vibe's keyed-get guarantees. Restart may reread bounded byte ranges; only one logical verified result is accepted. Do not claim exactly-once network transfer or invent provider operation IDs.

Retain the owner's cap: one exclusive genuine Telegram session lease per authorized Kaggle run; splitting text/archive/proof readiness does not authorize additional runs. A ready text worker remains usable during archive reconnect, FloodWait or a pending render. Release the affected archive/proof permit while waiting without disabling SQL/BGE search.

## Read, render and result are different states

Distinguish verified original bytes, render readiness, accepted localization, artifact readiness, accepted result and external delivery. Persist exact accepted payloads and IDs through the existing fenced controller; a local Kaggle commit or an image send alone is not durable acceptance. Current attempts reject stale new writes while replaying already accepted commands.

A locator is bound to source SHA/revision, page, render hash/dimensions, crop/rotation/transform, quote/span and pixel scope. Retain the exact preview/render or an exactly reproducible descriptor until locator callbacks are terminal. Image delivery ACK is not permission to evict outstanding localization input. If the same preview cannot be recovered, start an explicit new render/localization generation and reject old callbacks.

Multi-page or multi-item operations return allowed completed results with per-item `ready`, `pending`, `unavailable`, `denied` or `failed`. Do not discard a verified page because another page is slow, or reveal inaccessible inventory in an error. Deliver real MCP ImageContent with actual MIME and separate canonical/provider/delivery hashes.

## Current access at every boundary

Apply the [artifact access policy](../rights.md): accepted illustration renditions are public; quote stripes/page/crop scans and localization previews require owner or the existing `scan_proof_read` grant for demo/expert/local_historian; full originals retain separate private access. Verified category assignment creates the audited capability grant atomically under [rights.md](../rights.md); `claimed` does not. Proof rendering checks the current grant/scope; category removal/demo expiry and manual revocation remain effective, with no grant recreation on reads. A proof renderer uses purpose-scoped internal source access after caller authorization, never owner impersonation.

Check withdrawal, source revision, current policy, grants and audience on warm cache, retry, callback and final delivery. An old receipt or signed asset URL is not a permanent grant. Shared Slack `public_only` can receive public illustrations, not the union of members' private proof grants. Output geometry must actually fit its permitted artifact class/scope.

Current access checks do not require retransferring or fully rehashing an unchanged verified immutable object. Concurrent requests may share one bounded in-flight byte download and leased cache object keyed by exact source identity. Verify the trusted digest while streaming and on install/restore; changed identity/bytes or an integrity doubt requires reverification. Render/cache refs also bind page/transform/generation, artifact class and scope; public illustrations never reuse restricted surrounding pixels as a public rendition. Actual ImageContent/MIME and the final per-audience authorization remain mandatory.

## Bounded recovery

| Observed outcome | Continuation |
| --- | --- |
| Pending, timeout, lost admission reply or unknown result | Reconcile the same key/operation; return bounded pending |
| Verified receipt, process crashed before installing bytes | Reuse receipt and verify/install exact bytes |
| Definitive HTTP 404/410 for the asset of that verified receipt | Retire only that attempt with compare-and-delete; admit a new read, never a publication |
| Exact terminal blocked code `telegram_transport_unhealthy` or `provider_read_deadline` in the Vibe path | Persist next read key and 15-minute cooldown atomically; concurrent callers share the transition |
| Authorization failure, digest mismatch, receipt conflict or unknown terminal code | Preserve evidence and report the specific failure; no blind fresh-key retry |
| FloodWait or confirmed transient dependency failure | Persist provider retry-after/cooldown, release capacity and resume fairly |

The guarded terminal recovery, keyed first verification GET, cooldown/fairness and missing-digest failure behavior were fixed in the [19 October remediation](../reports/illustration-audit-remediation-20261019.md). Preserve these repairs; they are not new unresolved defects. Direct Telegram/new-session/reset behavior needs its own target acceptance.

## Retention and verification

Byte-cache expiry does not erase unresolved attempt identity or a still-live locator dependency. Keep live operation state and compact receipts until terminal reconciliation/checkpoint requirements are satisfied; they are domain recovery state, not arbitrary copies of the entire hot journal. Completed payloads/receipts follow the bounded retention in the handoff. Older historical backups do not pin an unlimited local replay tail.

The versioned native-text continuation optimization in [ingestion](../ingestion.md) reuses an exact page/render reference only inside the same live visual context after actual image delivery. New/missing context or changed render requires real pixels again. It does not let a locator outlive its pinned preview, replace a required independent strip read, or turn a reference into visual-review evidence.

Verify slow completion, lost response, restart, concurrent admission, crash after payload return, current permission denial, expired asset, missing/mismatching hash, stale locator, partial result and receipt conflict. Mock tests of keyed Vibe reads do not prove a real direct Telegram download; acceptance includes exact-byte native readback using the actual target path.
