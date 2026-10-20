# Telegram source archive and bounded staging

Updated 2026-10-20. Normative target: [single implementation handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md). Current runtime evidence is recorded separately; this document does not provision credentials or mutate the archive.

## Placement and source lifecycle

One PostgreSQL/pgvector corpus with BGE-only retrieval and source/image processing runs on Kaggle. DevCoveer holds the existing MCP/ASGI authorization/controller and bounded durable request/change/delivery state. The notebook connects outbound over one WSS channel; do not create another scheduler, full-corpus staging host or polling control plane.

Only GEO content migrates. All old non-GEO book text/chunks/vectors/stories/drafts/contributions/cases are excluded; fresh MCP import reexamines verified original bytes and accepts a new semantic graph. Legacy chunk backfills/migration017 are not target cutover. An existing verified archive can supply the exact original without copying its old accepted transcription or review stamps. Preserve model-accepted text, semantic boundaries, locators and IDs durably; source SHA plus parser version cannot regenerate them.

Original source identity includes exact bytes/SHA and edition; native archive identity includes connection/chat/topic/message/document/origin. Read that registered identity, not a filename search or a guessed message. Full PDF/DjVu uses topic 2; illustrations use **forum topic 4**. A public illustration remains public even though its recovery archive is private. A private archive locator is never an access grant.

## Direct reads and one uploader

Kaggle reads approved originals/media directly using genuine separately authorized Telegram sessions supplied for implementation. The owner named `TELEGRAM_RKB1` and `TELEGRAM_RKB2` (the second is currently empty): verify actual independent authorization and archive access at stage 0. Bind one exclusive session lease to each Kaggle run; simultaneous runs cannot exceed verified available sessions or Kaggle capacity. More sessions extend the existing pool. Copying an existing StringSession/auth key is not a new independent authorization. Keep credentials out of repository, notebooks' public output, reports and logs. Account-wide provider limits still apply; honor FloodWait and per-session state without treating multiple sessions as permission to bypass the account budget.

VibePublish remains the sole archive uploader and owner of its native provider queue/pacing. Its DevCoveer bridge uses the existing authenticated loopback HTTP endpoint and a dedicated nonowner service identity restricted to the archive destination and topics 2/4. Kaggle is not on that loopback host; use the existing controller/bridge route for upload work. No browser/human OAuth, owner bearer, extra daemon or alternate uploader is needed. Follow the [same-host policy](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/docs/operations/local-service-connections.md).

Existing transport configuration/identity/grants are infrastructure, not old content to delete at cutover. Provider-side target binding must be enforced; a service credential never becomes unrestricted owner access. Direct retrieval does not require new publications, a new mirror index or relaxed source ownership.

## Authorization and verified bytes

Apply [artifact access](../rights.md) before retrieval and again before delivery. Public accepted illustration renditions are distinct from restricted source-page/crop/stripe scans and private full files. Owner/demo/expert/local_historian proof access uses current `scan_proof_read` authorization; verified category assignment materializes the audited grant under [rights.md](../rights.md), while claimed/revoked/expired categories are not an alternate read path; purpose-scoped internal rendering may read an original only to produce that authorized artifact. It must not return the full original or impersonate its owner.

Shared Slack `public_only` may display public illustrations, without using members' private credentials. Recheck current source/revision, withdrawal, policy and any scoped grant on cache, retry and late callback. Never expose private native archive URLs/session material as public illustration links.

Before marking an archive verified, confirm native DOCUMENT identity, exact destination/topic/origin and bytes against a retained trusted expected SHA. Source SHA, canonical/display crop SHA, provider-rendition SHA and transport-delivery SHA are distinct. Missing or mismatching provider hash is a failure, not a reason to manufacture a new expected hash. Stable historical archive aliases may reference one verified identical original, but do not silently collapse separate edition/source identities.

## Deterministic source processing

Detect source format from container signatures. PDF uses PyMuPDF; DjVu uses DjVuLibre (`djvused`, `ddjvu`, `djvutxt`) through the existing source API. Rendering/native text extraction is deterministic. Semantic transcription, image/caption meaning and geometry decisions come from the calling model, not a new server OCR/VLM/LLM recognizer. `book_pages` remains one source API.

Do not archive every rendered page as an import or proof gate. Render bounded requested pages/regions from verified originals; page readiness and precise quote geometry are per-item. For proof, bind source SHA/revision, physical/printed page, dimensions/transform and preview hash, retain live locator dependencies, and return actual MIME/ImageContent. See [illustration/proof pipeline](illustration-pipeline.md).

## Admission, retry and fairness

Reserve the measured cache/staging/render budget before I/O. Count concurrent downloads, decompression, full originals, previews, generated crops and provider-bound copies; a per-file limit alone is not a total bound. Backpressure before admission must be resumable. Already accepted payloads retain a verified recovery path during provider outages.

Persist stable publication/read keys and attempt state. Unknown send outcome first reconciles the same operation; it never triggers a fresh publication. Only a proven zero-dispatch failed/blocked send may use Vibe's established `retry_failed` gate with the original operation and stable recovery key. Dispatched/uncertain work cannot use that escape. Respect provider retry-after and the existing minimum retry interval; release execution capacity while waiting.

Reads continue through [durable read continuation](archive-read-continuation.md). The released Vibe terminal-read recovery persists a next key and 15-minute cooldown only for `telegram_transport_unhealthy` / `provider_read_deadline`. Auth/digest/unknown failures do not mint keys. Verified-asset 404/410 is a separate bounded read-expiry path. Direct Telegram transfer can reread bounded chunks after restart; do not claim exactly-once network transfer.

Use one fair controller/maintenance owner. Source/vector/story backlog cannot starve accepted archive work indefinitely; ready receipts take precedence over cooling retries. Preserve the established Vibe account-wide rolling pacing, including all topics/album members and stricter FloodWait. A source upload does not consume a separate independent allowance merely by using another topic.

## GC and bounded recovery

Temporary original/crop eviction requires verified exact archive/recovery proof and no active import/read/render/locator dependency. Image delivery ACK alone does not close a localization callback. An unresolved attempt's identity is not evicted with its byte cache. Pending/unknown publication state must reach reconciliation or an explicit terminal decision before its live recovery metadata can expire.

Use the existing bounded maintenance path, registered objects, dry-run inventory and explicit replacement evidence; do not whole-bucket-delete or add a second GC service. Keep compact source identity/locator/provenance and receipts needed by current domain state, not all old payloads indefinitely. A later read reserves capacity again and checks the source/revision/current policy before restoring bytes.

Hot DevCoveer journal retention is bounded by the agreed hours/bytes; checkpoints, current replay bases and live operation state make truncation safe. Older Dataset/Yandex versions may remain historical recovery points with an explicitly older RPO rather than pinning all local journal segments. Before dropping the only accepted tail needed by a supported base, verify an independent durable checkpoint/tail. Measure Dataset version deletion/retention and creation overlap: switching a pointer is not evidence that old versions stopped consuming capacity. A daily remote backup plus only the last few local hours cannot guarantee a few-hour RPO if DevCoveer is lost. Use the handoff's actual checkpoint cadence and honest failure-specific restore claim.

GEO-only cutover permits an established bounded rollback backup; it does not require a permanent copy/export of non-GEO content. Rollback after new target acceptance preserves the exact target tail and receipts, not an overwrite with the old whole database.

## Historical baseline and target acceptance

The [19 October remediation](../reports/illustration-audit-remediation-20261019.md) and [live acceptance](../reports/illustration-archive-live-acceptance-20261019.md) record successful dedicated-service archive retrieval and closed keyed-GET/cooldown/fairness/digest/reserve findings. The former OAuth-rotation transition, migration017 backfill and earlier "M2M not done" state are preserved only in the [immutable old runbook](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/operations/telegram-archive.md); they are not active rollout instructions.

Accept the new path with native exact-byte original and topic 4 rendition readbacks, genuine session identity, cache eviction/restore, provider outages/FloodWait, denied full-file/public-scan requests, public illustration delivery, source withdrawal, reset with pending locator and target-tail recovery. Record actual release identities and per-capability outcomes. A healthy endpoint, archive row, old receipt or green CI is not a fresh native-byte acceptance.
