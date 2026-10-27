# Telegram source archive and bounded staging

Updated 2026-10-21 from the [owner's explicit corrections](https://mira-projects.slack.com/archives/C0C8RT9JB2P/p1792573398928879). Normative target: [single handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md) and [owner alignment](../designs/rkb-owner-alignment-20261021.md). The [audit](../reports/rkb-requirements-code-audit-20261021.md) distinguishes preserved legacy source, target wiring, tests and live acceptance. This document does not provision credentials, publish files or certify deployment.

## Placement and ownership

One PostgreSQL/pgvector working corpus with resident BGE runs in Kaggle. Source staging, final-input tokenization, indexing and ordinary domain operations execute there. DevCoveer retains existing MCP/OAuth, admission, run ownership, the single owner-only import lease and bounded control/replay state, not another working corpus or vector store.

**Telegram upload and download both run directly from Kaggle through Telethon. VibePublish is not in the target archive path.** Large originals and media do not pass through DevCoveer. Reuse accepted source/figure identifiers and useful operation semantics; do not copy the Vibe provider framework, extra daemon or a second uploader into the notebook. Independently authorized social publication integrations are separate from this archive.

Original identity preserves edition, source revision and source metadata; archive identity preserves the exact configured chat/topic/message/media locator. A filename or nearby message is not a substitute. Existing topic 2 originals and topic 4 illustrations are retained after checking actual destination configuration. Archive locators are not access grants and private archive URLs/credentials do not become public illustration links.

Only GEO content migrates. Old books, chunks, vectors, stories, drafts, contributions and cases are not copied into the new corpus; their functions remain required for fresh data. An existing original can be read from its registered archive without copying old accepted transcription/review stamps. A source hash or parser version cannot regenerate a model's accepted choices.

For newly accepted originals, `original_archive` uses the existing leased
Kaggle Telethon client, records the message/document locator in the intake row
and creates a receipt only after document presence is checked. This first PR
does not yet replace S3-based restoration of lost native handles: Telegram
restore and eight historical-source migration belong to dependent PR2.
Historical immutable receipts are not rewritten.
A retained `original_telegram_send_unknown` result is reconciled on the next
normal status call through a fresh `original_verify` metadata observation.
It searches the exact intake caption in the configured topic and never sends
when no document is visible. Both original unknown receipts remain unchanged.
After reviewed native/API rollout, normal owner `book_ingest(status)` continues
the archive of an already verified original when its Telegram sidecar is absent,
including books with accepted stage state. Each call progresses original status
once. Current owner authorization and the owned intake row select the source;
the normal flow calls the existing `original_acquire`
restore phase (S3 GET into Kaggle) and the native `original_archive` phase
(Telethon send from Kaggle) before writing a sidecar locator. It does not
download original bytes to Dev. No repair command, script, forged Principal or
standalone runtime access is required. Archive failures are logged and leave
the existing verified original usable; the immutable old receipt is retained.
Existing sidecars avoid S3 HEAD checks.
Repeated status calls reuse the restored handle for the same immutable
receipt. If an unresolved send also loses native bytes, the archive remains
unresolved without a fresh restore/upload; the verified original stays usable. Existing
sidecars are reused, so eight locators need not mean eight new documents.
Uploads preserve the retained original filename (or an original.pdf/.djvu
fallback) and the native-verified format's MIME, rather than the workspace .source name.

## Media success: presence, not custom byte verification

The owner explicitly selected Telegram as a trusted media store. After an upload, verify that the returned registered locator resolves to an accessible expected photo/document in the intended archive. Retain that native reference and operation outcome. **Do not require another full download, a provider checksum or comparison of uploaded/downloaded SHA values before archive success.** Telegram photo processing may change bytes; this is not evidence of failure.

This exception is scoped to Telegram media transport. It does not remove current access checks, source/edition binding, exact command idempotency, code artifact checks or PostgreSQL snapshot integrity. Reattaching a different original is still a separate source-identity concern. Do not relabel a snapshot or command as Telegram media to skip its own recovery checks.

An unavailable/deleted media object is reported explicitly. It does not silently become a successful empty image, another edition or permission to republish automatically. A locator's existence proves that archive object, not that all new PG knowledge has already reached a Dataset checkpoint.

## Illustrations: photo and conditional high-resolution document

Preserve chunk↔figure links, source/page/revision, printed captions and separately labelled model visual descriptions. Normal delivery uses a Telegram photo. Save an additional high-quality document only when the source resolution exceeds what the actual pinned upload path and returned photo rendition preserve. Record both locators when two forms are needed; do not create two files for every figure automatically.

The photo/document decision uses observed dimensions and actual provider behavior, not an invented universal 1280/2560 limit. Format/aspect rejection has an explicit outcome and supported fallback. No post-upload byte hashing is introduced to determine photo quality. On high-quality request use the retained document when available, and report its absence honestly otherwise.

When a relevant returned chunk has illustrations, the calling model fetches the real pixels and decides visual relevance. Captions/descriptions help retrieval but do not replace looking at the image. Ordinary text search remains independent of archive readiness; media can be pending/unavailable separately.

## Sessions, one import and simple pacing

The owner supplied configuration names `TELEGRAM_RKB1` and `TELEGRAM_RKB2`; historical mention that the second was empty is not a current readiness observation. Verify actual independent authorization and archive access before use. One authorization is exclusively leased to a run. Copying a session string is not a new independent authorization. Credentials stay out of Git, logs, reports and automatically published notebook outputs.

There is exactly one active owner-only book import. One archive uploader uses configurable pacing and obeys actual FloodWait/retry-after. Do not claim20files/min as a universal provider quota or build a distributed global limiter solely for hypothetical parallel imports. Do not multiply sessions or change accounts to bypass limits. Current run/session ownership remains necessary despite this simplification.

Persist the minimal operation/native reference needed to resume an interrupted upload and avoid duplicate publication. Lost send acknowledgment is UNKNOWN, not proof that nothing was sent; reconcile the same operation before retry. If a result cannot be resolved reliably, keep it explicitly unknown instead of blind resend. This is operation bookkeeping, not cryptographic verification of Telegram. Release waiting resource permits and continue independent reads.

## Access and requested page/proof work

Apply [artifact policy](../rights.md) before access and at delivery. Accepted illustrations are public, page/quote scans require owner or verified demo/expert/local_historian with current `scan_proof_read` and permitted audience/scope, full PDF/DjVu and private drafts have separate grants. Shared Slack never unions participants' private rights. A service cannot impersonate the owner or disclose the complete original while rendering a permitted proof.

The calling model owns semantic reading. If the client already exposes the real source pages, import does not trigger another backend parse/render. Existing PDF/DjVu helpers supply only requested missing pixels or proof pages; native text is optional assistance, not an import acceptance gate. No backend OCR, hidden weaker extraction model, mandatory all-book scan or archive of every rendered page is introduced.

For a stripe request: found chunk → its exact page → requested page pixels → model-selected bbox/polygons → deterministic drawing. Reuse accepted geometry only for the same source/render/quote. Retain preview dimensions/transform/generation while a locator callback is live; stale geometry requires a new localized request. Current rights apply to both preview and final output. See [illustration/proof](illustration-pipeline.md).

## Resource bounds and caches

Bound original/crop/render buffers and streaming sizes; reserve real CPU/RAM/disk capacity before expensive work. Same-source/page consumers share a single in-flight download/render when safe. Use registered source/media identity and local cache generation to reuse warm content; do not re-download/hash/repack an unchanged object on each read. Auth/class/geometry/scope remain part of cache issuance checks. A public crop cannot reuse restricted neighboring page text as public output.

Text search, one import, media and snapshot work share measured permits with a protected search reserve. Ready reads dispatch immediately, never wait for a full batch or Telegram preflight. `text_search_ready`, `archive_read_ready` and `proof_ready` remain independent inside the authorized run. FloodWait or original cache miss does not unload healthy PG/BGE or justify a replacement notebook.

## Eviction and independent database recovery

Temporary bytes can be evicted when the required media remains accessible by its registered locator and no live ingestion/read/render/locator dependency needs the local copy. Preview delivery alone does not terminate a localization callback. Unresolved operation metadata is not expired merely with byte cache TTL. Use bounded existing maintenance, not a second GC service or account-wide deletion.

PostgreSQL recovery is separate: the versioned Kaggle Dataset is primary persistence; Yandex S3 receives an independent weekly backup of confirmed state. Preserve exact accepted model results, domain outcomes, IDs and replay ordering. Final durable status requires proven external coverage; newer uncovered work remains pending. The old online S3 journal is an existing implementation, not implicit authority to replace Dataset primary; its successor binding is explicitly resolved under the [handoff](../prompts/rkb-scalable-kaggle-implementation-20261019.md).

A weekly backup and a few local hours do not by themselves prove zero loss after simultaneous Kaggle/Dev failure. Keep actual persisted cursor, pending age, Dataset version and backup age distinct. Never remove the sole accepted recovery copy or overwrite newer target work with an old SQLite rollback snapshot. These database guarantees do not add custom Telegram media hashes.

## Historical lessons and target acceptance

The [19 October remediation](../reports/illustration-audit-remediation-20261019.md), [live report](../reports/illustration-archive-live-acceptance-20261019.md) and [old immutable runbook](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/a8cf41b50b8ccb62409d046d009d53c09d74b1d7/docs/operations/telegram-archive.md) describe the former Vibe route. Preserve lessons about duplicate sends, expired temporary assets, bounded retries and fair progress. Its custom digest/keyed-GET/cooldown implementation is not a mandatory target protocol and its successful tests do not certify Telethon.

Target acceptance includes actual direct uploads/reads, photo/conditional document, media presence without checksum comparison, exclusive session, one import, FloodWait/UNKNOWN, normal cache eviction/restore, current denial/withdrawal and real chunk-linked image delivery. Do not delete production data to manufacture a test. Record exact source/runtime versions and per-capability evidence. A healthy endpoint, source file, old receipt or green CI is not full target acceptance.
