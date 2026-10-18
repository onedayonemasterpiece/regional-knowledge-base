# Telegram source archive and bounded staging

Apply migration 017 only after `verify_telegram_storage_migration.py` passes in
the isolated PostgreSQL database. `migrate_telegram_text.py` backfills **all**
legacy chunks from whole-projection and range hashes, then verifies every stored
chunk SHA and unchanged vector/corpus digests. It also recovers exact region text
from hash-verified staged graphs or exact matching historical chunk digests.
Legacy incomplete regions stay explicit and prevent their graph evidence GC.

`source_text` in Postgres is canonical accepted transcription. Fetch/indexing/
graph evidence prefer authorized Postgres text; legacy object reads remain a
migration fallback only. New finalize writes neither text projections nor page
renders. It writes exact region text, chunks and temporary crop bytes.

The existing single indexing owner reconciles at most four sources per bounded
archive pass, prioritizing already recorded Telegram readbacks, then admitted
publications, then new uploads. Illustrations reconcile independently of embedding
work, with VibePublish enforcing the shared native media rate budget. Use the mode-0600 resource grant
`RKB_VIBEPUBLISH_GRANT_FILE` with the dedicated KB destination alias,
`source_thread_ref=https://t.me/c/4368830579/2` and
`thread_ref=https://t.me/c/4368830579/4`. VibePublish must already run the distinct
`TELEGRAM_KNOWLEDGE_BASE` connection. There is no direct Telegram client in RKB.

Exact source origin includes the logical root URI and source SHA. Same-key replay
uses the original operation. Native DOCUMENT/topic/origin and downloaded SHA must
match before setting `source_archive_status=verified`. Historical duplicate roots
remain separate; their identical owner/source bytes share one explicitly recorded
`source_archive_origin_ref` and verified archive reference.

Source format is detected from container signatures. PDF uses PyMuPDF; DjVu uses
DjVuLibre (`djvused`, `ddjvu`, `djvutxt`). Install the
system `djvulibre-bin` package, or set `RKB_DJVU_ROOT` to the locally extracted
package root. Rendering/native text extraction is deterministic, with no OCR,
VLM, semantic recognizer or caption extraction. `book_pages` remains one API.

New source reservations use one capacity mutex and an 800 MiB default known-object
budget (`RKB_STAGING_MAX_BYTES`, at most 1 GiB). Pending/unverified sources remain
staged across provider outage/restart. Capacity exhaustion fails explicitly before
uploading more bytes. New graph updates retain an immutable current pointer;
superseded snapshots are GC-eligible, with one-hour reader/recovery grace.

Run `telegram_storage_gc.py <retained-report-path>` first, inspect its dry run,
then add `--apply`. GC only visits registered DB objects with explicit replacement
proof. It detaches migrated text/render/finalized graph/crop pointers and marks
binary metadata `deleted_at`; it never deletes catalog/source identities. Source
bytes become eligible only after exact archive proof and no active ingestion.
Illustration crops require a verified archive. Source reads fall back to verified
Telegram bytes with full exact hash checking; crop reads use provider bytes after
actor authorization. Set `RKB_STORAGE_GC_ENABLED=1` after guarded acceptance to
run at most one bounded 100-object pass per minute under the existing maintenance
owner. No whole-bucket deletion or independent scheduler is introduced.

Terminal source failures stay pending. A proven zero-dispatch blocked/failed
operation is retried through Vibe's existing `retry_failed` gate with its original
publication/revision/attempt and a stable recovery key. Dispatched or uncertain
outcomes are refused by that gate; they are never replaced with a fresh send.
Failed retries use a 30-second minimum retry interval. A deliberate new revision
after source-cache GC reserves capacity again, revives the existing cache metadata
under the document lifecycle lock and starts a fresh cache grace period.

## Private mirror OAuth rotation and recovery

All RKB clients sharing `RKB_VIBEPUBLISH_GRANT_FILE` must serialize refresh-token
rotation with a private, permanent sibling `.refresh.lock` file. A client
adopts an already-refreshed on-disk grant rather than reusing an old refresh
token; after HTTP 401 it first checks whether another client already replaced
the rejected access token. The grant stays owner/client/issuer bound and is
atomically written mode 0600. No credential contents are logged.

A lock prevents concurrent replay; it cannot repair a refresh-token family that
VibePublish already revoked, nor extend the OAuth family's fixed lifetime.
Recover only through normal PKCE authorization and **explicit owner consent**,
with the registered callback for a legitimate RKB client and its previously
authorized scope/bindings. Do not redirect a RKB grant into the ChatGPT
connector callback, modify registrations or issue a grant in SQLite as a
workaround. A missing/incompatible RKB callback needs a separately authorized
registration correction or dedicated RKB client first. Until provider DOCUMENT
readback and matching original SHA-256 succeed, source archive status remains
`pending`; unchanged book contents must not be reimported.

## Mandatory unified on-host connection policy (2026-10-18)

Design decision **Fixed**; M2M archive migration **Not done** until deployed and
the original document is recovered. Canonical: [DevCoveer same-host S2S](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/main/docs/operations/local-service-connections.md).
The previous VibePublish OAuth grant/ChatGPT callback is legacy failure state,
not a sanctioned cross-service connector. RKB's indexer should use an existing
loopback VibePublish endpoint with a dedicated service principal restricted to
archive operations on the KB group, topic 2 (original PDF/DjVu) and topic 4
(illustration files). No owner session/bearer, browser, refresh token, local
HTTPS roundtrip or new gateway. Preserve source UUID/SHA, 1st-pass extraction,
revision, journal operation key, existing `pending` state and media evidence.
Implement/verify provider-side target enforcement before switching credentials.
A green PR or healthy server does not prove archived bytes. Acceptance requires
provider get/readback and byte-for-byte original SHA-256 match; then mark verified.

## Dedicated loopback service bearer — code support (2026-10-18)

The archived-grant file may use `auth_mode=local_service` instead of user OAuth,
with `issuer=https://mcp-vibepublish.kenigevents.ru`,
`local_endpoint=http://127.0.0.1:18765`, a pinned HTTP `Host`, and a dedicated
non-owner `service_token`. Source/illustration code retains the same archive
URI, request key and native readback protocol. This mode rejects an HTTP endpoint
outside loopback and never calls OAuth refresh. Credentials stay in a private
mode-0600 file, never documentation, logs, GitHub or chat.

Prerequisites before switching the production file: VibePublish's exact topic
grants (2/4), a non-owner principal with scoped `media.store`, protected
service-token provisioning, current provider worker and hash-verified original
retrieval. **Code support is not deployment**. The previously accepted article,
10 stories and source revision must stay unchanged. Verify source_archive_status
by live MCP only after the original archive document is actually retrieved.

## Archive fairness under a sustained story-vector backlog (2026-10-18)

Story vectors retain normal priority, but their backlog must not starve
original-document archiving indefinitely. The existing indexing owner admits
one archive/mirror maintenance pass at least once per 60 seconds when no source
vector publication is pending, even if story-vector jobs remain queued. A
failed provider call retains the exact original and existing request identity;
it never causes a second publication. This is the existing indexer loop, not
an extra scheduler or service. A green CI does not replace live PDF SHA-256
provider readback and the `source_archive_status=verified` acceptance gate.
