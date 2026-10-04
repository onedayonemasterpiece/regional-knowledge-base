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

The existing single indexing owner archives one source per pass and reconciles
illustrations independently of embedding work. Use the mode-0600 resource grant
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
