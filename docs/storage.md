# Storage, privacy and lifecycle

## SQLite / Supabase v2 implementation boundary (2026-10-05)

The canonical task is [SQLite corpus + Supabase vectors + on-demand proof](prompts/rkb-sqlite-supabase-ondemand-codex-v2-20261005.md).
The old v1 page archive and capacity prompt are superseded. No local PostgreSQL,
page archive/topic/spool, whole-book readiness gate or new IAM is introduced.

The v2 release selects persistent SQLite with `RKB_SQLITE_CORPUS_PATH`.
SQLite owns corpus text, catalog, rights/ACL, page/region mapping, ingestion,
graph/POI and publication state. Supabase owns only E5/BGE vectors and minimal
ID/revision/hash/scope anchors. The existing application actor bridge authorizes
local reads and writes; MCP bearers never enter the vector plane. Fetch/catalog
and FTS lexical-only search remain available without Supabase. See the
[deployment and recovery runbook](operations/sqlite-v2.md).

## Storage boundary — SQLite corpus, Supabase vectors, Telegram originals

The current S3-compatible bucket is capacity-constrained (about 1 GiB) and must
**not** be treated as the permanent corpus archive.

Long-lived storage is split deliberately:

```text
VibePublish / Telegram, TELEGRAM_KNOWLEDGE_BASE connection
  topic /2 = exact original source files (PDF, DjVu, ...)
  topic /4 = extracted illustration documents

SQLite (persistent service state, WAL)
  = catalog / ACL / rights / revisions / graph / POI outbox
  = exact chunk/region text, search material and FTS5
  = source SHA/archive ref, page/region/illustration mapping
  = staged vector publication metadata (no embedding values)

Supabase Postgres
  = minimal chunk/document/revision/source/text hashes + owner scope anchors
  = unchanged pgvector E5/BGE embeddings and HNSW indexes

S3-compatible object storage
  = bounded ingestion/cache workspace only
  = temporary source copy while Telegram archival is pending
  = at most the current staged graph / temporary derived files
  != permanent corpus archive

local disk
  = bounded disposable work/cache
```

The exact original file is archived through VibePublish as a Telegram DOCUMENT
using the dedicated Knowledge Base Telegram session/connection
`TELEGRAM_KNOWLEDGE_BASE`. Regional Knowledge stores the verified media-store
reference and SHA-256; Telegram object identity never replaces application ACL.

The source topic is `https://t.me/c/4368830579/2`.
The illustration topic is `https://t.me/c/4368830579/4`.

Object storage may be used as a temporary safety buffer until exact Telegram
provider readback succeeds. Once the Telegram source archive is verified and the
active/staged state no longer requires the temporary object, the S3 copy must be
eligible for deletion/GC rather than accumulating indefinitely.

## Why not global content-addressed public keys

Global hash paths create an existence oracle: a caller can infer that another
user uploaded the same private file. The database stores SHA-256 for integrity and
internal deduplication, but external object identities are opaque and
user/document scoped.

Internal deduplication may be introduced later only if it never exposes object
existence or changes ACL semantics.

## Separate source privacy from content visibility

A crucial distinction:

- `source_visibility`: the raw PDF/scan uploaded by a person; default and
  normally permanent value is private.
- `content_visibility`: normalized text/metadata/search projection; private,
  workspace or public.
- `media_visibility`: derived illustration crop; may differ from both.

A public or statutory-access work uploaded from a private scan does **not** make
the user's exact PDF, annotations, ex libris, marginalia or scan metadata public
automatically.

## VibePublish / Telegram

VibePublish is the provider boundary for the Knowledge Base binary archive.

Use the dedicated Telegram session/connection `TELEGRAM_KNOWLEDGE_BASE` so
Knowledge Base source/media transfers do not occupy the ordinary VibePublish
Telegram connection lane. This isolates our own queue/rate budget; Telegram may
still impose account-wide/provider-wide limits independently.

Two private topics are reserved:

- `https://t.me/c/4368830579/2` — exact original book/source files as DOCUMENT;
- `https://t.me/c/4368830579/4` — extracted illustrations as DOCUMENT.

Regional Knowledge remains the semantic/ACL authority. VibePublish owns Telegram
transport identity, durable send/replay/readback and provider pacing.

A verified source entry stores immutable origin such as:

```text
origin.system = regional_knowledge
origin.ref = knowledge://documents/<document_id>/source
origin.sha256 = <exact original bytes sha256>
```

A verified illustration entry continues to use
`knowledge://illustrations/<id>`.

Search uses SQLite FTS and Supabase vectors, independently of a live Telegram
request. Reprocessing a source may require the archived original; if that archive
is unavailable, the existing parsed revision remains searchable but source
reconstruction must report unavailable rather than fabricate bytes.

## Deletion

Deleting a private document removes:

1. active retrieval rows;
2. derived object-storage objects;
3. original object after retention/recovery policy;
4. service-owned media mirrors where deletion is supported.

If a separately curated public corpus item exists, it has its own provenance and
lifecycle; deleting one user's private source cannot silently delete a public
canonical record.

## Chunk text and retrieval material

Canonical accepted text lives only in persistent SQLite. Supabase has no permanent corpus text, catalog, geometry, graph or POI copy.

Keep two separate values:

- `source_text` — exact text/transcription accepted by ChatGPT as source evidence;
- `search_material` — deterministic retrieval augmentation containing source text,
  printed captions and explicitly labelled model observations.

Each keeps its own SHA-256 identity. FTS and E5/BGE indexing are derived from
these fields. A model observation must never be returned as if it were a printed
quotation.

The legacy text-projection/range-read path may remain temporarily during migration,
but new imports must not require a permanent text blob in S3.

## Attached source ingestion — PDF and DjVu

ChatGPT file parameters provide a temporary `download_url` and stable
`file_id`. The start call downloads the source immediately; the temporary URL is
never persisted.

Real corpus sources include at least PDF and DjVu. Source type is detected from
bytes/container signature, not trusted filename extension.

The source path is deliberately **file-backed**, not whole-file-in-memory:

```text
temporary ChatGPT HTTPS URL
  -> DNS/public-address preflight
  -> DNS-pinned streaming download
  -> bounded local temporary source
  -> SHA-256 + deterministic format/container inspection
  -> VibePublish archive to Telegram /2
  -> optional temporary S3 safety copy until verified archive/readback
  -> local temporary file removed
```

Format adapters may deterministically report page count, render a requested page
to an image and expose an embedded/native text layer when the container already
has one. They must not perform OCR, semantic recognition, captioning or layout AI.

The downloader:

- accepts HTTPS only and port 443;
- rejects credentials, fragments, redirects, localhost and IP-literal URLs;
- resolves the hostname before download and rejects any non-global address;
- pins the approved DNS answers into the HTTP connector for the request;
- ignores proxy environment variables and cookies;
- disables automatic content decompression and requests identity encoding;
- streams in bounded chunks instead of loading the whole PDF into Python heap;
- defaults to **128 MiB** maximum source size;
- allows an operator-configured `RKB_MAX_PDF_BYTES`, hard-capped at **512 MiB**;
- requires a supported source signature/container before admission; initially PDF
  and DjVu are mandatory production formats.

Production should additionally use restrictive egress/network policy. The model
never receives object-store credentials or raw object keys.

SQLite keeps only an opaque `source_object_id`, never the raw S3 key in the
user-readable ingestion row.

A failed start remains private. Retrying the same attached `file_id` and exact
source hash reuses the existing ingestion/document and reconciles the
deterministic source object instead of creating a second document or object row.

## Page reads

`book_pages` first authorizes the ingestion job. It then resolves the exact
temporary/archive source, verifies full SHA-256 and asks the deterministic source
adapter to render only the requested small page batch.

For PDF this may use PyMuPDF. DjVu uses a deterministic DjVu decoder/renderer.
That decoder is transport, not recognition.

The returned page image is what ChatGPT reads. Optional native/embedded text is a
hint only and never proves visual completeness.

Ingestion still performs no automatic OCR. On-demand scan quote proof may use a bounded Flash-Lite reader (at most two calls/page), independently validate visible text/crop reading, and draw stripes in code. This helper never changes accepted corpus text.

## Staged graph and finalization

Semantic parsing does not write half-complete page graphs into active database
tables. Each `stage` call merges into the canonical staged graph and replaces its current
immutable, content-hashed pointer. Superseded snapshots become GC-eligible after
the recovery grace; only the latest active-ingestion graph is required. The ingestion row exposes
only its opaque object ID.

Model-facing keys are deliberately short and local. The server deterministically
derives region, illustration and chunk UUIDs from the document/revision/page and
those keys. Semantic chunk text is assembled server-side from referenced source
regions, so the model does not need to duplicate long text inside a second tool
argument.

Only `finalize` materializes a staged revision into SQLite. It creates:

- exact SQLite chunk/region source text with SHA-256 per chunk;
- page and region provenance rows without permanent page-render objects;
- region relations;
- temporary exact illustration crops derived from source-page bbox coordinates,
  eligible for deletion after verified archive;
- illustration provenance rows;
- local FTS and pending vector-publication metadata.

The existing indexers publish E5/BGE remotely without holding a SQLite writer
lock. Activation requires both spaces and exact source/revision/ID/text hashes;
a pending replacement leaves the previous active revision searchable.

The activation RPC independently checks complete page indexes, unresolved review
flags, textual-region coverage and cross-document provenance before changing
`active_revision`. Ordinary authenticated tokens cannot directly patch the
active revision or mutate an already active materialized revision.

## Implemented migration and lifecycle

Migration 017 is historical. The v2 exact snapshot and verified SQLite backup
replace remote corpus details; no OCR or embedding regeneration is performed. New imports omit permanent page renders/text projections.
Verified source/illustration archives replace temporary source/crop objects;
registered-object GC preserves active-ingestion and unverified-source recovery.
The [operator procedure](operations/telegram-archive.md) defines capacity bounds,
one-hour cache grace, exact source readback and opt-in bounded maintenance GC.


## Private originals and proof cache

`RKB_ORIGINAL_CACHE_DIR` defaults to the persistent service state directory's
`originals` subdirectory, outside the checkout. Originals are keyed internally by
exact source SHA, bounded by `RKB_ORIGINAL_CACHE_MAX_BYTES` (default 512 MiB).
`RKB_ORIGINAL_CACHE_IDLE_HOURS` defaults to **4 hours**. Each authorized original-byte access
renews the idle time. An authorized warm proof also renews an already cached original without
downloading an absent one. Private source ownership is rechecked before warm reads;
parsed text grants do not imply access to the original.

Cross-process file leases protect installs/read copies and prevent cleanup of
in-use files. Existing indexing maintenance checks idle originals and proof
cache every minute, even without MCP requests. Interrupted download partials are
removed only when no lease is held. Eligible least-recently-used originals are evicted to reserve incoming size,
even at capacity. A full cache with only leased entries fails closed
instead of exceeding its installed-original budget. Download working copies and
SQLite/WAL/backup bytes are separate measured disk consumers.

Proof cache is limited to 64 MiB and requested evidence only. It is keyed by
source SHA, exact fragment/revision/quote, region, locator/model version. Repeated
proof reads reauthorize access and return cached WebP without new inference.
No page images or proof results are automatically published or archived.
