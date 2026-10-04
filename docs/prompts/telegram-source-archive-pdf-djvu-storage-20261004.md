# Regional Knowledge — correct storage boundary, dedicated Telegram archive and PDF/DjVu thin ingestion

Date: 2026-10-04

This is an implementation task after the robust-import acceptance currently on
canonical main.

Read first:

- docs/reports/thin-ingestion-telegram-source-archive-decision-20261004.md
- docs/storage.md
- docs/architecture.md
- docs/ingestion.md
- docs/reports/robust-book-import-dedup-illustrations-media-acceptance-20261004.md

Also inspect VibePublish current runtime. The owner has created a separate
Telegram session secret named exactly:

`TELEGRAM_KNOWLEDGE_BASE`

Do not ask the owner to recreate it.

## Goal

Move Regional Knowledge to the intended compact storage boundary:

1. exact original source books are durably archived through VibePublish in
   Telegram topic `https://t.me/c/4368830579/2`;
2. extracted illustrations continue in topic
   `https://t.me/c/4368830579/4`;
3. both use the dedicated VibePublish Telegram connection/session
   `TELEGRAM_KNOWLEDGE_BASE`;
4. Supabase stores parsed chunk text/search material and pgvector/FTS state;
5. the ~1 GiB object-store bucket becomes bounded staging/cache, not permanent
   corpus storage;
6. ingestion supports PDF and DjVu as thin deterministic source containers;
7. ChatGPT remains the only semantic recognizer/parser.

## VibePublish prerequisite

Inspect the existing multi-connection/session model before changing it.

The current source historically starts the ordinary production Telegram worker
with `TELEGRAM_VIBE_PUBLISH`. Wire the Knowledge Base session as a distinct
provider connection/lane rather than replacing the ordinary session.

Use the smallest production shape supported by current VibePublish: either a
second provider worker instance or existing multi-connection runtime. Do not
redesign VibePublish.

Required properties:

- distinct connection identity for `TELEGRAM_KNOWLEDGE_BASE`;
- ordinary VibePublish Telegram operations continue on their current connection;
- Knowledge source/illustration operations route only through the KB connection;
- provider lane and rolling media-budget state are isolated by connection;
- durable request/replay/readback semantics stay unchanged;
- same Telegram account may still receive upstream account-wide FloodWait and
  must recover normally.

Extend media-store binary ingress only as needed to support generic Telegram
DOCUMENT sources such as PDF and DjVu, not only image documents.

Do not inspect, OCR or semantically parse source files inside VibePublish.

## Source archive contract

For an admitted source after exact SHA-256 is known:

- target topic: `https://t.me/c/4368830579/2`;
- provider form: Telegram DOCUMENT;
- stable request key derived from owner + logical document + source SHA;
- exact bytes;
- preserve useful filename;
- immutable origin:
  - system = regional_knowledge
  - ref = knowledge://documents/<document_id>/source
  - sha256 = exact source hash.

Persist the verified media-store entry ref in Regional Knowledge only after exact
provider readback.

Exact replay produces zero additional Telegram documents.

The existing illustration contract continues in /4 on the same dedicated KB
connection.

## PDF/DjVu source abstraction

Replace PDF-only product assumptions with a small source adapter protocol.

Mandatory format support:

### PDF

- signature/container validation;
- page count;
- requested page render;
- existing native embedded text/block hints where available.

### DjVu

- signature/container validation;
- page count;
- requested page render;
- expose embedded text only if already present in the DjVu container and cheaply
  retrievable.

A deterministic DjVu decoder/renderer is allowed.

Explicitly forbidden:

- OCR;
- Tesseract;
- VLM;
- LLM;
- automatic semantic layout;
- automatic caption/entity extraction.

ChatGPT sees page images and decides/transcribes semantics.

Keep `book_pages` format-agnostic at the MCP contract level; do not expose two
parallel ingestion APIs for PDF and DjVu.

## Supabase text migration

Add durable chunk source text to Supabase using the smallest schema that preserves
current ACL/RLS.

Recommended:

- `rkb_chunks.source_text text`, or an equivalent one-to-one chunk-text table if
  measurements show a clear benefit;
- keep `text_sha256`;
- keep existing `search_material` and `search_material_sha256`;
- FTS remains derived from deterministic search material/source text as currently
  accepted;
- E5/BGE vector tables remain unchanged except for reading text from Postgres.

Backfill existing active/historical chunks from the verified legacy text
projections before changing read paths.

Verify exact hashes for every migrated chunk.

Switch `fetch`, indexing and graph discovery away from permanent S3 text range
reads.

Do not delete legacy text projections until readback proves the Postgres text
migration complete.

## Bounded object-storage lifecycle

Add the minimal delete/GC capability currently missing from ObjectStore.

After migration:

- do not permanently create page-render objects;
- do not permanently create text-projection objects for new revisions;
- do not retain every staged-graph snapshot;
- do not retain illustration crops after their provider archive is verified,
  unless an explicit short-lived cache policy keeps them;
- original source may remain temporarily until Telegram source archive is
  verified.

For staged graph recovery, retain at most the latest graph needed by an active
ingestion; superseded snapshots are GC-eligible. After finalize, retain no
intermediate graph object unless a documented recovery requirement needs one.

Run a guarded GC of old unreferenced objects only after code/readback is accepted.
Never bulk-delete first.

## Existing source migration

For existing real source roots, including Gause:

1. use exact existing source bytes;
2. archive each unique owner/source SHA once to topic /2 through the dedicated KB
   connection;
3. verify native Telegram/VibePublish readback and SHA;
4. write archive refs/status to Supabase;
5. only after successful verification may the corresponding permanent S3 source
   become GC-eligible.

Do not semantically reimport Gause in this task.

Historical ambiguous duplicate roots remain explicit; do not silently merge them.

## Illustration storage correction

Existing illustration semantics remain.

For future imports:

- store page/bbox/caption/model observation in Supabase;
- generate crop deterministically from source;
- archive eligible crop to /4 via the KB connection;
- persist verified provider ref + source crop SHA;
- durable S3 crop is not required after verified archive/readback.

`illustration_fetch` may use a short cache or fetch the Telegram illustration
document; if unavailable, it may regenerate from the archived source file.

## Acceptance

Use real public product paths where practical.

Prove:

1. ordinary VibePublish Telegram traffic still uses its original connection.
2. KB source/media traffic uses `TELEGRAM_KNOWLEDGE_BASE`.
3. a small PDF source is archived to /2 with exact hash/origin/readback.
4. a small DjVu fixture is archived to /2 and `book_pages` renders at least two
   model-visible pages.
5. no OCR/semantic recognizer runs for either format.
6. identical source replay creates zero additional Telegram source files.
7. source archive outage leaves bounded staging pending rather than losing bytes.
8. existing Gause exact source is archived to /2 once without semantic reimport.
9. chunk text backfill hashes match all migrated legacy chunks.
10. fetch/search/E5/BGE/graph behavior is unchanged after moving chunk text to
    Supabase.
11. current E5/BGE active coverage remains complete.
12. new import no longer requires permanent text projection/page renders.
13. staged graph replacement does not accumulate immutable full snapshots.
14. at least one verified illustration remains fetchable after its temporary S3
    crop is removed.
15. guarded GC reports bytes/objects before and after and never deletes a still
    referenced object.
16. RKB and VibePublish full relevant tests/CI pass.
17. exact production runtime readback is recorded.

Do not stress the real Telegram topics with bulk synthetic uploads. Use only the
minimum real canaries needed; use deterministic/fake provider tests for queue
volume.

## Expected end state

```text
Telegram TELEGRAM_KNOWLEDGE_BASE
  /2 exact source books: PDF, DjVu, ...
  /4 illustrations

Supabase
  semantic corpus + source refs
  source_text + search_material
  FTS + E5/BGE pgvector
  graph + ACL + revisions

S3
  bounded staging/cache only
```

Create:
`docs/reports/telegram-source-archive-pdf-djvu-storage-acceptance-20261004.md`

At the end return exact RKB/VibePublish SHAs, Telegram source/illustration
readbacks, DjVu acceptance, Supabase storage deltas, S3 bytes before/after GC,
CI, and YES/NO whether corrected ChatGPT-led Gause reimport should start.
