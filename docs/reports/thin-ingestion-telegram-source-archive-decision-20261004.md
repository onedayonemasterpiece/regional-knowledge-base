# Regional Knowledge — thin ingestion, Telegram source archive and PDF/DjVu boundary

Date: 2026-10-04

## Product invariant

Regional Knowledge MCP is a thin deterministic document orchestrator.

It does **not** contain OCR, VLM, layout AI, semantic extraction or another LLM.
ChatGPT performs recognition/transcription and semantic understanding.

Supported real source containers must include at least:

- PDF;
- DjVu.

A format adapter may only:

- validate/detect container bytes;
- report page count/basic deterministic metadata;
- render a requested page to model-visible image bytes;
- expose an embedded/native text layer already present in the source;
- provide deterministic page coordinates/byte hashes where available.

The page image is the authoritative visual material presented to ChatGPT. Native
text is only a hint and never proves source completeness.

## Dedicated Telegram transport

Knowledge Base binary traffic uses the dedicated VibePublish Telegram
session/connection:

`TELEGRAM_KNOWLEDGE_BASE`

This is intentionally distinct from the ordinary VibePublish Telegram session so
large book/media traffic does not occupy the ordinary provider lane.

Two private Telegram topics are reserved:

- source books/files: `https://t.me/c/4368830579/2`;
- extracted illustrations: `https://t.me/c/4368830579/4`.

The dedicated connection provides application-level queue/pacing isolation. It
does not imply Telegram itself can never apply account-wide/provider-wide limits.

## Source archive

The exact original user file is archived as a Telegram DOCUMENT via VibePublish.

Persist in Supabase:

- document/source identity;
- owner;
- exact source SHA-256;
- source format;
- original filename/media type where useful;
- verified VibePublish media-store entry ref;
- archive status.

Use immutable origin:

```text
origin.system = regional_knowledge
origin.ref = knowledge://documents/<document_id>/source
origin.sha256 = <exact original bytes SHA-256>
```

Exact-source dedupe remains owner + SHA-256.

A corrected import of the same bytes uses `new_revision` on the same logical
document rather than creating another source archive entry.

## Object storage role

The current ~1 GiB object-storage bucket is not the corpus archive.

It is a bounded staging/cache surface only:

- temporary source copy while source archival/readback is pending;
- latest recoverable staged graph if needed;
- short-lived page/crop cache.

It must not permanently accumulate:

- every original book;
- page JPEGs for every revision;
- illustration crop replicas;
- immutable copies of every intermediate staged graph;
- normalized text projections.

After exact Telegram source readback and successful materialization, temporary
objects are GC-eligible according to recovery needs.

## Supabase role

Supabase/Postgres is the durable semantic/retrieval plane:

- documents/revisions/ACL/rights;
- source archive refs/hashes/formats;
- chunk source text;
- deterministic search material;
- pages/regions/relations/illustration metadata;
- entity graph;
- FTS;
- E5 pgvector(384);
- BGE pgvector(1024);
- ingestion/index/discovery state.

Large original binaries stay out of Postgres; compact parsed book text does not
need to be exiled to S3.

## Page and illustration behavior

During active ingestion, the temporary source can be rendered repeatedly without
re-downloading Telegram.

After ingestion, if a page must be revisited:

```text
Telegram source archive
  -> bounded local/temp cache
  -> deterministic PDF/DjVu page render
  -> ChatGPT
  -> cache discarded
```

An illustration is canonically described by source document/revision, page,
bbox, caption, optional model observation and hashes. The binary crop can be
regenerated from the source and is additionally archived in Telegram /4 when
eligible.

## Failure behavior

Telegram/VibePublish outage must not corrupt parsing state.

A source temporary copy must not be deleted until a verified durable source
archive exists. If Telegram archival is pending, bounded staging retains the
source and exposes pending state.

If an old Telegram archive is later unavailable, existing parsed/searchable
revisions remain usable. Reprocessing reports source unavailable rather than
inventing/reconstructing bytes from derived text.

## Do not add

- Tesseract or another OCR engine;
- server-side vision-language model;
- server-side semantic chunker;
- DjVu OCR pipeline;
- local vector database;
- new media scheduler;
- second copy of all binaries in object storage.

The only required format-specific code is deterministic container decoding and
page rendering.
