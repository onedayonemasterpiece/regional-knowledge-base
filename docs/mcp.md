# MCP surface and model optimization

## Tool surface

Keep the public model-facing API small and goal-oriented:

1. `search(query)` — standard read-only search compatible with ChatGPT company knowledge/deep research.
2. `fetch(id)` — standard read-only fetch with full evidence, provenance and illustration descriptors.
3. `book_find(query, limit<=8)` — bounded deterministic title/author lookup over books the caller may read.
4. `book_ingest(...)` — resumable ingestion workflow for a new attachment or a new revision of an archived source.
5. `book_pages(...)` — read-only staged page batches returned as model-visible MCP image content.
6. `document_access(...)` — inspect/change visibility and sharing, subject to rights and role checks.
7. `profile()` — tiny identity/profile read for clients that display connected account identity.
8. `illustration_fetch(id)` — authorized canonical crop as ImageContent, with
   printed caption and explicitly labelled model observation metadata. Available
   in the full profile; Live retains its existing bounded evidence surface.

The normal user does not supply `duplicate_policy`, document UUIDs, cursors or lifecycle commands.
For an attached new source the model uses `start`; for a named existing book it resolves
the book with `book_find` and uses `reprocess(document_id)`. Internal attached-source
metadata still supports `duplicate_policy=reuse|new_revision` for compatibility and tests.
See [ingestion](ingestion.md) for source identity and visual review requirements.
Chunk `fetch` includes illustration URI, source page/bbox, caption, observation,
crop provenance, visibility/rights and a verified private mirror reference when
present. An observation never replaces the returned printed `text`.

Internal vector, FTS, object-store, crop and embedding operations are not model tools.

## Search/fetch compatibility

`search` returns:
```json
{"results":[{"id":"chunk-or-document-id","title":"...","url":"https://..."}]}
```

`fetch` returns:
```json
{
  "id":"...",
  "title":"...",
  "text":"...",
  "url":"https://...",
  "metadata":{
    "document_id":"...",
    "pages":[12,13],
    "provenance":[],
    "footnotes":[],
    "illustrations":[]
  }
}
```

URLs must be stable, user-openable evidence pages, not expiring object-store URLs.

## Ingestion

### User-level contract

The expected user requests are intentionally simple:

```text
"Добавь эту книгу в базу знаний региона" + attached PDF/DjVu
"Переимпортируй Гаузе, прошлый импорт был неполный"
```

The model owns orchestration. For a new attachment it starts ingestion and carries it
through page review, staging, validation and finalization. For an existing source it
first calls `book_find`; if one plausible match remains it calls
`book_ingest(command="reprocess", document_id=...)`. The server requires the exact
source to be present in the verified Telegram source archive and creates/resumes the
next staged revision on the same logical document. A re-upload is requested only when
that verified archive is genuinely unavailable. Multiple plausible catalog matches are
a user-facing disambiguation case: show title, author and year; hidden IDs are never
a user requirement. A pending archive is a recovery/wait blocker, not an immediate
request to upload the book again.
An explicitly selected owned logical document remains usable when historical roots
share the same source SHA; the roots stay separate. An attached-source start without
that selection continues to fail closed on ambiguous historical identities.

### Internal resumable workflow

`book_ingest` remains command-oriented because ingestion is one resumable state machine:

```text
new source: start(file) -> ingestion_id
existing source: book_find(query) -> reprocess(document_id) -> ingestion_id
book_pages(ingestion_id) -> mixed text metadata + MCP ImageContent blocks
stage(ingestion_id, pages, semantic chunks)
validate(ingestion_id)
finalize(ingestion_id)
status(ingestion_id)
```

Every ingestion result includes a compact `next_action` so the model can distinguish
continue-pages/staging, validate, finalize, wait for server work, resume interrupted
finalization, completion and a real blocker. Complete staged page coverage persists
the validate action for later status/restart; it does not replace semantic validation.
The model performs semantic
reading/recognition; the MCP does not run OCR, VLM, LLM parsing or semantic extraction.

The OpenAI tool descriptor marks only the top-level new-source `file` parameter with
`_meta["openai/fileParams"]`.

`book_pages` returns page renders through MCP image content blocks, not JSON URLs that
the model would need to fetch separately. Each batch is deliberately small (default
4, maximum 8 pages) so vision context is bounded.

Current implementation checkpoint:
- `book_find`: bounded RLS-filtered title/author lookup with deterministic ranking;
- `start(file)`: bounded HTTPS file-parameter download, exact SHA-256, PDF/DjVu
  inspection, private source staging and private document/job creation;
- `reprocess(document_id)`: owner-authorized verified Telegram-source-archive read,
  exact SHA verification and an idempotent `new_revision` start on the same
  `document_id`, without asking the user to upload the book again;
- `book_pages`: small deterministic JPEG page renders plus bounded native/embedded source
  text blocks/bboxes, maximum 8 pages per call;
- `stage`: the model submits page-local short keys (`region_key`,
  `illustration_key`) and semantic chunk references; the server creates stable
  UUIDv5 identities and derives chunk text from referenced regions. Chunks are
  coherent retrieval passages rather than native PDF blocks: prefer about
  800–1,800 characters when practical, join genuine continuations across pages,
  attach fragmentary body text to context, and avoid material conservatively at
  risk of exceeding the current 512-token encoder cap. Validation reports
  non-blocking fragmentation/size diagnostics. Figures may also carry an explicit
  clockwise `display_rotation_degrees` of 0/90/180/270; source geometry remains
  unchanged;
- canonical searchable text and graph material are persisted in Postgres; transient
  staged graph/source objects remain replaceable implementation artifacts rather than
  the user-facing source of truth;
- `validate`: requires complete page coverage, valid relations/illustrations,
  retrieval coverage and no unresolved `needs_review` regions;
- `finalize`: builds the Postgres text/search projection, embeddings/FTS, exact
  source crops, pages/regions/relations/illustrations/chunks and then asks the database
  to revalidate the materialized revision before atomically switching
  `active_revision`;
- retries of the same ChatGPT `file_id` are idempotent at the SQL boundary,
  including concurrent starts; archived reprocess uses a stable per-active-revision
  source identity so lost responses resume the same staged revision.

Staged books remain invisible to retrieval until the final activation RPC.

## Live profile

Interactive Live consumers use a separate optimized server profile
(`RKB_MCP_PROFILE=live`) that exposes **one tool only**:

```text
knowledge_search(query, max_evidence=3)
```

Fast path:

```text
Live
  -> knowledge_search
      -> Supabase vector + lexical + RRF
      -> parallel exact evidence range fetches from object storage
  -> compact evidence pack
  -> Live answer
```

This intentionally avoids the two model round-trips of generic `search -> fetch`.
The full ChatGPT/deep-research profile keeps standard `search` and `fetch`
semantics plus ingestion/access tools.

Rules:
- default evidence count is 3, hard maximum 5;
- no illustration bytes in the first response;
- illustrations are descriptors/IDs until explicitly requested;
- ingestion, sharing and rights tools never appear in the Live profile;
- deep enrichment remains optional/non-blocking;
- Live transport/capture/session lifecycle is provided by the shared
  `live-interaction` framework.

This minimizes declaration bytes, tool-choice ambiguity and conversational latency
for weak/fast Live models.

## Public and private search

Anonymous/public-only search may be exposed separately later. Authenticated search always applies row-level ACL before ranking candidates. Never retrieve private rows and filter them only after vector search.
### Native previews and source completeness

Native extraction is a transport aid, never a certificate of page completeness.
Each `native_blocks` entry reports `original_length`, `offset`, `end`,
`truncated`, and an optional `continuation` cursor. Read that cursor with the
same authorized `book_pages(ingestion_id, cursor)` call: it returns at most
1000 characters of one block and the source page image. Repeating a cursor
returns the same portion; concatenate by offset, without duplicating retries.
A page's `native_text_info.blocks_continuation` exposes blocks beyond the
80-block preview cap, and then subsequent blocks during a continuation read.
Finish a block's own continuation before advancing to the next block. The
ordinary numeric page cursor remains the next *page*, not a completeness claim.
Page text remains capped at 24000 characters; use block portions for complete
native material. Empty native text can still accompany a meaningful scanned
image or caption and always requires model visual review.

`StagePageInput.source_material` defaults to `unreviewed`; `preview` and
`full_native` alone cannot pass validation. Every page, including an image-only
page, needs `visual_reviewed` plus a concise `source_review_note` describing the
model's source review. The server validates this explicit attestation and graph
coverage; it cannot certify the truth of a semantic review. Old staged graphs
lack the attestation and must be reviewed/restaged before finalization. This
contract does not alter already active revisions or reimport their source.
Split long source material into bounded regions (maximum 8000 characters each)
with correct reading order; continuation portions are not automatically new
semantic regions. Parsing/recognizing scans remains ChatGPT's work.
