# MCP surface and model optimization

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
  `document_id`, without asking the user to upload the book again. Normal replay
  resumes a still-running archived reprocess. When the user explicitly wants to
  replace a still-pending reprocess, call the same tool with
  `metadata.duplicate_policy="new_revision"`; this deliberately allocates one
  newer staged revision while replay of that explicit restart remains idempotent;
- `book_pages`: small deterministic JPEG page renders plus bounded native/embedded source
  text blocks/bboxes, maximum 8 pages per call;
- `stage`: the model submits page-local short keys (`region_key`,
  `illustration_key`) and semantic chunk references; the server creates stable
  UUIDv5 identities and derives chunk text from referenced regions. Chunks are
  coherent retrieval passages rather than native PDF blocks or page-sized units:
  target about 256 encoder tokens (typically 800–1,000 characters, broadly
  700–1,100 on the measured books), join genuine continuations across pages, and
  attach fragmentary body text to context. Validation counts the **final augmented
  search material** with both pinned tokenizers; exceeding either deployed
  512-token hard limit blocks finalization instead of silently truncating.
  Figures may also carry an explicit
  clockwise `display_rotation_degrees` of 0/90/180/270; source geometry remains
  unchanged;
- canonical searchable text and graph material are persisted in SQLite; transient
  staged graph/source objects remain replaceable implementation artifacts rather than
  the user-facing source of truth;
- `validate`: requires complete page coverage, valid relations/illustrations,
  retrieval coverage and no unresolved `needs_review` regions. A new revision of
  the **same archived source SHA-256** may reuse page-level visual-review status
  from an older revision only when that older revision already reached the
  publication gate and the stored page itself was `visual_reviewed` with a note;
  this reuses source review, not old chunking or semantic structure;
- `finalize`: builds the SQLite text/FTS projection and pending vector publication, exact
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
      -> BGE-first semantic retrieval in a compact authorized document scope
      -> optional bounded lexical/alias branch when explicitly selected
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

Anonymous/public-only search may be exposed separately later. Authenticated search
always computes an authorized document/revision scope locally before vector
ranking. The remote vector plane receives only that compact document scope, not a
full list of active chunk IDs. Returned candidate IDs/revisions/hashes are then
validated against local SQLite authority before hydration. Never retrieve private
rows and filter them only after vector search.
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

## Minimal entity graph tools

`book_ingest(stage)` accepts one `entity_candidates` bundle (up to 32 typed nodes
and 64 evidenced relations). Each quote must match its exact staged chunk and
region. Node kinds: person/event/historical_thread/poi_ref. Relation shapes are
restricted to participated_in, occurred_at and member_of.

`graph_stage(document_id, revision, candidates)` adds model-authored bounded
candidates to an owned active revision; it does not reimport or declare the full
source complete. `graph_stage(entity_id, alias)` adds one sourced alias and queues
reverse discovery. Explicit entity IDs reuse only same-owner/kind identities.
Unknown/ambiguous POI resolution remains unresolved; there is no name-based merge.

`graph_fetch(entity_id, limit<=20)` returns one authorized entity, bounded
aliases/mentions, and one-hop neighbors. Every relation includes exact
chunk/page/region/quote evidence; fetch the referenced chunk before asserting a
fact. Aliases and discovered mentions are candidates, not automatically accepted
identity truth. `graph_related` uses the existing multilingual retrieval and can
return a pending main job in its retrieval envelope.

Live exposes no graph-write or traversal tools. Private source existence and
relations stay evidence-RLS scoped even when a referenced canonical POI is public.

`graph_stage(poi_discovery_ref="streetstory://poi/<uuid>")` schedules a canonical
POI/version discovery request even before that POI has a graph evidence seed.
Names/version are verified through Street Story's identity projection; callers
cannot inject a new canonical alias. `graph_fetch(discovery_job_id=...)` returns
at most 20 still-authorized source candidates for the requesting actor. No
entity/fact is automatically materialized from these hits. A later ChatGPT
review may stage their exact evidence through the normal contract.

## Automatic indexing readiness

A finalized active revision is indexed automatically in E5 and BGE; source
activation does not wait for Kaggle. Normal imports require no backfill command.
`book_ingest(status)` on a finalized ingestion and search/Live evidence outputs
include actor-scoped `indexing` counts/state. Regular MCP `indexing_status` accepts
an optional authorized document ID and returns active/ready/missing counts,
worker state and effective mode; it exposes no titles, text or foreign inventory.
During incomplete BGE coverage search uses complete E5, otherwise lexical, with
main pending. When BGE is ready it is the default main semantic branch; E5+BGE
equal-weight fusion remains an explicit diagnostic/compatibility mode rather than
the default because the measured multilingual fixture showed lower recall than
BGE alone. Interactive BGE query waiting is bounded by
`RKB_BGE_QUERY_WAIT_SECONDS` (0.8 s by default); a missed deadline returns the
fast fallback with a truthful pending state and resumable main job. General FTS
is optional and separately bounded by `RKB_LEXICAL_BUDGET_MS`; exact caller
aliases remain independent signals. Repeat status in a later turn; do not hold an
interactive turn open waiting for remote indexing. See
[operations](operations/automatic-indexing.md).


## v2 corpus and proof tools

- `catalog(command=list|find|get, query='', kind?, cursor?, limit<=100)` lists
  accessible sources with SQLite details. Empty query lists sources. Pagination
  excludes unauthorized roots before calculating cursors; old revisions do not
  create duplicate catalog entries. `book_find` remains compatible.
- `source_proof(id, quote, physical_page_index?)` authorizes the evidence and
  private original owner, resolves one exact fragment, verifies source hash and
  renders only the requested page. It returns JSON plus a real WebP image when
  localization is validated. Native PDF quads come first. Scan proof additionally
  requires `RKB_SCAN_PROOF_ENABLED=1` and dedicated `RKB_GEMINI_API_KEY`.

`RKB_PROOF_MODEL` accepts only the configured Flash-Lite reader family, including
`gemini-3.1-flash-lite` and `gemini-2.5-flash-lite`. Pro/image-generation models are
rejected. Calls use 25-second timeouts, <=4M pixels, 4096 output tokens and <=2
calls/page. The second call reads proposed strips without the expected quote.
The server checks text equivalence, convex in-page bounds, order and stripe area,
then paints transparent yellow polygons. Ambiguity, mismatches and provider
failures return explicit unavailability; confidence/echo never authorizes exact.

The Live profile retains its existing read-only knowledge search/fetch adapter.
No ingestion, catalog or proof tools are added to the default Live bundle.

Catalog list/find/get preserve unknown fields and contributor roles. `catalog_cover`
returns only a registered real source cover/title page. `source_proof` supports
same-page multi-region quotes and page-by-page multi-page quotes; an explicit
page requires a quote scoped to that page. Native PDF quads are checked first.
Native matching may retain up to three immediately adjacent trailing punctuation
characters from the source word box when the requested quote omits them; internal
text still must match exactly. Scans and DjVu use bounded Flash-Lite localization plus an independent crop read
without the expected quote. Code applies yellow stripes; ambiguous or mismatched
results fail closed. Warm model proofs recheck `RKB_SCAN_PROOF_ENABLED` and
source ownership, so capability revocation also rejects a warm cache hit.