# MCP surface and model optimization

## Tool surface

Keep the public model-facing API small and goal-oriented:

1. `search(query)` — standard read-only search compatible with ChatGPT company knowledge/deep research.
2. `fetch(id)` — standard read-only fetch with full evidence, provenance and illustration descriptors.
3. `book_ingest(...)` — resumable ingestion state/write workflow and file-aware start.
4. `book_pages(...)` — read-only staged page batches returned as model-visible MCP image content.
5. `document_access(...)` — inspect/change visibility and sharing, subject to rights and role checks.
6. `profile()` — tiny identity/profile read for clients that display connected account identity.
7. `illustration_fetch(id)` — authorized canonical crop as ImageContent, with
   printed caption and explicitly labelled model observation metadata. Available
   in the full profile; Live retains its existing bounded evidence surface.

`book_ingest` start metadata accepts `duplicate_policy=reuse|new_revision`.
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

`book_ingest` is resumable and command-oriented because ingestion is one user goal with shared state:

```text
start(file) -> ingestion_id
book_pages(ingestion_id) -> mixed text metadata + MCP ImageContent blocks
stage(ingestion_id, pages, semantic chunks)
validate(ingestion_id)
finalize(ingestion_id)
status(ingestion_id)
```

The OpenAI tool descriptor marks the top-level file parameter with `_meta["openai/fileParams"]`.

`book_pages` returns page renders through MCP image content blocks, not JSON URLs that
the model would need to fetch separately. Each batch is deliberately small (default
4, maximum 8 pages) so vision context is bounded.

Current implementation checkpoint:
- `start(file)`: bounded HTTPS file-parameter download, exact SHA-256, PDF
  inspection, private Object Storage persistence and private document/job creation;
- `book_pages`: small deterministic JPEG page renders plus bounded native PDF
  text blocks/bboxes, maximum 8 pages per call;
- `stage`: the model submits page-local short keys (`region_key`,
  `illustration_key`) and semantic chunk references; the server creates stable
  UUIDv5 identities and derives chunk text from referenced regions;
- the full staged graph is an immutable hashed JSON object in Object Storage,
  not a partially materialized Postgres graph;
- `validate`: requires complete page coverage, valid relations/illustrations,
  retrieval coverage and no unresolved `needs_review` regions;
- `finalize`: builds the UTF-8 text projection, embeddings/FTS, exact source
  crops, pages/regions/relations/illustrations/chunks and then asks the database
  to revalidate the materialized revision before atomically switching
  `active_revision`;
- retries of the same ChatGPT `file_id` are idempotent at the SQL boundary,
  including concurrent starts.

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
main pending. Repeat status in a later turn; do not hold an interactive turn open
waiting for remote indexing. See [operations](operations/automatic-indexing.md).
