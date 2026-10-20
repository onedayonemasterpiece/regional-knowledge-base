# MCP surface, read models and compatibility

Active target2026-10-20. [Single handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md), [ingestion](ingestion.md), [rights](rights.md) and [stories](story-registry.md) define behavior. Existing external tool names are retained; changed envelopes are versioned. Old non-GEO IDs do not migrate, but clients are not broken for convenience.

## Goal-oriented surfaces

| Group | Contract |
|---|---|
| `search` / `fetch` | Standard full-profile source evidence search and fetch; stable user-openable evidence references, exact provenance and illustration descriptors. Internal vectors/FTS/storage are not tools. |
| `knowledge_search` | Live-only compact evidence pack in one call: query, max_evidence default3/max5, optional document_ids/main_job_id. No import/admin/graph/proof tool dump in default Live profile. |
| `book_find`, `catalog` | Deterministic bounded title/author lookup (book_find limit≤8); catalog list/find/get and allowed metadata update, keyset cursor/limit≤100. No full-catalog Python sort. |
| `book_ingest`, `book_pages` | One resumable command workflow with start/reprocess/rechunk/stage/validate/finalize/status and small real page ImageContent batches. Source semantic interpretation belongs to calling model. |
| `document_access`, `profile` | Inspect/change permitted grants and small verified identity readback. No client-supplied actor or role as authority. |
| `illustration_fetch`, `catalog_cover`, `source_proof` | Real images with class-specific rights, exact revision/geometry/hash/MIME, per-item continuation. Cover must be a registered real asset, not fabricated. A mostly textual title page is not made public by naming it a cover. |
| Story operations | Existing search/get/history/extract/reconcile/edit/merge/archive/restore, source registration, assertions/evidence/assessment, variants/reviews and publication records. Actual saved body shown by default. |
| Graph/GEO | entity_list, graph_fetch/related/stage, poi_registry search/get/select/create, poi_context and geo queue; bounded source evidence and actual owner receipt. |
| Calendar/measurements | Existing story_calendar, dated claims/history, observation_search/compare and typed edit ops; facts can exist without mandatory Story. |
| Cases/participants/research | Existing compact search/get/history/changes, evidence/contribute/conclusion/support, participants/grants, discussion/export/source needs/drafts/operation status groups. No tool per UI button or second expert MCP. |

Full search retains the compatible `results[{id,title,url}]` projection; fetch retains id/title/text/url/metadata (source/page/provenance/footnotes/illustrations). Rich status and revision fields extend the versioned envelope, not silently replace its type. Stable URL is not an expiring object-store credential or an unbuilt public page. Old strict PH evidence/mode normalizer gets an explicit versioned adapter for new pending/result/artifact modes.

## Search and read contracts

Query all active accepted book text/bibliography without per-book owner ACL. Request document/article scope, current revision/withdrawal and private derived content still apply. PostgreSQL does scope/indexed ranking/batched hydration; no list of all document IDs sent to another vector service. BGE only on Kaggle, bounded lexical/alias complement, common2s evidence deadline. E5 fallback is not a target mode.

Warm backend p95≤1000ms, MCP p95≤1500ms. A cold worker yields honest lexical_degraded/pending and actor-bound main_job_id; repeat that same job/query to continue. Empty preliminary hits are not a definitive absence. Index vector counts do not prove loaded worker readiness. Projection lag/current snapshot/min_data_seq are explicit; after save route to eligible fresh data or return truthful pending, never false not-found.

Live returns small evidence and image descriptors initially; request image explicitly through allowed progressive client capability. Source quote/attribution/calendars/uncertainty survive voice. Ordinary stories read the saved basic body or saved adaptation and put bibliography on screen. No LLM generation during `get` to conceal missing narrative.

`story_get` default returns exact Variant.body/ref/revision/hash with bibliography separately. A candidate without body returns body=null plus availability/reason. Search distinguishes ready narrative from dossier and gives a full short body or clearly marked excerpt + exact reference/cursor. Read-export follows Story read scope; publish-export additionally requires approval/capability. Compact mode cannot silently fall back to summary. Private author drafts remain private even though imported shared body may be an editorial draft.

## Ingestion boundaries

New-source top-level `file` keeps `_meta["openai/fileParams"]`. PDF/DjVu belongs to start/reprocess; bounded stage JSON is a separate transport of the same typed data. New source default4/max8 page ImageContent, exact source/revision, region/span hashes, visual review, semantic completeness and BGE tokenizer gates remain. Full [ingestion contract](ingestion.md) records block/page continuations and limits; do not equate preview with full source reading.

Every ingestion response exposes next_action and real source/index/knowledge progress. Facts/dates/entities must survive zero stories. Sufficient episode saves a default basic narrative and verifies normal readback. Source activation and persisted research/compose frontier are atomic; new shared source does not auto-publish or confer human endorsement.

Accepted-source extraction uses existing story_extract/corpus_read checkpoints and the same facts/body contract without reimport. Rechunk only changes derived passages of verified unchanged accepted source; catalog updates do not change source/evidence. Owner personal-route restrictions apply even if a Slack bot was invoked by owner.

## Graph and structured knowledge

Stage bounded graph bundle≤32 nodes/64 evidenced directed relations. Node kinds person/organization/event/historical_thread/poi_ref; alias/mention/type/temporal context determine candidate identity, not an automatic name merge. graph_fetch returns one entity with at most20 one-hop relations and paged aliases/mentions. graph_related uses same retrieval and pending envelope. graph_stage accepts source-grounded new facts/alias and optional discovery request, never a full graph dump.

Source-place can exist without coordinates/Street Story ID. Verify selected/created physical POI through its owner; graph discovery hits are not automatically facts/identity. Existing external canonical aliases come from owner projection, not caller injection. Public POI does not expose private drafts/attachments linked to it.

`story_calendar(month,day,calendar,limit,cursor)` selects current exact day-level event datings, not publication dates/approximate years. Preserve original_date_text, calendar/precision/date_role, competing datings and explicit supersession. Do not silently convert Julian/Gregorian/unspecified. Existing event_dates_page/history and record_event_date retain exact source evidence.

RecordObservation keeps original numeral, exact decimal, metric_key/unit_code, subject/period/method/precision and evidence. Existing observations_page/history, story_observation_search and story_observation_compare stay bounded; compare refuses unverified arithmetic across mismatched subjects/periods/methods. The new storage path permits these facts outside a Story without deleting the old compatible operations.

## Proof and artifacts

Illustration returns a genuine canonical accepted figure crop publicly, printed caption separate from labelled model observation. Stripe/page quote proof requires owner/demo/expert/local_historian capability, current scope and authorized audience. Full original download is separate. Trusted artifact class/geometry and separate cache identities prevent full-page-as-illustration bypass.

Keep native PDF quads first, same-page multi-region and page-by-page multi-page quotes; explicit page requires quote scoped there. Native matching may tolerate existing bounded adjacent punctuation without altering internal text. Scans/DjVu use the existing configured Flash-Lite localization/independent strip-read adapter where enabled, not image generation or another general ingestion LLM. Preserve current bounded policy:25s provider call timeout,≤4M pixels,4096 output tokens,≤2 calls/page; async proof must not hold the2s evidence search. Second visual read does not receive expected quote. Verify convex in-page bounds, order/stripe area and text equivalence; confidence/echo alone cannot authorize exact proof.

Source SHA/revision/physical page/render hash/dimensions/transform/task generation bind locator. Pin preview until terminal callback; changed pixels require new locator. Authorize both quote_proof and internal source_archive purpose access without owner impersonation. On cache hits/delayed output recheck grants and withdrawal. Ready/pending/denied/unavailable/failed per item; retain operation ID through ArchiveReadPending. Return actual ImageContent and actual MIME, with distinct source/display/provider/delivery hashes.

## Durable request envelopes and acceptance

Transport received/pending, remote durable accepted, domain saved and delivered/result-hash ACK are different. Shared person/dialog sequence, fair admission, current request generation and service scope cannot be bypassed by an explicit ID. Ordinary read-only search avoids remote journal writes; durable question/mutation preserves exact payload/outcome under [storage](storage.md).

Test real clients with a non-owner, direct forbidden calls, revoked link/grant, cross-route stale callback, private draft history, actual body/image readback and reconnect result ACK. Endpoint existence, cached catalog visibility or green unit tests alone do not establish client support.

Verified demo/expert/local_historian assignment materializes the existing scan_proof_read policy grant with audit and category revision; demo expiry is inherited. Claimed categories grant nothing. Current capability/scope is checked at render time; explicit revocation is not recreated on read, and removing a category withdraws its policy-derived grant. No new IAM or per-image approval.
