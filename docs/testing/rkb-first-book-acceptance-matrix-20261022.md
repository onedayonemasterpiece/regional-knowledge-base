# RKB first real book: acceptance + public-query gold fixture matrix

**2026-10-22 STATUS: SOURCE-ONLY PREPARATION. All real book/visual/Story/Saved acceptance cases are UNVERIFIED.** This is **not** a production-ready declaration or a replacement for protected 100-books/100,000-chunks/quality/latency/capacity acceptance gates.

Owner contracts (must accompany handoff):
- [Implementation §§3, 6–8, 11, 14](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bd138b2449f68cabb2cbfa3df552df8471f40c78/docs/prompts/rkb-scalable-kaggle-implementation-20261019.md)
- [Owner alignment: real pixels, Saved vs APPLIED, one research workflow](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bd138b2449f68cabb2cbfa3df552df8471f40c78/docs/designs/rkb-owner-alignment-20261021.md)
- [Existing first-book applicable DUR/REC, IMP/SRC, STO, PRF/VIS, ARC, FAST matrix](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/bd138b2449f68cabb2cbfa3df552df8471f40c78/docs/testing/rkb-scalable-kaggle-acceptance-20261019.md)
- [Existing latency leaf / censoring contract](rkb-public-query-latency-workload-20261022.md)

Opus alone owns shared candidate and live Kaggle PG/BGE/Datataset/import/restart; Шкипер performs only its separately granted read-only work. Линза changes only the owned tests and documents, with no live/provider/PG action. Synthetic fixture success does not grant access or certify source pixels.

## Pre-run exact nonsecret source / version receipt

Opus must provide immutable canonical integration branch **base SHA + HEAD SHA**, exact owned run/attachment, book source ID, bibliographic work ID, **edition ID**, original PDF/DjVu **SHA-256, bytes and MIME**, verified archive locator and **actual physical page count**; independently selected actual source pages with render/pixel sha, caption/figure links and issue provenance. Pin current schema/SQL/model/tokenizer, accepted source revision, Dataset **verified version and base cursor**, manifest SHA, applied/projection seq and visibility epoch. Filename, OCR-only transcript, local APPLIED and source count cannot replace these proofs.

First-book restricted search profile `limited_first_book` means **exactly 1 accepted genuine book with ≥1 visible passage**, not representative corpus readiness. Separate later `comparison_corpus` (≥2 accepted sources/32 passages) remains non-production-scale. Neither numeric profile proves that the source contains meaningful facts, figures or actual narrative; those require evidence review.

## Product acceptance cases (same selected genuine book, no fabricated text)

| ID | Existing API/workflow | Actual positive receipt required | Negative / current state |
|---|---|---|---|
| BOOK-01 | `book_ingest(start)`, original archive | Source SHA/bytes/edition/work/revision, correct PDF/DjVu MIME and **verified physical page count**, archive readback | Same filename/new bytes does not silently reuse edition — **UNVERIFIED** |
| PAGE-01 | `book_pages` and native portions | Actual page **ImageContent pixels**, page index/render hash, preview/full native offsets, opt-in same visual context continuation | OCR, empty native layer, preview-only do not prove visual review — **UNVERIFIED** |
| PAGE-02 | Selected regions + figures | Real body/footnote/table/caption geometry; chunk↔figure reference and labelled description; page/region SHA and text provenance; answer-relevant pixel returned where authorized | Missing figure or content silently dropped — **UNVERIFIED** |
| SRC-01 | `stage / validate / finalize / status` | Source-bound Unicode offsets/region hashes and accepted immutable source revision; reviewed text vs interpretation, exact stage batch/replay | Changed payload requires separate revision, no duplicate stage — **UNVERIFIED** |
| FACT-01 | `source_fact_read`, graph read | Genuinely present date/place/person/measurement/claim, exact source/page/revision/region, retrievable fact and graph relation | Unreviewed OCR fact or withdrawn/stale source not issued — **UNVERIFIED** |
| STORY-01 | `story_get/story_search` | One **genuine**, complete, approved Story/default `Variant.body`, exact revision/source+bibliography, chronology/uncertainty and conclusion | Summary/title/`variants=[]` is not Story.body. If selected book has no genuine episode: **NOT DEMONSTRATED**, choose another appropriate accepted book later; NEVER invent story — **UNVERIFIED** |
| CONT-01 | Source/story research continuation | Accepted frontier for long page/windows, exact next offset/page/render; one coherent cross-window dossier; restart retains original accepted continuation | No lost frontier, no unsourced cross-edition splice — **UNVERIFIED** |
| DUR-01 | Existing ControlSpool + Dataset primary | Verified remote intent → PG applied with exact IDs/cursor → native-readback-confirmed Dataset version/outcome → final Saved; activation and frontier atomic | `APPLIED/persistence_pending` cannot be Saved; UNKNOWN cannot blindly resend Telegram — **UNVERIFIED** |
| RETR-01 | Public `search_evidence/fetch/source_graph` | Search correct active revision using pinned BGE+lexical; **actual** exact passage locator/fetch bytes/continuation and source-dependent graph; observed frontier ≥ required | Stale/revoked/foreign evidence fenced; no zero-hit synthetic PASS — **UNVERIFIED** |
| VIS-01 | `illustration_fetch / scan_proof_read` | Authorized actual figure pixels, caption/page and MIME, stable transform/locator; allowed proof scopes only for owner/demo/expert/local_historian | Public illustration does not grant full PDF/stripe scan. If integration not wired: **BLOCKED**, not fake pixels — **UNVERIFIED** |
| ARC-01 | Owned direct Telethon archive | Immutable covered send-intent, scoped independent session, visible locator/media availability; unknown effect reconciled by exact ID | No blind resend; Telegram photo recompression is not a SHA corruption claim — **UNVERIFIED** |
| REC-01 | Owner-controlled Dataset restore | Exact verified Dataset version/base+WAL/control tail/PG/schema/model, Story.body/pixels/frontier survives restore and readback | Older Dataset version must not overwrite Saved — **UNVERIFIED** |

These are applicable existing product requirements, **not twelve newly invented independent release gates**. If a source genuinely has no good story or illustrative material, report not-demonstrated; do not manufacture it, and use a suitable source in later product acceptance. Mass thresholds from §14 remain separately mandatory, not demanded from one book.

Negative product probes are run **outside** timed public-query cohort: N01 wrong/stale source revision, N02 unauthorized full-page/stripe, N03 lost visual context must resend actual ImageContent, N04 retry/lost ACK no duplicate content, N05 locally APPLIED not remotely covered → pending/no Saved/no Telegram effect, N06 unknown Telegram ACK no automatic resend, N07 withdrawn/revoked source excluded by output gate, N08 absent Story.body never returns fabricated narrative. Every actual probe needs exact nonsecret evidence and owner execution. Present status for **all: NOT RUN**.

## Freeze exact eight positive quality queries only AFTER human source review

For the existing `public_query_latency_leaf.py` fixed q01–q08 cases, freeze private exact UTF-8 question text, each `question_sha256`, one or more exact **human-validated** evidence locator SHA-256 values, canonical source ID/content revision, approved source edition, corpus manifest/hash/version, and reviewer receipt. Do not publish question texts or source passages in git/raw timings. Recommended case intentions, chosen only when the actual book supports them:

| ID | Source-grounded intention |
|---|---|
| q01 | Bibliographic edition/source original identity |
| q02 | Dated fact with exact source page/region |
| q03 | Named person/place relation from the source |
| q04 | Cross-page/footnote evidence + continuation |
| q05 | Figure or caption question **based on inspected pixels**, not OCR |
| q06 | Source-backed episode/narrative evidence, if genuine; separate STORY-01 remains required |
| q07 | Exact source-language quote/region with stable fetch locator |
| q08 | Human-reviewed multilingual query (e.g. RU→DE), only if source-gold exists |

The pre-existing leaf now accepts `gold_manifest` for the exact operator-owned eight questions. Manifest codec `rkb.first-book-query-gold.v1` requires `state=human_reviewed`, equal `profile`, `database_generation_ref` and `corpus_manifest_sha256` to its pre-run owner readback, exact edition/review SHA-256 and ordered cases (q01..q08). Each case binds hash of question bytes, UUID source ID, positive source revision, 1–5 unique SHA-256 **exact evidence locators**, human reviewer SHA and basis: `human_reviewed_source_text`, `human_reviewed_actual_pixels`, `approved_story_body` or `owner_verified_bibliography`. **OCR-only/unverified/wrong identity/extra private payload is rejected before public query dispatch**. Only canonical `gold_fixture_sha256` is retained alongside question SHA in run/before-after receipts. This *metadata integrity guard* is **not independent proof returned results match gold**: approved operator must compare actual source+locator+revision and negative outcomes separately. Where pixels or reviewer gold are unavailable → **gold fixture unavailable / quality gate UNVERIFIED**, no invented visual hit.

## Existing latency harness, no new measurement system

Two frozen cohorts: serial concurrency1 then concurrency2; **2 warmups and 6 measured calls per q per cohort** = 32 warmups + 96 measured = 128 total, no retries. Same q bytes, gold digest, source revision and corpus manifest across A/B. Backend evidence deadline 2s; client 3s is **right-censored guard**, not a new SLO. Per cohort show completed-success-only p50/p95, timeout/error rate and raw censor counts; whole-population completion p95 is null if any censored observation. Include explicit benchmark semaphore wait separately from MCP callback (which includes server admission/transport); overlapping tokenize/BGE/PG/serialize controller stages **not summed**; CPU/RSS client only without native owner readback. This is a limited-first-book observation, **not a warm production p95 or held-out mass corpus Hit@K proof**.

## Outstanding inputs and next owners

**Missing:** first actual accepted book with exact file/edition/page count; actual allowed pixels; human-approved q01–q08 source+locator review; story body/continuation and graph receipts; verified Dataset remote Saved and exact activation; Opus canonical integration base+HEAD/file ownership; existing authorized read-only MCP runtime slot and telemetry. No live/source/provider/PG calls, no false visual gold or latency gain.

Шкипер was given NEW intake test leaf `9699e260205e40f86c49ed426482269baa9a7bd2` with **NEW test NOT RUN**; six unrelated existing frozen tests PASS do not prove the new selector. It must run only in its permitted writable candidate, not denied `src_b833`. Opus owns final merge into **one** canonical candidate, with each contributor providing exact head/paths/test receipts. First-book acceptance and real MCP latency begin **only after** Opus confirms accepted corpus and scoped read-only slot.
