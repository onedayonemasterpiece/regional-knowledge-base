# Acceptance: citation → accepted article/chapter → readable PDF (P0) and audio (P1)

2026-10-26 · **planned tests**, not live PASS. Companion: [design](../designs/rkb-source-unit-pdf-and-audio-20261026.md). Use public-domain or wholly synthetic **nonprivate** fixture sources; do not commit books, private text, scans, signed URLs, credentials or generated outputs. This work must not run on RKB live Kaggle except under owner/Шкипер-controlled acceptance.

## P0 Definition of Done

P0 is delivered only if a real authenticated end-to-end request can select at least two answer citations, identify exact logical source units, return a durable async operation, and retrieve a usable **author-faithful** multipage PDF with provenance, valid source-specific rights, and audit-safe receipt. A passing standalone PDF-render function or fixture unit test **is not** P0 acceptance.

### A. Authoritative selection and completeness

| ID | Fixture / action | Required assertion |
| --- | --- | --- |
| A01 | Two cited spans within one fully accepted article | Exactly one exported article, no chunk repetition, one linked article title and full exact text |
| A02 | Two cited spans in different chapters of one edition | One compiled document with two *distinct* unit sections and no unauthorized intervening text |
| A03 | Two cited works in different editions/books | Separate correct publisher/year/ISBN/UDK metadata per work (when actually present); no metadata bleed or false unification |
| A04 | Article A ends and article B starts on same physical page | Source-span boundary, not whole-page selection; **no text from B** in A |
| A05 | Heading starts midway through a region; footnote continues to next page | Exact start/end and note association; no repeated/omitted span |
| A06 | Citation from page accepted while rest of unit is unreviewed/pending | Search still works; unit export returns `needs_source_review` and **no partial PDF marked ready** |
| A07 | Ambiguous TOC, missing last heading or source reading-order conflict | Preview asks to confirm boundary or returns explicit unavailable; never guesses page count |
| A08 | Imported image-only DjVu with no accepted transcription | Export denied as not source-complete; no quiet OCR/native-text substitute |
| A09 | Withdrawn/inactive or changed source revision between preview/start/download | Revalidate current source/rights; no stale cached deliverable |
| A10 | Repeated citation, repeated operation request, changed idempotency payload | One unit/one operation for identical input; conflict for changed payload; no duplicate job |

### B. Verbatim fidelity and layout

| ID | Fixture / action | Required assertion |
| --- | --- | --- |
| B01 | Russian text: «ёлка», ё, «», —, №, superscripts, abbreviations, NBSP, historical spelling | Extracted PDF body (excluding explicitly separated editorial matter) equals canonical accepted ordered author text under **documented lossless line-wrap normalization only**; no semantic rewrites |
| B02 | German text: umlauts, ß, old place names, uncommon Unicode | Embedded font/ToUnicode, no missing glyph squares; text selectable/searchable |
| B03 | Long paragraph/short para/2-page heading | First-line paragraph indentation, sensible line height and widows/orphans; no heading orphaned at page end |
| B04 | Footnotes, nested lists, table, caption, formula | Supported structures reproduced exactly; unsupported structure fails visibly, never silently dropped |
| B05 | 2, 10, 30 physical source pages | PDF opens in at least two common viewers, stable layout/bookmarks where applicable, selectable Unicode text, measured filesize/render time |
| B06 | Source printed page 7 vs physical index 3 | Both retained with accurate distinction; generated PDF page numbers are separate |
| B07 | Several cited points in one unit | PDF bookmarks/appendix navigation point to correct paragraph **without inserting nonauthor text into body** |
| B08 | ISBN/ISSN/DOI/UDK/BBK absent or conflicting | Only attested edition-specific values printed; conflicts flagged; no fabricated identifiers |
| B09 | Personal/private source URL or token hidden in record | Neither PDF metadata nor pages nor download filename include it |
| B10 | Visual regression | A4 template, readable text size, embedded licensed font, red-line indent, adequate margins, distinct front matter; compare rendered page samples after engine changes |

Automated verification should compare ordered `(source_id, revision, unit_id, region_id, codepoint-span)` manifest with the accepted source projection, accounting for cross-page continuation. **Do not** use PDF-extracted words alone to assert rights or unit completeness. Artifact final verification includes MIME, nonempty bytes, page count, `pdftotext`/equivalent, mapped unit list, and successful authorized user readback. No per-line artificial replacement of original punctuation.

### C. Authorization, delivery and concurrency

| ID | Fixture / action | Required assertion |
| --- | --- | --- |
| C01 | `demo` or `expert` has `scan_proof_read`, book rights unknown | Quote stripe may follow its original policy; **chapter PDF denied** |
| C02 | Teacher/professional label but no confirmed export permission | Chapter PDF denied despite tariff/label |
| C03 | Licensed source restricted to one unit and purpose | Only explicitly allowed unit+output format; cannot use PDF grant to bypass audio restriction |
| C04 | Permission withdrawn after job began; cache hit under old policy | Final delivery denied, cache inaccessible; no stale link privilege |
| C05 | Shared Slack audience vs private user citation | No private PDF/URL sent to shared thread; no union of participant grants |
| C06 | Source owner/current actor different, forged source_id or source_revision | Server resolves/authorizes actor-scoped refs, denies untrusted overrides |
| C07 | Multiple selections with one unauthorized unit | No silent partial output; user can explicitly proceed with only allowed units |
| C08 | Retry, restart, ambiguous receipt and cold Kaggle startup | Same job identity recovered; no duplicate exports or indefinite blocking search |
| C09 | Concurrent search/import and a 30-page render | Measured no breach of protected warm-search p95 / MCP p95; PDF bounded independently |
| C10 | One user submits large batch while another requests small PDF | Bounded fair admission, deterministic error/queue state, no resource exhaustion |
| C11 | Ready receipt points to missing/corrupt bytes | No `ready` without verified end-user readback; safe resynthesis or explicit error |
| C12 | Original source unavailable but accepted text complete | No mandatory original PDF download/page render for text-only PDF; extra image permission remains separate |

**Acceptance artifacts:** synthetic/public test source revision + coverage map; typed request/result receipts; resource consumption; rendered page screenshots and visual review; text and span comparison outcome; signed delivery readback without leaking credentials; code SHA and tests on exact PR head. Distinguish fixture PASS, source-only, deployed and live accepted.

## P1 test plan (no delivery claim)

| ID | Test | Required assertion |
| --- | --- | --- |
| T01 | Same pinned 2-, 10-, 30-minute Russian source, German source, proper names/date stress | Actual cold/warm TTFA, full wall time, RTF, cost, completed audio duration; ASR alignment and manual listening scores |
| T02 | Qwen 1.7B vs 0.6B vs eligible GPU vs Realtime mini | Same input/voice-rights scope; no undocumented fallback or unmeasured speed claim |
| T03 | Repeat only failed chunks after process or Kaggle session death | Verified cached chunks reusable **only with exact source+profile+model+voice+chunk signature** |
| T04 | 429 on status poll while Kaggle job still RUNNING | Keep exact handle/attempt, provider_wait; do not mark failed or submit again |
| T05 | Failed assembly / truncated audio / broken MP3 | No ready until external authorized readback, duration/byte/segment validation |
| T06 | Mid-job rights withdrawal / different user / old cache | Authorization rechecked at delivery; deny unauthorized cross-user output |
| T07 | Temporary private Dataset with versions and GC | Confirm real deletion/retention/version behavior, recovery after restart, no raw private text/audio in logs |
| T08 | Long-form full-run mirroring Wonderful Lections incident | Show one genuine longform output complete, not merely a `synthesizing` heartbeat or 24/57 progress |

## Decisions blocking P0 code activation

- Source-specific full-unit reproduction permission policy and evidence format; separate voice/audio permission.
- Exact representational unit boundary and completeness proof for chapters beyond existing article search scope.
- Whether eligible scope includes captions/illustrations and which structural layouts are required for launch.
- Concrete transport and temporary private artifact retention with download authorization for actual MCP clients.

Until these are resolved, a doc PR may merge as a **proposal**; an exporter must fail closed rather than silently generate copyrighted or incomplete longform content. No automatic requirement-contract update or Kaggle runtime change follows from this planning document.
