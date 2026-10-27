# Native original intake

An accepted visual stage authorizes deterministic publication of that exact
packet's text and semantic components. Their existing source-command settlement
wakes the next component without another client status call. An accepted stage
reply permits reading the next cursor from its frozen page packet while
publication remains processing; retain unfinished passages and current tail
context. Once current packet progress permits the existing native tail query,
known tails retain their exact `continue_tail` response and refs. Acceptance
does not assert complete coverage, APPLIED or Dataset Saved. An internal proof
binds the accepted stage hash, person, source, ingestion, batch and current
runtime; each new component rechecks canonical owner/import rights and existing
history/UNKNOWN/writer fences. It grants no new stage, next page or finish, and
neither retains an HTTP access lease nor constructs an owner Principal. Restart
recovery visits at most 16 compact progress rows per maintenance tick;
duplicates reuse existing operation keys. UNKNOWN remains pending without
automatic redelivery. APPLIED components still require verified Dataset
persistence before being reported Saved.

This opt-in target extension uses the existing recovery-phase jobs, authenticated
WSS, document resource admission and durable result acknowledgments. It changes
neither the running notebook nor existing launch configuration automatically.
`TargetLaunchSpec.original_intake` takes `OriginalIntakeConfig`: exact allowed
archive HTTPS origins, exact parser package versions, a byte limit and a finite
transfer deadline. Parser packages must also be present in the independently
attested dependency inventory and exact Stage0 wheel closure. No host parser
fallback or dependency installation outside Stage0 is used.

The additive metadata lock is
`scripts/production/manifests/kaggle-cp313-x86_64-pdf-wheels.json`: all 50 original
CP313 wheel rows are unchanged, plus the official 25,762,500-byte PyMuPDF 1.28.2
`cp310-abi3-manylinux_2_28_x86_64` wheel. Its metadata declares no mandatory Python
dependencies; this PDF path needs no Pillow, OCR or layout-analysis package.
The lock records exact official URL/hash, wheel metadata hash, bundled ELF hashes
and dynamic-library/symbol requirements. Use `parser_packages=(("pymupdf",
"1.28.2"),)` together with that exact complete Stage0 lock. The Dev CP313.15
tokenizer-only fixture rendered ten synthetic pages without Torch, model weights
or model inference. The target image still requires its own observed ABI/load
proof; the fixture is not Kaggle execution evidence.

`OriginalIntakeController` retains the existing queue, spool, Participants,
object-store client and current binding callbacks. Its constructor performs no
provider effect. `begin(RequestAuthority, operation_id=..., source_id=...,
ingestion_id=..., file=ChatFile(...), max_bytes=...)` stages only metadata in the
existing control database. The existing top-level MCP `file` argument and
`openai/fileParams` remain owned by the public importer. Every method requires
the current owner import route and current native/writer binding.

The fixed phases are:

1. `original_acquire`: Kaggle downloads the ephemeral ChatFile HTTPS URL using
   public-address DNS pinning, no redirects, no cookies/proxy, a 45-second budget
   and streaming byte/hash bounds. The URL is attached only during dispatch and
   is absent from durable job identity/results and logs. It can be renewed while
   retaining the logical file/ingestion identity. Actual bytes determine format,
   hash, size and physical page count; metadata labels cannot supply these facts.
2. `original_archive`: the Kaggle-native retained original is sent as a
   document to private Telegram topic 2 by the same leased Telethon client.
   Exact chat/topic/message/document metadata is read back (presence only)
   and the locator saved with the receipt. UNKNOWN never triggers a blind second
   send; the operation key and exact caption support reconciliation. Originals
   are not sent through Dev or VibePublish.
3. `original_verify` retains S3 derivative-image verification with a private
   ticket; without a ticket it observes a Telegram original by exact intake
   caption/presence after a lost send ACK, without downloading or sending.
   Historical S3 original receipts remain readable without rewriting accepted
   source identity; new original receipts refer to Telegram.
4. `original_pages`: the retained original is rendered by the pinned isolated
   PDF parser. Each call returns one physical page's bounded metadata. Its real
   JPEG derivative follows the same archive/readback phases, after which Dev may
   retrieve that bounded derivative for MCP ImageContent. Originals never follow
   this Dev download path. `next_cursor` may continue text and blocks on that
   same physical page before advancing to the next page. The opaque cursor binds
   physical page, block position, block character offset and plain-text offset;
   it is consumed by the existing `book_pages`, with no extra public tool.

`verified(context, ingestion_id)` returns only a locally retained workflow
receipt after fresh current-authority and native storage-version checks. Its
`.original` property supplies the exact source-import original binding. The
receipt includes source/ingestion/request identities, actual size/hash,
content-addressed stable archive key, native version, page count, parser pin,
acquisition hash and native readback hash. Signed URLs are not receipt locators.

When an unrendered page needs a lost ephemeral native handle, the controller
restores only from that retained verified receipt. New Telegram originals use
the saved message/document locator: the same leased downloader retrieves bytes
directly into private Kaggle scratch and the existing source/parser checks apply.
Old immutable S3 receipts still work with their original conditional GET path
until the sidecar Telegram locator has been migrated; afterwards the same
old receipt remains unchanged but restore prefers Telegram. Source/ingestion identity and original receipt are immutable. The
restore sub-operation is persisted before dispatch and reused after cancellation
or lost acknowledgment. It creates no original PUT, external attachment download
or source import. Pending intake without a verified receipt cannot take this
path. Ordinary metadata/result replay needs no restoration when the exact
archived page derivative is already available.

Unsupported format/DjVu, verified-byte mismatch and page-count limits return
`state=rejected` with a fixed safe reason. These known validation outcomes are
persisted and remain actionable through `status`; they cannot become an endless
UNKNOWN retry. Raw parser errors, URLs and provider error bodies are never
returned as reasons.

`pages(...)` returns the existing `RenderedPageBatch`. Page IDs are
`uuid5(source_id, "page:1:<physical index>")`. `native_pages(...)` returns an
immutable bounded `NativePagesProof`: exact original receipt hash and canonical
page metadata including original SHA, parser pin, actual block indices, normalized
geometry, observed text and derivative hash. `native_pages` walks every required
continuation and verifies complete text/block lengths and whole-text hashes
before assembling a deterministic complete page proof. Block 13 and long blocks
remain reachable; missing, reordered or truncated segments cannot yield a proof.
Continuation requests reuse the exact rendered image and do not rasterize again.
Each complete page proof is bounded to 2 MiB and the book proof to 8 MiB; exceeding
those processing limits refuses acceptance rather than discarding source text.
Preview truncation and missing CID Unicode mappings remain explicit. Staging
uses only complete observed spans and cannot infer semantic research from them.
Model-authored accepted passages still use the existing DOCUMENT jobs and exact
materialized tokenizer counts; there is no second encoding phase or local BGE.

The initial PDF path admits up to 32 pages (including the independently found
10-page acceptance candidate), with one page rendered per call. The shared PDF/DjVu
model-reader page policy preserves RGB colour and starts at JPEG q85, up to the
source's native long edge (at most 3200 px). Its 8 MiB output budget falls back
through q80/q70/q55, then 2400/1600 px (never upscale DjVu above native resolution).
The former 1600/1280 px and 256 KiB ceiling was a 2026-10-20 pilot transport
limit, not a product requirement. Actual width, height, scale, quality, JPEG
bytes and byte limit appear in `preview_render` and `native_text_info`;
`preview_only` still means a model-reading derivative, not archival source proof.
Already issued page receipts are immutable. After native/runtime replacement,
controller resolves the previous JPEG by its original/issued SHA256 under the
same content-addressed page archive and bound HEAD/conditional GET; its byte
hash, preview geometry and page proof must match the earlier issuance. A newly
unissued page uses the new policy. Missing or changed archive bytes fail closed. Original physical page dimensions
remain separate. Original PDF bytes and complete native text are not reduced by this
preview policy. The parser child
has fixed code, a 30-second CPU budget, 60-second wall budget, 2-GiB address-space
limit and bounded output. Scratch lives only under the verified `/kaggle/temp`
root; cleanup removes the exact owned intake leaf. Original/derivative byte and
disk reservation limits are finite. PDF parsing is container inspection and
rendering, not OCR, rights clearance or semantic extraction.

DjVu is recognized and returns `original_djvu_pinned_parser_required` before
archive admission. A verified DjVuLibre executable/library closure is not yet
configured for this target extension. Existing legacy DjVu behavior is not a
claim of target support. Large multipart originals and a complete import of a
large research graph remain separate
workflows. Unknown/cancelled native phases remain durable pending operations;
they are never reported as saved or accepted source imports.

Tests use actual SQLite/OAuth/control admission, native file hashes, transport
reconnect/result replay and synthetic HTTP/parser peers. Additional tests use the
installed PDF library on generated synthetic 10-page and truncated-text fixtures.
No real provider
download, archive write, PG startup, model load or semantic import acceptance is
claimed by these tests.



## Direct Telegram PDF ingress (source seam, not a live credential binding)

`book_ingest(start, telegram_original=...)` is an alternative to the existing
`file` attachment, only on the configured Kaggle source profile. Do not combine
both. The strict metadata object contains `chat_id`, `topic_id`, `message_id`,
`document_id`, `sha256`, and `size`. A Vibe archive entry or topic link is not a
complete locator; resolve the exact document from authoritative archive metadata.
No session, token, API hash, download URL, bytes or arbitrary path is accepted.
The existing owner-personal import authorization and public-rights requirements
still apply. The normal attachment/fileParams path is unchanged.

The locator becomes immutable original-acquire arguments under the existing
queue/WSS request hash. The original spec and all historical PDF/parser receipt
codecs remain unchanged. The native worker uses a separate
`NativeTelegramOriginalDownloader`, borrowing an already-authorized Telethon
client from the existing runtime owner, never creating or logging in a session.
The configured archive chat/topic must match; the run's exclusive session lease
and the current WSS writer binding must remain current before and during the
transfer. A bounded 64 KiB stream goes directly from Telegram to the private
Kaggle workspace. Exact message/document/topic, expected source SHA/size, PDF
magic and the existing pinned parser are checked. Partial files are removed on
failure/cancellation. No original bytes are returned through Dev or Vibe.

Composition is intentionally explicit: the authorized runtime owner supplies
`TargetRuntime(..., telegram_original_downloader=NativeTelegramOriginalDownloader(
client=existing_owned_client, chat_id=registered_chat, topic_id=registered_topic,
current=exclusive_session_lease_is_current))`. The downloader borrows the client;
it does not connect/disconnect it, discover credentials, infer a lease, or mutate
archive permissions. An absent binding returns
`original_telegram_binding_required`, not a fabricated successful import.
The operator must provide the existing service credential/session capability,
its current exclusive ownership proof and the registered archive scope. This
patch does not establish any of those from the mere presence of an environment
variable, create a Telegram authorization, or activate a live notebook.

This is **ingress only**. The existing archive/verification, retained-file
restore, staging and durability mechanisms are preserved, including their
currently declared limits. It does not replace persistence with Telegram or
claim a complete Gause import/search, large-book activation, or Dataset durability.
Tests use synthetic Telegram/parser/storage peers, not a private book or provider.

## Selected-image staging and explicit native continuation

`book_pages` now returns `native_text_info.source_image_receipt_sha256`, an exact
binding of the issued image, original receipt, physical page identity, parser
and render geometry. The controller persists only this small metadata record;
it is neither extracted text nor pixels. Echo the hash in the corresponding
`StagePageInput.source_image_receipt_sha256`, with `source_material=visual_reviewed`
and a substantive `source_review_note`. The hash proves which image was issued,
not that a human/model actually read it or that its transcription is correct.

This selected-image mode accepts model-authored regions, text and geometry
without comparing them with a native text layer. Staging reads only already
issued receipts, never calls `book_pages` or scans the rest of the book. A
missing/mismatched image, another original, unreviewed input or a revoked owner
authority fails closed. Receipts survive controller restart; exact batch retry
uses the existing deterministic page/region/chunk identities and stage merge.
Eight pages, 100 chunks, 12 story candidates and 1 MiB JSON are batch envelopes,
not whole-book truncation limits. On legacy profiles the existing bounded control-storage budget still applies;
only the explicit schema14 profile below uses native PG staging.

On legacy profiles, source-manifest.v1 and accepted source.import.v2–v4 keep
native-block provenance and their historical single-command limits. An image
stage on those profiles still returns `source_image_activation_revision_required`
before embedding/acceptance. Old accepted bytes are not upgraded or relabelled.

### Schema14 native source profile (implemented source, not a live rollout)

The exact new profile is `rkb-v4-sql001-014-v1` /
`rkb-v4-native-source-activation-v1` / `rkb-python-apply-native-source-v1`.
It uses the same authenticated owner MCP entrypoint, original issuance authority,
ControlSpool, remote acceptance/outcome journal, shared compute admission and
native PG writer. There is no second model/service/queue.

1. `stage` accepts up to eight **already issued** page images and bounded
   model-authored content. The exact versioned batch includes the original,
   metadata, public-rights declaration, image receipts and research continuation.
   Batches chain by immutable hash and ordinal in native PG. Dev keeps compact
   progress plus the existing exact durable command spool, not an accumulated
   semantic graph, a complete book download or document vectors. Exact retries
   retain their original key; UNKNOWN acceptance is read back, not resequenced.
   Reissued identical pages can add chunks; an explicit corrected transcription
   replaces dependent projected material but never erases accepted batch bytes.
2. `validate` invokes native deterministic semantic/tokenizer preflight **before**
   sealing. It checks complete original-page coverage, exact selected provenance,
   supported semantic inputs and every complete BGE input's 512-token bound.
   Known invalid input yields a durable rejected-precondition receipt while
   leaving staging open for a corrected batch; an unchanged rejected head is
   not blindly retried. No model reading, OCR, rendering or inference is invented.
3. Explicit `finalize` begins materialization. Each accepted
   `source.materialize.v1` command computes at most **one** input using the same
   verified resident BGE/tokenizer. The vector is installed directly into native
   PG, never sent through Dev. Polling resumes one ordinal, retains UNKNOWN and
   reconciles an already installed vector without another forward. No PG
   connection/transaction is held across tokenizer/model awaits. Shared admission
   can schedule reads between commands. `status` alone does not invent finalize.
4. After every input has an exact applied outcome, `source.activate.v1` atomically
   installs public sources/passages/stories/facts/graph and image-origin proofs.
   Intermediate stages/candidates/vectors are private writer tables, not search
   results. The image manifest expressly calls supplied text
   `model_transcription`; it never asserts native block offsets. Native readers
   select the new verifier only under the exact schema14 profile. `Saved` /
   `finalized` is returned only after remote durability of the activation outcome.

SQL001–012 and old codecs/functions stay unchanged; additive SQL013/014 and
versioned helper functions provide the new profile. Installing it into an existing
live database requires its separately authorized, verified schema transition;
shipping this source does not mutate an installed worker or claim live migration.
Notebook packaging and native recovery retain the materializer and exact source
hash; accepted materializer pins cannot silently use changed lowering code.
Canonical knowledge records exclude only rebuildable embedding floats, never
model-selected text or evidence. Restore recomputes vectors from exact accepted
inputs with the pinned resident model, not another model-reading pass.

Per-batch JSON has a 700 KiB source payload ceiling to fit the unchanged 1 MiB
accepted-command envelope after encoding. The chain permits up to 2,500 batches /
original pages, native compilation has a 128 MiB raw-input resource ceiling and
32 MiB canonical projection ceiling, and at most 280,000 derived inputs. These
are explicit resource refusals, not silent truncation or raised legacy codec caps.
This text/story/fact/graph slice rejects nonempty illustration/media/article
activation that it cannot faithfully lower. Those components remain explicit
unsupported input, not silently dropped content or a claim of complete book parity.
Native inputs with unknown nested semantic fields are likewise rejected.

The 174-page regression uses actual typed receipt/page-count contracts and
synthetic metadata/text, not the private book, real Telegram or Kaggle. It
checks multi-batch staging, exact retry and no hidden page acquisition. New
disposable-PG fixtures exercise public activation/search with synthetic unit
vectors, separately from actual resident-BGE/live acceptance. Client-supplied pages outside `book_pages` still
need their own trusted issuance binding; a caller-created hash is not evidence.
