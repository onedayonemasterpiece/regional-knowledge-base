# Native original intake

This opt-in target extension uses the existing recovery-phase jobs, authenticated
WSS, document resource admission and durable result acknowledgments. It changes
neither the running notebook nor existing launch configuration automatically.
`TargetLaunchSpec.original_intake` takes `OriginalIntakeConfig`: exact allowed
archive HTTPS origins, exact parser package versions, a byte limit and a finite
transfer deadline. Parser packages must also be present in the independently
attested dependency inventory and exact Stage0 wheel closure. No host parser
fallback or dependency installation outside Stage0 is used.

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
2. `original_archive`: the retained native file is uploaded directly with an
   exact conditional PUT ticket from the existing S3 client. Dev receives no
   original bytes. A lost PUT acknowledgment is reconciled by native provider
   HEAD before another effect is attempted.
3. `original_verify`: a scoped conditional GET ticket binds the exact provider
   version. Kaggle streams the object into fresh private scratch and verifies
   its complete size/hash. Dev independently checks HEAD size/version before
   retaining `VerifiedOriginalReceipt`. A HEAD or upload acknowledgment alone
   cannot produce this receipt.
4. `original_pages`: the retained original is rendered by the pinned isolated
   PDF parser. Each call returns one physical page's bounded metadata. Its real
   JPEG derivative follows the same archive/readback phases, after which Dev may
   retrieve that bounded derivative for MCP ImageContent. Originals never follow
   this Dev download path.

`verified(context, ingestion_id)` returns only a locally retained workflow
receipt after fresh current-authority and native storage-version checks. Its
`.original` property supplies the exact source-import original binding. The
receipt includes source/ingestion/request identities, actual size/hash,
content-addressed stable archive key, native version, page count, parser pin,
acquisition hash and native readback hash. Signed URLs are not receipt locators.

`pages(...)` returns the existing `RenderedPageBatch`. Page IDs are
`uuid5(source_id, "page:1:<physical index>")`. `native_pages(...)` returns an
immutable bounded `NativePagesProof`: exact original receipt hash and canonical
page metadata including original SHA, parser pin, actual block indices, normalized
geometry, observed text and derivative hash. Preview truncation and missing
CID Unicode mappings remain explicit. Staging must use only observed spans and
must not infer complete native text or semantic research from these previews.
Model-authored accepted passages still use the existing DOCUMENT jobs and exact
materialized tokenizer counts; there is no second encoding phase or local BGE.

The initial PDF path admits up to 32 pages (including the independently found
10-page acceptance candidate), with one page rendered per call. The parser child
has fixed code, a 30-second CPU budget, 60-second wall budget, 2-GiB address-space
limit and bounded output. Scratch lives only under the verified `/kaggle/temp`
root; cleanup removes the exact owned intake leaf. Original/derivative byte and
disk reservation limits are finite. PDF parsing is container inspection and
rendering, not OCR, rights clearance or semantic extraction.

DjVu is recognized and returns `original_djvu_pinned_parser_required` before
archive admission. A verified DjVuLibre executable/library closure is not yet
configured for this target extension. Existing legacy DjVu behavior is not a
claim of target support. Large multipart originals, restoring lost ephemeral
native handles, and a complete import of a large research graph remain separate
workflows. Unknown/cancelled native phases remain durable pending operations;
they are never reported as saved or accepted source imports.

Tests use actual SQLite/OAuth/control admission, native file hashes, transport
reconnect/result replay and synthetic HTTP/parser peers. Additional tests use the
installed PDF library on generated synthetic 10-page and truncated-text fixtures.
No real provider
download, archive write, PG startup, model load or semantic import acceptance is
claimed by these tests.
