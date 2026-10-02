# Storage, privacy and lifecycle

## Object storage is canonical

Corpus data does **not** live in GitHub. Use a private S3-compatible bucket
(initial provider may be Yandex Object Storage; code must depend on the S3
contract, not provider-specific APIs).

Recommended logical keys:

```text
users/<user_id>/documents/<document_id>/source/<source_sha256>.pdf
users/<user_id>/documents/<document_id>/pages/<page_id>.webp
users/<user_id>/documents/<document_id>/illustrations/<illustration_id>.png
users/<user_id>/documents/<document_id>/graph/<revision>.json
users/<user_id>/documents/<document_id>/text/<revision>.txt
```

Keys are not authorization. Every read is authorized from database state before
the server resolves an object locator or streams bytes.

## Why not global content-addressed public keys

Global hash paths create an existence oracle: a caller can infer that another
user uploaded the same private file. The database stores SHA-256 for integrity and
internal deduplication, but external object identities are opaque and
user/document scoped.

Internal deduplication may be introduced later only if it never exposes object
existence or changes ACL semantics.

## Separate source privacy from content visibility

A crucial distinction:

- `source_visibility`: the raw PDF/scan uploaded by a person; default and
  normally permanent value is private.
- `content_visibility`: normalized text/metadata/search projection; private,
  workspace or public.
- `media_visibility`: derived illustration crop; may differ from both.

A public or statutory-access work uploaded from a private scan does **not** make
the user's exact PDF, annotations, ex libris, marginalia or scan metadata public
automatically.

## VibePublish

VibePublish MediaBank is a secondary integration surface, not canonical storage.
Mirror an illustration only when its policy permits that use. Store its immutable
VibePublish origin/entry reference alongside the illustration; do not make
Telegram availability part of search correctness.

Private user assets are not mirrored to the operator's Telegram media bank by
default.

## Deletion

Deleting a private document removes:

1. active retrieval rows;
2. derived object-storage objects;
3. original object after retention/recovery policy;
4. service-owned media mirrors where deletion is supported.

If a separately curated public corpus item exists, it has its own provenance and
lifecycle; deleting one user's private source cannot silently delete a public
canonical record.

## Text projections and range reads

Full normalized region/chunk text is not stored in Supabase. Ingestion writes
UTF-8 text projections to private object storage. A retrieval chunk stores only a
server-only `text_object_id`, byte offsets, SHA-256, `tsvector`, embedding and
compact metadata.

`fetch` first resolves the chunk using the caller's JWT and RLS. Only after that
authorization succeeds may the server use its service credential to resolve the
exact object locator and issue an S3 Range GET. Returned bytes are hash-verified
before decoding. The service-role credential is never used for candidate search.

## Attached PDF ingestion source

ChatGPT file parameters provide a temporary `download_url` and stable
`file_id`. The start call downloads the source immediately; the temporary URL is
never persisted.

The source path is deliberately **file-backed**, not whole-file-in-memory:

```text
temporary ChatGPT HTTPS URL
  -> DNS/public-address preflight
  -> DNS-pinned streaming download
  -> bounded local temporary PDF
  -> SHA-256 + PyMuPDF inspection
  -> S3 upload_file
  -> temporary file removed
```

The downloader:

- accepts HTTPS only and port 443;
- rejects credentials, fragments, redirects, localhost and IP-literal URLs;
- resolves the hostname before download and rejects any non-global address;
- pins the approved DNS answers into the HTTP connector for the request;
- ignores proxy environment variables and cookies;
- disables automatic content decompression and requests identity encoding;
- streams in bounded chunks instead of loading the whole PDF into Python heap;
- defaults to **128 MiB** maximum source size;
- allows an operator-configured `RKB_MAX_PDF_BYTES`, hard-capped at **512 MiB**;
- requires a PDF signature before admission.

Production should additionally use restrictive egress/network policy. The model
never receives object-store credentials or raw object keys.

Postgres keeps only an opaque `source_object_id`, never the raw S3 key in the
user-readable ingestion row.

A failed start remains private. Retrying the same attached `file_id` and exact
source hash reuses the existing ingestion/document and reconciles the
deterministic source object instead of creating a second document or object row.

## Page reads

`book_pages` first authorizes the ingestion job under the user's JWT. Only then
does the server resolve the exact source object with its service credential,
download it to a bounded temporary file, verify the full SHA-256 and render only
the requested small page batch with PyMuPDF.

The PDF bytes are therefore not retained on local disk between calls and do not
become a high-memory runtime dependency.

## Staged graph and finalization

Semantic parsing does not write half-complete page graphs into active database
tables. Each `stage` call merges into a canonical staged graph and writes a new
immutable, content-hashed `document_graph` object. The ingestion row exposes
only its opaque object ID.

Model-facing keys are deliberately short and local. The server deterministically
derives region, illustration and chunk UUIDs from the document/revision/page and
those keys. Semantic chunk text is assembled server-side from referenced source
regions, so the model does not need to duplicate long text inside a second tool
argument.

Only `finalize` materializes a revision into Postgres. It creates:

- one UTF-8 text projection with exact byte ranges and SHA-256 per chunk;
- page and region provenance rows;
- region relations;
- exact illustration crops derived from source-page bbox coordinates;
- illustration provenance rows;
- FTS and vector retrieval rows.

The activation RPC independently checks complete page indexes, unresolved review
flags, textual-region coverage and cross-document provenance before changing
`active_revision`. Ordinary authenticated tokens cannot directly patch the
active revision or mutate an already active materialized revision.
