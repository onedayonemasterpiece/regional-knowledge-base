# Storage, privacy and lifecycle

## Object storage is canonical

Corpus data does **not** live in GitHub. Use a private S3-compatible bucket (initial provider may be Yandex Object Storage; code must depend on the S3 contract, not provider-specific APIs).

Recommended logical keys:

```text
tenants/<tenant_id>/documents/<document_id>/source/<source_sha256>.pdf
tenants/<tenant_id>/documents/<document_id>/pages/<page_id>.webp
tenants/<tenant_id>/documents/<document_id>/illustrations/<illustration_id>.png
tenants/<tenant_id>/documents/<document_id>/graph/<revision>.json
```

Keys are not authorization. Every read is authorized from database state before issuing a short-lived signed URL or streaming bytes.

## Why not global content-addressed public keys

Global hash paths create an existence oracle: a caller can infer that another user uploaded the same private file. The database stores SHA-256 for integrity and internal deduplication, but external object identities are opaque and tenant/document scoped.

Internal deduplication may be introduced later only if it never exposes object existence or changes ACL semantics.

## Separate source privacy from content visibility

A crucial distinction:

- `source_visibility`: the raw PDF/scan uploaded by a person; default and normally permanent value is private.
- `content_visibility`: normalized text/metadata/search projection; private, workspace or public.
- `media_visibility`: derived illustration crop; may differ from both.

A public-domain work uploaded from a private scan does **not** make the user's exact PDF, annotations, ex libris, marginalia or scan metadata public automatically.

## VibePublish

VibePublish MediaBank is a secondary integration surface, not canonical storage. Mirror an illustration only when its policy permits that use. Store its immutable VibePublish origin/entry reference alongside the illustration; do not make Telegram availability part of search correctness.

Private user assets are not mirrored to the operator's Telegram media bank by default.

## Deletion

Deleting a private document removes:
1. active retrieval rows;
2. derived object-storage objects;
3. original object after retention/recovery policy;
4. service-owned media mirrors where deletion is supported.

If a separately curated public corpus item exists, it has its own provenance and lifecycle; deleting one user's private source cannot silently delete a public canonical record.


## Text projections and range reads

Full normalized region/chunk text is not stored in Supabase. Ingestion writes
UTF-8 text projections to private object storage. A retrieval chunk stores only a
server-only `text_object_id`, byte offsets, SHA-256, `tsvector`, embedding and
compact metadata.

`fetch` first resolves the chunk using the caller's JWT and RLS. Only after that
authorization succeeds may the server use its service credential to resolve the
exact object locator and issue an S3 Range GET. Returned bytes are hash-verified
before decoding. The service-role credential is never used for candidate search.
