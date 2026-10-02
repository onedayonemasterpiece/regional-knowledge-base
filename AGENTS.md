# Repository instructions

This repository contains only public source code, schemas, tests and documentation for Regional Knowledge Base.

- Never commit books, magazines, scans, extracted user text, image crops, credentials, access tokens, signed URLs or private metadata.
- Canonical corpus binaries and derived artifacts live in private S3-compatible object storage; searchable metadata and ACLs live in Supabase.
- All private reads and all writes are authorized server-side. Model/tool annotations are hints, never authorization.
- Keep the MCP surface goal-oriented and small. The Live adapter exposes search/fetch only; ingestion is not part of the default Live capability bundle.
- Preserve provenance: document -> page -> region -> relation -> illustration. Chunks are derived and replaceable.
- Original uploads are private by default. Never publish an uploaded source merely because its publication date looks old.
- Public visibility requires a verified rights basis. Rights uncertainty fails closed to private.
- Cross-service user identity is the stable Supabase Auth subject. Never forward an end-user bearer token to another MCP; use resource-specific tokens or a dedicated service identity.
- New interactive Live work must reuse onedayonemasterpiece/live-interaction.
- Every behavior change needs focused tests and matching documentation.
