# Repository instructions

This repository contains only public source code, schemas, tests and documentation for Regional Knowledge Base.

- Never commit books, magazines, scans, extracted user text, image crops, credentials, access tokens, signed URLs or private metadata.
- Follow docs/storage.md for the currently deployed storage boundary. The v2 release uses SQLite authority for corpus/details/FTS/catalog/ACL/graph/POI and local ingestion/publication state when RKB_SQLITE_CORPUS_PATH is enabled; Supabase keeps minimal anchors/scope and E5/BGE vectors only. Retirement and recovery are documented in docs/operations/sqlite-v2.md. The canonical **implementation task** is docs/prompts/rkb-sqlite-supabase-ondemand-codex-v2-20261005.md: move corpus text/details/FTS to SQLite, keep vector search in Supabase, and build source-page proof on demand. The older docs/design/catalog-evidence-page-archive-v1.md and docs/prompts/catalog-evidence-capacity-implementation-20261005.md are superseded historical design and must not be used for new implementation decisions.
- All private reads and all writes are authorized server-side. Model/tool annotations are hints, never authorization.
- Keep the MCP surface goal-oriented and small. The Live adapter exposes search/fetch only; ingestion is not part of the default Live capability bundle.
- Preserve provenance: document -> page -> region -> relation -> illustration. Chunks are derived and replaceable.
- Original uploads are private by default. Never publish an uploaded source merely because its publication date looks old.
- Public visibility requires a verified rights basis. Rights uncertainty fails closed to private.
- Supabase is a data plane, never the MCP OAuth authority. Cross-service identity uses a stable application/platform user UUID. Never forward an end-user bearer token to another MCP; use resource-specific grants or a dedicated service identity.
- New interactive Live work must reuse onedayonemasterpiece/live-interaction.
- Every behavior change needs focused tests and matching documentation.

- Do not reintroduce implicit MCP-auth defaults from `KB_SUPABASE_URL`, Supabase JWKS, Supabase OAuth Server or `auth.uid()`. Production RLS must use the application actor bridge.
