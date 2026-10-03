# Runtime configuration

No credentials or environment files belong in Git.

Required production settings:

| Name | Purpose |
|---|---|
| `RKB_RESOURCE_URL` | exact public MCP resource URL used for RFC 8707 audience/resource binding |
| `RKB_OAUTH_ISSUER` | shared Supabase Auth OAuth/OIDC issuer |
| `RKB_OAUTH_JWKS_URL` | issuer JWKS endpoint |
| `KB_SUPABASE_URL` | Regional Knowledge project API URL; preferred over generic `SUPABASE_URL` |
| `KB_SUPABASE_PUBLISHABLE_KEY` | current low-privilege Supabase API key for user-JWT/RLS requests; preferred over legacy anon key |
| `KB_SUPABASE_SECRET_KEY` | current server-only elevated Supabase API key for exact object-locator lookup after successful user-RLS authorization; never used for search/ranking |
| `KB_SUPABASE_SESSION_CONNECTION` | PostgreSQL Session Pooler connection string for migrations/ops from IPv4-only DevCoveer |
| `KB_SUPABASE_DIRECT_CONNECTION` | direct PostgreSQL connection string; IPv6 on Supabase Free and therefore not usable from the current IPv4-only DevCoveer host |
| `KB_SUPABASE_ANON_KEY` / `KB_SUPABASE_SERVICE_ROLE_KEY` | legacy fallbacks only; current publishable/secret keys are preferred |
| `RKB_PUBLIC_BASE_URL` | stable evidence-page base URL returned by search/fetch |
| `RKB_EMBEDDING_ENDPOINT` | external OpenAI-compatible embeddings endpoint; optional for lexical-only degraded mode |
| `RKB_EMBEDDING_MODEL` | configured 768-dimension embedding model |
| `RKB_EMBEDDING_API_KEY` | server-side embedding-provider credential |
| `RKB_S3_ENDPOINT` | S3-compatible object-store endpoint |
| `RKB_S3_REGION` | object-store region |
| `RKB_S3_BUCKET` | private corpus bucket |
| `RKB_S3_ACCESS_KEY_ID` | server-side object-store credential |
| `RKB_S3_SECRET_ACCESS_KEY` | server-side object-store credential |

The server must separate two database access modes:

1. **user-RLS path** — search, catalog and all ordinary authorization decisions use the user's verified JWT so Supabase RLS remains authoritative;
2. **service-only path** — internal object locator lookup, object upload and signed URL generation may use server credentials only *after* a user-RLS authorization check has succeeded.

Never expose object-store access keys, service-role Supabase keys or raw object keys through MCP.

`RKB_DEV_NOAUTH=1` exists only for local deterministic tests and must be rejected by deployment configuration.

## Database-size invariant

Corpus body text must not be stored in Postgres. Region/chunk text lives in
private object storage. Postgres keeps hashes, byte-range locators, `tsvector`,
embeddings and compact metadata only. This is a hard invariant for the 500 MiB
Free-tier target.


## Current infrastructure status — 2026-10-03

A dedicated Regional Knowledge Supabase project has now been added to the
canonical shared DevCoveer environment at `/home/dev/.env`.

Present:
- `KB_SUPABASE_URL`;
- `KB_SUPABASE_DIRECT_CONNECTION`.

Still required before live DB/runtime acceptance:
- `KB_SUPABASE_SESSION_CONNECTION` copied from Supabase Dashboard → Connect →
  Session pooler; the current DevCoveer network is IPv4-only while the Free-plan
  direct database endpoint is IPv6;
- `KB_SUPABASE_PUBLISHABLE_KEY`;
- `KB_SUPABASE_SECRET_KEY`.

No migration is considered applied until the migration runner successfully
connects through the session pooler and performs schema/RLS/RPC readback.
OAuth resource binding remains a later acceptance gate.


## API key compatibility

Runtime accepts project-prefixed current keys first and legacy names only as
fallback:

```text
KB_SUPABASE_PUBLISHABLE_KEY
  -> KB_SUPABASE_ANON_KEY
  -> SUPABASE_PUBLISHABLE_KEY
  -> SUPABASE_ANON_KEY

KB_SUPABASE_SECRET_KEY
  -> KB_SUPABASE_SERVICE_ROLE_KEY
  -> SUPABASE_SECRET_KEY
  -> SUPABASE_SERVICE_ROLE_KEY
```

New `sb_secret_*` keys are sent in the `apikey` header and are not treated as
JWT bearer tokens. User requests still carry the actual user access JWT in the
`Authorization` header, preserving RLS.
