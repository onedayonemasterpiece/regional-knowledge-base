# Runtime configuration

No credentials or environment files belong in Git.

Required production settings:

| Name | Purpose |
|---|---|
| `RKB_RESOURCE_URL` | exact public MCP resource URL used for RFC 8707 audience/resource binding |
| `RKB_OAUTH_ISSUER` | shared Supabase Auth OAuth/OIDC issuer |
| `RKB_OAUTH_JWKS_URL` | issuer JWKS endpoint |
| `KB_SUPABASE_URL` | Regional Knowledge project API URL; preferred over generic `SUPABASE_URL` |
| `KB_SUPABASE_JWKS_URL` | project-scoped Supabase JWKS endpoint; accepted directly by runtime |
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

A dedicated Regional Knowledge Supabase project is connected through the canonical
shared DevCoveer environment.

Live-verified:
- `KB_SUPABASE_URL`;
- `KB_SUPABASE_SESSION_CONNECTION` via IPv4 Session Pooler;
- `KB_SUPABASE_DIRECT_CONNECTION` is structurally valid but resolves IPv6-only
  from the current DevCoveer host and is therefore not used for operations;
- `KB_SUPABASE_PUBLISHABLE_KEY` is a current `sb_publishable_*` key;
- `KB_SUPABASE_SECRET_KEY` is a current `sb_secret_*` key;
- `KB_SUPABASE_JWKS_URL` returns a usable asymmetric signing key;
- Auth health, PostgREST publishable access and secret access return HTTP 200;
- OIDC discovery returns the expected issuer/JWKS and publishes authorization and
  token endpoints.

Database rollout:
- migrations `001_core.sql` through `005_poi_media_outbox.sql` are applied;
- 15 RKB tables exist;
- 25 RLS policies exist;
- `pgcrypto` and `vector` are installed;
- DB size at acceptance was 11.83 MiB;
- forbidden corpus body columns are absent from Postgres;
- schema/RLS/RPC readback completed successfully;
- live two-user RLS acceptance passed: owner isolation, workspace membership,
  explicit document grant, anonymous isolation and viewer update denial;
- temporary acceptance users were deleted afterward; the Auth project remains
  clean.

OAuth 2.1 Server status:
- OIDC discovery and asymmetric JWKS are live;
- MCP OAuth discovery at
  `/.well-known/oauth-authorization-server/auth/v1` currently returns 404,
  which means Supabase OAuth 2.1 Server is not enabled yet;
- enabling it requires the Supabase Dashboard or an account-level Management API
  token; project `sb_secret_*` credentials cannot change project Auth config.

Still required for full MCP deployment:
- enable Supabase OAuth 2.1 Server and configure the consent authorization path;
- `RKB_RESOURCE_URL` once the public MCP URL is assigned;
- resource-bound OAuth client acceptance against that URL;
- `RKB_PUBLIC_BASE_URL`;
- private S3-compatible object storage;
- embedding provider configuration;
- ChatGPT connection and resource/client acceptance.

The absence of these later runtime dependencies does not invalidate the live
Supabase database setup.

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
