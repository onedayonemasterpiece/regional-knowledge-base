# Runtime configuration

No credentials or environment files belong in Git.

Required production settings:

| Name | Purpose |
|---|---|
| `RKB_RESOURCE_URL` | exact public MCP resource URL used for RFC 8707 audience/resource binding |
| `RKB_OAUTH_ISSUER` | shared Supabase Auth OAuth/OIDC issuer |
| `RKB_OAUTH_JWKS_URL` | issuer JWKS endpoint |
| `SUPABASE_URL` | project API URL |
| `SUPABASE_ANON_KEY` | public client key used only with the caller's JWT/RLS path |
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