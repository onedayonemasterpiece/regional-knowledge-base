# Runtime configuration

No credentials or environment files belong in Git.

## Application auth plane

The MCP auth plane is independent from Supabase.

| Name | Purpose |
|---|---|
| `RKB_RESOURCE_URL` | exact public Knowledge MCP resource URL |
| `RKB_AUTH_ISSUER` | application/platform OAuth issuer |
| `RKB_AUTH_JWKS_URL` | application OAuth signing-key endpoint |
| `RKB_DEV_NOAUTH` | local deterministic tests only; never production |
| `RKB_ALLOW_LEGACY_SUPABASE_USER_JWT` | temporary test-only escape hatch for the old PostgREST bearer path; never beta/production |

The completed beta will add the application authorization-server settings
(client/redirect/owner login state) required by the implementation. Those values
must be RKB/platform-auth values, not Supabase OAuth settings.

## Supabase data plane

| Name | Purpose |
|---|---|
| `KB_SUPABASE_SESSION_CONNECTION` | canonical Postgres Session Pooler connection used by the server/migrations from DevCoveer |
| `KB_SUPABASE_DIRECT_CONNECTION` | optional direct PostgreSQL connection; currently IPv6-only from this host |
| `KB_SUPABASE_URL` | project API URL for bounded admin/health compatibility where needed |
| `KB_SUPABASE_PUBLISHABLE_KEY` | low-privilege project API key; not an MCP identity credential |
| `KB_SUPABASE_SECRET_KEY` | server-only Supabase API/admin key; never an end-user identity |
| `KB_SUPABASE_ANON_KEY` / `KB_SUPABASE_SERVICE_ROLE_KEY` | legacy key-name fallbacks only |

`KB_SUPABASE_JWKS_URL` is not part of the target MCP authentication design.
Supabase Auth/OAuth discovery is not a deployment gate.

### Target production DB path

The finished beta must use `KB_SUPABASE_SESSION_CONNECTION` with a direct
Postgres RLS bridge:

- application OAuth subject is a stable UUID;
- each transaction sets that UUID as the application actor;
- queries run under a non-bypass RLS role;
- database policies read the application actor, not `auth.uid()`;
- user OAuth bearer values are never sent to Supabase.

Until that bridge is implemented, `SupabaseRestBackend` is intentionally
disabled by default.

## Object storage

| Name | Purpose |
|---|---|
| `RKB_S3_ENDPOINT` | S3-compatible object-store endpoint |
| `RKB_S3_REGION` | object-store region |
| `RKB_S3_BUCKET` | private corpus bucket |
| `RKB_S3_ACCESS_KEY_ID` | server-side object-store credential |
| `RKB_S3_SECRET_ACCESS_KEY` | server-side object-store credential |

Never expose object-store credentials, raw object keys or signed URLs through MCP.

## Embeddings

| Name | Purpose |
|---|---|
| `RKB_EMBEDDING_ENDPOINT` | external OpenAI-compatible embeddings endpoint |
| `RKB_EMBEDDING_MODEL` | 768-dimension embedding model |
| `RKB_EMBEDDING_API_KEY` | provider credential |
| `RKB_PUBLIC_BASE_URL` | optional HTTP evidence-page base; canonical knowledge:// URIs work without it |

Embeddings are explicit opt-in. The service never inherits `OPENAI_API_KEY`,
`GOOGLE_API_KEY`, `GEMINI_API_KEY` or any other shared provider credential.
Without the complete dedicated `RKB_EMBEDDING_*` configuration it performs no
external embedding request and degrades to lexical retrieval.

Lexical-only degradation is allowed as a failure mode, not as the final hybrid
beta acceptance.

## Database-size invariant

Corpus body text is not stored in Postgres. Region/chunk text lives in private
object storage. Postgres stores hashes, byte-range locators, `tsvector`,
embeddings and compact metadata. The operational target remains comfortably below
the 500 MiB Free-tier database limit.

## Current infrastructure status — 2026-10-03

Already live/verified:
- dedicated Regional Knowledge Supabase project;
- `KB_SUPABASE_SESSION_CONNECTION` via IPv4 Session Pooler;
- project API keys;
- migrations 001–005;
- pgcrypto + vector;
- private object-storage bucket;
- DB size approximately 12 MiB at acceptance;
- corpus body columns absent from Postgres;
- earlier owner/workspace/grant RLS acceptance.

Important correction:

The earlier acceptance used Supabase user-JWT / `auth.uid()` identity. That was
useful to validate the policy shape but is **not the final MCP identity path**.
Before product beta, migration and acceptance must be repeated with the
application-owned actor/RLS bridge described above.

Supabase OAuth 2.1 Server is not required and must not appear in the beta setup
instructions.
