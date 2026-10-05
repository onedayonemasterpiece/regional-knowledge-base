# Runtime configuration

No credentials or environment files belong in Git.

## Database connection budget

Production uses the Supabase Session Pooler. RKB_DB_POOL_MAX bounds the
per-process direct-PostgreSQL pool and defaults to **4**. The standard runtime has
three long-lived database users (MCP server, automatic indexing, graph discovery),
so the default reserves at most 12 of a 15-session pool and leaves capacity for a
bounded maintenance process. Production/acceptance scripts that open an additional
backend should set RKB_DB_POOL_MAX=1.

This is a connection-budget limit, not a query concurrency target. Do not raise it
per process without accounting for all long-lived RKB processes sharing the same
Session Pooler.

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
| `RKB_EMBEDDING_API_KEY` | provider credential; may be a local placeholder for a loopback sidecar |
| `RKB_EMBEDDING_SPACE` | stable vector-space identity, e.g. `local:multilingual-mpnet-base-v2:v1` |
| `RKB_PUBLIC_BASE_URL` | optional HTTP evidence-page base; canonical knowledge:// URIs work without it |

Embeddings are explicit opt-in. The service never inherits `OPENAI_API_KEY`,
`GOOGLE_API_KEY`, `GEMINI_API_KEY` or any other shared provider credential.
Without the complete dedicated `RKB_EMBEDDING_*` quartet it performs no
embedding request and degrades to lexical retrieval. `RKB_EMBEDDING_SPACE` is
stored with every embedded chunk and must match the query vector space; vectors
from different models are never compared even when their dimensions are equal.

Lexical-only degradation is allowed as a failure mode, not as the final hybrid
beta acceptance.

### Current local semantic retrieval controls

| Name | Purpose |
|---|---|
| `RKB_FAST_E5_ENABLED` | enable the pinned local E5 sidecar |
| `RKB_BGE_ENABLED` | enable the durable BGE main tier |
| `RKB_BGE_WARM_MODE` | `bge` is the measured default; diagnostic alternatives are `bge_lexical`, `e5_bge`, `e5_bge_lexical` |
| `RKB_BGE_QUERY_WAIT_SECONDS` | maximum interactive wait for a ready-worker BGE query before truthful fast fallback; default 0.8, hard maximum 3 |
| `RKB_LEXICAL_BUDGET_MS` | bounded ordinary SQLite FTS branch budget; default 100 ms |
| `RKB_BGE_TOKENIZER_PATH` | optional override for the SHA-pinned BGE tokenizer used only by local ingestion token-count validation |

The default BGE tokenizer path is
`~/.local/share/regional-knowledge-base/fast-e5/bge-m3-tokenizer.json`. It is
public model metadata, not a credential, but it is provisioned outside Git to keep
the repository small. New ingestion validation fails closed if exact E5/BGE token
counts cannot be obtained.

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