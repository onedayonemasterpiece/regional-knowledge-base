# MCP authentication and database identity

## Decision

Regional Knowledge Base must not use Supabase OAuth as its MCP authorization
server.

The two planes are independent:

```text
ChatGPT / Codex
      |
      | OAuth 2.1
      v
application/platform auth
      |
      | verified actor: platform subject UUID
      v
Regional Knowledge MCP
      |
      | transaction-local DB actor context
      v
Supabase Postgres / pgvector / RLS
```

Supabase is the managed **data plane**: Postgres, pgvector and database
operations. It is not the end-user OAuth provider for the MCP.

No Supabase Dashboard OAuth switch is required for this project.

## MCP authorization server

Authenticated ChatGPT MCPs still require OAuth 2.1. The authorization server is
application-owned and separate from Supabase.

For the beta, implement the smallest safe flow needed for one owner while keeping
the contracts multi-user capable:

- authorization code + PKCE S256;
- exact `resource` binding to the Knowledge MCP URL;
- explicit consent;
- short-lived access tokens;
- refresh rotation and replay revocation;
- discovery/protected-resource metadata;
- stable UUID subject;
- no secrets in URLs/logs/repository.

Use the already deployed/tested Wonderful Lections OAuth implementation as a
behavioral donor. Do not couple the new implementation to Wonderful Lections
business state.

The resource server verifies application tokens with generic
`RKB_AUTH_ISSUER`, `RKB_AUTH_JWKS_URL` and `RKB_RESOURCE_URL`. It must never
derive these values from `KB_SUPABASE_URL`.

## Database actor identity

The MCP bearer must **not** be forwarded to Supabase PostgREST.

Target production data path:

1. verify the application OAuth token;
2. resolve its stable UUID subject;
3. open a transaction through `KB_SUPABASE_SESSION_CONNECTION`;
4. switch to a non-bypass RLS role;
5. set a transaction-local application actor UUID;
6. execute SQL/RPC under RLS;
7. reset automatically at transaction end.

The database must use an application-owned helper such as
`rkb_current_actor_id()`, not `auth.uid()`, as the source of user identity.

The existing `SupabaseRestBackend` is transitional and explicitly disabled by
default because it forwards the caller bearer to PostgREST. It may only be
enabled by `RKB_ALLOW_LEGACY_SUPABASE_USER_JWT=1` for bounded legacy tests and
must not be used by the finished beta.

## Identity registry

Do not require Supabase Auth users for application identity.

Use an application-owned UUID user/identity table in the RKB schema. The beta has
one owner UUID. Future shared platform auth can issue the same stable platform
UUID without changing document ownership or corpus rows.

Email/display name are profile attributes, not identity keys.

## Cross-service access

A bearer minted for one MCP resource is not forwarded to another service.

For future private cross-service operations, the calling service obtains a
resource-specific grant/delegation. Public/system corpus maintenance may use a
narrow service identity.

This design keeps the user identity layer replaceable without moving the
Supabase database or corpus objects.
