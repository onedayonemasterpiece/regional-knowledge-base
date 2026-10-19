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

The beta originally authenticated one owner. The disagreement candidate also
authenticates registered canonical participants through the same authorization
server and resource; this candidate is not deployed yet. Both paths preserve:

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
one configured owner UUID and explicitly registered participant UUIDs. Personal
credentials authenticate those existing participants; they never create a user,
capability, source grant or verified external identity. Future shared platform
auth can bind the same UUID without changing ownership or corpus rows.

Email/display name are profile attributes, not identity keys.

## Cross-service access

A bearer minted for one MCP resource is not forwarded to another service.

For future private cross-service operations, the calling service obtains a
resource-specific grant/delegation. Public/system corpus maintenance may use a
narrow service identity.

This design keeps the user identity layer replaceable without moving the
Supabase database or corpus objects.

## Grok connector

Public MCP URL: `https://knowledge.kenigevents.ru/mcp`.
Add a custom Grok connector with that URL; no manually supplied client secret is
needed. Dynamic registration accepts public (`none`) PKCE clients as well as
existing confidential ChatGPT clients. Grok HTTPS callbacks on `grok.com` are
admitted, then bound to the exact URI registered by that client. Other callback
origins remain rejected unless explicitly configured by the operator.

On the currently deployed owner beta, sign in on the Regional Knowledge consent
page with the existing private
`/home/dev/.local/state/regional-knowledge-base/owner-login-secret`. The candidate
also accepts the registered expert's own personal login code, then requires
separate resource consent. Enter the code only on the RKB authorization server,
not in the AI conversation or connector configuration. The consent page shows
the authenticated person and offers logout/account switching. This is separate
from DevCoveer's owner key.
An omitted OAuth `resource` is bound to this server's sole knowledge resource;
explicit foreign resources remain rejected. Existing clients, credentials,
grants, corpus and worker configuration are preserved.


## Personal expert onboarding in the disagreement candidate

The owner first provisions the real person and explicit expiring capabilities
through `disagreement_participants`. The same canonical `rkb_users.id` is used
by personal OAuth and verified Slack/VK/platform bindings. Names, categories,
client-supplied UUIDs and academic weights do not authenticate a person.

With the existing RKB environment and existing OAuth state/key, the local owner
can issue a cryptographically random personal login code to a protected file:

```bash
regional-knowledge-personal-auth issue --person PERSON_UUID --days 30 \
  --output /home/dev/.local/state/regional-knowledge-base/personal/expert.json \
  --reason 'Registered expert enrollment'
```

The parent directory must already be private (0700), the output must not exist,
and the new file has mode 0600. The command prints only metadata/receipt, never the
code. Deliver it privately to the intended person through an authorized secure
channel. Never commit it, put it into an artifact/report, paste it into an AI
chat, or reuse the owner login secret. A lost command response is reconciled by
`issue` with the same file/person/reason and `--resume`; it preserves the original
credential ID, bytes and expiry instead of issuing another credential. Expiry
is 1..90 days, default 30. No public MCP operation issues login codes.

The expert adds the existing MCP URL in a compatible client and enters this
personal code on RKB's login page. This is authentication at the authorization
server, followed by authorization-code/PKCE and explicit resource consent; the
OAuth token endpoint does not accept a password grant. Personal research does
not run through Slack or depend on dot. Current capabilities and source/case
ACL are checked by the server. The existing single OAuth scope name does not
grant owner tools: importing or managing participants remains denied to experts.

List/revoke using the same configured local authority:

```bash
regional-knowledge-personal-auth list --person PERSON_UUID
regional-knowledge-personal-auth revoke --credential CREDENTIAL_UUID \
  --expected-epoch 1 --reason 'Access revoked'
```

Revocation invalidates all sessions, pending authorization codes, access and
refresh grants derived from that credential, including rotated families.
Current account/profile disable or expiry independently blocks login, consent,
token use and refresh. Capability/source ACL revocation is also checked on each
operation. Revoking one Slack binding affects requests derived from that binding;
it does not impersonate or replace a separate personal credential. Native login
codes are hashed inside the existing encrypted OAuth store; canonical people
and permissions remain in the existing corpus. A stable file lock serializes
CLI/web-worker mutations across atomic state-file replacement. Lock waits and
participant validation execute off the shared HTTP loop.

The candidate's HTTP tests cover native non-owner login, PKCE, current source
ACL, forbidden tools, refresh, revoke and restart for admitted ChatGPT/Grok
callback shapes. These are synthetic protocol tests. Individual live clients,
real non-owner enrollment and full Slack PMI remain NOT_RUN until actual
participants and provider/client readback prove them. The unchanged deployed
owner-only beta must not be described as already supporting this candidate.

Protocol references: [OAuth security BCP](https://www.rfc-editor.org/rfc/rfc9700.html)
and [resource indicators](https://www.rfc-editor.org/rfc/rfc8707.html).

The existing one-tool `live` MCP read profile also uses current canonical-person
policy. It still exposes only `knowledge_search`; moving to that profile cannot
bypass revoked participant search capability. This change adds no voice/video
transport or new Live framework copy. Configured OAuth and corpus owner UUIDs
must agree; mismatched configuration is rejected rather than creating another
owner. An issued personal login code is not accepted as an MCP bearer token.
