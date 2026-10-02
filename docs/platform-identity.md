# Platform identity: one user, many MCP products

## Decision

Use one shared **Supabase Auth OAuth 2.1/OIDC issuer** for the product family,
while every product remains a separate MCP resource server.

```text
                       Supabase Auth
                  one account / stable sub
                           |
       +-------------------+-------------------+
       |                   |                   |
 Knowledge MCP      Wonderful Lections   Projects Hub
 resource A          resource B           resource C
       |                   |                   |
 Street Story        VibePublish          future MCPs
```

A person can therefore use several products without creating several identities.
The canonical identity is `issuer + sub`; email/name are mutable profile data.

Authorization is deliberately **not** shared automatically. Each service owns its
roles, workspaces, ACLs and quotas.

## Resource-bound OAuth

OpenAI MCP clients send the RFC 8707 `resource` value through authorization and
token exchange. Every MCP publishes RFC 9728 protected-resource metadata pointing
to the shared issuer.

Supabase accepts the MCP `resource` parameter, but ordinary OAuth access tokens
default to the normal Supabase audience. Production therefore needs an explicit
resource-audience rule. The platform uses:

- one pre-registered OAuth client identity per product/resource for first-party
  ChatGPT/plugin connections;
- one distinct OAuth client identity for each first-party cross-service
  integration;
- a Supabase Custom Access Token Hook that maps approved `client_id` values to
  the exact MCP resource audience;
- server verification of issuer, signature, expiry, subject, client ID and exact
  audience/resource.

A token for Knowledge must be rejected by Wonderful Lections and vice versa.

Do not rely on OAuth application scopes for product authorization: Supabase's
OAuth server currently supports standard identity scopes, while database access
is controlled by RLS/client policy.

## Why not unrestricted DCR initially

Dynamic Client Registration is useful, but an unknown dynamically-created
`client_id` cannot safely be mapped to one exact resource by a hook that only
receives token claims/client identity.

Therefore production v1 uses known product client registrations for exact
audience binding. DCR can be enabled for experiments only after a live test proves
resource-bound token issuance for that client path. This is an interoperability
constraint, not a reason to create another user database.

## User experience

For a non-technical user:

1. connect the first product;
2. authenticate once at the common sign-in domain;
3. approve that product;
4. when connecting another product, the existing auth session identifies the same
   account and the user normally sees only the new resource consent.

No API keys, tenant IDs, database names or bearer tokens appear in the user flow.

A future common “Connected services” page can show every granted MCP and revoke
them independently.

## Cross-service synergy

There are two valid patterns.

### Model orchestration

When ChatGPT has Knowledge and Wonderful Lections connected, the model can call
both independently under their own tokens. No backend token sharing is needed.

### First-party service delegation

A service such as Projects Hub may need Knowledge during its own Live session.
Projects Hub then acts as an OAuth client of the Knowledge resource:

```text
user -> Projects Hub
          |
          +-- "Enable Regional Knowledge"
                    |
              shared login session
                    |
              user consent once
                    |
          encrypted Knowledge refresh grant
                    |
          resource-bound Knowledge access token
```

This is the preferred way for Projects Hub Live, Wonderful Lections or Street
Story to reuse private user knowledge.

Do **not** forward a Projects Hub bearer token to Knowledge. Do not use a global
service-role credential to impersonate arbitrary users.

Service identities are reserved for non-user-specific operations such as public
corpus maintenance or a tightly scoped VibePublish media mirror.

## Live-model consequence

The shared identity architecture does not mean one giant MCP. Live models should
see a small capability bundle. Projects Hub can expose a Knowledge adapter with
only `search` and `fetch`; its stored user delegation handles authorization
server-side.

This preserves the shared `live-interaction` limit discipline and avoids
dumping every product tool into every Live session.

## Migration

Wonderful Lections already has a tested tenant-bound OAuth implementation and
remains operational as a rollback path. Migration order:

1. deploy the shared Supabase issuer and consent UI;
2. prove exact Knowledge resource/audience binding;
3. connect one user through ChatGPT;
4. prove second-resource connection resolves to the same `sub`;
5. migrate Wonderful Lections only after refresh/revocation/resource-isolation
   acceptance;
6. add Projects Hub delegated Knowledge access;
7. migrate other MCPs incrementally.

Never flag-day every product onto a new issuer.
