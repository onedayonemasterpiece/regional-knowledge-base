# OAuth, identity and multi-MCP architecture

The platform-level decision is documented in
[platform-identity.md](platform-identity.md). This file defines the Regional
Knowledge Base resource-server side of that contract.

## Shared issuer, separate resource

Regional Knowledge Base uses the same Supabase Auth OAuth 2.1/OIDC issuer as the
other first-party MCP products, but it remains a separate protected resource.

The stable user identity is `issuer + sub`. Email, display name and product-local
tenant/workspace names are not identity keys.

The Knowledge MCP publishes RFC 9728 protected-resource metadata and requires an
access token bound to its exact HTTPS MCP resource. OpenAI clients propagate the
RFC 8707 `resource` value during OAuth; production acceptance must prove the
resulting token is actually audience-bound before enabling the connector.

## Production client model

For production v1 use a **known OAuth client per product/resource** rather than
depending on unrestricted Dynamic Client Registration.

Reason: Supabase currently exposes standard identity scopes only, and its Custom
Access Token Hook can reliably differentiate OAuth clients by `client_id`.
Therefore the platform can map:

```text
client_id: chatgpt-knowledge       -> aud: https://knowledge.../mcp
client_id: projects-hub-knowledge -> aud: https://knowledge.../mcp
client_id: wl-knowledge            -> aud: https://knowledge.../mcp
```

Other products use their own target audiences.

DCR remains useful for experiments, but it is not the production security
assumption until a real client proves exact resource-bound token issuance for
that path.

Official references:
- https://developers.openai.com/plugins/build/auth
- https://supabase.com/docs/guides/auth/oauth-server/mcp-authentication
- https://supabase.com/docs/guides/auth/oauth-server/token-security

## Token verification

The MCP server verifies:

- asymmetric JWT signature through issuer JWKS;
- exact issuer;
- expiry/not-before;
- stable subject;
- OAuth client identity;
- exact expected resource/audience.

A generic Supabase token whose audience is only `authenticated` is not accepted
as a production MCP token.

## Authorization is RLS, not OAuth scope text

Supabase OAuth currently supports the standard identity scopes
`openid email profile phone`. They do not form our application authorization
boundary.

Every row is authorized through:

- user `sub`;
- workspace membership;
- explicit document grants;
- resource visibility;
- client policy where needed.

Tool descriptions and read-only annotations improve model behavior but never grant
access.

## One user connecting several MCPs

The first connection performs login + resource consent. Later product connections
reuse the same authorization-server browser session, so the same human receives
the same `sub` and should not have to create another account.

Each grant remains independently revocable. Revoking Wonderful Lections must not
revoke Knowledge unless the user/account itself is disabled.

## Cross-service use

Never forward a token minted for one MCP to another MCP.

When Projects Hub, Wonderful Lections or Street Story needs the user's private
Knowledge data, that service becomes an OAuth client of the Knowledge resource.
The user authorizes that integration once and the calling service stores the
refresh grant encrypted at rest. It then obtains Knowledge-audience access tokens
normally.

Dedicated service identities are allowed only for non-user-specific tasks such as
public-corpus maintenance or a tightly scoped VibePublish mirror. They cannot be
used to impersonate arbitrary users.

## Workspaces

Personal data needs no synthetic workspace; ownership by `sub` is enough.
Shared collaboration uses stable workspace UUIDs and current membership rows.
Do not put large or mutable membership lists into JWT claims.

## Migration

Wonderful Lections' existing tested OAuth remains a rollback path. Do not
flag-day multiple products.

Migration gates are:

1. shared issuer + consent UI live;
2. Knowledge exact audience test;
3. one real ChatGPT connection;
4. second MCP connection produces the same `sub` but a different resource token;
5. refresh rotation/revocation acceptance;
6. first service-to-service user delegation;
7. only then migrate additional existing products.
