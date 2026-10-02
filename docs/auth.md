# OAuth, identity and multi-MCP architecture

## Goal

One person may connect Regional Knowledge Base, Wonderful Lections, Street Story, Projects Hub and VibePublish to ChatGPT/Codex without creating separate product identities.

The services remain separate resource servers. Identity is shared; authorization is not flattened.

## Shared authorization server

Target issuer: **Supabase Auth OAuth 2.1/OIDC**.

Supabase provides authorization-code + PKCE, refresh rotation, OIDC/JWKS, Dynamic Client Registration and MCP-oriented protected-resource integration. Use asymmetric signing keys.

Each MCP publishes RFC 9728 protected-resource metadata that points to the same issuer:

```text
shared issuer
    |
    +-> knowledge.example/mcp
    +-> wonderful-lections.example/mcp
    +-> street-story.example/mcp
    +-> projects-hub.example/mcp
    +-> vibepublish.example/mcp
```

Every token remains resource-bound. A token minted for one MCP must be rejected by another.

## Stable identity

The stable platform user key is the issuer + JWT `sub` (Supabase user UUID). Email is profile data, never the primary key.

Each service maps that subject to its own:
- memberships;
- roles;
- quotas;
- domain resources;
- audit trail.

This gives cross-service identity without sharing business tables.

## Authorization: important Supabase limitation

Supabase OAuth Server currently exposes only standard identity scopes (`openid email profile phone`); custom application scopes are not yet an authorization boundary.

Therefore:
- do not treat a requested OAuth scope such as `knowledge.write` as security;
- use RLS and explicit service roles/memberships for every row;
- use OAuth `client_id` to distinguish clients where useful;
- enforce write/read rules in the service even if the model/tool descriptor claims a narrower capability.

Tool annotations and descriptions are UX hints only.

## Resource binding

The MCP endpoint URL is the RFC 8707 resource identifier. Production acceptance must prove that the Supabase OAuth flow carries the `resource` value through authorization/token exchange and that the access token is audience-bound to the exact MCP resource (directly or through the configured access-token hook).

The MCP server verifies:
- signature via issuer JWKS;
- issuer;
- expiry/not-before;
- subject;
- client identity;
- expected resource/audience.

Do not enable a permissive “any Supabase token is accepted” mode in production.

## Workspaces

Personal data requires no workspace: ownership by `sub` is sufficient.

Shared collaboration uses stable workspace UUIDs and memberships. Membership is a platform concept but every service still enforces its local resource rules. Do not place large workspace lists into JWTs; memberships change and token claims become stale. Query RLS-backed membership state.

## Second and later MCP connections

The first MCP connection performs sign-in + consent. Later MCPs use the same authorization-server login session, so the user should normally see only the resource-specific consent/connection step, not create another account.

## Service-to-service calls

Never forward the user's Knowledge Base bearer token to VibePublish or Wonderful Lections.

Use one of:
1. a dedicated service identity with the minimum target-service capability;
2. a future token-exchange/delegation mechanism when the shared issuer supports a reviewed design.

Carry the end-user subject only as audited request context, not as a substitute credential.

## Migration

Wonderful Lections' existing, tested OAuth implementation remains operational during transition. It is a requirements donor and rollback path. Migration to the shared issuer happens only after end-to-end connection, refresh, revocation and resource-audience tests pass.

Projects Hub is an orchestration/Live surface, not the authorization server. This avoids turning one product into a mandatory identity monolith.
