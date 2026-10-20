# MCP authentication and delegated identity

Target contract2026-10-20; implementation and live client checks remain required. See [handoff](prompts/rkb-scalable-kaggle-implementation-20261019.md) and [rights](rights.md).

## Application authority

Existing application-owned OAuth stays on DevCoveer. PostgreSQL/pgvector in Kaggle is a data plane, never the user OAuth provider. Do not derive issuer/JWKS/resource from Supabase configuration and do not restore Supabase Auth users/auth.uid() as the identity source.

Preserve authorization code + PKCE S256, exact resource/audience and registered redirect binding, consent, short access lifetime, refresh rotation/replay revocation, discovery and stable UUID subject. `RKB_AUTH_ISSUER`, `RKB_AUTH_JWKS_URL`, `RKB_RESOURCE_URL` remain explicit. No secrets in URL/log/repository. Existing grants/credentials are infrastructure, not excluded non-GEO content; do not reset them during content cutover.

Resolve verified provider+tenant/workspace+subject to the canonical RKB person. Email, display name, Slack mention, client-supplied actor/dialog ID and claimed expertise do not choose an actor. Categories/credentials/subject weight are versioned separately from capability grants. A mistaken binding puts affected attribution/support into review rather than erasing history.

## Data and route enforcement

Server-side permission is current person capabilities ∩ case/artifact scope ∩ delivery audience ∩ executor service grant/request fence where delegated. Use transaction-local verified actor context and non-bypass SQL roles for domain access; reset at transaction end and test pooled-connection actor leakage. No user bearer forwarding to PostgreSQL or another MCP.

Accepted active book text and bibliography have no owner filter. Public illustrations are a separate artifact class. Stripe quote/page scans require owner or verified demo/expert/local_historian with `scan_proof_read`; original PDF/DjVu rights remain independent. Private drafts, history/receipt replay and derived snippets recheck their dependencies. Shared channels do not union participants' private grants. Cache hits and delayed callbacks recheck withdrawal/revocation.

Import/reprocess, participant administration and credential binding remain owner personal-route operations. A bot invoked by owner cannot bypass the expert-route ceiling. A case manager/owner does not automatically read another person's private draft. Exact selected-fragment publication requires its own capability and does not publish the rest.

## Delegated clients and services

Personal MCP clients use resource-bound user grants. Slack resolves the verified sender, persists request/dialog/route and invokes the same resource through a dedicated executor identity; Dot's credentials/author name are not substituted. Mira in Projects Hub uses a real target-resource provider, verified mapping, progressive capabilities and durable pending/result/hash ACK through existing live-interaction. Search-only adapter success is not multi-user acceptance.

Same-host Street Story/VibePublish calls use scoped technical identities on loopback/Unix socket, as required by the host integration policy. Kaggle uses authenticated TLS and constrained run credentials, not loopback to a different host or a full owner token. Existing credentials are never written into Dataset/control journal.

Fencing blocks stale new effects/results while preserving exact commands durably accepted in prior generations. Remote control recovery includes revocation versions and UNKNOWN; uncertain authority leaves writes/delivery closed until reconciliation.

## Client setup and acceptance

The intended public MCP resource remains `https://knowledge.kenigevents.ru/mcp`. Preserve existing confidential clients and public PKCE dynamic registration, exact allowlisted callback origin/URI validation and explicit foreign-resource rejection. A missing resource may bind only to the server's one configured resource under its existing compatibility contract; never infer another resource. Do not publish owner login credentials or ask experts to use owner consent secrets.

Verify each named client individually: actual non-owner sign-in, correct displayed identity, allowed research, denied administration/private cross-actor read, revocation, reconnect and saved result ACK. Naming ChatGPT/Grok/Slack/Mira does not establish compatibility. Real Telegram sessions are provided independently for implementation; never clone StringSession or evade provider account limits.

Verified demo/expert/local_historian assignment materializes the existing scan_proof_read policy grant with audit and category revision; demo expiry is inherited. Claimed categories grant nothing. Current capability/scope is checked at render time; explicit revocation is not recreated on read, and removing a category withdraws its policy-derived grant. No new IAM or per-image approval.
