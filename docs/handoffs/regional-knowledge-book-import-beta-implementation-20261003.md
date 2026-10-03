# Regional Knowledge Base — product completion executor prompt

Date: 2026-10-03
Repository: `onedayonemasterpiece/regional-knowledge-base`
Mode: implementation / deployment / acceptance, not another architecture audit.

## Owner goal

Deliver a product result the owner can immediately try from ChatGPT:

> attach a PDF book and say “add this book to the Regional Knowledge Base”.

After implementation ChatGPT must be able to authenticate, ingest the PDF in
small resumable page batches, stage text/layout/footnotes/illustrations/POI
evidence, validate/finalize, then search/fetch evidence from the imported book.

The owner must not operate Supabase UI or manually drive internal ingestion steps.

## Critical correction: Supabase is not the OAuth server

Do not ask the owner to enable any Supabase OAuth Server feature.

Target architecture:

```text
ChatGPT
  -> application-owned OAuth 2.1
  -> Regional Knowledge MCP
  -> direct Postgres RLS actor bridge
  -> Supabase Postgres/pgvector

Object bytes
  -> existing private S3-compatible object storage
```

Supabase is the managed data plane only.

Never forward the ChatGPT/MCP bearer to Supabase.
Never derive MCP issuer/JWKS from `KB_SUPABASE_URL`.
Never depend on `KB_SUPABASE_JWKS_URL` or Supabase OAuth Server for MCP auth.

## Current verified infrastructure

Preserve/reuse; do not rebuild without need:

- dedicated Regional Knowledge Supabase project is configured;
- `KB_SUPABASE_SESSION_CONNECTION` works through Session Pooler;
- migrations 001–005 are applied;
- pgvector + pgcrypto are available;
- database was roughly 12 MiB at last acceptance;
- private S3-compatible corpus bucket is configured and anonymous read is denied;
- deterministic ingestion already exists:
  `start -> pages -> stage -> validate -> finalize`;
- multimodal page/region/illustration graph exists;
- POI fact/media outbox exists;
- Street Story / Projects Hub contracts already exist;
- the old `SupabaseRestBackend` is now disabled by default because forwarding
  the caller bearer to PostgREST is an obsolete coupling.

Read current `main`, docs and open PRs before writing.

## Reuse requirement

Before auth implementation inspect the proven Wonderful Lections OAuth behavior:

- `onedayonemasterpiece/wonderful-lections/src/runtime/oauth.mjs`;
- `docs/operations/chatgpt-connection.md`;
- relevant PKCE/resource/refresh/revocation tests.

Use it as a behavioral donor only. Do not couple Knowledge business state to
Wonderful Lections.

## Tool/source roles

- ChatGPT designs/authors source changes.
- DevCoveer Direct Ops handles deterministic Git/test/deploy actions.
- OpenCode may apply already-designed mechanical patches/tests/ops, but must not
  redefine product semantics.
- Do not stop after a new plan. Continue through implementation, deployment and
  acceptance.

# Mandatory DoD

## D01 — clean baseline

- start from fresh current `main`;
- preserve unrelated worktrees/artifacts;
- record starting SHA;
- run current deterministic tests.

## D02 — complete auth/Supabase decoupling

Keep generic application JWT resource verification.

Runtime auth inputs:
- `RKB_AUTH_ISSUER`;
- `RKB_AUTH_JWKS_URL`;
- `RKB_RESOURCE_URL`.

Supabase env alone must never enable MCP auth.

`RKB_ALLOW_LEGACY_SUPABASE_USER_JWT` is test-only and must not be present in
effective beta deployment.

Acceptance:
- token for another MCP resource is rejected;
- no setup docs ask for Supabase OAuth.

## D03 — application-owned OAuth 2.1 beta

Implement the smallest safe OAuth server required by ChatGPT, independent from
Supabase.

Beta can have one owner, but structures must remain multi-user extensible.

Required:
- authorization code;
- PKCE S256;
- exact `resource` binding;
- protected-resource metadata;
- authorization-server metadata;
- explicit consent;
- expiring access tokens;
- refresh rotation;
- refresh replay family revocation;
- revocation endpoint;
- exact redirect allowlist;
- stable UUID subject;
- restart-durable encrypted/private auth state;
- no tokens/codes/secrets in URLs/logs/git.

Prefer the proven Wonderful Lections behavior. Do not build a generic identity
platform in this slice.

Acceptance:
- protocol tests;
- HTTPS discovery;
- authorization-code flow;
- refresh/revocation/replay negatives;
- authenticated MCP initialize/tools/list.

## D04 — application identity + RLS actor migration

Add a new additive migration; do not rewrite already-applied migrations.

Target:
- application-owned `rkb_users(id uuid ...)`;
- one beta owner UUID configured/seeded without a personal secret in git;
- ownership/grant/member FKs no longer require `auth.users`;
- introduce `rkb_current_actor_id()` backed by a transaction-local application setting;
- policies/functions use `rkb_current_actor_id()`, not `auth.uid()`.

Migration must be replay-safe and preserve existing data.

Acceptance:
- repo search finds no product RLS/function dependence on `auth.uid()`;
- ownership FKs no longer require Supabase Auth users.

## D05 — direct Postgres RLS backend

Production runtime uses `KB_SUPABASE_SESSION_CONNECTION`.

Use a bounded pool.

For each user transaction:
1. validate app subject UUID;
2. begin transaction;
3. switch to a non-bypass application role;
4. set actor UUID transaction-locally with parameterized SQL;
5. execute SQL/RPC;
6. commit/rollback clears actor.

Port beta operations:
- hybrid search;
- fetch;
- profile;
- document access;
- ingestion start/status/pages/stage/validate/finalize;
- object metadata lookups needed by those operations.

No end-user/app OAuth bearer may be sent to Supabase.

The legacy PostgREST bearer-forwarding backend may remain only for bounded tests
or be removed after parity.

## D06 — live RLS acceptance on the connected Supabase

Apply the new migration through Session Pooler.

Using temporary deterministic application users prove:
- owner A cannot read owner B private content;
- workspace membership works;
- nonmember denied;
- explicit document grant works;
- viewer cannot mutate owner data;
- public policy works as intended;
- null/invalid actor fails closed;
- pooled connection actor does not leak between transactions.

Delete temporary acceptance data afterward.