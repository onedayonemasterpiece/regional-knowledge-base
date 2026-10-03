# Regional Knowledge Base — beta acceptance report

Date: 2026-10-03
Repository: `onedayonemasterpiece/regional-knowledge-base`
Accepted source baseline: `ddb01b0971f2e0c6e97e7f68563a5f4749cbfaab`

## Result

The beta implementation is operational on DevCoveer and the complete application
path has been exercised against the connected Supabase/Postgres and private
S3-compatible object storage.

The only remaining blocker to a normal ChatGPT connection by hostname is the
public DNS A record for `knowledge.kenigevents.ru`. The HTTPS certificate,
Nginx route, application OAuth server and MCP runtime are already ready for that
hostname. DNS publication is blocked by expired Yandex Cloud CLI authorization;
no non-interactive Yandex token is currently available and the saved browser
session reaches a Yandex CAPTCHA. This report does not treat bypassing that
CAPTCHA or moving the edge to another provider as an acceptable workaround.

## Delivered source changes

Regional Knowledge main includes:

- PR #17 — application-owned OAuth runtime, direct Postgres/RLS backend and
  migrations 006–008;
- PR #18 — ChatGPT Dynamic Client Registration;
- PR #19 — canonical OAuth issuer/origin handling verified against the live edge;
- PR #20 — exact public MCP Host/Origin allowlist while keeping DNS-rebinding
  protection enabled.

The shared `vpn-server` repository also has merged PR #8 adding a fail-closed
typed Regional Knowledge edge route to that renderer. The currently active
public 80/443 edge is the newer `my-data-hub-control-plane` Nginx runtime, not
the legacy VPN renderer, so live acceptance used and verified the active edge
configuration directly.

## Runtime

Current Regional Knowledge user service:

- unit: `regional-knowledge-base.service`;
- state: active + enabled;
- listen: `127.0.0.1:8000` only;
- exact release source: main `ddb01b0971f2e0c6e97e7f68563a5f4749cbfaab`;
- private runtime state and OAuth state are outside Git;
- service environment is mode 0600;
- production database path uses `KB_SUPABASE_SESSION_CONNECTION`;
- MCP bearer tokens are not forwarded to Supabase.

Local runtime acceptance:

- `/health` = 200;
- OAuth authorization-server metadata = 200;
- protected-resource metadata = 200;
- DCR advertised;
- PKCE S256 advertised;
- unauthenticated `/mcp` = 401 with protected-resource challenge.

## D01 — clean baseline

PASS.

Implementation started from the then-current main and preserved unrelated
untracked acceptance material. Source changes were delivered as small PRs.
Current tracked workspace is clean on the accepted main SHA.

Latest source validation used the full suite after the public Host fix:

- 63 pytest tests passed;
- PR #20 CI passed before merge.

## D02 — Supabase/auth decoupling

PASS.

- Supabase is the data plane, not the MCP OAuth server.
- Application OAuth issuer/resource are independent from
  `KB_SUPABASE_URL` / Supabase Auth.
- Production uses the Session Pooler.
- Direct Postgres adapter never forwards the caller OAuth bearer to Supabase.
- Historical migrations may still contain `auth.uid()` as immutable history;
  migration 006 rewrites the live Regional Knowledge functions/policies to
  `rkb_current_actor_id()`.

## D03 — application-owned OAuth 2.1 beta

PASS.

A full external protocol acceptance was run through the real HTTPS edge IP with
TLS SNI `knowledge.kenigevents.ru`, so it did not depend on the missing DNS
record.

Verified end to end:

- HTTPS health;
- authorization-server discovery;
- protected-resource discovery;
- Dynamic Client Registration;
- authorization code;
- PKCE S256;
- exact resource binding;
- owner login;
- explicit consent;
- token exchange;
- authenticated MCP initialize;
- tools/list;
- six required MCP tools;
- hybrid search;
- fetch;
- refresh rotation;
- refresh replay rejection;
- replay revokes the active token family.

Search returned 8 results and fetch returned 1081 characters of sourced evidence
from the already-finalized real book.

OAuth state survives restart and is encrypted/private. Exact ChatGPT redirect
patterns are allowlisted. Public Host validation remains fail-closed; no wildcard
public Host/Origin was introduced.

## D04 — application identity / RLS actor

PASS.

Migration 006 is applied and replay-safe.

Verified:

- `rkb_users` is the application-owned identity registry;
- ownership/member/grant FKs use `rkb_users`, not Supabase Auth;
- transaction-local actor is exposed by `rkb_current_actor_id()`;
- runtime reduces each user transaction to non-bypass role `rkb_app`;
- invalid account UUIDs and disabled accounts fail closed;
- application actor state is transaction-local.

## D05 — direct Postgres RLS backend

PASS.

Production runtime is using the direct Session Pooler backend with a bounded
pool. Beta operations are available through MCP:

- search;
- fetch;
- profile;
- document access;
- book ingest start/status/stage/validate/finalize;
- page delivery with native text/blocks and JPEG images.

Live OpenAI embedding acceptance returned HTTP 200 with 768 dimensions.
The live vector column is `halfvec(768)` and outbox constraints are applied.

## D06 — full live RLS acceptance

PASS.

The final live acceptance used three temporary application-owned users and one
physical pooled Postgres connection. All temporary rows were removed afterward.

Verified:

- owner A cannot read owner B private document;
- owner B cannot read owner A private document before grant/membership;
- workspace member can read workspace-visible document;
- workspace nonmember is denied;
- explicit private document grant works;
- viewer cannot mutate owner document;
- rights-verified public normalized content is readable by a nonmember;
- private document without grant is denied;
- missing actor is rejected by the adapter;
- null actor under `rkb_app` cannot read private/workspace content;
- invalid actor UUID fails closed;
- public content remains public even with null actor, as intended by policy;
- actor GUC is cleared after transaction;
- reduced role is cleared after transaction;
- all checks reused the same physical pooled connection.

## Real corpus acceptance

A real 40-page 1893 public-domain source was finalized in the connected
infrastructure.

Observed persisted state:

- 40 pages;
- 855 regions;
- 35 retrieval chunks;
- 35/35 embeddings;
- active revision = 1;
- original PDF in private object storage;
- canonical graph snapshots;
- normalized text projection;
- hybrid search returned 8 results;
- fetch returned sourced text;
- POI fact evidence outbox was created.

Anonymous access to the canonical private source object is denied.

This first book was deliberately conservative in semantic staging and therefore
did not prove illustration/caption/footnote materialization by itself.

## Real multimodal MCP acceptance

PASS.

A temporary three-page excerpt was constructed from the real 1897 historical
source:

- Adolf Boetticher, *Die Bau- und Kunstdenkmäler der Provinz Ostpreussen*;
- Internet Archive item `bub_gb_vfFYAAAAYAAJ`;
- original pages 4, 21 and 331.

The excerpt was not treated as permanent corpus. It was used only to verify the
full mechanics and was deleted from Postgres and object storage after acceptance.

The acceptance went through the real external MCP path:

1. MCP `book_ingest(start)` received a ChatGPT-style file object over HTTPS;
2. the server downloaded and stored the PDF;
3. `book_pages` returned all three real JPEG page renders plus native blocks;
4. model-reviewed regions were staged as heading/body/footnote/figure/caption;
5. real `footnote_of`, `caption_of` and `illustrates` relations were staged;
6. a real `Abb. 214` illustration bbox and caption were linked;
7. graph validation returned `ready`;
8. finalize atomically activated the revision;
9. canonical page renders were materialized for all three pages;
10. the illustration crop was deterministically cut from the original PDF;
11. crop hash/caption provenance were persisted;
12. retrieval chunks linked the real footnote and illustration;
13. all chunks received embeddings;
14. hybrid search found the finalized evidence and fetch returned the source
    passage;
15. a private `poi.media_evidence.v1` event was created in
    `pending_authorization`, proving the Street Story media-evidence producer
    without dispatching fake acceptance data;
16. the temporary document, outbox row and all canonical/test S3 objects were
    deleted and absence was verified.

This proves the actual multimodal storage/graph/crop path. It does not claim that
a deterministic parser can semantically classify arbitrary books: semantic page
interpretation remains a ChatGPT/model responsibility; the server performs
deterministic render, validation, persistence, crop, indexing and ACL work.

## Public edge state

Ready except DNS:

- edge IPv4: `78.111.90.230`;
- active public edge: `my-data-hub-control-plane` Nginx;
- certificate SAN includes `knowledge.kenigevents.ru`;
- active Nginx route:
  `knowledge.kenigevents.ru -> 127.0.0.1:8000`;
- Nginx configuration test passes;
- TLS verification for `knowledge.kenigevents.ru` succeeds when connecting
  directly to the edge IPv4 with that SNI;
- `/health`, OAuth discovery and MCP work through that real TLS route.

Not ready:

- authoritative/public A record `knowledge.kenigevents.ru -> 78.111.90.230`
  is currently absent.

The Yandex Cloud CLI native session is expired and requests interactive
reauthorization. No `YC_TOKEN`, `YC_IAM_TOKEN` or `YANDEX_IAM_TOKEN` is
available in the owner environment. The saved browser profile reaches Yandex
CAPTCHA, which was not bypassed.

Once that single A record is restored, no additional application code,
certificate issuance or Nginx route work is expected; the public hostname must
still be re-run through the normal DNS/TLS/OAuth/MCP acceptance after the DNS
change.

## Beta conclusion

Application code, data plane, private corpus storage, application identity/RLS,
OAuth, MCP, retrieval and real multimodal ingestion are accepted.

The beta is **application-ready but not yet hostname-connectable from ChatGPT**
because of the one external Yandex DNS credential dependency above. No Supabase
UI action or Supabase OAuth configuration is required.
