# Geographic resolution intents — RKB's bounded durable outbox

**Scope:** RKB owns source mentions, actor ACL, exact graph evidence and the
intent/attempt/receipt ledger. Street Story alone owns canonical POI identity.
Regional Cartography alone will own source-observed map features/geometry/layers.

## Released contract surface (code acceptance is not production acceptance)

On an accepted graph **POI reference mention** (including book graph stage),
RKB saves `rkb_geo_intents`, scoped name variants and the first
`rkb_geo_attempts` **in the exact same SQLite transaction** as the graph
mention. No source passage, PDF/raster, Street Story secret, map bbox or
external network operation is included in that write transaction. The
ordinary source import never waits for a map vector worker.

A stable `request_id` keys
`(mention_id, accepted source revision/hash, resolver policy)`.
An `attempt_id` separately keys
`(intent, dependency kind/ref/revision, unchanged mention hash)`.
A revision update creates another attempt without replacing the earlier
receipt. Unrelated POI/layer revisions do not reopen an intent by themselves.
A terminal unmatched owner identity is **unresolved**, not `no_place_exists`.

The actor-scoped MCP tools (full and authorized story profiles, not the
default Live read-only profile) are:

| Tool | Bounded semantics |
| --- | --- |
| `geo_request(entity_id)` | idempotently backfill up to 20 original accepted mentions of one owned POI graph ref |
| `geo_status(request_id?,entity_id?,limit<=10)` | current versions, attempts/reasons, without exposing the passage |
| `geo_lookup(name,year?,limit<=10)` | exact normalized, ACL-checked source mention projection; year with unknown historical scope stays unknown |
| `geo_claim(limit<=5,lease_seconds<=180)` | finite source-fair claim packet, opaque lease token and monotonic fence |
| `geo_stage(attempt_id,lease_token,lease_fence,proposal,idempotency_key)` | typed candidate/unresolved/ambiguous/outside_coverage/rejected, no free-form SQL/HTTP |
| `geo_apply(attempt_id,lease_token,lease_fence,idempotency_key)` | checks every source proof and old/new owner identity, updates the SAME RKB place node and durable receipt |
| `geo_recheck(name,dependency_kind,dependency_ref,dependency_revision,limit<=30)` | exact-name dependency delta; replay is a no-op; no broad atlas/book scan |

The local graph-discovery worker reuses existing scheduling and invokes the
small bounded RKB-only queue consumer. It consults the **read-only**
Street Story canonical POI/alias projection. A unique owner match may
produce `linked_candidate`, explicitly **not** `identity_verified`.
Ambiguous/no owner candidate remains terminal pending a meaningful
revision event; transient local POI unavailability does not become a false
negative. Attempt fencing prevents a previous worker applying after
lease expiry. Lost responses can be recovered from durable receipts.
Source revision changes and revoked actors fail closed; editorial story
text and revisions are never mutated as a side effect.

## Contract boundaries and cartography readiness

The internal RKB MCP lease packet uses **`rkb.geo_claim.v1`**. It is
an owner-only authenticated source request, **not** an interservice envelope.
The future, separate producer protocol is `cartography.resolve.v1`, requiring
a verifiable issuer/audience, scope and canonical payload hash.
`cartography.projection.v1` and `cartography.decision.v1` need
conformance fixtures in the canonical Regional Cartography repository
and authenticated Street Story owner transport before allowing
cartographic geometry or a revised physical identity into RKB.

No accepted Kneiphof `layer_revision` exists at the first RKB handoff
checkpoint. A `geo_recheck(cartography_layer,...)` only queues relevant
candidate investigation; it does **not** authorize auto-binding to
unverified polygons. Modern point is not 1896 building geometry or
publicly navigable entrance. No RKB direct write into Cartography
PostGIS or Street Story SQLite is introduced.

User-facing latency targets require p95 measurement:
warm geo lookup ≤800 ms; the existing RKB search p95 ≤1500 ms and hard
2000 ms are not modified. Full 100k chunk/database capacity is a
separate protected gate.

## Required acceptance

1. Source-backed graph insertion creates one intent and initial attempt
   in the same SQLite transaction; invalid source evidence rolls back both.
2. First unresolved source stays intact; relevant owner-alias revision L2
   schedules exactly one fresh attempt and binds the **same** source graph
   node; unrelated alias and L2 replay make no work. Identical source
   evidence does not imply independent corroboration.
3. Two workers cannot both apply the same lease: fence increases on
   reacquisition. Same command+hash replay reads old durable receipt;
   same command+different body conflicts.
4. Source revision changed/grant revoked/disabled actor cannot append
   an accepted canonical identity. Historical scope unknown stays unknown.
5. Actual OAuth toolset, real live SQLite source, Street Story owner
   data, service restarted readback and book-search baseline verified
   independently from synthetic CI.

Any test lacking an accepted Kneiphof cartographic projection reports
the map-layer acceptance as **pending**, not a synthetic PASS.

## Production retry and fairness hardening (2026-10-09)

A transient Street Story owner outage is **not** an unresolved place.
The existing geo consumer releases a failed fenced lease into `retry_wait`
with exponential bounded backoff (15–600 seconds). Attempt number five
becomes the explicit `dependency_unavailable` terminal outcome, never
an orphaned `leased` row or infinite hot loop. A new matching owner
revision can create a new attempt after terminal exhaustion.

Claim batches rank at most one eligible attempt **per document before**
the final LIMIT, so the first 25 mentions of a long book cannot starve
another book. The age of an attempt contributes bounded priority aging.
`geo_status` reports actor-scoped state counts and oldest pending lag.
No other principal's queue depths or source names are returned.

The identity-only owner bridge rejects spatial relation fields even
for an exact POI match: a name or modern representative coordinate
cannot establish historical `within`/`same_site`/`occurred_at`.
Only the future accepted map projection plus Street Story owner
decision can supply spatially verified geometry.

No remote network is invoked while holding the source's SQLite write
lock; a full geo-resolution success is still a **candidate identity**,
not independently verified historical geometry.
