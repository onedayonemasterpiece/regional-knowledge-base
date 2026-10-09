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
| `geo_request(entity_id,cursor?,limit<=20)` | paginate evidence-backed **original spellings** on one existing owned POI graph ref; ignore empty-spelling vector-only discovery candidates |
| `geo_status(request_id?,entity_id?,limit<=10)` | current versions, attempts/reasons, without exposing the passage |
| `geo_receipt(idempotency_key)` | actor-scoped durable readback of stage/apply receipt after lost reply; does not repeat an owner action |
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

## Production-source pagination regression

On the real accepted zoo graph place, source discovery accumulated 81 mention
rows, but only 2 contained evidence-backed original place spellings. The
older `geo_request` looked at an arbitrary first 20 mixed mentions and
returned **zero** requests, despite two durable valid intents. This did not
remove or invalidate the automatically created intents but broke explicit
backfill and produced a misleading empty result.

The corrected `geo_request` uses a partial expression index on existing
`rkb_entity_mentions` rows, selecting only nonempty exact source spellings,
ordered by stable UUID and bounded by `limit<=20`. It accepts `cursor` for
the next bounded page and returns `next_cursor`, `complete`, `scanned`
and existing/new request IDs. Every returned source still undergoes owner,
active revision, exact region/chunk quote and authorization validation.
A later book owned by the same principal can attach its distinct evidence to
the SAME graph node without creating a new canonical POI or mixing private
sources owned by someone else.

The index is local to the canonical corpus SQLite and never copies graph or
source material to another authority. It does not call a vector model, map
provider or Street Story during import.

## Final production checkpoint (2026-10-09)

Actual RKB code release after PRs #100, #101 and #102 is
`4b7e39f4ce8d2eaa6caa6878459f94a5e94a52cf`; all three existing
MCP/indexing/graph-discovery services were verified against this immutable
source SHA. On the real authority: 99 intents / 99 attempts / 198 receipts,
including 86 POI identity candidates, seven ambiguous and six unresolved.
The accepted zoo source/event/story/owner POI link is unchanged and remains
`identity_state=candidate`, historical geometry `not_verified`.

The [full acceptance report](reports/rkb-cartography-geo-async-production-20261009.md)
records real SQLite snapshot restore/replay, 81/2 source mention pagination
regression, latency (local index timings only), MCP profile capability list,
CI evidence, ownership and separate pending Cartography/Street Story gates.

The canonical schema package is now merged in
[regional-cartography/contracts](https://github.com/onedayonemasterpiece/regional-cartography/tree/main/contracts)
(PR #2). This does **not** mean a remote signed/authenticated producer
exists. Its synthetic conformance fixtures passed locally; GitHub Actions
remains red with no runner step logs. Current connected ChatGPT MCP client
tool declarations have not yet refreshed to include the new `geo_*` methods.

## Crash recovery of the *automatic* consumer

After the initial v1 acceptance a reliability gap was discovered in
`worker_tick`: the headless daemon selected actors only from `pending` and
`retry_wait`. An interrupted `leased` or `staged` attempt whose lease
expired was therefore never revisited by the **automatic** worker, although
a manual `geo_claim` could reclaim it.

The existing worker now performs a bounded (`<=32`) SQLite-fenced expired
lease recovery **before** selecting actors. It atomically invalidates the
old lease token and staged proposal and requeues attempts with fewer than
five claims, without any Street Story calls under the write lock. The next
claim increments the fencing counter; the previous worker cannot stage or
apply its outdated decision. An expired fifth claim becomes
`retry_exhausted / lease_attempts_exhausted` rather than a leaked lease or
infinite retry loop. A later relevant owner/layer dependency revision still
creates a separate attempt with the same stable source intent. Concurrent
recovery workers cannot enqueue the same attempt twice.

Actor preselection is now indexed to active RKB users, so a revoked
principal's pending requests cannot create a headless polling loop. The
source/ACL checks remain inside every *new* fenced claim and apply.
This change does not mutate Story Registry, book text, vector indexes or
Street Story canonical identity. No new daemon/process is created.

This recovery is an **RKB-only owner-local retry mechanism**; it does not
constitute an authenticated Cartography/Street Story network mutation
transport or verified historic geometry.

## Activation gate for newly staged books — queue/source consistency

The geo outbox is saved atomically with a `poi_ref` mention, but the
mention may belong to a **staged** book revision that has not yet passed
vector readiness and `rkb_activate_revision`. Before this fix an active
consumer could claim such a pending geo attempt, see `source_revision >
active_revision` and incorrectly mark it `stale_source` permanently.
A large book whose BGE publication takes time was especially vulnerable;
this was an actual code-path race rather than a completed import failure.

New behavior:

- `enqueue_mention` persists the stable intent/attempt in the same
  source graph transaction with state `awaiting_activation` when the
  evidence's source revision is newer than the active book.
- An unauthorized other actor cannot read that intent; its owner may
  inspect `geo_status` with `source_access=staged_not_accepted`, but
  `geo_lookup` does not publish staged source evidence and the automatic
  worker never claims it.
- **Inside** the existing SQLite `rkb_activate_revision` transaction
  (after accepted source and graph revision update), exactly the
  matching future-revision intents become `pending`. Still-pending
  geo attempts for superseded earlier source revisions become
  `stale_source/superseded_source_revision`; their tokens/proposals are
  invalidated and their prior receipts preserved.
- If an operator tries to claim a historical prematurely-pending job,
  the source validity guard yields `source_not_activated` and parks the
  attempt instead of labeling an unaccepted future revision stale.
- Delayed activation when vectors are missing leaves intents dormant and
  cannot block the book or the existing Story Registry. Repeating the
  activation hook is idempotent.

Acceptance tests exercise the real `rkb_start_ingestion → stage pages,
regions, chunks + sourced graph entity → pending_vectors → E5/BGE ready
→ rkb_activate_revision` SQLite path, along with the expired/replaced
source fencing and unchanged evidence. They do not use fictitious
historical map geometry or imply Cartography network delivery.

## Same-pass extraction at new book ingestion (agent UX)

The backend does **not** run a hidden model for place identification.
The MCP calling model is responsible for reading the actual staged source,
not guessing from text hashes. The `book_ingest` tool and server instructions
now explicitly direct it to include a bounded `entity_candidates`
`GraphBundle` of **distinct, evidence-backed `poi_ref`** entities
during the SAME `book_pages → book_ingest(stage)` review already used
for semantic retrieval chunks and Story Registry story candidates.

Each place candidate must carry its exact original spelling and matching
`chunk_id / page_id / region_id / exact_quote`; its owner `poi_locator`
contains source-grounded name variants. Ambiguous identity and missing
historical maps are valid `unresolved` outcomes, not grounds to reject
a book. One entity per retrieval chunk or fabricated modern coordinates/
historical footprints are prohibited. An empty bundle is allowed when
the source has no reliable place mention.

The durable geo intent is written by the existing **graph stage** hook.
For a staged not-yet-accepted book revision its attempt waits in
`awaiting_activation`; the same SQLite activation transaction makes it
eligible only after the source/vector publication is accepted.
No second extraction prompt is required. The model must still perform
the semantic review; these instructions do not prove recall/completeness
of toponyms in previously imported books, and do not backfill private
source content into Cartography.
