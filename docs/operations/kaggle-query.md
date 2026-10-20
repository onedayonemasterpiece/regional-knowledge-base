# Kaggle-local typed QUERY executor

This source unit connects the authenticated controller's bounded `Query` to a
pinned tokenizer check and the existing Kaggle-local fused search. It does not
load a model, open a provider connection, add admission/dispatch, authorize a
person or wire the public WSS/MCP route. Baseline: `d335f079f45ddf126b0c3512eda6716d122e98d3`.
Repository instructions, the installed Kaggle skill/profile/migrations reference
and the QUERY/restore contracts were read. Real-provider/runtime acceptance
remains NOT RUN; PostgreSQL checks use only the guarded synthetic fixture.

## Execution and mandatory hybrid behavior

`execute_query(query, acquire=existing_pool.connection, tokenizer=..., encode=...)`
accepts a validated controller `Query`. Shape, actor fields and pinned adapter
metadata are not authorization. The caller supplies the existing authenticated
person/audience/admission/turn context and resident Kaggle encoder adapter.

`PinnedTokenizer` requires the exact BGE revision, tokenizer SHA-256 and inference
space from `bge_contract`. Its async count adapter must count the complete exact
no-prefix input including special tokens, without truncation. Metadata is an
attestation: deployment still must verify actual loaded tokenizer bytes/settings.
More than512tokens returns `invalid_query/token_limit_exceeded` before acquiring
SQL or inference. Unsupported kinds and malformed/duplicate UUID scope return
`invalid_query/invalid_filters` before tokenizer, SQL or encoder. This executor
supports source passages only; it does not invent facts, Story bodies or proofs.

The total remaining deadline is `min(query.deadline_ms,2000)` milliseconds,
starting before tokenization and including pool acquisition, snapshot/readiness,
encoding, all SQL branches, hydration and serialization. It is never refreshed
per hop. Inputs/outputs keep the existing1000-character/4096-byte query and
16KiB/256KiB frame limits. Synchronous adapters must not block the event loop;
actual runtime adapters must provide bounded asynchronous execution.

Every completed QUERY runs the actual encoder **and** native lexical and vector
SQL branches. There is no lexical-only fallback. Encoder exceptions or invalid
vectors produce `unavailable/dense_unavailable`, without evidence or retrieval
mode. SQL failure/deadline likewise cannot become an empty hybrid success.
`CancelledError` propagates; cancellation is not compute termination. The shared
encoder must retain its inference permit until its actual forward finishes.
Search retains/observes that task after the consumer times out and never waits
for a cancellation-resistant forward before returning the unavailable result.

The existing bounded candidate fusion/ANN result is incomplete. Therefore this
executor always returns completed searches as `degraded/hybrid` with reason
`bounded_retrieval`, including a legitimately empty completed result. It never
claims complete retrieval or dense quality acceptance. QUERY_RESULT permits
`hybrid` or no mode; lexical-only success is invalid in the strict protocol.

Optional telemetry uses a finite `asyncio.Queue.put_nowait`: full queues drop
telemetry without blocking retrieval. Monotonic `tokenize_ms`, `acquire_ms`,
`encode_ms`, `lexical_ms`, `dense_ms`, `hydrate_ms`, `search_ms`, `serialize_ms` and
`total_ms` contain only durations, never query text or private payloads. Unfinished
stages have no invented duration; late encoder completion cannot mutate an
already queued telemetry snapshot.

## Actual read frontier and snapshot

Install immutable `004_read_fences.sql` after001–003 through the existing ordered
DDL authority. It creates `read_frontier` with a NULL projection cursor and false
text/dense readiness. Existing sources get an explicit eligibility baseline;
this is not reconstruction of historical controller revocation deltas. No
implicit database/authority initializer is called.

Trusted writers use `publish_read_frontier(database,writer,visibility,seq,text,dense)`
under the same `recovery_state` row lock as apply. The function verifies database,
writer and actual local visibility epoch. Projection cursor cannot regress or
exceed applied cursor; ready flags require a known fully caught-up projection
cursor. A validating trigger also checks direct frontier updates. PUBLIC gets no
table/function privileges. These are controller-owned facts: calling the function
does not perform an index/model probe or establish correctness by itself.

Source metadata/acceptance/withdrawal, active-pointer changes and **serving**
passage writes automatically bump exact source eligibility/local visibility and
clear readiness in the mutating transaction. Statement triggers acquire the
existing writer row lock before domain-row locks. Inactive revision staging and
passages of unaccepted/withdrawn sources do not dirty the ready active reader.
Existing source/active mutators therefore cannot silently bypass fence updates;
trusted DDL, bulk restore/reset and grants remain exclusive controller operations.

Activation/withdrawal handlers must run through the existing atomic apply path,
so domain change, eligibility invalidation, applied cursor and ledger agree.
Projection completion is explicitly published only after all required lexical,
dense and hydration projections are verified for the declared cursor. Since the
apply wrapper advances its cursor after the domain handler, that handler cannot
predeclare `applied_seq+1` as ready. The existing projection completion step can
publish under the writer lock after commit; the intervening state is unavailable.
No scheduler or automatic `projection_seq=applied_seq` assumption is added.

Guarded fused search uses one REPEATABLE READ, READ ONLY transaction. Its snapshot
guard reads actual applied/projection cursors, readiness and local visibility
before encoder creation. Unknown/unready state, any projection lag or unmet
`min_data_seq` returns unavailable with observed cursors, without inference.
Hydration and exact active source/eligibility dependencies use that same snapshot.
A concurrent activation/withdrawal may leave this result a coherent older
snapshot; the upstream delayed-output gate must still reject affected evidence.
Reading newer cursors after retrieval is not used to claim freshness.
`observed_database_generation` comes from that same actual state row, keeping
cursor/dependency identity explicit rather than inferring a database from a number.

Local SQL `observed_visibility_epoch` is a distinct counter from
`query.actor.visibility_epoch` in Dev's authority context. No verified mapping is
implemented. The executor never copies or numerically compares unrelated epoch
spaces. Future routing must bind DB/run/connection identities and current complete
dependencies to the existing controller's accepted-mutation/visibility authority.
Until that output gate is wired, this unit claims no safe public delivery.

## Bounded evidence and real continuation

Every source item has mandatory exhaustive
`SourceDependency(source_id,content_revision,eligibility_revision)` entries exactly
covering `source_ids`, plus explicit variant/grant lists (empty for this source
retrieval). Missing or inconsistent snapshot dependencies fail closed. Source
content revision, eligibility revision and global local-visibility epoch remain
separate facts.

At most5items and24000total text characters are returned. Each passage portion is
at most8000characters. Bibliography portions are at most1024characters, and the
SQL locator remains complete at its1000-character limit. This keeps metadata
below12KiB even with worst-case six-byte JSON escaping. Encoding validates the
full256KiB result bound. Neither text nor bibliography is silently shortened:
partial output is an explicit excerpt with a typed `PassageContinuation` carrying
exact source/content/eligibility, passage ID, local visibility and Unicode text/
bibliography offsets. The short continuation string is a locator, not a capability.

`read_continuation` is an internal exact-row resolver **after renewed upstream
authorization**. It checks current readiness, scope, active revision, eligibility,
withdrawal and local epoch in another bounded read snapshot. It returns a distinct
`PassageRead`, with no retrieval mode or hybrid-search claim; it does not run search
or inference and is not a new public tool/wire operation. The caller must still
apply the current Dev authority/output gate. Expired/withdrawn locators fail
closed. SQL/cancellation cleanup finishes before connection reuse.

Tail reads remain `text_form=excerpt` even when no further bytes remain.
`continuation_complete=true` marks the terminal exact locator; it must not be read
again. When only bibliography remains, one overlapping final passage character
anchors the citation portion; offsets remain explicit. This never turns a tail
into a claim that the whole source passage was delivered.

Tests cover native cursors/readiness and min-sequence guards; simultaneous active
revision change with coherent older retrieval/dependencies; inactive/unaccepted/
withdrawn staging; active projection invalidation; mandatory encoder and dense
SQL; Unicode/control-character wire limits; continuation withdrawal and SQL
deadline; inference permit retention; cancellation/rollback; and strict protocol
dependency and mode validation. Stub vectors/token counts do not establish live
tokenizer/BGE residency, latency, recall or quality acceptance.
