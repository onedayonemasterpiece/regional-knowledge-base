# Public target source read boundary

This is source implementation evidence, not deployment or full product acceptance.
`KaggleSourceBackend` is explicitly injected into the existing `build_server`.
It shares the exact owning `BgeQueue`, `ReadAuthority` and RAM admission with
the existing authenticated worker channel. It has no local corpus, vector or
lexical fallback. BGE-only readiness, an unverified target observation, a changed
attachment, an explicit empty source scope and a missing final HTTP boundary
cannot produce a successful public search.

The existing opaque OAuth credential is resolved on Dev; its bearer is never
forwarded to Kaggle. QUERY contains only the trusted projection, finite source
filters and the remaining total two-second budget. The executor returns bounded
source evidence from actual lexical and dense execution. Source history coverage
must have been established by the native verifier outside the read path.

The source gate checks current authority, exact runtime/request identity,
observed cursors and complete source dependencies. The actual MCP HTTP request
scope retains a single bounded result until JSON issuance. The final boundary
refreshes and rerenders both MCP text and structured content, then checks the
synchronous mutation fence immediately before sending. Revoked or withdrawn
items, their metadata and continuation locators cannot remain in the old
serialized representation. Stateless JSON HTTP is required; no read replay store
or output capability is sent to the client. Failure or disconnect releases the
request-local ticket and authority. The shared compute claim stays held until
its independent worker release is reconciled.

Public `read_trace` carries a logical trace and server-minted attempt/request
reference. Admission emits dispatch correlation only when network dispatch is
attempted, so cancellation while waiting cannot claim worker execution. Final
controller terminal diagnostics are emitted after the output gate/send, with
loss tracked by the existing bounded lossy collector. Stage durations overlap
and must not be added or attributed to network latency by subtraction.

`tests/test_read_issuance.py` exercises the installed MCP JSON HTTP stack and
actual request context. `tests/test_kaggle_public_backend.py` uses real synthetic
OAuth identities, `BgeQueue`, runtime expectations and shared admission; its
source gate is a finite test double. Source history and gate tests exercise the
separate canonical implementations. Encoder/provider and integrated native PG
proof are separate acceptance steps, not implied by these tests.

This adapter currently covers the source-search vertical. Exact fetch transport,
catalog/import orchestration, independent facts/stories/entities/GEO/cases and
artifact operations are not implemented by this class. `fetch` explicitly
refuses; the class must not be enabled as a complete production backend until
the remaining product routes, recovery and live acceptance pass. The existing
production backend has not been switched by these changes.
