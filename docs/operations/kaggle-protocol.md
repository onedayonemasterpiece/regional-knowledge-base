# Kaggle read protocol v2 — source contract only

`regional_knowledge.kaggle_protocol` defines immutable strict Pydantic models and one
UTF-8 JSON codec boundary for a future authenticated outbound notebook WSS attached
to the existing controller. It creates no route, transport, scheduler, admission,
model, service identity, capability activation or deployed behavior. Durable
DISPATCH/RESULT/ACK/RESUME are intentionally outside this first read-only slice.
The normative target is the implementation handoff §§5.1–5.3 and §14.5.

The public tool handler **must take actor context from existing verified request
authority**, never from client arguments, model annotations, supplied person IDs,
HELLO or a successfully parsed model. `TrustedActorContext` is an internal
controller-to-worker projection, not verification or a grant. The handler must
perform existing actor admission and output authorization and recheck current
epochs, scope, withdrawal and grants before delayed delivery or continuation.
There is no API here that constructs trusted authority from a public payload.
A private channel still needs scoped run authentication, exact attachment ownership
and a server-assigned connection generation. None is implemented by this module.

- `HELLO` identifies the exact run/provider version/launch nonce/incarnation,
  server-assigned connection generation, JSON protocol, schema/model versions and
  observed snapshot/cursor. Text, archive and proof readiness are independent;
  required nullable fields preserve unknown facts. Credentials never enter frames.
- `QUERY` fixes operation `knowledge_search` version 2, request/turn/connection
  identity, verified actor/audience/context projection, epochs, freshness floor
  `min_data_seq` and **remaining total deadline budget** in milliseconds. There is
  no cross-host monotonic timestamp. Each hop must deduct elapsed time and queue
  waits; it cannot restart the budget. Tokenizer/prefix input must separately pass
  the pinned complete 512-token limit at inference admission. No tokenizer or
  model is loaded here.
- `QUERY_RESULT` separates status from retrieval mode. `ok` means completed hybrid
  search with observed data/projection cursors, including an honest empty result.
  Partial or lexical-only search is `degraded` with a reason. No-search failures
  have null mode and no evidence. Unknown cursors stay null, not zero. The caller
  must compare observed cursors with the original freshness floor and current
  request/generations/deadline; model parsing alone cannot establish freshness.
- `CANCEL_QUERY` carries the same read identity and finite reason. It requests
  cancellation and suppresses late delivery; it does not acknowledge freed CPU,
  stopped shared forwards, durable mutation cancellation or provider termination.

The finite filter grammar accepts optional explicit source IDs (1–32 distinct IDs)
and optional distinct kinds (`source`, `fact`, `story`, `entity`, `geo`, `case`).
Absent source IDs means bounded SQL scope, never Python enumeration of all books.
Unknown fields/operations/versions, duplicate JSON keys, numeric coercions and
non-finite numeric input are rejected. These shape checks do not implement SQL
execution, capability negotiation, fairness, cancellation, retry or output gates.

QUERY is capped at 1000 Unicode characters, 4096 UTF-8 query bytes and 16 KiB of
actual frame bytes. Evidence defaults to 3 and permits at most 5 items. Each item
has at most 8000 characters, total evidence text at most 24000 characters, metadata
at most 12 KiB, and the complete response at most 256 KiB UTF-8. HELLO/cancel
frames are bounded at 16 KiB. `decode_envelope` checks inbound raw bytes;
`encode_envelope` validates again and checks complete outbound bytes. Callers
must use the codec at the actual boundary, rather than serializing models directly.
Oversize is rejected without truncation; the eventual handler must map oversized
query failures to `invalid_query/query_too_large`, not return empty success.

Each evidence item requires exhaustive source, variant and grant dependency lists.
Variant/grant lists may explicitly be empty; source lists cannot. Composite
cross-book items preserve every source and grant dependency. Retrieval and the
output gate must prove that these lists are exhaustive and valid; a schema cannot
infer missing dependencies. Metadata is a finite title/citation/locator object,
not arbitrary candidates, vectors or base64 images. There is no bibliography
registry or hidden dependency deduplication in this slice.

Story items require an exact existing variant reference included in dependencies.
The worker must return the actual saved Variant.body when it fits. Excerpts are
explicitly marked and require a continuation locator; the schema cannot prove
that supplied text equals saved text. Continuations and context references are
locators and require current authorization at every read. No silent truncation,
Saved promise, remote receipt, durable replay or automatic retry is added.

Local contract tests use synthetic public values and no BGE, PostgreSQL, network
or credentials. WSS egress, live worker roundtrip, production integration,
performance and all product acceptance gates remain **NOT RUN** for this slice.
