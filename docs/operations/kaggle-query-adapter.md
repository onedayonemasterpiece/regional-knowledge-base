# Resident worker QUERY adapter

`KaggleWorkerQuery(acquire=existing_pool.connection,tokenizer=existing_pinned_tokenizer,
telemetry=finite_queue_or_none)` is an async callable for the existing
`BgeWssTransport(...,query_handler=handler)`. Source baseline:
`9811230d6cc95836b9cec09f2fa785a4e7186dcd`. Repository instructions and the installed
`/home/dev/.local/share/devcoveer/auth/codex/skills/kaggle-warm-debugging/SKILL.md`,
RKB profile and migrations reference were read. This unit changes public source
only; it does not enable a runtime factory or shared mode in any environment.

The trusted controller factory owns pool/model startup and shutdown and must
verify the actual loaded BGE/tokenizer artifacts and counting settings before
constructing this adapter. The adapter requires the existing exact
`PinnedTokenizer` type and compares its revision, inference space and tokenizer
hash to canonical `bge_contract.REVISION`, `SPACE` and `TOKENIZER_SHA256` on
construction and every call. It never manufactures a new metadata attestation,
infers model identity from an encoder callback or treats matching strings as proof
of loaded bytes. The supplied count implementation must be nonblocking and count
the exact complete no-prefix input including special tokens, without truncation.
The established512-token limit remains unchanged. There is no tokenizer/model
import, download, installation, local fallback or second encoder in this adapter.

Invoke `await handler(frame_dict,encode=resident_async_encoder)`. The existing WSS
worker supplies this encoder for that exact compute claim. The adapter passes it
to `execute_query` without wrapping its permit, spawning a second native forward
or cancelling the worker's independent Task. Both document and read inference
continue through the same worker lock and actual-forward ownership.

The callback accepts only the already authenticated worker's dictionary frame.
It serializes and strictly decodes through the existing envelope codec, enforces
the16KiB QUERY bound and a nonempty bounded controller-populated compute claim,
and requires the existing Query operation and deadline1–2000ms. Integer identity
fields exclude booleans; request/claim identifiers additionally meet the worker's
256-byte bound. A valid reply identity with invalid payload returns typed
`invalid_query/invalid_input` (or `query_too_large` for the wire bound), without
echoing diagnostics or payload. With no valid exact request/connection/turn
identity, `WorkerQueryInputError` carries only a fixed sanitized message. No reply
ID is invented; the existing transport can fail closed using its own validated
assignment context. Parsed actor fields and compute claims are neither login
proof nor permission. This callback is not a public authorization entry point.

Parsing time is deducted conservatively from the received remaining deadline
before calling the existing executor. That executor owns pinned token checking,
existing pool acquisition, the single repeatable-read read-only PG snapshot,
actual readiness/min-sequence checks before inference, lexical and vector SQL,
hydration, exact source dependencies and bounded evidence serialization. The
adapter revalidates the returned strict QueryResult, checks exact original
identity and returns an independent JSON dictionary. It checks the original total
deadline after serialization as well. It never fabricates observed cursors or
labels a continuation fetch as hybrid search. Present bounded search remains
`degraded/hybrid` with its existing explicit retrieval limitation.

The supplied lease factory must exclusively check out an open idle psycopg
connection with the existing tuple-row contract and return it only after executor
rollback/cleanup. Cancellation propagates through `execute_query` and that lease
without being turned into an ordinary result. A native forward can remain active
after SQL/consumer cancellation; only the existing worker may release its compute
claim after actual forward and handler cleanup finish. This adapter adds no
capacity authority, retry loop, durable result ACK, journal write or Saved receipt.
Unexpected pool/adapter errors become `unavailable/internal_error` with no private
exception text. Tokenizer contract unavailability becomes `unavailable/not_ready`.

Optional telemetry is the existing finite `asyncio.Queue`, passed unchanged to
`execute_query`; an unbounded queue is rejected at construction. Existing bounded
stage observations use dropping `put_nowait` rather than blocking a result or
creating another collector. No request/query/person/token text is logged here.
The adapter does not depend on a particular stage-event schema.

Tests use the actual typed executor and fused search with synthetic SQL lease
stubs, stub counts and a worker-style shielded async forward. They cover both
SQL retrieval branches, current cursors/dependencies, readiness before inference,
strict wire/claim/identity rejection, unchanged token limits, sanitized failures,
bounded dropping telemetry, timeout and cancellation cleanup while the forward
permit remains held. They do not establish native PG rollback behavior, artifact
identity, BGE residency, provider operations, public delivery or latency/quality
acceptance. Real isolated PG integration and explicit factory injection remain
root/controller work; no production DSN or runtime initialization is introduced.
