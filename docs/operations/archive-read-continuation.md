# Durable archive read continuation

Deploy the matching VibePublish opt-in `media_store get` idempotency implementation
before this RKB change. Old Vibe deployments ignore get request keys and do not
provide the required lost-response guarantee. This changes retrieval only, never
source publication or archive grants.

The existing private OriginalCache SQLite stores one opaque scope, random request
key and operation ID per owner/issuer/archive entry. Admission identity is committed
before provider I/O. Repeating an invocation replays the SAME keyed get, which
rechecks current Vibe ACL/binding, and resumes the same durable operation. An
operation-ID conflict fails closed. No credentials or source text are stored in
this metadata.

The synchronous polling budget remains bounded. A provider still running returns
`archive_source_pending` with its operation ID, not a missing-original verdict.
Retrying the same source resumes the attempt. Lost admission responses, process
restarts, network failures, authorization failures and terminal provider failures
do not silently generate fresh requests.

Verified receipts remain recorded even after payload return: a caller may crash
before installing the original in the byte cache. Only a definitive HTTP404/410
from that verified receipt's asset retires the attempt, allowing the next call to
make a new retrieval with a new key. This is not a new publication. Other HTTP
failures retain the receipt. Compare-and-delete cannot remove a concurrent newer
attempt.

Attempt metadata is bounded by the archive entries requested and is deliberately
not evicted by the original-byte cache's idle/size cleanup. Evicting an unresolved
admission would discard the information needed for safe replay. Terminal provider
failures remain available for diagnosis rather than being retried indefinitely.

Regression coverage includes slow provider completion, restart, lost response,
concurrent admission, crash after payload return, owner/entry isolation, current
authorization denial, expired assets, unknown failures and receipt conflicts.
Unit tests model the paired Vibe contract; release acceptance must additionally
exercise the actual deployed keyed-get endpoint and current source-byte hashes.
