# Conditional S3 recovery adapter

`S3ConditionalRecoveryStore` implements the narrow `ConditionalRecoveryStore`
transport around the existing `S3ObjectStore.client` and `.bucket`. It does not
create credentials, clients, a writer registry, service, scheduler or queue.
`object_store.py` remains unchanged. This is source code plus deterministic SDK
fixtures: **actual provider/bucket conditional-write verification is NOT RUN**.
Product accepted/Saved routing must remain gated until that verification and the
controller/PG recovery integration are complete.

## Existing authority and native conditions

Construction requires an explicit restricted key prefix ending in `/` and an
authoritative `writer_guard(generation)` async context manager supplied by the
existing controller. It must validate generation under its existing authority
lock and hold that authority until context exit. Generation/revocation changes
must acquire the same authority. A function that only checks generation and then
releases its lock is insufficient. The adapter cannot discover or substitute
authority from a supplied generation string.

```python
recovery_store = S3ConditionalRecoveryStore(
    client=existing_object_store.client,
    bucket=existing_object_store.bucket,
    prefix=authorized_recovery_stream_prefix,
    writer_guard=existing_controller.writer_guard,
)
```

Every `compare_and_swap` acquires that guard across the complete native
`PutObject` client call, for manifest heads, canonical outcomes, acceptance proofs,
bootstrap checkpoints and operation bindings. `expected_token=None` sends
`IfNoneMatch="*"`; an expected native
ETag sends `IfMatch=expected_token` unchanged, including quotes and multipart
format. `put_if_absent` sends `IfNoneMatch="*"`; it is intended for immutable
orphan-safe segment/manifest bytes. Canonical outcomes and binding/index updates
use generation-guarded CAS.

Native calls run in a thread. Cancelling the async waiter does not stop a running
SDK request, so the adapter shields and drains it before releasing the generation
guard, even after repeated cancellation. Cancellation still reaches the caller
after the request finishes. A cancelled/timeout caller must reconcile exact
remote publication; it must not assume that publication failed. Existing client
connect/read timeouts and retry limits must be finite, because the authority
cannot safely release while a native publication is still running.
An SDK timeout does not prove the remote server stopped processing a request.
The local guard serializes client calls and authority transitions; unresolved
remote requests still require exact native-condition reconciliation. No server
cancellation or remote generation-fence capability is inferred from that guard.

The installed SDK's `PutObject` input shape must expose both `IfMatch` and
`IfNoneMatch`; otherwise construction raises `ConditionalApiUnavailable` without
a provider call. This verifies only client expressibility. Unsupported provider
errors fail closed. There is no read-then-unconditional-write emulation, hidden
unconditional retry, additional credential or fallback service.

## Bounded reads and errors

`read` calls native `GetObject` within the restricted namespace. It requires an
integer `ContentLength` and nonempty native ETag. A declared oversized length is
rejected before reading the body. Streaming reads consume at most `max_bytes+1`
bytes, detect undeclared overflow/truncation and always attempt to close the
body. Successful reads preserve exact bytes and return the opaque ETag, without
substituting a locally calculated checksum for the provider condition token.
The recovery journal separately verifies immutable SHA-256 and exact lengths.

Only `NoSuchKey`/a compatible native object 404 means verified absence. Missing
bucket, authorization errors, inconsistent error code/status, timeouts and other
failures become `StoreUnavailable`, never `None`. Only native
`PreconditionFailed`/412 means a failed write condition and returns `False`.
Every 409, including `ConditionalRequestConflict`, remains
`StoreUnavailable`: an earlier request or SDK retry could still commit after a
client timeout. The journal must preserve UNKNOWN until exact publication or a
permanently advanced head proves the outcome. Unsupported operations and
authentication failures also remain unavailable/uncertain. No 409 authorizes
an unconditional retry. The adapter does not retry uncertain writes itself.

Provider error messages, bucket/key details and request payloads are excluded
from raised transport messages, and provider exception chaining is suppressed.
Out-of-prefix/traversal/control-character keys and invalid arguments fail before
any native operation. This prefix restriction is additional scope enforcement;
it does not replace server authorization or bucket/IAM permissions.

## Verification boundary

Tests use a boto-style fake client and synthetic bytes only. They inspect actual
request keyword arguments, conditional failures, native ETag preservation,
bounded/chunked GET and body closure, auth/absence distinction, lost ACK readback,
SDK shape rejection, namespace rejection and ordering of native publication,
generation transitions and cancellation. A journal-plus-adapter regression
delays the first native head commit, times it out, returns conditional 409 on the
second request, then proves both attempts remain UNKNOWN until the first exact
commit is reconciled. No provider, bucket, credentials,
network, dependency installation, notebook or accepted product input is used.

Before real routing, verify the installed SDK and selected bucket's native
create-only and IfMatch behavior, concurrent requests/cross-process visibility,
lost-response reconciliation, precise error mapping and authoritative guard
integration. Verify IAM grants/prefix ownership and finite SDK timeouts/retries
without publishing private material in evidence. Those checks remain **NOT RUN**;
green fake-client tests do not establish real remote durability or product Saved.
