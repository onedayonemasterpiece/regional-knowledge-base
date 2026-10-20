from __future__ import annotations

import asyncio
import io
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from regional_knowledge.recovery_journal import (
    JournalLimit, JournalUnknown, RecoveryJournal, StaleWriter, StoreUnavailable,
)
from regional_knowledge.recovery_s3 import ConditionalApiUnavailable, S3ConditionalRecoveryStore


PREFIX = "recovery/v1/synthetic-stream/"
SECRET_MARKER = "do-not-echo-provider-message"


class BotoFailure(Exception):
    def __init__(self, code, status):
        super().__init__(SECRET_MARKER)
        self.response = {"Error": {"Code": code, "Message": SECRET_MARKER},
                         "ResponseMetadata": {"HTTPStatusCode": status}}


class Body(io.BytesIO):
    def __init__(self, value, chunk_size=None):
        super().__init__(value)
        self.requests = []
        self.chunk_size = chunk_size

    def read(self, amount=-1):
        self.requests.append(amount)
        return super().read(min(amount, self.chunk_size) if self.chunk_size else amount)


class Client:
    def __init__(self):
        shape = SimpleNamespace(members={"IfMatch": None, "IfNoneMatch": None})
        operation = SimpleNamespace(input_shape=shape)
        self.meta = SimpleNamespace(service_model=SimpleNamespace(operation_model=lambda name: operation))
        self.calls = []
        self.objects = {}
        self.version = 0
        self.put_failure = None
        self.get_failure = None
        self.lost_ack = False
        self.response = None
        self.entered = threading.Event()
        self.release = None
        self.authority = None

    def put_object(self, **kwargs):
        self.calls.append(("put", kwargs))
        if self.authority:
            assert self.authority.lock.locked()
            self.authority.events.append("native-enter")
        self.entered.set()
        if self.release:
            assert self.release.wait(timeout=5), "fixture publisher timed out"
        if self.put_failure:
            raise self.put_failure
        key = kwargs["Key"]
        old = self.objects.get(key)
        if kwargs.get("IfNoneMatch") == "*" and old:
            raise BotoFailure("PreconditionFailed", 412)
        if "IfMatch" in kwargs and (old is None or old[1] != kwargs["IfMatch"]):
            raise BotoFailure("PreconditionFailed", 412)
        self.version += 1
        token = f'"native-opaque-{self.version}"'
        self.objects[key] = (kwargs["Body"], token)
        if self.authority:
            self.authority.events.append("native-published")
        if self.lost_ack:
            raise TimeoutError(SECRET_MARKER)
        return {"ETag": token}

    def get_object(self, **kwargs):
        self.calls.append(("get", kwargs))
        if self.get_failure:
            raise self.get_failure
        if self.response is not None:
            return self.response
        value = self.objects.get(kwargs["Key"])
        if value is None:
            raise BotoFailure("NoSuchKey", 404)
        return {"Body": Body(value[0]), "ContentLength": len(value[0]), "ETag": value[1]}


class Authority:
    def __init__(self):
        self.generation = "writer-1"
        self.lock = asyncio.Lock()
        self.events = []

    @asynccontextmanager
    async def guard(self, generation):
        async with self.lock:
            if generation != self.generation:
                raise StaleWriter("retired generation")
            self.events.append("guard-enter")
            try:
                yield
            finally:
                self.events.append("guard-exit")

    async def advance(self):
        async with self.lock:
            self.generation = "writer-2"
            self.events.append("generation-advanced")


def fixture():
    client = Client()
    authority = Authority()
    store = S3ConditionalRecoveryStore(client=client, bucket="existing-private-bucket",
                                       prefix=PREFIX, writer_guard=authority.guard)
    return client, authority, store


@pytest.mark.asyncio
async def test_native_conditions_and_exact_opaque_etag_with_existing_client():
    client, _, store = fixture()
    key = PREFIX + "head"
    assert await store.put_if_absent(key, b"first")
    assert not await store.put_if_absent(key, b"must-not-overwrite")
    first = await store.read(key, max_bytes=10)
    assert first.data == b"first" and first.token == '"native-opaque-1"'
    assert await store.compare_and_swap(key, b"next", expected_token=first.token, writer_generation="writer-1")
    assert not await store.compare_and_swap(key, b"stale", expected_token=first.token, writer_generation="writer-1")
    assert await store.compare_and_swap(PREFIX + "outcomes/op", b"outcome", expected_token=None,
                                        writer_generation="writer-1")
    puts = [args for operation, args in client.calls if operation == "put"]
    assert puts[0] == {"Bucket": "existing-private-bucket", "Key": key, "Body": b"first",
                       "ContentType": "application/json", "IfNoneMatch": "*"}
    assert puts[2]["IfMatch"] == first.token and "IfNoneMatch" not in puts[2]
    assert puts[-1]["IfNoneMatch"] == "*" and "IfMatch" not in puts[-1]
    assert sum(operation == "get" for operation, _ in client.calls) == 1  # No CAS read-then-write emulation.


@pytest.mark.asyncio
@pytest.mark.parametrize("code,status", [("NoSuchKey", 404), ("404", 404), ("NotFound", 404), ("NoSuchKey", None)])
async def test_only_verified_missing_object_is_absent(code, status):
    client, _, store = fixture()
    client.get_failure = BotoFailure(code, status)
    assert await store.read(PREFIX + "missing", max_bytes=10) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("code,status", [
    ("AccessDenied", 403), ("NoSuchBucket", 404), ("AccessDenied", 404),
    ("InternalError", 500), ("NoSuchKey", 500), ("InvalidAccessKeyId", 403),
])
async def test_auth_and_other_read_failures_are_unavailable_not_absent(code, status):
    client, _, store = fixture()
    client.get_failure = BotoFailure(code, status)
    with pytest.raises(StoreUnavailable) as error:
        await store.read(PREFIX + "object", max_bytes=10)
    assert SECRET_MARKER not in str(error.value)
    assert error.value.__suppress_context__


@pytest.mark.asyncio
@pytest.mark.parametrize("code,status", [("PreconditionFailed", 412), ("412", 412)])
async def test_only_native_condition_failure_returns_false(code, status):
    client, _, store = fixture()
    client.put_failure = BotoFailure(code, status)
    assert not await store.put_if_absent(PREFIX + "segment", b"bytes")
    assert not await store.compare_and_swap(PREFIX + "head", b"bytes", expected_token='"etag"', writer_generation="writer-1")


@pytest.mark.asyncio
@pytest.mark.parametrize("code,status", [
    ("AccessDenied", 403), ("Conflict", 409), ("OperationAborted", 409),
    ("AccessDenied", 412), ("ConditionalRequestConflict", 409),
    ("ConditionalRequestConflict", 500), ("NotImplemented", 501),
])
async def test_other_write_failures_remain_unknown_and_do_not_fallback(code, status):
    client, _, store = fixture()
    client.put_failure = BotoFailure(code, status)
    with pytest.raises(StoreUnavailable) as error:
        await store.compare_and_swap(PREFIX + "head", b"bytes", expected_token='"etag"', writer_generation="writer-1")
    assert SECRET_MARKER not in str(error.value)
    assert error.value.__suppress_context__
    assert len(client.calls) == 1 and client.calls[0][1]["IfMatch"] == '"etag"'


@pytest.mark.asyncio
async def test_lost_ack_is_unknown_while_native_bytes_are_reconcilable():
    client, authority, store = fixture()
    client.lost_ack = True
    with pytest.raises(StoreUnavailable) as error:
        await store.compare_and_swap(PREFIX + "outcomes/op", b"exact", expected_token=None, writer_generation="writer-1")
    assert SECRET_MARKER not in str(error.value)
    assert (await store.read(PREFIX + "outcomes/op", max_bytes=5)).data == b"exact"
    assert authority.events == ["guard-enter", "guard-exit"]


@pytest.mark.asyncio
async def test_journal_409_retry_cannot_reject_an_earlier_late_head_commit():
    from test_recovery_journal import command

    class LateClient(Client):
        pending = None
        head_attempts = 0

        def put_object(self, **kwargs):
            if kwargs["Key"].endswith("/head"):
                self.head_attempts += 1
                if self.head_attempts == 1:
                    self.pending = kwargs
                    raise TimeoutError("late first native request")
                if self.head_attempts == 2:
                    raise BotoFailure("ConditionalRequestConflict", 409)
            return super().put_object(**kwargs)

    client = LateClient()
    authority = Authority()
    journal = RecoveryJournal(None, stream="late-cas-synthetic")
    journal.store = S3ConditionalRecoveryStore(
        client=client, bucket="existing-private-bucket", prefix=journal.prefix + "/",
        writer_guard=authority.guard,
    )
    for _ in range(2):
        with pytest.raises(JournalUnknown):
            await journal.seal([command()], predecessor=None, writer_generation="writer-1")
    Client.put_object(client, **client.pending)
    accepted = await journal.seal([command()], predecessor=None, writer_generation="writer-1")
    assert accepted.manifest == (await journal.head()).reference


@pytest.mark.asyncio
async def test_bounded_get_rejects_content_length_before_stream_read_and_closes():
    client, _, store = fixture()
    body = Body(b"oversized")
    client.response = {"Body": body, "ContentLength": 9, "ETag": '"opaque"'}
    with pytest.raises(JournalLimit):
        await store.read(PREFIX + "object", max_bytes=8)
    assert body.requests == [] and body.closed


@pytest.mark.asyncio
async def test_stream_overflow_is_bounded_to_max_plus_one_and_closed():
    client, _, store = fixture()
    body = Body(b"larger than its reported length", chunk_size=2)
    client.response = {"Body": body, "ContentLength": 4, "ETag": '"opaque"'}
    with pytest.raises(JournalLimit):
        await store.read(PREFIX + "object", max_bytes=4)
    assert body.closed
    assert body.requests == [5, 3, 1]


@pytest.mark.asyncio
@pytest.mark.parametrize("length,token", [(5, '"opaque"'), (None, '"opaque"'), (3, None), (3, ""), (True, '"opaque"')])
async def test_incomplete_length_or_version_proof_is_unavailable_and_closed(length, token):
    client, _, store = fixture()
    body = Body(b"abc")
    client.response = {"Body": body, "ContentLength": length, "ETag": token}
    with pytest.raises(StoreUnavailable):
        await store.read(PREFIX + "object", max_bytes=10)
    assert body.closed


@pytest.mark.asyncio
async def test_chunked_success_preserves_exact_bytes_and_closes():
    client, _, store = fixture()
    body = Body(b"abc", chunk_size=1)
    client.response = {"Body": body, "ContentLength": 3, "ETag": '"opaque-multipart-17"'}
    result = await store.read(PREFIX + "object", max_bytes=3)
    assert result.data == b"abc" and result.token == '"opaque-multipart-17"'
    assert body.closed and body.requests == [4, 3, 2, 1]


@pytest.mark.asyncio
async def test_namespace_is_restricted_for_every_operation():
    client, _, store = fixture()
    for key in ("other/head", PREFIX, PREFIX + "../escape", PREFIX + "/bad", PREFIX + "bad\nkey"):
        with pytest.raises(ValueError):
            await store.read(key, max_bytes=10)
        with pytest.raises(ValueError):
            await store.put_if_absent(key, b"x")
        with pytest.raises(ValueError):
            await store.compare_and_swap(key, b"x", expected_token=None, writer_generation="writer-1")
    assert not client.calls


def test_missing_guard_and_unsupported_sdk_fail_closed_without_requests():
    client = Client()
    with pytest.raises(ValueError):
        S3ConditionalRecoveryStore(client=client, bucket="bucket", prefix=PREFIX, writer_guard=None)
    client.meta = None
    with pytest.raises(ConditionalApiUnavailable):
        S3ConditionalRecoveryStore(client=client, bucket="bucket", prefix=PREFIX, writer_guard=Authority().guard)
    client = Client()
    del client.meta.service_model.operation_model("PutObject").input_shape.members["IfMatch"]
    with pytest.raises(ConditionalApiUnavailable):
        S3ConditionalRecoveryStore(client=client, bucket="bucket", prefix=PREFIX, writer_guard=Authority().guard)
    assert not client.calls


@pytest.mark.asyncio
async def test_stream_timeout_is_sanitized_and_body_is_closed():
    client, _, store = fixture()

    class FailedBody(Body):
        def read(self, amount=-1):
            raise TimeoutError(SECRET_MARKER)

    body = FailedBody(b"abc")
    client.response = {"Body": body, "ContentLength": 3, "ETag": '"opaque"'}
    with pytest.raises(StoreUnavailable) as error:
        await store.read(PREFIX + "object", max_bytes=3)
    assert body.closed and SECRET_MARKER not in str(error.value)
    assert error.value.__suppress_context__


@pytest.mark.asyncio
async def test_generation_transition_waits_for_native_publication_and_old_writer_is_denied():
    client, authority, store = fixture()
    client.authority = authority
    client.release = threading.Event()
    publication = asyncio.create_task(store.compare_and_swap(
        PREFIX + "head", b"first", expected_token=None, writer_generation="writer-1"))
    assert await asyncio.to_thread(client.entered.wait, 2)
    transition = asyncio.create_task(authority.advance())
    await asyncio.sleep(0)
    assert not transition.done() and authority.lock.locked()
    client.release.set()
    assert await publication
    await transition
    assert authority.events == ["guard-enter", "native-enter", "native-published", "guard-exit", "generation-advanced"]
    calls = len(client.calls)
    with pytest.raises(StaleWriter):
        await store.compare_and_swap(PREFIX + "outcomes/op", b"stale", expected_token=None, writer_generation="writer-1")
    assert len(client.calls) == calls


@pytest.mark.asyncio
async def test_cancellation_keeps_guard_until_background_native_request_finishes():
    client, authority, store = fixture()
    client.authority = authority
    client.release = threading.Event()
    publication = asyncio.create_task(store.compare_and_swap(
        PREFIX + "outcomes/op", b"outcome", expected_token=None, writer_generation="writer-1"))
    assert await asyncio.to_thread(client.entered.wait, 2)
    publication.cancel()
    transition = asyncio.create_task(authority.advance())
    await asyncio.sleep(0)
    assert not publication.done() and not transition.done() and authority.lock.locked()
    publication.cancel()  # Repeated cancellation must not free the fence early either.
    await asyncio.sleep(0)
    assert authority.lock.locked()
    client.release.set()
    with pytest.raises(asyncio.CancelledError):
        await publication
    await transition
    assert authority.events.index("native-published") < authority.events.index("generation-advanced")
