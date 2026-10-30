"""Resident callback glue with actual typed executor and synthetic PG leases."""
import asyncio
from contextlib import asynccontextmanager
import json
from uuid import UUID

import pytest

from regional_knowledge.bge_contract import REVISION, SPACE, TOKENIZER_SHA256
from regional_knowledge.kaggle_protocol import Query, QueryResult, decode_envelope
from regional_knowledge.kaggle_query import PinnedTokenizer
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery, WorkerQueryInputError
from test_kaggle_pg_search import FixtureConnection

SOURCE = '00000000-0000-0000-0000-000000000001'
PASSAGE = '10000000-0000-0000-0000-000000000001'
VECTOR = [1.] + [0.] * 1023


def frame(**changes):
    query = Query(request_id='synthetic-read', connection_generation=4, turn_generation=9,
        query='synthetic harbour', min_data_seq=12, deadline_ms=2000, compute_claim='controller-claim',
        actor=dict(person_id='verified-context-only', audience='personal', auth_epoch=2, visibility_epoch=100))
    result = json.loads(query.model_dump_json())
    result.update(changes)
    return result


async def count(_):
    return 12


def pinned(callback=count):
    return PinnedTokenizer(callback, REVISION, TOKENIZER_SHA256, SPACE)


class ServingConnection(FixtureConnection):
    """SQL fixture API only: does not claim native PG semantics or operations."""
    def __init__(self, *, ready=True):
        super().__init__()
        self.ready = ready

    async def fetchone(self):
        return (12, 12 if self.ready else None, 3, self.ready, self.ready, 'synthetic-db')

    async def fetchall(self):
        if 'lexical_candidates(' in self.statement or 'dense_candidates(' in self.statement:
            return [(UUID(PASSAGE), .5)]
        if 'hydrate(' in self.statement:
            return [(UUID(PASSAGE), UUID(SOURCE), 1, 'p1', 'synthetic complete body', 'synthetic bibliography', None, False)]
        if 'SELECT a.source_id,a.revision,v.eligibility_revision' in self.statement:
            return [(UUID(SOURCE), 1, 7)]
        return []


class Lease:
    def __init__(self, connection=None):
        self.connection = connection or ServingConnection()
        self.entered = self.exited = 0

    @asynccontextmanager
    async def acquire(self):
        self.entered += 1
        try:
            yield self.connection
        finally:
            assert self.connection.cleaned
            self.exited += 1


@asynccontextmanager
async def forbidden_acquire():
    raise AssertionError('rejected callback reached SQL')
    yield


async def forbidden_encode(_):
    raise AssertionError('rejected callback reached inference')


@pytest.mark.asyncio
async def test_callback_runs_actual_typed_fused_executor_and_returns_strict_bounded_result():
    lease = Lease()
    telemetry = asyncio.Queue(maxsize=1)
    seen = []
    async def encode(text):
        seen.append(text)
        return VECTOR
    handler = KaggleWorkerQuery(acquire=lease.acquire, tokenizer=pinned(), telemetry=telemetry)
    result = await handler(frame(), encode=encode)
    validated = decode_envelope(json.dumps(result).encode())
    assert type(validated) is QueryResult
    assert (validated.request_id, validated.connection_generation, validated.turn_generation) == ('synthetic-read', 4, 9)
    assert validated.status == 'ok' and validated.reason is None and validated.mode == 'hybrid'
    assert validated.observed_data_seq == validated.observed_projection_seq == 12
    assert validated.observed_database_generation == 'synthetic-db' and validated.observed_visibility_epoch == 3
    assert validated.evidence[0].dependencies.source_revisions[0].eligibility_revision == 7
    assert seen == ['synthetic harbour'] and lease.entered == lease.exited == 1
    statements = [sql for sql, _ in lease.connection.statements]
    assert any('lexical_candidates(' in sql for sql in statements)
    assert any('dense_candidates(' in sql for sql in statements)
    stages = telemetry.get_nowait()
    assert 'synthetic harbour' not in repr(stages)


@pytest.mark.asyncio
@pytest.mark.parametrize('changes', [
    {'compute_claim': None}, {'compute_claim': ''}, {'compute_claim': True},
    {'compute_claim': '𐀀' * 100}, {'deadline_ms': 2001}, {'deadline_ms': True},
    {'operation': 'read_continuation'}, {'extra': 'synthetic-private-value'},
    {'actor': {'person_id': 'synthetic-private-value'}}, {'query': 'x' * 1001},
])
async def test_invalid_wire_never_reaches_tokenizer_sql_or_encoder_and_never_echoes_payload(changes):
    async def forbidden_count(_):
        raise AssertionError('invalid wire reached tokenizer')
    handler = KaggleWorkerQuery(acquire=forbidden_acquire, tokenizer=pinned(forbidden_count))
    result = await handler(frame(**changes), encode=forbidden_encode)
    assert result['status'] == 'invalid_query' and result['reason'] == 'invalid_input'
    assert result['mode'] is None and result['evidence'] == []
    assert 'synthetic-private-value' not in json.dumps(result)


@pytest.mark.asyncio
async def test_missing_claim_and_oversized_wire_rejected_before_resources():
    handler = KaggleWorkerQuery(acquire=forbidden_acquire, tokenizer=pinned())
    missing = frame(); del missing['compute_claim']
    assert (await handler(missing, encode=forbidden_encode))['reason'] == 'invalid_input'
    result = await handler(frame(extra='private' * 3000), encode=forbidden_encode)
    assert result['reason'] == 'query_too_large' and 'private' not in json.dumps(result)


@pytest.mark.asyncio
@pytest.mark.parametrize('bad', [None, [], b'private', 'private', {'request_id': 'private'},
    frame(connection_generation=True), frame(turn_generation=True), frame(request_id='𐀀' * 100)])
async def test_no_reply_identity_fails_closed_without_invented_ids_or_diagnostics(bad):
    handler = KaggleWorkerQuery(acquire=forbidden_acquire, tokenizer=pinned())
    with pytest.raises(WorkerQueryInputError) as error:
        await handler(bad, encode=forbidden_encode)
    assert str(error.value) == 'invalid worker QUERY identity'


@pytest.mark.asyncio
@pytest.mark.parametrize('ready,min_seq,reason', [(False, 12, 'not_ready'), (True, 13, 'projection_lag')])
async def test_actual_snapshot_readiness_before_resident_encode(ready, min_seq, reason):
    lease = Lease(ServingConnection(ready=ready))
    handler = KaggleWorkerQuery(acquire=lease.acquire, tokenizer=pinned())
    result = await handler(frame(min_data_seq=min_seq), encode=forbidden_encode)
    assert result['status'] == 'unavailable' and result['reason'] == reason
    assert result['observed_data_seq'] == 12 and lease.exited == 1
    assert not any('dense_candidates' in sql for sql, _ in lease.connection.statements)


@pytest.mark.asyncio
async def test_token_limit_and_loaded_tokenizer_failure_fail_before_sql():
    async def too_many(text):
        assert text == 'synthetic harbour'
        return 513
    handler = KaggleWorkerQuery(acquire=forbidden_acquire, tokenizer=pinned(too_many))
    result = await handler(frame(), encode=forbidden_encode)
    assert result['status'] == 'invalid_query' and result['reason'] == 'token_limit_exceeded'
    async def failed(_):
        raise RuntimeError('synthetic-private-value')
    handler = KaggleWorkerQuery(acquire=forbidden_acquire, tokenizer=pinned(failed))
    result = await handler(frame(), encode=forbidden_encode)
    assert result['reason'] == 'not_ready' and 'synthetic-private-value' not in json.dumps(result)


@pytest.mark.asyncio
@pytest.mark.parametrize('caller_cancel', [False, True])
async def test_cancel_or_long_read_preserves_worker_owned_forward(caller_cancel):
    lease = Lease()
    entered, finish = asyncio.Event(), asyncio.Event()
    permit = asyncio.Lock()
    native = None
    async def actual():
        async with permit:
            entered.set()
            await finish.wait()
            return VECTOR
    async def encode(_):
        nonlocal native
        native = asyncio.create_task(actual())
        return await asyncio.shield(native)
    handler = KaggleWorkerQuery(acquire=lease.acquire, tokenizer=pinned())
    task = asyncio.create_task(handler(frame(deadline_ms=1000 if caller_cancel else 30), encode=encode))
    try:
        await asyncio.wait_for(entered.wait(), .5)
        if caller_cancel:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await asyncio.sleep(.05)
            assert not task.done() and permit.locked()
            finish.set()
            assert (await task)['status'] == 'ok'
        assert lease.connection.cleaned and lease.exited == 1
        if caller_cancel:assert permit.locked() and not native.done()
    finally:
        finish.set()
        if native is not None:
            await native
    assert not permit.locked()


@pytest.mark.asyncio
async def test_unexpected_pool_failure_sanitized_and_bounded_telemetry_drops():
    @asynccontextmanager
    async def failed_acquire():
        raise RuntimeError('synthetic-private-value')
        yield
    telemetry = asyncio.Queue(maxsize=1); telemetry.put_nowait({'occupied': 1})
    handler = KaggleWorkerQuery(acquire=failed_acquire, tokenizer=pinned(), telemetry=telemetry)
    result = await handler(frame(), encode=forbidden_encode)
    assert result['reason'] == 'internal_error' and 'synthetic-private-value' not in json.dumps(result)
    assert telemetry.get_nowait() == {'occupied': 1}


def test_constructor_requires_existing_pinned_contract_and_finite_telemetry():
    with pytest.raises(ValueError, match='lease'):
        KaggleWorkerQuery(acquire=None, tokenizer=pinned())
    with pytest.raises(ValueError, match='pinned'):
        KaggleWorkerQuery(acquire=forbidden_acquire, tokenizer=object())
    with pytest.raises(ValueError, match='finite'):
        KaggleWorkerQuery(acquire=forbidden_acquire, tokenizer=pinned(), telemetry=asyncio.Queue())


@pytest.mark.asyncio
async def test_contract_rechecked_before_each_callback_and_wrong_executor_identity_rejected(monkeypatch):
    tokenizer = pinned()
    handler = KaggleWorkerQuery(acquire=forbidden_acquire, tokenizer=tokenizer)
    object.__setattr__(tokenizer, 'space', 'wrong-space')
    assert (await handler(frame(), encode=forbidden_encode))['reason'] == 'not_ready'
    handler = KaggleWorkerQuery(acquire=forbidden_acquire, tokenizer=pinned())
    async def wrong_identity(*args, **kwargs):
        return QueryResult(request_id='wrong', connection_generation=4, turn_generation=9,
            status='unavailable', mode=None, reason='not_ready', observed_data_seq=None, observed_projection_seq=None)
    monkeypatch.setattr('regional_knowledge.kaggle_worker_query.execute_query', wrong_identity)
    assert (await handler(frame(), encode=forbidden_encode))['reason'] == 'internal_error'
