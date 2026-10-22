import asyncio
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace

import pytest
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.kaggle_health import (
    HEALTH_SQL, DatabaseHealth, collect_health, probe_database_health,
)
from regional_knowledge.kaggle_public_backend import TargetReadBinding, current_binding
from test_kaggle_public_backend import target
from test_read_authority import auth
from test_participants import people
from test_kaggle_query import env, PG, publish
from test_kaggle_pg_driver import database


def binding():
    return TargetReadBinding('run', 'incarnation', 1, 'synthetic-db', 5, '106')


class Native:
    closed = False
    row_factory = tuple_row

    def __init__(self, rows=None, delay=False):
        self.row_factory = tuple_row
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.rows = rows if rows is not None else [('synthetic-db', 'rkb-v4-sql001-014-v1', 5, 5, False)]
        self.sql = []
        self.delay = delay
        self.acquired = 0
        self.released = 0
        self.after_query = None

    @asynccontextmanager
    async def acquire(self):
        self.acquired += 1
        try:
            yield self
        finally:
            self.released += 1
            assert self.info.transaction_status == TransactionStatus.IDLE

    @asynccontextmanager
    async def transaction(self):
        self.info.transaction_status = TransactionStatus.INTRANS
        try:
            yield
        finally:
            self.info.transaction_status = TransactionStatus.IDLE

    @asynccontextmanager
    async def cursor(self):
        yield self

    async def execute(self, sql, params=None):
        self.sql.append((sql, params))
        if sql == HEALTH_SQL:
            if self.delay:
                await asyncio.sleep(60)
            if self.after_query:
                self.after_query()

    async def fetchmany(self, limit):
        assert limit == 2
        return self.rows[:limit]


@pytest.mark.asyncio
async def test_native_health_uses_one_existing_read_only_transaction_and_no_corpus():
    native = Native()
    result = await probe_database_health(acquire=native.acquire, binding=binding(), binding_current=lambda _: True)
    assert result.ready and result.sql_response
    assert result.applied_seq == result.projection_seq == 5
    assert result.schema_revision == 'rkb-v4-sql001-014-v1'
    assert 0 <= result.elapsed_ms < 2000
    assert native.acquired == native.released == 1
    assert native.sql[0][0] == 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'
    assert native.sql[-1][0] == HEALTH_SQL
    assert not any(word in HEALTH_SQL.lower() for word in ('passage', 'original', 'token', 'password'))


@pytest.mark.asyncio
@pytest.mark.parametrize('row,reason', [
    (('other-db', 's1', 5, 5, False), 'pg_generation_mismatch'),
    (('synthetic-db', 's1', 6, 5, False), 'pg_projection_lag'),
    (('synthetic-db', 's1', 5, 5, True), 'pg_prior_write_pending'),
    (('synthetic-db', 's1', 4, 4, False), 'pg_sequence_behind'),
    (('synthetic-db', 's1', True, 5, False), 'pg_metadata_invalid'),
    (('synthetic-db', None, 5, 5, False), 'pg_metadata_invalid'),
])
async def test_sql_responded_does_not_imply_readiness(row, reason):
    result = await probe_database_health(acquire=Native([row]).acquire, binding=binding(), binding_current=lambda _: True)
    assert result.sql_response and not result.ready and result.reason == reason


@pytest.mark.asyncio
async def test_native_timeout_releases_transaction_and_propagates_caller_cancel():
    native = Native(delay=True)
    result = await probe_database_health(acquire=native.acquire, binding=binding(),
                                         binding_current=lambda _: True, timeout_seconds=.02)
    assert result.reason == 'pg_timeout' and not result.ready
    assert native.acquired == native.released == 1
    task = asyncio.create_task(probe_database_health(acquire=native.acquire, binding=binding(), binding_current=lambda _: True))
    await asyncio.sleep(.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert native.acquired == native.released == 2


@pytest.mark.asyncio
async def test_native_lease_change_after_query_is_not_ready():
    native = Native()
    current = [True]
    native.after_query = lambda: current.__setitem__(0, False)
    result = await probe_database_health(acquire=native.acquire, binding=binding(), binding_current=lambda _: current[0])
    assert not result.ready and result.reason == 'pg_binding_changed'


@pytest.mark.asyncio
async def test_cold_health_does_not_allocate_demand_or_call_native(tmp_path, monkeypatch):
    queue = BgeQueue(tmp_path / 'cold.sqlite')
    def forbidden(*args, **kwargs):
        pytest.fail('health attempted maintenance/startup')
    for name in ('maintenance', 'request_startup', 'status'):
        monkeypatch.setattr(queue, name, forbidden)
    result = await collect_health(queue=queue, native_probe=forbidden, controller_state='inactive')
    assert result['controller']['state'] == 'inactive'
    assert result['notebook']['status'] == 'absent'
    assert result['reason'] == 'notebook_not_ready' and result['postgres'] is None
    with queue.connect(write=False) as db:
        assert db.execute('SELECT count(*) FROM runs').fetchone()[0] == 0
        assert db.execute('SELECT startup_ref FROM control WHERE id=1').fetchone()[0] is None


@pytest.mark.asyncio
async def test_ready_queue_alone_is_not_sql_response_and_controller_is_independent(target):
    async def unavailable(observed):
        return DatabaseHealth(observed, reason='pg_query_failed')
    result = await collect_health(queue=target.queue, native_probe=unavailable)
    assert result['controller']['state'] == 'unknown'
    assert result['notebook']['status'] == 'ready'
    assert result['postgres']['sql_response'] is False and not result['read_ready']


@pytest.mark.asyncio
async def test_existing_queue_and_native_probe_form_one_exact_health_response(target):
    expected = current_binding(target.queue)
    native = Native([(expected.database_generation, 's1', expected.min_data_seq, expected.min_data_seq, False)])
    async def observe(value):
        return await probe_database_health(acquire=native.acquire, binding=value,
            binding_current=lambda b: current_binding(target.queue) == b)
    result = await collect_health(queue=target.queue, native_probe=observe, controller_state='active')
    assert result['read_ready'] and result['postgres']['sql_response']
    assert result['notebook']['provider_version'] == int(expected.provider_version)
    assert result['notebook']['heartbeat_age_seconds'] == 0
    assert result['notebook']['source'] == 'controller_queue'
    assert set(result['notebook']) == {'status', 'launch_state', 'provider_version',
        'connection_generation', 'source', 'heartbeat_age_seconds'}
    serialized = json.dumps(result)
    for private in ('token', 'launch_nonce', 'runtime_incarnation', 'provider_ref', 'run_id'):
        assert private not in serialized


@pytest.mark.parametrize('fields', [
    {}, {'schema_revision': 's1', 'applied_seq': 5, 'projection_seq': 5},
    {'schema_revision': 's1', 'applied_seq': True, 'projection_seq': 1, 'pending': False},
    {'schema_revision': 's1', 'applied_seq': 5, 'projection_seq': 4, 'pending': False},
])
def test_incomplete_or_inconsistent_callback_receipt_is_not_ready(fields):
    assert not DatabaseHealth(binding(), sql_response=True, **fields).ready


@pytest.mark.asyncio
async def test_old_attachment_or_exception_payload_cannot_be_reported_as_healthy(target):
    async def stale(expected):
        with target.queue.connect() as db:
            db.execute('UPDATE runs SET channel_active=0 WHERE id=?', (expected.run_id,))
        return DatabaseHealth(expected, sql_response=True, schema_revision='s1', applied_seq=5, projection_seq=5, pending=False)
    result = await collect_health(queue=target.queue, native_probe=stale)
    assert not result['read_ready'] and result['reason'] == 'attachment_changed'
    assert result['postgres'] is None


@pytest.mark.asyncio
async def test_health_redacts_native_failure_and_bounds_missing_reply(target):
    async def failure(_):
        raise RuntimeError('private credential and document payload')
    result = await collect_health(queue=target.queue, native_probe=failure)
    assert result['reason'] == 'health_probe_unavailable'
    assert 'private' not in json.dumps(result)
    async def timeout(_):
        await asyncio.sleep(60)
    result = await collect_health(queue=target.queue, native_probe=timeout, timeout_seconds=.1)
    assert result['reason'] == 'health_timeout' and not result['read_ready']


@PG
@pytest.mark.asyncio
async def test_health_sql_on_existing_disposable_github_pg17(env):
    await publish(env)
    expected = TargetReadBinding('fixture-run', 'fixture-incarnation', 1, 'query-fixture', 5, '106')
    result = await probe_database_health(acquire=env['acquire'], binding=expected, binding_current=lambda _: True)
    assert result.ready and result.schema_revision == 's1'
    assert result.applied_seq == result.projection_seq == 5


def test_public_heartbeat_health_is_sanitized_and_never_allocates(tmp_path, monkeypatch):
    from regional_knowledge.kaggle_health import heartbeat_health
    queue = BgeQueue(tmp_path / 'cold-public.sqlite')
    for name in ('maintenance', 'request_startup', 'status'):
        monkeypatch.setattr(queue, name, lambda *a, **k: pytest.fail('health allocated work'))
    cold = heartbeat_health(queue)
    assert cold['source'] == 'heartbeat_observation' and cold['reason'] == 'notebook_not_ready'
    assert not cold['read_ready'] and cold['notebook']['status'] == 'absent'


def test_public_heartbeat_health_reports_runtime_observation_without_identifiers(target):
    from regional_knowledge.kaggle_health import heartbeat_health
    expected = current_binding(target.queue)
    result = heartbeat_health(target.queue)
    assert result['source'] == 'heartbeat_observation'
    assert result['notebook']['status'] == 'ready'
    assert result['notebook']['provider_version'] == int(expected.provider_version)
    assert set(result['postgres']) == {'schema_revision', 'applied_seq', 'projection_seq'}
    assert result['read_ready'] == (result.get('reason') is None)
    text = json.dumps(result)
    for secret in (expected.run_id, expected.runtime_incarnation, 'token', 'launch_nonce', 'provider_ref'):
        assert secret not in text


def test_public_heartbeat_health_drops_observation_when_attachment_changes(target, monkeypatch):
    from regional_knowledge import kaggle_health
    capture = kaggle_health._control_snapshot
    def disconnect_after_capture(queue):
        result = capture(queue)
        with queue.connect() as db:
            db.execute('UPDATE runs SET channel_active=0 WHERE id=?', (result[2].run_id,))
        return result
    monkeypatch.setattr(kaggle_health, '_control_snapshot', disconnect_after_capture)
    result = kaggle_health.heartbeat_health(target.queue)
    assert result['reason'] == 'attachment_changed' and not result['read_ready']
    assert 'postgres' not in result
