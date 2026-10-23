import asyncio
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace

import pytest
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.kaggle_schema_migrate import latest_schema_version
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
        self.rows = rows if rows is not None else [('synthetic-db', 14, 5, 5, False)]
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
    assert result.schema_version == 14
    assert 0 <= result.elapsed_ms < 2000
    assert native.acquired == native.released == 1
    assert native.sql[0][0] == 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'
    assert native.sql[-1][0] == HEALTH_SQL
    assert not any(word in HEALTH_SQL.lower() for word in ('passage', 'original', 'token', 'password'))


@pytest.mark.asyncio
@pytest.mark.parametrize('row,reason', [
    (('other-db', 1, 5, 5, False), 'pg_generation_mismatch'),
    (('synthetic-db', 1, 6, 5, False), 'pg_projection_lag'),
    (('synthetic-db', 1, 5, 5, True), 'pg_prior_write_pending'),
    (('synthetic-db', 1, 4, 4, False), 'pg_sequence_behind'),
    (('synthetic-db', 1, True, 5, False), 'pg_metadata_invalid'),
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
    native = Native([(expected.database_generation, 1, expected.min_data_seq, expected.min_data_seq, False)])
    async def observe(value):
        return await probe_database_health(acquire=native.acquire, binding=value,
            binding_current=lambda b: current_binding(target.queue) == b)
    result = await collect_health(queue=target.queue, native_probe=observe, controller_state='active')
    assert result['read_ready'] and result['postgres']['sql_response']
    assert 'saved_seq' in result['postgres']
    assert result['postgres']['saved_seq'] is None  # no verified Dataset base in fixture
    assert result['notebook']['provider_version'] == int(expected.provider_version)
    assert result['notebook']['heartbeat_age_seconds'] == 0
    assert result['notebook']['source'] == 'controller_queue'
    assert set(result['notebook']) == {'status', 'launch_state', 'provider_version',
        'connection_generation', 'source', 'heartbeat_age_seconds'}
    serialized = json.dumps(result)
    for private in ('token', 'launch_nonce', 'runtime_incarnation', 'provider_ref', 'run_id'):
        assert private not in serialized


@pytest.mark.parametrize('fields', [
    {}, {'schema_version': 1, 'applied_seq': 5, 'projection_seq': 5},
    {'schema_version': 1, 'applied_seq': True, 'projection_seq': 1, 'pending': False},
    {'schema_version': 1, 'applied_seq': 5, 'projection_seq': 4, 'pending': False},
])
def test_incomplete_or_inconsistent_callback_receipt_is_not_ready(fields):
    assert not DatabaseHealth(binding(), sql_response=True, **fields).ready


@pytest.mark.asyncio
async def test_old_attachment_or_exception_payload_cannot_be_reported_as_healthy(target):
    async def stale(expected):
        with target.queue.connect() as db:
            db.execute('UPDATE runs SET channel_active=0 WHERE id=?', (expected.run_id,))
        return DatabaseHealth(expected, sql_response=True, schema_version=1, applied_seq=5, projection_seq=5, pending=False)
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
    assert result.ready and result.schema_version == latest_schema_version()
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
    assert set(result['postgres']) == {'schema_version', 'applied_seq', 'projection_seq', 'saved_seq'}
    assert result['postgres']['saved_seq'] is None
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


def test_native_health_import_does_not_require_controller_or_mcp():
    import subprocess
    import sys
    source = '''
import importlib.abc
import sys
forbidden = ('mcp', 'regional_knowledge.kaggle_public_backend',
             'regional_knowledge.read_authority', 'regional_knowledge.bge_queue')
class BlockController(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == prefix or fullname.startswith(prefix + '.') for prefix in forbidden):
            raise ImportError('controller import attempted: ' + fullname)
sys.meta_path.insert(0, BlockController())
from regional_knowledge.kaggle_database_probe import probe_sql_health
assert callable(probe_sql_health)
assert not any(name == prefix or name.startswith(prefix + '.')
               for name in sys.modules for prefix in forbidden)
'''
    subprocess.run([sys.executable, '-c', source], check=True, capture_output=True, timeout=10)


@pytest.mark.asyncio
async def test_native_server_statement_timeout_is_sanitized(target):
    from psycopg.errors import QueryCanceled
    native = Native()
    execute = native.execute
    async def timeout(sql, params=None):
        if sql == HEALTH_SQL:
            raise QueryCanceled('private SQL diagnostic')
        await execute(sql, params)
    native.execute = timeout
    result = await probe_database_health(acquire=native.acquire, binding=binding(), binding_current=lambda _: True)
    assert result.reason == 'pg_timeout' and not result.ready
    assert 'private' not in json.dumps(result.wire())
    assert native.acquired == native.released == 1


def test_health_saved_seq_is_only_verified_dataset_pointer_not_live_applied_cursor(tmp_path):
    """The status reader does no provider I/O and never guesses a moving PG head."""
    from types import SimpleNamespace as NS
    from regional_knowledge.kaggle_health import _verified_saved_seq
    from test_dataset_recovery_store import prepared

    p=prepared(tmp_path)
    generation=p.checkpoint.database_generation
    assert _verified_saved_seq(p.q,NS(database_generation=generation))==p.checkpoint.base_sequence
    # A different writer generation has no verified pointer in this queue.
    assert _verified_saved_seq(p.q,NS(database_generation='other-generation')) is None
    # No status read promotes or mutates the current checkpoint version.
    assert p.store.latest_checkpoint==p.checkpoint
