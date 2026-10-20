"""Bounded catalog probe; native cases require the existing disposable PG guard."""
import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
from types import SimpleNamespace

from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
import pytest

from regional_knowledge.kaggle_database_probe import (
    NativeDatabaseProbe, DatabaseProbeUnavailable, TABLES, FUNCTIONS, TRIGGERS,
)
from test_kaggle_query import env, publish, PG
from test_kaggle_pg_driver import database


def catalog():
    functions = {}
    for name in FUNCTIONS:
        table = any(name.startswith('rkb_v4.' + stem) for stem in (
            'lexical_candidates(', 'dense_candidates(', 'hydrate(', 'lexical_candidates_scoped(',
            'dense_candidates_scoped(', 'hydrate_scoped('))
        result = ('record' if table else 'uuid[]' if 'checked_source_scope(' in name else
                  'void' if any(s in name for s in ('publish_read_frontier(', 'prepare_read_frontier(')) else
                  'boolean' if 'check_source_withdraw_v1(' in name else
                  'bytea' if 'apply_source_withdraw_v1(' in name else 'trigger')
        functions[name] = dict(kind='f', returns_set=table, result=result,
                               security_definer=False, definition='CREATE FUNCTION synthetic')
    return dict(environment={'runtime': dict(server_version_num=170010, server_encoding='UTF8',
                    vector_version='0.8.2', vector_schema='public', collation_version=None,
                    actual_collation_version=None)},
                tables={name: dict(kind='r') for name in TABLES}, columns={}, constraints={},
                indexes={name: dict(table='passage_search', method=method, valid=True, ready=True, live=True)
                         for name, method in (('passage_dense','hnsw'), ('passage_lexical','gin'))},
                triggers={table+':'+name: dict(table=table, name=name, internal=False, enabled='O')
                          for table, name in TRIGGERS}, functions=functions,
                text_config={name:dict(config=name) for name in ('russian','german','simple')})


def state(**changes):
    return dict(database_generation='synthetic-db', schema_revision='s1', function_revision='f1',
                executor_revision='e1', applied_seq=5, projection_seq=5, visibility_epoch=3,
                text_search_ready=True, dense_search_ready=True, pending=False,
                last_migration_id=None, last_migration_hash=None, **changes)


class Connection:
    closed = False

    def __init__(self):
        self.row_factory = tuple_row
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.catalog = catalog()
        self.state = state()
        self.sql = []
        self.exits = []
        self.delay = False
        self.entered = asyncio.Event()

    @asynccontextmanager
    async def transaction(self):
        self.info.transaction_status = TransactionStatus.INTRANS
        try:
            yield
        except BaseException:
            self.exits.append('rollback')
            raise
        else:
            self.exits.append('commit')
        finally:
            self.info.transaction_status = TransactionStatus.IDLE

    def cursor(self, name=None):
        return Cursor(self, name)

    @asynccontextmanager
    async def acquire(self):
        try:
            yield self
        finally:
            assert self.info.transaction_status == TransactionStatus.IDLE


class Cursor:
    def __init__(self, connection, name):
        self.connection, self.name, self.rows = connection, name, []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def execute(self, sql, params=None):
        self.connection.sql.append((sql, params))
        if self.name:
            from regional_knowledge.kaggle_database_probe import CATALOG_QUERIES
            section = CATALOG_QUERIES[int(self.name.rsplit('_',1)[1])][0]
            rows = self.connection.catalog[section]
            self.rows = []
            for key in sorted(rows):
                raw = json.dumps(rows[key])
                size = len(raw.encode())
                self.rows.append((key, raw if size<=params[0] else None, size))
            self.rows = self.rows[:params[1]]
        elif 'vector_dims' in sql:
            self.rows = [(1024,)]
        elif 'recovery_state s CROSS JOIN' in sql:
            self.rows = [(json.dumps(self.connection.state),)]

    async def fetchone(self):
        if self.name and self.connection.delay:
            self.connection.entered.set()
            await asyncio.sleep(10)
        return self.rows.pop(0) if self.rows else None


@pytest.mark.asyncio
async def test_actual_observations_are_canonical_bounded_and_not_expected_strings():
    connection = Connection()
    probe = NativeDatabaseProbe()
    first, fields = await probe(acquire=connection.acquire)
    assert fields['database_generation'] == 'synthetic-db'
    assert fields['applied_seq'] == fields['projection_seq'] == 5
    assert fields['text_search_ready'] and fields['dense_search_ready']
    assert fields['archive_read_ready'] is fields['proof_ready'] is None
    connection.state.update(applied_seq=6, projection_seq=5)
    second, fields = await probe(acquire=connection.acquire)
    assert first == second and fields['applied_seq'] == 6 and fields['projection_seq'] == 5
    assert not fields['text_search_ready'] and not fields['dense_search_ready']
    name = next(iter(FUNCTIONS))
    connection.catalog['functions'][name]['definition'] += ' changed'
    third, _ = await probe(acquire=connection.acquire)
    assert third != first and len(third) < 1024*1024
    assert connection.sql[0][0] == 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'
    assert all('LIMIT %s' in sql and params[1] <= 513 for sql,params in connection.sql if 'FROM (' in sql)
    assert connection.exits == ['commit']*3


@pytest.mark.asyncio
@pytest.mark.parametrize('case,code', [
    ('missing_index','database_indexes_not_ready'), ('invalid_index','database_indexes_not_ready'),
    ('missing_function','database_functions_incomplete'), ('set_function','database_function_contract_invalid'),
    ('disabled_trigger','database_triggers_not_ready'), ('wrong_vector','database_native_runtime_mismatch'),
    ('bad_cursor','database_state_invalid'), ('bool_cursor','database_state_invalid')])
async def test_incomplete_or_invalid_native_contract_fails_closed(case, code):
    connection = Connection()
    if case == 'missing_index': del connection.catalog['indexes']['passage_dense']
    if case == 'invalid_index': connection.catalog['indexes']['passage_dense']['valid'] = False
    if case == 'missing_function': connection.catalog['functions'].pop(next(iter(FUNCTIONS)))
    if case == 'set_function': connection.catalog['functions']['rkb_v4.check_source_withdraw_v1(bytea,jsonb,jsonb,text)']['returns_set'] = True
    if case == 'disabled_trigger': next(iter(connection.catalog['triggers'].values()))['enabled'] = 'D'
    if case == 'wrong_vector': connection.catalog['environment']['runtime']['vector_version'] = 'unverified'
    if case == 'bad_cursor': connection.state['projection_seq'] = 6
    if case == 'bool_cursor': connection.state['applied_seq'] = True
    with pytest.raises(DatabaseProbeUnavailable, match='^'+code+'$'):
        await NativeDatabaseProbe()(acquire=connection.acquire)
    assert connection.exits == ['rollback']


@pytest.mark.asyncio
@pytest.mark.parametrize('limits', [dict(max_catalog_bytes=10), dict(max_catalog_rows=2), dict(max_definition_bytes=10)])
async def test_streaming_catalog_limits_rollback(limits):
    connection = Connection()
    with pytest.raises(DatabaseProbeUnavailable, match='^database_catalog_bound$'):
        await NativeDatabaseProbe(**limits)(acquire=connection.acquire)
    assert connection.exits == ['rollback']


@pytest.mark.asyncio
async def test_deadline_cancel_and_native_errors_cleanup_and_sanitize():
    connection = Connection()
    connection.delay = True
    with pytest.raises(DatabaseProbeUnavailable, match='^database_probe_deadline$'):
        await NativeDatabaseProbe(timeout_seconds=.01)(acquire=connection.acquire)
    connection.entered.clear()
    task = asyncio.create_task(NativeDatabaseProbe()(acquire=connection.acquire))
    await connection.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError): await task
    assert connection.exits == ['rollback','rollback']

    @asynccontextmanager
    async def failed():
        raise RuntimeError('secret native connection information')
        yield
    with pytest.raises(DatabaseProbeUnavailable) as error:
        await NativeDatabaseProbe()(acquire=failed)
    assert str(error.value) == 'database_probe_failed'


@pytest.mark.parametrize('kwargs', [dict(timeout_seconds=True), dict(timeout_seconds=float('nan')),
                                  dict(max_catalog_rows=0), dict(max_catalog_bytes=2**21)])
def test_configuration_is_finite(kwargs):
    with pytest.raises(ValueError): NativeDatabaseProbe(**kwargs)


async def install_five(env):
    async with env['admin'].transaction():
        await env['admin'].execute((Path(__file__).resolve().parents[1]/'sql/kaggle/005_source_withdraw.sql').read_text(), prepare=False)


@PG
@pytest.mark.asyncio
async def test_native_catalog_frontier_and_trigger_hash(env):
    await install_five(env)
    probe = NativeDatabaseProbe()
    before, fields = await probe(acquire=env['acquire'])
    assert fields['database_generation'] == 'query-fixture' and fields['applied_seq'] == 5
    assert not fields['text_search_ready'] and not fields['dense_search_ready']
    # A lagged projection cannot truthfully publish ready=true. The production
    # writer contract rejects that combination; observe a real unready lag.
    async with env['admin'].transaction():
        epoch = (await (await env['admin'].execute(
            'SELECT visibility_epoch FROM rkb_v4.read_frontier')).fetchone())[0]
        await env['admin'].execute("SELECT rkb_v4.publish_read_frontier('query-fixture','writer-1',%s,4,false,false)", (epoch,))
    lagged, fields = await probe(acquire=env['acquire'])
    assert fields['projection_seq'] == 4 and not fields['text_search_ready']
    await publish(env)
    ready, fields = await probe(acquire=env['acquire'])
    assert before == lagged == ready and fields['text_search_ready'] and fields['dense_search_ready']
    async with env['admin'].transaction():
        await env['admin'].execute('CREATE TRIGGER probe_extra AFTER UPDATE ON rkb_v4.sources FOR EACH ROW EXECUTE FUNCTION rkb_v4.bump_source_visibility()')
    changed, _ = await probe(acquire=env['acquire'])
    assert changed != ready
    assert env['connection'].info.transaction_status == TransactionStatus.IDLE


@PG
@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['DROP INDEX rkb_v4.passage_dense',
    "UPDATE pg_catalog.pg_index SET indisvalid=false WHERE indexrelid='rkb_v4.passage_dense'::regclass"])
async def test_native_missing_or_invalid_index_and_cleanup(env, change):
    await install_five(env)
    async with env['admin'].transaction():
        await env['admin'].execute(change)
    with pytest.raises(DatabaseProbeUnavailable, match='^database_indexes_not_ready$'):
        await NativeDatabaseProbe()(acquire=env['acquire'])
    assert env['connection'].info.transaction_status == TransactionStatus.IDLE


@PG
@pytest.mark.asyncio
async def test_native_timeout_rolls_back_before_connection_reuse(env):
    await install_five(env)
    async with env['admin'].transaction():
        await env['admin'].execute('LOCK TABLE rkb_v4.recovery_state IN ACCESS EXCLUSIVE MODE')
        started = asyncio.get_running_loop().time()
        with pytest.raises(DatabaseProbeUnavailable, match='^database_probe_(deadline|cancelled)$'):
            await NativeDatabaseProbe(timeout_seconds=.05)(acquire=env['acquire'])
        assert asyncio.get_running_loop().time() - started >= .02, 'must reach the native lock wait'
    assert env['connection'].info.transaction_status == TransactionStatus.IDLE
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT 1')).fetchone() == (1,)
