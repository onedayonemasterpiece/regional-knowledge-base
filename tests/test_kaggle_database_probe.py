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
    NativeDatabaseProbe, DatabaseProbeUnavailable,
)
from test_kaggle_query import env, publish, PG
from test_kaggle_pg_driver import database


def state(**changes):
    return dict(database_generation='synthetic-db', schema_version=16, server_version_num=170011, pgvector_version='0.8.2', postgis_version=None, applied_seq=5, projection_seq=5, visibility_epoch=3, text_search_ready=True, dense_search_ready=True, pending=False) | changes


class Connection:
    closed=False
    row_factory=tuple_row
    def __init__(self):
        self.info=SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.state=state();self.sql=[];self.exits=[];self.delay=False;self.entered=asyncio.Event()
    @asynccontextmanager
    async def transaction(self):
        self.info.transaction_status=TransactionStatus.INTRANS
        try:yield
        except BaseException:self.exits.append('rollback');raise
        else:self.exits.append('commit')
        finally:self.info.transaction_status=TransactionStatus.IDLE
    @asynccontextmanager
    async def acquire(self):
        try:yield self
        finally:assert self.info.transaction_status==TransactionStatus.IDLE
    async def execute(self,sql,*args):
        self.sql.append(sql)
        return self
    async def fetchall(self):
        self.entered.set()
        if self.delay:await asyncio.sleep(10)
        return [(json.dumps(self.state),)]


@pytest.mark.asyncio
async def test_version_frontier_observation_ignores_catalog_and_pg_patch_release():
    connection=Connection();probe=NativeDatabaseProbe()
    raw,fields=await probe(acquire=connection.acquire)
    assert raw==b'{}' and fields['schema_version']==16 and fields['text_search_ready']
    connection.state.update(server_version_num=170006,projection_seq=4)
    _,fields=await probe(acquire=connection.acquire)
    assert fields['applied_seq']==5 and not fields['text_search_ready'] and not fields['dense_search_ready']
    assert len(connection.sql)==4
    assert all('pg_get_functiondef' not in sql and 'relacl' not in sql for sql in connection.sql)
    assert connection.exits==['commit','commit']


@pytest.mark.asyncio
@pytest.mark.parametrize('update,code',[
    ({'schema_version':None},'database_native_runtime_mismatch'),
    ({'schema_version':True},'database_native_runtime_mismatch'),
    ({'server_version_num':160011},'database_native_runtime_mismatch'),
    ({'pgvector_version':'0.7.0'},'database_native_runtime_mismatch'),
    ({'projection_seq':6},'database_state_invalid'),
    ({'applied_seq':True},'database_state_invalid'),
    ({'pending':True},'database_state_invalid'),
])
async def test_invalid_version_environment_or_frontier_fails_closed(update,code):
    connection=Connection();connection.state.update(update)
    with pytest.raises(DatabaseProbeUnavailable,match='^'+code+'$'):
        await NativeDatabaseProbe()(acquire=connection.acquire)
    assert connection.exits==['rollback']


@pytest.mark.asyncio
async def test_deadline_cancel_and_native_errors_cleanup_and_sanitize():
    connection=Connection();connection.delay=True
    with pytest.raises(DatabaseProbeUnavailable,match='database_probe_deadline'):
        await NativeDatabaseProbe(timeout_seconds=.01)(acquire=connection.acquire)
    connection.entered.clear()
    task=asyncio.create_task(NativeDatabaseProbe()(acquire=connection.acquire))
    await connection.entered.wait();task.cancel()
    with pytest.raises(asyncio.CancelledError):await task
    assert connection.exits==['rollback','rollback']
    @asynccontextmanager
    async def failed():
        raise RuntimeError('private native diagnostics')
        yield
    with pytest.raises(DatabaseProbeUnavailable,match='^database_probe_failed$'):
        await NativeDatabaseProbe()(acquire=failed)


@pytest.mark.parametrize('timeout',[True,float('nan'),0,11])
def test_configuration_is_finite(timeout):
    with pytest.raises(ValueError):NativeDatabaseProbe(timeout_seconds=timeout)


async def install_five(env):
    async with env['admin'].transaction():
        await env['admin'].execute('DELETE FROM rkb_v4.schema_migrations')
        await env['admin'].execute('INSERT INTO rkb_v4.schema_migrations VALUES(4)')


@PG
@pytest.mark.asyncio
async def test_native_version_and_frontier_survive_nullable_column(env):
    await install_five(env)
    probe=NativeDatabaseProbe()
    _,fields=await probe(acquire=env['acquire'])
    assert fields['schema_version']==4 and fields['applied_seq']==5
    await publish(env)
    async with env['admin'].transaction():
        await env['admin'].execute('ALTER TABLE rkb_v4.sources ADD COLUMN probe_nullable text')
    _,fields=await probe(acquire=env['acquire'])
    assert fields['schema_version']==4 and fields['text_search_ready'] and fields['dense_search_ready']
    assert env['connection'].info.transaction_status==TransactionStatus.IDLE


@PG
@pytest.mark.asyncio
async def test_native_timeout_rolls_back_before_connection_reuse(env):
    await install_five(env)
    async with env['admin'].transaction():
        await env['admin'].execute('LOCK TABLE rkb_v4.recovery_state IN ACCESS EXCLUSIVE MODE')
        with pytest.raises(DatabaseProbeUnavailable,match='database_probe_deadline'):
            await NativeDatabaseProbe(timeout_seconds=.05)(acquire=env['acquire'])
    assert env['connection'].info.transaction_status==TransactionStatus.IDLE
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT 1')).fetchone()==(1,)
