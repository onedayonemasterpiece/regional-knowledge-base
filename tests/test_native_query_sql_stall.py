"""Real server statement cancellation through the existing worker and transport."""
from contextlib import asynccontextmanager

import pytest
from psycopg.pq import TransactionStatus

from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from regional_knowledge.expert_slack_search_reply import _error_reply
from test_bge_shared_worker import running, query
from test_kaggle_query import PG, env, database, publish, tokenizer


@PG
@pytest.mark.asyncio
async def test_actual_statement_cancel_releases_shared_slot_and_next_query_succeeds(env, caplog):
    await publish(env)
    interrupted = []
    settings = []

    class Cursor:
        def __init__(self, native):
            self.native = native

        def __getattr__(self, name):
            return getattr(self.native, name)

        async def execute(self, statement, params=()):
            if "set_config('statement_timeout'" in statement:
                settings.append(params)
            if 'lexical_candidates(' in statement and not interrupted:
                interrupted.append(True)
                # Test-only scaling: a REAL server statement timeout, not an
                # injected QueryCanceled or a host/wire/queue elapsed deadline.
                await self.native.execute("SELECT set_config('statement_timeout','20ms',true)")
                await self.native.execute('SELECT pg_sleep(1)')
                pytest.fail('the server statement timeout did not fire')
            return await self.native.execute(statement, params)

    class Connection:
        def __init__(self, native):
            self.native = native

        def __getattr__(self, name):
            return getattr(self.native, name)

        @asynccontextmanager
        async def cursor(self):
            async with self.native.cursor() as cursor:
                yield Cursor(cursor)

    @asynccontextmanager
    async def acquire():
        async with env['acquire']() as native:
            try:
                yield Connection(native)
            finally:
                assert native.info.transaction_status == TransactionStatus.IDLE

    handler = KaggleWorkerQuery(acquire=acquire, tokenizer=tokenizer())
    async with running(handler) as (peer, task, loads):
        await peer.put(query(request='sql-stall-1', claim='sql-claim-1', text='PRIVATE_QUERY_MARKER'))
        first = await peer.wait('QUERY_RESULT', predicate=lambda f: f['request_id']=='sql-stall-1')
        assert first['status']=='unavailable' and first['reason']=='native_query_stalled'
        assert first['mode'] is None and first['evidence']==[]
        assert 'Повторите запрос.' in _error_reply(first['reason'])
        await peer.wait('RESOURCE_RELEASED', predicate=lambda f: f['resource_id']=='sql-stall-1')
        assert env['connection'].info.transaction_status == TransactionStatus.IDLE
        assert 'native_query_sql_cancel' in caplog.text and 'sqlstate=57014' in caplog.text
        assert 'PRIVATE_QUERY_MARKER' not in caplog.text
        await peer.put(query(request='sql-next-2', claim='sql-claim-2'))
        second = await peer.wait('QUERY_RESULT', predicate=lambda f: f['request_id']=='sql-next-2')
        assert second['status']=='ok' and second['mode']=='hybrid'
        await peer.wait('RESOURCE_RELEASED', predicate=lambda f: f['resource_id']=='sql-next-2')
        assert len(interrupted)==1 and len(settings)>1 and len(loads)==1
        assert all(params==('60000',) for params in settings)
        assert peer.sessions==1 and not task.done()
