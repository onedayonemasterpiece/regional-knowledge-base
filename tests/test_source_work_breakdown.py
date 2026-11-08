"""Fictional prepared vectors; no corpus, model weights or provider calls."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from regional_knowledge import kaggle_source_batch as batch
from regional_knowledge.query_telemetry import SOURCE_WORK, source_work_observation


@pytest.mark.asyncio
async def test_actual_encode_reuse_counts_are_command_scoped(monkeypatch):
    knowledge = {'passages': [{'passage_id': 'new', 'body': 'Fictional input.'},
                              {'passage_id': 'prior', 'body': 'Fictional prior input.'}],
                 'semantic': {'stories': []}}
    vector = [1., *([0.] * 1023)]
    monkeypatch.setattr(batch, 'validate_command', lambda _: dict(batch_index=1, component='text',
        materializer_sha256=__import__('regional_knowledge.kaggle_source_materializer',fromlist=['materializer_fingerprint']).materializer_fingerprint()))
    class Connection:
        @asynccontextmanager
        async def transaction(self):yield
        async def execute(self, sql, args=None):
            value = None
            if 'SELECT knowledge' in sql:value = (knowledge,)
            if 'SELECT embedding' in sql:value = (vector,) if args[1]=='prior' else None
            if 'install_source_batch_vector_v1' in sql:value = (args[4],)
            async def fetchone():return value
            return SimpleNamespace(fetchone=fetchone)
    @asynccontextmanager
    async def acquire():yield Connection()
    calls=[]
    async def encode(text):
        calls.append(text); await asyncio.sleep(.005); return vector
    worker=SimpleNamespace(_current=lambda: None, encode=encode)
    command=SimpleNamespace(ids={'source_id':'fictional'}, operation_key='op', sequence=1,encode=lambda:b'command')
    async def run(observe):
        if not observe:
            await batch.materialize_packet(worker, command, acquire=acquire, database=SimpleNamespace(database_generation='db',writer_generation='w'))
            return None
        with source_work_observation() as timings:
            await batch.materialize_packet(worker, command, acquire=acquire, database=SimpleNamespace(database_generation='db',writer_generation='w'))
            assert SOURCE_WORK.get() is timings
            return timings
    first, second, absent=await asyncio.gather(run(True),run(True),run(False))
    assert first is not second and absent is None and SOURCE_WORK.get() is None
    for timings in (first,second):
        assert timings['encoded_count']==timings['reused_count']==1
        assert timings['materialize_seconds']>=timings['encode_seconds']>=.005
        assert set(timings)=={'encoded_count','reused_count','materialize_seconds','encode_seconds'}
    assert calls==['Fictional input.']*3
    with pytest.raises(RuntimeError), source_work_observation():raise RuntimeError('fictional cancellation')
    assert SOURCE_WORK.get() is None
