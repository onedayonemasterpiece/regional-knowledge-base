import contextlib
import types

import pytest

import regional_knowledge.kaggle_source_batch as batch
from regional_knowledge.kaggle_source_materializer import SourceMaterializationRejected


class _Cursor:
    async def fetchone(self):
        return None


class _Connection:
    async def execute(self, *args):
        return _Cursor()

    @contextlib.asynccontextmanager
    async def transaction(self):
        yield


@contextlib.asynccontextmanager
async def _acquire():
    yield _Connection()


async def _materialize(monkeypatch, error):
    async def load_current(*args, **kwargs):
        raise error
    async def load_stage_batch(*args, **kwargs):
        return b'raw'
    monkeypatch.setattr(batch, 'validate_command', lambda command: {
        'batch_index': 0, 'component': 'semantic', 'ingestion_id': 'i', 'last_batch_sha256': batch.digest(b'raw')})
    monkeypatch.setattr(batch, 'load_stage_batch', load_stage_batch)
    monkeypatch.setattr(batch, 'load_current', load_current)
    command = types.SimpleNamespace(ids={'source_id': 's'}, operation_key='k', encode=lambda: b'c')
    worker = types.SimpleNamespace(_current=lambda: None)
    with pytest.raises(SourceMaterializationRejected) as raised:
        await batch.materialize_packet(worker, command, acquire=_acquire, database=None)
    return str(raised.value)


@pytest.mark.asyncio
@pytest.mark.parametrize('label', ['native_source_context_chunk_missing', 'native_source_context_entity_missing'])
async def test_context_rejection_names_the_exact_missing_reference(monkeypatch, label):
    assert await _materialize(monkeypatch, ValueError(label)) == label


@pytest.mark.asyncio
@pytest.mark.parametrize('error', [ValueError('private text'), KeyError('chunks'), TypeError('x')])
async def test_other_context_errors_keep_the_generic_reason(monkeypatch, error):
    assert await _materialize(monkeypatch, error) == 'native_source_packet_context_invalid'
