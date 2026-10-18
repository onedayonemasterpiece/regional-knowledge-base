"""New mirror admission pauses under pressure; durable receipt work continues."""
from types import SimpleNamespace

import pytest
from regional_knowledge import illustration_mirror as module
from regional_knowledge.illustration_mirror import IllustrationMirror
from test_illustration_mirror import Vibe
from test_illustration_mirror_sqlite import mirror_source


@pytest.mark.asyncio
async def test_low_disk_defers_new_put_without_losing_crop(mirror_source, monkeypatch):
    source = mirror_source
    client = Vibe(source.owner)
    client.fail = False
    seen = []
    def usage(path):
        seen.append(path)
        return SimpleNamespace(free=2 * 1024**3 - 1)
    monkeypatch.setattr(module.shutil, 'disk_usage', usage)
    before = source.backend.corpus.one('rkb_illustrations', source.illustration)
    try:
        assert await IllustrationMirror(source.backend, client).tick() == 0
        after = source.backend.corpus.one('rkb_illustrations', source.illustration)
        assert seen == [source.backend.corpus.path.parent]
        assert after['mirror_error_type'] == 'MirrorAdmissionDeferred'
        assert not after['mirror_operation_id']
        assert after['crop_object_id'] == before['crop_object_id']
        assert after['source_crop_sha256'] == before['source_crop_sha256']
        assert client.operations == [] or not client.operations
        monkeypatch.setattr(module.shutil, 'disk_usage', lambda path: SimpleNamespace(free=2 * 1024**3))
        assert await IllustrationMirror(source.backend, client).tick() == 0
        admitted = source.backend.corpus.one('rkb_illustrations', source.illustration)
        assert admitted['mirror_operation_id']
        assert admitted['mirror_error_type'] is None
        client.advance(0)
        def forbidden(path):
            raise AssertionError('already-admitted work must not check capacity')
        monkeypatch.setattr(module.shutil, 'disk_usage', forbidden)
        assert await IllustrationMirror(source.backend, client).tick() == 1
        assert len(client.operations) == len(client.entries) == 1
    finally:
        await source.backend.aclose()


@pytest.mark.asyncio
async def test_capacity_probe_failure_never_admits_put(mirror_source, monkeypatch):
    source = mirror_source
    client = Vibe(source.owner)
    client.fail = False
    def unavailable(path):
        raise OSError('filesystem capacity unavailable')
    monkeypatch.setattr(module.shutil, 'disk_usage', unavailable)
    try:
        assert await IllustrationMirror(source.backend, client).tick() == 0
        after = source.backend.corpus.one('rkb_illustrations', source.illustration)
        assert after['mirror_error_type'] == 'OSError'
        assert not after['mirror_operation_id']
        assert not client.operations
    finally:
        await source.backend.aclose()
