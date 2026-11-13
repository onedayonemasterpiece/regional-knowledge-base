"""Provider delivery must retain exact nested archive bytes for replay binding."""
import hashlib
import io
import json
from pathlib import Path
import runpy
import tarfile
import os

from regional_knowledge import kaggle_stage0 as stage0
from test_kaggle_stage0 import download, prepared, spec


def test_opaque_input_preserves_nested_archive_bytes(tmp_path):
    launcher = runpy.run_path(str(Path(__file__).parents[1]/'scripts/production/rkb_pg_recovery_launcher.py'))
    mounted = tmp_path/'mounted'
    mounted.mkdir()
    archive_bytes = io.BytesIO()
    with tarfile.open(fileobj=archive_bytes, mode='w:gz') as archive:
        content = b'exact immutable historical source'
        entry = tarfile.TarInfo('fingerprint/sql/original.sql')
        entry.size = len(content)
        archive.addfile(entry, io.BytesIO(content))
    raw = archive_bytes.getvalue()
    manifest = json.dumps(dict(files=[dict(name='batch-code.tar.gz', size=len(raw),
                                          sha256=hashlib.sha256(raw).hexdigest())])).encode()
    with tarfile.open(mounted/'recovery-input.blob', mode='w:') as archive:
        for name, content in [('batch-code.tar.gz', raw), ('input-manifest.json', manifest)]:
            entry = tarfile.TarInfo(name)
            entry.size = len(content)
            archive.addfile(entry, io.BytesIO(content))
    restored = launcher['unpack_input'](mounted, tmp_path/'private', hashlib.sha256(manifest).hexdigest())
    assert (restored/'batch-code.tar.gz').read_bytes() == raw
    assert (restored/'input-manifest.json').read_bytes() == manifest
    assert not (restored/'fingerprint').exists()


def test_staged_weight_allows_existing_native_stage0_under_private_umask(tmp_path, monkeypatch):
    launcher = runpy.run_path(str(Path(__file__).parents[1]/'scripts/production/rkb_pg_recovery_launcher.py'))
    parent = tmp_path/'kaggle'
    parent.mkdir(mode=0o711)
    temporary = parent/'temp'
    temporary.mkdir(mode=0o711)
    root = temporary/'rkb-artifacts'
    raw = b'pinned fixture weight'
    source = tmp_path/'weight.bin'
    source.write_bytes(raw)
    item = download(name='model/weight.bin', data=raw)
    plan = spec(item)
    monkeypatch.setattr(stage0, 'TEMP_ARTIFACT_ROOT', root)
    monkeypatch.setattr(stage0, '_versions', lambda packages: dict(packages))
    class NoNetwork:
        def open(self, *args, **kwargs):
            raise AssertionError('network not needed')
    monkeypatch.setattr(stage0.urllib.request, 'build_opener', lambda *args: NoNetwork())
    previous = os.umask(0o077)
    try:
        launcher['stage_weight'](source, root, item.name)
        # No missing bytes: this fixture must never download or install.
        result = stage0.prepare_stage0(prepared(root, plan), plan.wire())
    finally:
        os.umask(previous)
    assert result['downloaded_bytes'] == 0
    assert root.stat().st_mode & 0o777 == 0o711
    assert (root/'model').stat().st_mode & 0o777 == 0o700
    assert (root/item.name).read_bytes() == raw
