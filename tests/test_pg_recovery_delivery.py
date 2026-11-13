"""Provider delivery must retain exact nested archive bytes for replay binding."""
import hashlib
import io
import json
from pathlib import Path
import runpy
import tarfile


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
