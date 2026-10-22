"""Owner-assigned112/113 boundary using actual bounded native assembly/extraction."""
import json

import pytest

from regional_knowledge import kaggle_native_bundle as native
from test_kaggle_native_bundle import deb_bytes, sha, PLATFORM


def packages(root, count):
    result = []
    for number in range(count):
        name = f'library-{number}'
        raw, _ = deb_bytes([(f'usr/lib/{name}.so', 'file', name.encode())])
        archive = name + '.deb'
        (root / archive).write_bytes(raw)
        result.append(dict(name=name, archive=archive, size=len(raw), sha256=sha(raw)))
    return result


def test_112_nested_packages_assemble_and_extract_with_two_top_level_artifacts(tmp_path, monkeypatch):
    declared = packages(tmp_path, 112)
    result = native.assemble_native_collection(tmp_path, declared,
        identity=PLATFORM, include_prefixes=('usr/lib',))
    assert result['package_count'] == result['file_count'] == 112
    assert len(result['artifacts']) == 2
    monkeypatch.setattr(native, 'TRUSTED_ROOT', tmp_path)
    monkeypatch.setattr(native, '_platform_identity', lambda: dict(PLATFORM))
    spec = result['spec']
    verified = native.prepare_native_bundle(tmp_path, spec['manifest_name'], spec['manifest_sha256'],
                                            inventory=result['artifacts'])
    assert verified.current() and len(verified.pins) == 112
    assert (verified.prefix / 'usr/lib/library-111.so').read_bytes() == b'library-111'


def test_113_nested_packages_are_refused_by_builder_and_manifest(tmp_path):
    declared = packages(tmp_path, 113)
    with pytest.raises(native.NativeBundleError):
        native.assemble_native_collection(tmp_path, declared, identity=PLATFORM, include_prefixes=('usr/lib',))
    assert not (tmp_path / 'native-packages.tar').exists()
    valid = native.build_native_manifest(tmp_path, declared[:112],
                                        identity=PLATFORM, include_prefixes=('usr/lib',))
    extra = dict(declared[-1], unpacked_bytes=1)
    valid['packages'].append(extra)
    with pytest.raises(ValueError, match='native_bundle_inventory_invalid'):
        native._validate_manifest(valid)
