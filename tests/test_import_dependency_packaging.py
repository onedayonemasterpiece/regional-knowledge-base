import importlib.util
import json
import io
from pathlib import Path

import pytest


def packaging():
    path = Path(__file__).resolve().parents[1] / 'scripts/production/prepare_import_dependencies.py'
    spec = importlib.util.spec_from_file_location('import_dependencies', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def inputs(m):
    return [json.loads((m.MANIFESTS / name).read_text()) for name in
            ('kaggle-cp313-x86_64-pdf-wheels.json', 'telethon-stage0-input-20261022.json')]


def placement(candidate):
    wheel = next(p for p in candidate['packages'] if p['package'] == 'pyaes')
    return dict(approved=True, receipt_ref='synthetic-test-receipt', size=wheel['size'],
                sha256=wheel['sha256'], url='https://fixtures.invalid/' + wheel['sha256'] + '/' + wheel['filename'])


def test_missing_hosting_never_emits_deployable_lock():
    m = packaging()
    base, candidate = inputs(m)
    result = m.merge_wheels(base, candidate)
    assert result['status'] == 'UNRESOLVED'
    assert result['runtime_lock'] is None
    assert 'url' not in result['pyaes']
    assert candidate['packages'][0]['package'] == 'telethon'


def test_actual_base_pdf_closure_preserved_and_stage0_uses_exact_telethon_name():
    m = packaging()
    base, candidate = inputs(m)
    result = m.merge_wheels(base, candidate, placement(candidate))
    assert result['codec'] == 'rkb.candidate_wheel_lock.v1'
    assert result['packages'][:len(base['packages'])] == base['packages']
    assert len(result['packages']) == len(base['packages']) + 4
    assert any(p['package'] == 'pymupdf' for p in result['packages'])
    names = {p['package'] for p in result['packages']}
    assert 'Telethon' in names and 'telethon' not in names and 'cryptg' not in names
    assert result['wheel_bytes'] == base['wheel_bytes'] + candidate['wheel_bytes']
    # The runtime actually validates HTTPS downloads, not just package labels.
    from regional_knowledge.kaggle_stage0 import Stage0Download
    for row in result['packages'][-4:]:
        Stage0Download.from_wire({k: row[k] for k in ('name', 'size', 'sha256', 'url')} | {'executable': False})


@pytest.mark.parametrize('field,value', [('approved', False), ('sha256', '0'*64),
    ('size', 1), ('receipt_ref', ''), ('url', 'http://fixtures.invalid/wheel'),
    ('url', 'https://fixtures.invalid/wheel'), ('url', 'https://fixtures.invalid/wheel?token=secret')])
def test_placement_is_exact_approved_content_addressed(field, value):
    m = packaging()
    base, candidate = inputs(m)
    receipt = placement(candidate)
    receipt[field] = value
    with pytest.raises(ValueError):
        m.merge_wheels(base, candidate, receipt)


def dataset_placement(candidate,version=7):
    receipt=placement(candidate);receipt.pop('url')
    wheel=next(p for p in candidate['packages'] if p['package']=='pyaes')
    receipt['dataset']=dict(owner='fixture-owner',slug='rkb-bootstrap',version=version,filename=wheel['filename'])
    return receipt


def test_versioned_dataset_receipt_preserves_all_pins_and_does_not_invent_https():
    m=packaging();base,candidate=inputs(m)
    first=m.merge_wheels(base,candidate,dataset_placement(candidate))
    second=m.merge_wheels(base,candidate,dataset_placement(candidate,version=8))
    assert first['packages'][:len(base['packages'])]==base['packages']
    assert first['wheel_bytes']==second['wheel_bytes']==base['wheel_bytes']+candidate['wheel_bytes']
    assert first['telegram_dataset_source']=='fixture-owner/rkb-bootstrap/7'
    for lock,version in ((first,7),(second,8)):
        row=next(p for p in lock['packages'] if p['package']=='pyaes')
        original=next(p for p in candidate['packages'] if p['package']=='pyaes')
        assert all(row[k]==original[k] for k in ('size','sha256','version'))
        assert row['url']==f"kaggle-dataset://fixture-owner/rkb-bootstrap/{version}/{original['filename']}"
        assert lock['telegram_placement_receipt']=='synthetic-test-receipt'
    # Dataset-aware Stage0 is an explicit PR246 runtime dependency. This
    # packaging branch does not pretend the old HTTPS-only parser supports it.


@pytest.mark.parametrize('field,value',[
    ('version',0),('version',True),('version','latest'),('version',10000000000),
    ('owner','../escape'),('slug','rkb/bootstrap'),('filename','different.whl'),
])
def test_dataset_receipt_rejects_unpinned_or_ambiguous_identity(field,value):
    m=packaging();base,candidate=inputs(m);receipt=dataset_placement(candidate)
    receipt['dataset'][field]=value
    with pytest.raises(ValueError,match='versioned pyaes Dataset'):
        m.merge_wheels(base,candidate,receipt)


def test_dataset_placement_requires_the_same_exact_byte_receipt_as_https():
    m=packaging();base,candidate=inputs(m)
    for field,value in (('approved',False),('sha256','0'*64),('size',1),('receipt_ref','')):
        receipt=dataset_placement(candidate);receipt[field]=value
        with pytest.raises(ValueError):m.merge_wheels(base,candidate,receipt)
    receipt=dataset_placement(candidate);receipt['url']=placement(candidate)['url']
    with pytest.raises(ValueError):m.merge_wheels(base,candidate,receipt)


def test_native_union_preserves_base_and_refuses_hidden_upgrade():
    m = packaging()
    base = json.loads((m.MANIFESTS / 'kaggle-native-noble-pg17.11-base-lock.json').read_text())
    extra = dict(release='noble', architecture='amd64', roots={'djvulibre-bin': m.DJVU_VERSION},
                 metadata=[], dependency_edges=[], packages=[dict(base['packages'][0])])
    merged = m.merge_native(base, extra)
    assert merged['packages'] == sorted(base['packages'], key=lambda p: p['name'])
    assert set(base['roots']) <= set(merged['roots'])
    extra['packages'][0]['sha256'] = '0'*64
    with pytest.raises(ValueError, match='existing native pin differs'):
        m.merge_native(base, extra)


def test_interrupted_download_retries_without_accepting_partial_bytes(tmp_path, monkeypatch):
    m = packaging()
    class Response(io.BytesIO):
        def geturl(self):
            return 'https://fixtures.invalid/package.deb'
    calls = []
    def request(*args, **kwargs):
        calls.append(True)
        if len(calls) == 1:
            raise TimeoutError('synthetic interrupted transfer')
        return Response(b'complete')
    monkeypatch.setattr(m, 'urlopen', request)
    monkeypatch.setattr(m.time, 'sleep', lambda _: None)
    target = tmp_path / 'package.deb'
    result = m.download('https://fixtures.invalid/package.deb', target, maximum=100,
                        expected_size=8, expected_sha=m.digest(b'complete'))
    assert len(calls) == 2 and result['size'] == 8
    assert target.read_bytes() == b'complete'
    assert not target.with_name('package.deb.partial').exists()


def test_captured_ci_closure_reports_all_conflicts_and_bound_without_a_fake_manifest():
    m = packaging()
    base = json.loads((m.MANIFESTS / 'kaggle-native-noble-pg17.11-base-lock.json').read_text())
    extra = json.loads((m.MANIFESTS / 'kaggle-native-djvu-noble-lock-20261022.json').read_text())
    review = m.native_review(base, extra)
    assert {r['package'] for r in review['conflicts']} == {'libhogweed6t64', 'liblzma5', 'libnettle8t64'}
    assert review['union_package_count'] == 103 and review['current_max_packages'] == 112
    assert not review['package_bound_exceeded'] and review['status'] == 'UNRESOLVED'
    assert review['deployable'] is False and review['dependencies_removed'] is False
    assert review['manifest'] is None and review['elf_receipt'] is None


def test_preserved_metadata_resolves_real_constraints_without_base_pin_changes():
    m = packaging()
    base = json.loads((m.MANIFESTS / 'kaggle-native-noble-pg17.11-base-lock.json').read_text())
    resolved = m.preserved_native_closure(base)
    review = m.native_review(base, resolved)
    assert review['conflicts'] == []
    assert review['union_package_count'] == 103
    assert review['package_bound_exceeded'] is False
    assert review['status'] == 'COMPATIBLE'
    assert resolved['roots']['djvulibre-bin'] == m.DJVU_VERSION
    assert set(base['roots']) <= resolved['roots'].keys()
    # A dependency closure is still not a native ELF execution receipt.
    assert resolved['native_execution_verified'] is False
    assert review['manifest'] is None and review['elf_receipt'] is None

