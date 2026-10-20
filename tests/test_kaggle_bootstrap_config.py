"""Concrete launch metadata with synthetic native archive bytes; no effects."""
import json
from pathlib import Path

import pytest

from regional_knowledge.kaggle_bootstrap_config import build_bootstrap_target
from regional_knowledge.kaggle_launch_config import target_launch_from_wire
from test_kaggle_native_bundle import fixture as native_fixture,sha


@pytest.mark.parametrize('parser_enabled',(False,True))
def test_concrete_builder_combines_full_receipts_without_fake_ready_pins(native_fixture,parser_enabled):
    names=('usr/lib/postgresql/17/bin/initdb','usr/lib/postgresql/17/bin/postgres',
        'usr/lib/postgresql/17/bin/pg_config','usr/lib/postgresql/17/lib/vector.so',
        'usr/share/postgresql/17/extension/vector.control','usr/share/postgresql/17/extension/vector--0.8.2.sql')
    files=[dict(path=name,size=1,sha256=sha(b'x'),package='fixture',member=name,executable=index<3)
        for index,name in enumerate(names)]
    root,spec,inventory,_=native_fixture(entries=[(name,'file',b'x') for name in names],files=files)
    receipt=dict(codec='rkb.native_collection_artifacts.v1',spec=spec.wire(),artifacts=list(inventory))
    downloads=[dict(name=item['name'],size=item['size'],sha256=item['sha256'],
        url='https://fixture.invalid/'+item['name'],executable=False) for item in inventory]
    lock=json.loads((Path(__file__).resolve().parents[1]/'scripts/production/manifests/kaggle-cp312-x86_64-wheels.json').read_bytes())
    if parser_enabled:
        lock['packages'].append(dict(package='pymupdf',version='1.28.2',
            name='wheels-cp312/pymupdf-1.28.2-cp312-cp312-manylinux_2_28_x86_64.whl',
            url='https://fixture.invalid/synthetic-parser.whl',size=1,sha256=sha(b'x')))
        lock['wheel_bytes']+=1
    options=dict(wheel_lock=lock,native_receipt=receipt,native_manifest_bytes=(root/spec.manifest_name).read_bytes(),
        native_downloads=downloads,database_generation='fixture-new-generation',writer_generation='writer-1',
        uid=65534,gid=65534,identity_evidence='explicit candidate; actual lookup mandatory')
    if parser_enabled:
        from regional_knowledge.kaggle_original_native import OriginalIntakeConfig
        options['original_intake']=OriginalIntakeConfig(
            ('https://archive.fixture.invalid',),(('pymupdf','1.28.2'),))
    target=build_bootstrap_target(**options)
    assert target.bootstrap_only and target.dependency_sha256 is None and target.schema_sha256 is None
    assert target.database.cluster.startswith('/kaggle/temp/rkb-')
    assert len(target.stage0.wheels)==len(lock['packages'])
    assert len(target.artifacts)==len(lock['packages'])+8
    if parser_enabled:
        from regional_knowledge.kaggle_target_launch import source_bundle
        from regional_knowledge.kaggle_stage0 import _inventory,MAX_PREPARED_FILES
        _,_,source_entries=source_bundle(Path(__file__).resolve().parents[1])
        combined=list(target.artifacts)+source_entries
        prepared=dict(root=target.artifact_root,entries=combined,
            max_total_bytes=sum(item['size'] for item in combined))
        assert 128<len(combined)<=MAX_PREPARED_FILES
        assert len(_inventory(prepared))==len(combined)
        overflow=[dict(name='overflow/'+str(n),size=1,sha256=sha(b'x'),role='code')
            for n in range(MAX_PREPARED_FILES+1)]
        with pytest.raises(ValueError,match='stage0_prepared_inventory_required'):
            _inventory(dict(prepared,entries=overflow))
    restored=target_launch_from_wire(json.loads(json.dumps(target.wire())))
    assert restored.wire()==target.wire()
    if parser_enabled:
        assert type(restored.original_intake) is OriginalIntakeConfig
        with pytest.raises(ValueError,match='dependency attestation'):
            build_bootstrap_target(**dict(options,wheel_lock=dict(lock,packages=lock['packages'][:-1])))
        with pytest.raises(ValueError,match='exact Stage0 wheel closure'):
            build_bootstrap_target(**dict(options,original_intake=OriginalIntakeConfig(
                ('https://archive.fixture.invalid',),(('pymupdf','0.0.1'),))))
        bad=target.wire();bad['original_intake']['unknown']=True
        with pytest.raises(ValueError,match='original_deployment_invalid'):
            target_launch_from_wire(bad)
    with pytest.raises(ValueError,match='actual native manifest'):
        build_bootstrap_target(**dict(options,native_manifest_bytes=b'{}'))
    with pytest.raises(ValueError,match='hosted inputs'):
        build_bootstrap_target(**dict(options,native_downloads=downloads[:-1]))
