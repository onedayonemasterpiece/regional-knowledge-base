"""Exact dataset pin/input verification only. No provider, PG or model effects."""
import hashlib
import json
import os
from pathlib import Path
import pytest

from regional_knowledge.kaggle_dataset_base import (
    DatasetFile,DatasetBasePin,DatasetBaseUnavailable,verify_mounted_dataset_base,
    DatasetCheckpoint,verify_mounted_journal,
)


def test_checkpoint_pins_actual_next_execution_and_native_journal_bytes(tmp_path):
    mount,root,pin=fixture(tmp_path,sequence=7)
    data=canonical(dict(codec='rkb.dataset-journal.v1',objects=[]))
    path=root/'journal-control.json';path.write_bytes(data);path.chmod(0o400)
    journal=DatasetFile(path.name,len(data),hashlib.sha256(data).hexdigest())
    checkpoint=DatasetCheckpoint(pin.owner_slug,pin.dataset_slug,pin.dataset_version,
        pin.database_generation,pin.base_sequence,pin.manifest,pin.archive,pin.control,journal)
    assert DatasetCheckpoint.from_wire(checkpoint.wire())==checkpoint
    executed=checkpoint.for_execution(run_id='actual-next-run',notebook_version=88)
    assert executed.run_id=='actual-next-run' and executed.notebook_version==88
    assert checkpoint.source==pin.ref+'/'+str(pin.dataset_version)
    assert verify_mounted_journal(checkpoint,mount_root=mount)==journal.sha256
    path.chmod(0o600);path.write_bytes(data.replace(b'[]',b'{}'));path.chmod(0o400)
    with pytest.raises(DatasetBaseUnavailable,match='file_hash_differs'):
        verify_mounted_journal(checkpoint,mount_root=mount)


def canonical(value):
    return json.dumps(value,sort_keys=True,ensure_ascii=False,
        allow_nan=False,separators=(',',':')).encode()


def fixture(tmp_path, *, database_generation='source-db', sequence=0):
    mount=tmp_path/'kaggle-input'
    mount.mkdir(mode=0o700,parents=True)
    root=mount/'rkb-region-base'
    root.mkdir(mode=0o700)
    archive=b'fixture: this is not a native PostgreSQL snapshot'
    ar_sha=hashlib.sha256(archive).hexdigest()
    control=canonical(dict(codec='rkb.source-base-control.v1',stream='rkb',
        database_generation=database_generation,sequence=sequence,
        journal_head=None,catalog_sha256='a'*64,acl_sha256='b'*64,
        registry_sha256='c'*64,routes=[],heads=[]))
    control_sha=hashlib.sha256(control).hexdigest()
    manifest=canonical(dict(codec='rkb.pg-physical-base.v1',stream='rkb',
        database_generation=database_generation,writer_generation='old-writer',
        applied_sequence=sequence,journal_head=None,
        versions=dict(catalog_sha256='a'*64,server_version_num=170011,
            pgvector_version='0.8.2',model_sha256='d'*64,tokenizer_sha256='e'*64,
            schema_revision='s1',function_revision='f1',executor_revision='e1'),source_control=dict(
                sha256=control_sha,length=len(control),key='rkb/base-control'),
        blob=dict(key='rkb/snapshots/blobs/blob',size=len(archive),sha256=ar_sha,
                  native_version='fixture-blob-version')))
    values=(('base-manifest.json',manifest),('source-control.json',control),
            ('postgres.tar',archive))
    for name,data in values:
        path=root/name
        path.write_bytes(data);path.chmod(0o400)
    parts=[DatasetFile(name,len(data),hashlib.sha256(data).hexdigest())
           for name,data in values]
    pin=DatasetBasePin('example-owner','rkb-region-base',19,87,'run-identity',
        database_generation,sequence,parts[0],parts[2],parts[1])
    return mount,root,pin


def verify(mount,pin,**updates):
    kw=dict(observed_dataset_ref=pin.ref,observed_dataset_version=pin.dataset_version,
            observed_notebook_version=pin.notebook_version,
            observed_run_id=pin.run_id,mount_root=mount)
    kw.update(updates)
    return verify_mounted_dataset_base(pin,**kw)




@pytest.mark.asyncio
async def test_dataset_attestation_flows_into_existing_native_pg_prepare(tmp_path):
    """Same native factory path receives exact verified bytes, not a PG clone."""
    from types import SimpleNamespace
    from regional_knowledge.kaggle_pg_snapshot_native import NativeSnapshotFactory,SnapshotLimits
    mount,root,pin=fixture(tmp_path)
    checked=verify(mount,pin)
    versions=json.loads(checked.manifest_bytes)['versions']
    calls=[]
    factory=object.__new__(NativeSnapshotFactory)
    factory.current=lambda binding: binding==('run-identity','incarnation',3)
    factory.database=SimpleNamespace(database_generation='source-db')
    factory.identity=('run-identity','nonce','incarnation')
    factory.model_pins={'model_sha256':'d'*64,'tokenizer_sha256':'e'*64}
    factory.limits=SnapshotLimits(timeout_seconds=5)
    async def native_prepare(binding,*,operation_key,**kwargs):
        calls.append((binding,operation_key,kwargs))
        return 'NATIVE_PREPARE_SEAM_ONLY'
    factory.prepare=native_prepare

    params=dict(operation_key='dataset-base-restore-1',pin=pin,
        observed_dataset_ref=pin.ref,observed_dataset_version=pin.dataset_version,
        observed_notebook_version=pin.notebook_version,observed_run_id=pin.run_id,
        expected_versions=versions,workspace='/private-native-existing',
        mount_root=mount)
    binding=('run-identity','incarnation',3)
    result=await factory.prepare_dataset_base(binding,**params)
    assert result=='NATIVE_PREPARE_SEAM_ONLY' and len(calls)==1
    actual,operation,arguments=calls[0]
    assert actual==binding and operation=='dataset-base-restore-1'
    assert arguments['archive_path']==root/'postgres.tar'
    assert arguments['manifest_bytes']==checked.manifest_bytes
    assert arguments['control_bytes']==checked.control_bytes
    assert arguments['expected_manifest_sha256']==pin.manifest.sha256
    assert arguments['expected_versions']==versions
    assert arguments['workspace']=='/private-native-existing'

    calls.clear()
    from regional_knowledge.kaggle_dataset_base import DatasetBaseUnavailable
    with pytest.raises(DatasetBaseUnavailable,match='dataset_version_or_run_unverified'):
        await factory.prepare_dataset_base(binding,**dict(params,observed_dataset_version=20))
    with pytest.raises(DatasetBaseUnavailable,match='dataset_version_or_run_unverified'):
        await factory.prepare_dataset_base(binding,**dict(params,observed_notebook_version=None))
    assert calls==[], 'native PostgreSQL prepare must not start after invalid mount proof'

    from regional_knowledge.kaggle_pg_snapshot_native import SnapshotError
    with pytest.raises(SnapshotError,match='resident_model_tokenizer_changed'):
        await factory.prepare_dataset_base(binding,**dict(params,
            expected_versions=dict(versions,model_sha256='f'*64)))
    assert calls==[]


def test_generated_target_source_inventory_contains_dataset_verifier():
    from regional_knowledge.kaggle_target_launch import MODULES,source_bundle
    root=Path(__file__).resolve().parents[1]
    assert 'kaggle_dataset_base' in MODULES
    _,files,entries=source_bundle(root)
    path='regional_knowledge/kaggle_dataset_base.py'
    data=files[path].encode('utf-8')
    assert data==(root/'src'/path).read_bytes()
    assert any(e['name'].endswith(path) and e['sha256']==hashlib.sha256(data).hexdigest()
               for e in entries)


def test_exact_dataset_version_and_three_files_readback(tmp_path):
    mount,root,pin=fixture(tmp_path)
    checked=verify(mount,pin)
    assert checked.archive_path==root/'postgres.tar'
    assert checked.pin is pin
    assert json.loads(checked.manifest_bytes)['applied_sequence']==0
    assert json.loads(checked.control_bytes)['codec']=='rkb.source-base-control.v1'
    assert DatasetBasePin.from_wire(pin.wire())==pin
    assert checked.pin.dataset_version==19  # not guessed from mount slug
    assert list(root.iterdir()) and not (root/'data').exists()


@pytest.mark.parametrize('changed',[
    dict(observed_dataset_ref='another-owner/rkb-region-base'),
    dict(observed_dataset_version=20),
    dict(observed_notebook_version=88),
    dict(observed_run_id='different-run'),
    dict(observed_dataset_version=None),
])
def test_missing_or_mismatched_provider_identity_fails_closed(tmp_path,changed):
    mount,_,pin=fixture(tmp_path)
    with pytest.raises(DatasetBaseUnavailable,match='version_or_run_unverified'):
        verify(mount,pin,**changed)


def test_modified_archive_or_control_never_becomes_valid_snapshot(tmp_path):
    mount,root,pin=fixture(tmp_path)
    path=root/'postgres.tar'
    path.chmod(0o600);path.write_bytes(b'forged byte change, also wrong length')
    with pytest.raises(DatasetBaseUnavailable,match='file_identity_differs|file_hash_differs'):
        verify(mount,pin)
    mount,root,pin=fixture(tmp_path/'second')
    path=root/'source-control.json'
    path.chmod(0o600);original=path.read_bytes()
    path.write_bytes(original.replace(b'"stream":"rkb"',b'"stream":"bad"'))
    with pytest.raises(DatasetBaseUnavailable,match='file_hash_differs'):
        verify(mount,pin)


def test_unsafe_symlink_and_writable_files_are_rejected(tmp_path):
    mount,root,pin=fixture(tmp_path)
    archive=root/'postgres.tar'
    archive.unlink()
    archive.symlink_to(tmp_path/'elsewhere.tar')
    with pytest.raises(DatasetBaseUnavailable):
        verify(mount,pin)
    archive.unlink()
    archive.write_bytes(b'fixture: this is not a native PostgreSQL snapshot')
    archive.chmod(0o666)
    with pytest.raises(DatasetBaseUnavailable,match='file_identity_differs'):
        verify(mount,pin)


@pytest.mark.parametrize('readonly',[False,True])
def test_effective_readonly_mount_with_permissive_modes(tmp_path,monkeypatch,readonly):
    from types import SimpleNamespace
    from regional_knowledge import kaggle_dataset_base as module
    mount,root,pin=fixture(tmp_path,sequence=7)
    data=canonical(dict(codec='rkb.dataset-journal.v1',objects=[]))
    path=root/'journal-control.json';path.write_bytes(data)
    journal=DatasetFile(path.name,len(data),hashlib.sha256(data).hexdigest())
    checkpoint=DatasetCheckpoint(pin.owner_slug,pin.dataset_slug,pin.dataset_version,
        pin.database_generation,pin.base_sequence,pin.manifest,pin.archive,pin.control,journal)
    root.chmod(0o777)
    for entry in root.iterdir():entry.chmod(0o666)
    monkeypatch.setattr(module.os,'fstatvfs',lambda fd:SimpleNamespace(f_flag=os.ST_RDONLY if readonly else 0))
    if not readonly:
        with pytest.raises(DatasetBaseUnavailable,match='dataset_mount_writable'):verify(mount,pin)
        with pytest.raises(DatasetBaseUnavailable,match='dataset_mount_writable'):
            verify_mounted_journal(checkpoint,mount_root=mount)
        return
    assert verify(mount,pin).archive_path==root/'postgres.tar'
    assert verify_mounted_journal(checkpoint,mount_root=mount)==journal.sha256
    archive=root/'postgres.tar'
    original=archive.read_bytes();archive.write_bytes(bytes([original[0]^1])+original[1:])
    with pytest.raises(DatasetBaseUnavailable,match='dataset_file_hash_differs'):verify(mount,pin)


@pytest.mark.parametrize('name',['../book','subdir/base.tar','/absolute','', '..'])
def test_no_arbitrary_file_path_as_dataset_locator(name):
    with pytest.raises(DatasetBaseUnavailable):
        DatasetFile(name,12,'f'*64)


def test_manifest_control_and_generation_bind_independently(tmp_path):
    mount,root,pin=fixture(tmp_path,database_generation='db-A')
    manifest=root/'base-manifest.json'
    # A mutated, differently-pinned canonical manifest cannot claim another
    # database generation when restoring this exact source/PG target.
    value=json.loads(manifest.read_bytes())
    value['database_generation']='db-B'
    data=canonical(value)
    manifest.chmod(0o600);manifest.write_bytes(data);manifest.chmod(0o400)
    new=DatasetFile(manifest.name,len(data),hashlib.sha256(data).hexdigest())
    from dataclasses import replace
    pin=replace(pin,manifest=new)
    with pytest.raises(DatasetBaseUnavailable,match='pg_manifest_binding_differs'):
        verify(mount,pin)
