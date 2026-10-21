"""Exact dataset pin/input verification only. No provider, PG or model effects."""
import hashlib
import json
from pathlib import Path
import pytest

from regional_knowledge.kaggle_dataset_base import (
    DatasetFile,DatasetBasePin,DatasetBaseUnavailable,verify_mounted_dataset_base,
)


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
            pgvector_version='0.8.2'),source_control=dict(
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
