"""Metadata-only closure with synthetic same-release snapshots."""
import hashlib
from pathlib import Path

import pytest

from regional_knowledge.kaggle_native_recipe import (
    NativeRecipeError,PackageSnapshot,debian_version_compare,resolve_native_closure,
)


def package(name,version='1',depends='',**changes):
    fields=dict(Package=name,Version=version,Architecture='amd64',Filename='pool/'+name+'_'+version+'.deb',
                Size='12',SHA256=hashlib.sha256((name+version).encode()).hexdigest())
    if depends:fields['Depends']=depends
    fields.update(changes)
    return '\n'.join(key+': '+value for key,value in fields.items())+'\n\n'


def snapshot(tmp_path,*records,release='jammy',name='Packages'):
    path=tmp_path/name;data=''.join(records).encode();path.write_bytes(data)
    return PackageSnapshot(path,release,'https://archive.ubuntu.com/ubuntu/',hashlib.sha256(data).hexdigest())


@pytest.mark.parametrize('left,right',[
    ('1','2'),('1.0~rc1','1.0'),('1.0','1.0-1'),('1:0','2:0'),('1.01','1.02'),
    ('1.0-1ubuntu1','1.0-1ubuntu2'),('17~~','17.11'),('2.5.4','2.5.18+dfsg-0ubuntu0.22.04.3'),
])
def test_debian_order_handles_epochs_revisions_tilde_and_digits(left,right):
    assert debian_version_compare(left,right)<0
    assert debian_version_compare(right,left)>0
    assert debian_version_compare(left,left)==0


def test_same_release_full_depends_predepends_alternatives_and_virtual_providers(tmp_path):
    source=snapshot(tmp_path,package('postgres','17.11','libc (>= 2.35), unavailable | provider',**{'Pre-Depends':'loader'}),
        package('libc','2.34'),package('libc','2.35','loader'),package('loader','1'),
        package('virtual-implementation','4',**{'Provides':'provider (= 3)'}))
    lock=resolve_native_closure([source],release='jammy',roots={'postgres':'17.11'})
    assert {item['name']:item['version'] for item in lock['packages']}=={
        'postgres':'17.11','libc':'2.35','loader':'1','virtual-implementation':'4'}
    assert lock['total_download_bytes']==48 and not lock['target_abi_observed'] and not lock['native_execution_verified']
    assert all(item['source_sha256']==source.sha256 for item in lock['packages'])


def test_missing_dependency_is_not_assumed_in_target_base(tmp_path):
    source=snapshot(tmp_path,package('postgres','17.11','libc (>= 2.35)'))
    with pytest.raises(NativeRecipeError,match='dependency_unavailable:postgres:libc'):
        resolve_native_closure([source],release='jammy',roots={'postgres':'17.11'})


def test_cross_release_metadata_cannot_supply_missing_native_library(tmp_path):
    jammy=snapshot(tmp_path,package('postgres','17.11','libc'),name='jammy')
    noble=snapshot(tmp_path,package('libc','2.39'),release='noble',name='noble')
    with pytest.raises(NativeRecipeError,match='same_release_plan'):
        resolve_native_closure([jammy,noble],release='jammy',roots={'postgres':'17.11'})


def test_exact_owner_root_pin_cannot_drift_to_metadata_newer_version(tmp_path):
    source=snapshot(tmp_path,package('postgres','17.11'),package('postgres','17.12'))
    result=resolve_native_closure([source],release='jammy',roots={'postgres':'17.11'})
    assert result['packages'][0]['version']=='17.11'


def test_tampered_metadata_and_unsupported_binary_syntax_fail_closed(tmp_path):
    source=snapshot(tmp_path,package('postgres','17.11'))
    source.path.write_text('tampered')
    with pytest.raises(NativeRecipeError,match='metadata_hash_mismatch'):
        resolve_native_closure([source],release='jammy',roots={'postgres':'17.11'})
    source=snapshot(tmp_path,package('postgres','17.11','library [amd64]'))
    with pytest.raises(NativeRecipeError,match='syntax_unsupported'):
        resolve_native_closure([source],release='jammy',roots={'postgres':'17.11'})
