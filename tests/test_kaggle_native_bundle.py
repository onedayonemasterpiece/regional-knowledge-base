"""Synthetic bounded .deb fixtures only; no download/install/PG/model action."""
from dataclasses import replace
import bz2
import gzip
import hashlib
import io
import json
import lzma
from pathlib import Path
import tarfile

import pytest

import regional_knowledge.kaggle_native_bundle as native
from regional_knowledge.kaggle_native_bundle import (
    NativeBundleError,NativeBundleSpec,VerifiedNativeBundle,prepare_native_bundle,
)

PLATFORM=dict(os_id='ubuntu',os_version_id='22.04',machine='x86_64',libc_name='glibc',libc_version='2.35')


def sha(data):return hashlib.sha256(data).hexdigest()


def tar_bytes(entries):
    output=io.BytesIO()
    with tarfile.open(fileobj=output,mode='w',format=tarfile.PAX_FORMAT) as archive:
        for name,kind,data in entries:
            info=tarfile.TarInfo(name)
            if kind=='file':
                info.size=len(data);archive.addfile(info,io.BytesIO(data))
            else:
                info.type={'symlink':tarfile.SYMTYPE,'hardlink':tarfile.LNKTYPE,'directory':tarfile.DIRTYPE,
                           'fifo':tarfile.FIFOTYPE}[kind]
                if kind in ('symlink','hardlink'):info.linkname=data
                archive.addfile(info)
    return output.getvalue()


def ar_bytes(members):
    result=b'!<arch>\n'
    for name,data in members:
        header=f'{name+"/":<16}{0:<12}{0:<6}{0:<6}{"100644":<8}{len(data):<10}'.encode()+b'`\n'
        assert len(header)==60
        result+=header+data+(b'\n' if len(data)%2 else b'')
    return result


def deb_bytes(entries,compression='xz'):
    data=tar_bytes(entries)
    if compression=='zst':
        zstd=pytest.importorskip('compression.zstd')
        compressed=zstd.compress(data)
    else:
        compressed={'xz':lzma.compress,'gz':gzip.compress,'bz2':bz2.compress,'none':lambda value:value}[compression](data)
    suffix='' if compression=='none' else '.'+compression
    # This control payload must never be interpreted or executed.
    archive=ar_bytes([('debian-binary',b'2.0\n'),('control.tar.xz',b'forbidden maintainer script'),
                      ('data.tar'+suffix,compressed)])
    return archive,len(data)


@pytest.fixture
def fixture(tmp_path,monkeypatch):
    root=tmp_path/'kaggle-working';root.mkdir()
    monkeypatch.setattr(native,'TRUSTED_ROOT',root)
    monkeypatch.setattr(native,'_platform_identity',lambda:dict(PLATFORM))
    def build(entries=None,files=None,compression='xz',mutate=None):
        entries=entries or [('usr/bin/postgres','file',b'pinned executable'),
                           ('usr/lib/libfixture.so.1','file',b'pinned shared library'),
                           ('usr/lib/libfixture.so','symlink','libfixture.so.1'),
                           ('usr/share/postgresql/17/vector.control','file',b'pinned extension descriptor')]
        body,unpacked=deb_bytes(entries,compression)
        archive=root/'downloads/package.deb';archive.parent.mkdir(exist_ok=True);archive.write_bytes(body)
        default=[dict(path=name,size=len(data),sha256=sha(data),package='fixture',member=name,
            executable=name=='usr/bin/postgres') for name,kind,data in entries if kind=='file']
        if files is None:
            files=default
            if any(name=='usr/lib/libfixture.so' for name,_,_ in entries):
                files.append(dict(path='usr/lib/libfixture.so',size=len(b'pinned shared library'),
                    sha256=sha(b'pinned shared library'),package='fixture',member='usr/lib/libfixture.so',executable=False))
        manifest=dict(codec='rkb.native_bundle.v1',platform=dict(PLATFORM),packages=[dict(name='fixture',
            archive='downloads/package.deb',size=len(body),sha256=sha(body),unpacked_bytes=unpacked)],
            files=files,total_seconds=120)
        if mutate:mutate(manifest)
        raw=json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()
        path=root/'native-manifest.json';path.write_bytes(raw)
        spec=NativeBundleSpec(path.name,sha(raw))
        inventory=tuple(dict(name=name,size=len(data),sha256=sha(data),role='code') for name,data in
            [(path.name,raw),('downloads/package.deb',body)])
        return root,spec,inventory,manifest
    return build


def prepare(value,**kwargs):
    root,spec,inventory,_=value
    return prepare_native_bundle(root,spec.manifest_name,spec.manifest_sha256,inventory=inventory,**kwargs)


def test_small_descriptor_roundtrip_rejects_unbounded_unknown_fields():
    value=NativeBundleSpec('native-manifest.json','a'*64)
    assert NativeBundleSpec.from_wire(value.wire())==value
    assert len(json.dumps(value.wire()))<200
    with pytest.raises(ValueError):NativeBundleSpec('../escape','a'*64)
    with pytest.raises(ValueError):NativeBundleSpec.from_wire(dict(value.wire(),files=[]))


@pytest.mark.parametrize('compression',['xz','gz','bz2','none','zst'])
def test_verified_extraction_materializes_link_as_regular_file_and_retains_capability(fixture,compression):
    value=fixture(compression=compression)
    root,spec,inventory,manifest=value
    cap=prepare(value)
    assert type(cap) is VerifiedNativeBundle and cap.prefix==spec.prefix(root)
    assert cap.manifest_sha256==spec.manifest_sha256 and cap.current()
    assert len(cap.pins)==4
    for path,size,digest,executable in cap.pins:
        file=Path(path)
        assert file.is_file() and not file.is_symlink() and file.stat().st_size==size and sha(file.read_bytes())==digest
        assert file.stat().st_mode&0o777==(0o500 if executable else 0o400)
    assert (cap.prefix/'usr/lib/libfixture.so').read_bytes()==b'pinned shared library'
    assert not list(root.glob('.rkb-native-part-*'))
    with pytest.raises(TypeError):
        VerifiedNativeBundle(root,cap.prefix,spec.manifest_name,spec.manifest_sha256,(),(),None)


def test_cache_rechecks_all_files_and_refuses_tampered_or_unknown_prefix(fixture,monkeypatch):
    value=fixture();cap=prepare(value)
    def forbidden(*a,**k):raise AssertionError('cache was extracted again')
    monkeypatch.setattr(native,'_extract_package',forbidden)
    assert prepare(value).current()
    extra=cap.prefix/'unexpected';extra.write_text('preserve')
    assert not cap.current()
    with pytest.raises(NativeBundleError):prepare(value)
    assert extra.read_text()=='preserve'


def test_capability_allows_explicit_group_read_execute_but_detects_byte_changes(fixture):
    value=fixture();cap=prepare(value)
    for path,_,_,execute in cap.pins:Path(path).chmod(0o550 if execute else 0o440)
    assert cap.current()
    target=Path(cap.pins[0][0]);target.chmod(0o600);target.write_bytes(b'tampered')
    assert not cap.current()
    with pytest.raises(NativeBundleError):prepare(value)
    assert target.read_bytes()==b'tampered'


def test_unknown_actual_or_claimed_abi_has_no_fallback(fixture,monkeypatch):
    value=fixture()
    monkeypatch.setattr(native,'_platform_identity',lambda:dict(PLATFORM,os_version_id='99.00'))
    with pytest.raises(NativeBundleError,match='abi_mismatch'):prepare(value)
    monkeypatch.setattr(native,'_platform_identity',lambda:dict(PLATFORM))
    with pytest.raises(NativeBundleError,match='abi_mismatch'):prepare(value,identity=dict(PLATFORM,machine='arm64'))
    assert not value[1].prefix(value[0]).exists()


@pytest.mark.parametrize('kind',['manifest-hash','archive-hash','archive-size','manifest-role','archive-role'])
def test_exact_top_code_inventory_binding_is_required(fixture,kind):
    root,spec,inventory,manifest=fixture()
    inventory=[dict(item) for item in inventory]
    if kind=='manifest-hash':inventory[0]['sha256']='0'*64
    elif kind=='archive-hash':inventory[1]['sha256']='0'*64
    elif kind=='archive-size':inventory[1]['size']+=1
    elif kind=='manifest-role':inventory[0]['role']='weight'
    else:inventory[1]['role']='weight'
    with pytest.raises(NativeBundleError):
        prepare_native_bundle(root,spec.manifest_name,spec.manifest_sha256,inventory=inventory)
    assert not spec.prefix(root).exists()


@pytest.mark.parametrize('name',['../escape','/absolute','double//slash'])
def test_tar_path_escape_and_special_files_fail_before_publication(fixture,name):
    value=fixture(entries=[(name,'file',b'bad')],files=[dict(path='safe',size=3,sha256=sha(b'bad'),
        package='fixture',member='safe',executable=False)])
    with pytest.raises(NativeBundleError):prepare(value)
    assert not value[1].prefix(value[0]).exists() and not list(value[0].glob('.rkb-native-part-*'))


@pytest.mark.parametrize('target',['../../../etc/passwd','/etc/passwd','missing'])
def test_links_cannot_escape_or_read_unpinned_host_file(fixture,target):
    files=[dict(path='alias',size=6,sha256=sha(b'secret'),package='fixture',member='alias',executable=False)]
    value=fixture(entries=[('alias','symlink',target)],files=files)
    with pytest.raises(NativeBundleError):prepare(value)
    assert not value[1].prefix(value[0]).exists()


def test_hardlink_to_pinned_regular_target_is_materialized(fixture):
    entries=[('original','file',b'exact'),('alias','hardlink','original')]
    files=[dict(path=name,size=5,sha256=sha(b'exact'),package='fixture',member=name,executable=False)
           for name in ('original','alias')]
    cap=prepare(fixture(entries=entries,files=files))
    assert (cap.prefix/'alias').read_bytes()==b'exact' and not (cap.prefix/'alias').is_symlink()
    assert (cap.prefix/'alias').stat().st_ino!=(cap.prefix/'original').stat().st_ino


def test_link_cycle_and_unpinned_target_are_refused(fixture):
    entries=[('a','symlink','b'),('b','symlink','a')]
    files=[dict(path=name,size=0,sha256=sha(b''),package='fixture',member=name,executable=False) for name in ('a','b')]
    with pytest.raises(NativeBundleError,match='link_cycle'):prepare(fixture(entries=entries,files=files))


def test_source_data_hash_mismatch_and_bomb_bound_never_publish(fixture):
    value=fixture(mutate=lambda manifest:manifest['files'][0].update(sha256='0'*64))
    with pytest.raises(NativeBundleError):prepare(value)
    value=fixture(mutate=lambda manifest:manifest['packages'][0].update(unpacked_bytes=1024))
    with pytest.raises(NativeBundleError):prepare(value)
    assert not list(value[0].glob('rkb-native-*'))


def test_symlinked_archive_parent_and_published_prefix_are_refused(fixture):
    value=fixture();root,spec,_,_=value
    prefix=spec.prefix(root);outside=root.parent/'outside';outside.mkdir();(outside/'preserve').write_text('keep')
    prefix.symlink_to(outside,target_is_directory=True)
    with pytest.raises(NativeBundleError):prepare(value)
    assert (outside/'preserve').read_text()=='keep'


def test_disk_budget_is_checked_before_creating_partial_prefix(fixture,monkeypatch):
    value=fixture()
    monkeypatch.setattr(native.shutil,'disk_usage',lambda root:type('Disk',(),{'free':0})())
    with pytest.raises(NativeBundleError,match='insufficient_disk'):prepare(value)
    assert not list(value[0].glob('.rkb-native-part-*'))


def test_atomic_publish_never_overwrites_preexisting_directory(fixture,monkeypatch):
    value=fixture();root,spec,_,_=value
    original=native._publish
    def race(root_fd,temporary,prefix):
        destination=root/prefix;destination.mkdir();(destination/'unknown').write_text('preserve')
        return original(root_fd,temporary,prefix)
    monkeypatch.setattr(native,'_publish',race)
    with pytest.raises(NativeBundleError):prepare(value)
    assert (spec.prefix(root)/'unknown').read_text()=='preserve'
    assert not list(root.glob('.rkb-native-part-*'))


def test_offline_builder_derives_all_hashes_and_roundtrips_runtime_extraction(fixture):
    root,_,inventory,manifest=fixture()
    packages=[{key:value for key,value in item.items() if key!='unpacked_bytes'} for item in manifest['packages']]
    built=native.build_native_manifest(root,packages,identity=PLATFORM,
        include_prefixes=('usr/bin','usr/lib','usr/share/postgresql'))
    assert len(built['files'])==4 and built['packages'][0]['unpacked_bytes']==manifest['packages'][0]['unpacked_bytes']
    assert {item['path']:item['sha256'] for item in built['files']}=={item['path']:item['sha256'] for item in manifest['files']}
    raw=json.dumps(built,sort_keys=True,separators=(',',':')).encode()
    (root/'built.json').write_bytes(raw)
    top=(*inventory,dict(name='built.json',size=len(raw),sha256=sha(raw),role='code'))
    cap=prepare_native_bundle(root,'built.json',sha(raw),inventory=top)
    assert cap.current() and len(cap.pins)==4


def test_builder_cross_package_library_alias_and_runtime_regularization(fixture):
    root,_,inventory,manifest=fixture(entries=[('usr/lib/libbase.so','symlink','libbase.so.1')],files=[
        dict(path='usr/lib/libbase.so',size=7,sha256=sha(b'library'),package='fixture',member='usr/lib/libbase.so',executable=False)])
    body,unpacked=deb_bytes([('usr/lib/libbase.so.1','file',b'library')])
    (root/'downloads/base.deb').write_bytes(body)
    packages=[{key:value for key,value in item.items() if key!='unpacked_bytes'} for item in manifest['packages']]
    packages.append(dict(name='base',archive='downloads/base.deb',size=len(body),sha256=sha(body)))
    built=native.build_native_manifest(root,packages,identity=PLATFORM,include_prefixes=('usr/lib',))
    assert len(built['files'])==2
    raw=json.dumps(built,sort_keys=True,separators=(',',':')).encode();(root/'built.json').write_bytes(raw)
    top=(*inventory,dict(name='built.json',size=len(raw),sha256=sha(raw),role='code'),
         dict(name='downloads/base.deb',size=len(body),sha256=sha(body),role='code'))
    cap=prepare_native_bundle(root,'built.json',sha(raw),inventory=top)
    assert (cap.prefix/'usr/lib/libbase.so').read_bytes()==b'library'


def test_builder_cannot_assume_alias_target_outside_selected_closure(fixture):
    root,_,_,manifest=fixture()
    packages=[{key:value for key,value in item.items() if key!='unpacked_bytes'} for item in manifest['packages']]
    with pytest.raises(NativeBundleError,match='link_target_unpinned'):
        native.build_native_manifest(root,packages,identity=PLATFORM,include_prefixes=('usr/lib/libfixture.so',))


def collection_fixture(value,*,extra=None,collection_entries=None):
    root,oldspec,inventory,manifest=value
    manifest=json.loads(json.dumps(manifest))
    entries=[]
    for package in manifest['packages']:
        entries.append((package['archive'],'file',(root/package['archive']).read_bytes()))
    if extra:entries.extend(extra)
    body=tar_bytes(collection_entries if collection_entries is not None else entries)
    (root/'native-packages.tar').write_bytes(body)
    manifest.update(codec='rkb.native_bundle.v2',collection=dict(archive='native-packages.tar',size=len(body),sha256=sha(body)))
    raw=json.dumps(manifest,sort_keys=True,separators=(',',':')).encode();(root/'collection-manifest.json').write_bytes(raw)
    spec=NativeBundleSpec('collection-manifest.json',sha(raw))
    top=(dict(name=spec.manifest_name,size=len(raw),sha256=spec.manifest_sha256,role='code'),
         dict(name='native-packages.tar',size=len(body),sha256=sha(body),role='code'))
    return root,spec,top,manifest


def test_one_top_collection_transitively_verifies_nested_debs_and_all_native_files(fixture):
    value=collection_fixture(fixture())
    root,spec,inventory,manifest=value
    # Prove source .debs are taken from the pinned outer archive, not ambient files.
    (root/'downloads/package.deb').unlink()
    cap=prepare(value)
    assert len(inventory)==2 and len(cap.pins)==4 and cap.current()
    assert prepare(value).current()
    cache=root/('rkb-native-archives-'+manifest['collection']['sha256'])
    assert (cache/'downloads/package.deb').is_file()
    (cache/'downloads/package.deb').chmod(0o600);(cache/'downloads/package.deb').write_bytes(b'tampered')
    with pytest.raises(NativeBundleError):prepare(value)


@pytest.mark.parametrize('kind',['unknown','symlink','traversal','duplicate'])
def test_outer_collection_never_expands_unpinned_unsafe_or_duplicate_members(fixture,kind):
    value=fixture();root,spec,inventory,manifest=value
    path=manifest['packages'][0]['archive'];body=(root/path).read_bytes()
    if kind=='unknown':entries=[(path,'file',body),('unknown.deb','file',body)]
    elif kind=='symlink':entries=[(path,'symlink','../../outside')]
    elif kind=='traversal':entries=[('../outside','file',body)]
    else:entries=[(path,'file',body),(path,'file',body)]
    wrapped=collection_fixture(value,collection_entries=entries)
    with pytest.raises(NativeBundleError):prepare(wrapped)
    assert not wrapped[1].prefix(root).exists()
    assert not list(root.glob('.rkb-native-archives-part-*'))


def test_builder_materializes_internal_directory_alias_as_only_pinned_regular_leaves(fixture):
    root,_,inventory,manifest=fixture(entries=[('lib/runtime/real/data','file',b'pinned'),
        ('lib/runtime/alias','symlink','real')],files=[
        dict(path='lib/runtime/real/data',size=6,sha256=sha(b'pinned'),package='fixture',member='lib/runtime/real/data',executable=False)])
    packages=[{key:value for key,value in item.items() if key!='unpacked_bytes'} for item in manifest['packages']]
    built=native.build_native_manifest(root,packages,identity=PLATFORM,include_prefixes=('lib/runtime',))
    aliases=[item for item in built['files'] if item['path']=='lib/runtime/alias/data']
    assert len(aliases)==1 and aliases[0]['member']=='lib/runtime/real/data'
    raw=json.dumps(built,sort_keys=True,separators=(',',':')).encode();(root/'built.json').write_bytes(raw)
    top=(*inventory,dict(name='built.json',size=len(raw),sha256=sha(raw),role='code'))
    cap=prepare_native_bundle(root,'built.json',sha(raw),inventory=top)
    assert cap.current() and (cap.prefix/'lib/runtime/alias/data').read_bytes()==b'pinned'
    assert not (cap.prefix/'lib/runtime/alias').is_symlink()


def test_assembly_produces_two_deterministic_pinned_launch_files(fixture):
    root,_,_,manifest=fixture()
    packages=[{key:value for key,value in item.items() if key!='unpacked_bytes'} for item in manifest['packages']]
    receipt=native.assemble_native_collection(root,packages,identity=PLATFORM,
        include_prefixes=('usr/bin','usr/lib','usr/share/postgresql'),manifest_name='assembled.json')
    assert receipt['package_count']==1 and receipt['file_count']==4 and len(receipt['artifacts'])==2
    assert not receipt['native_execution_verified'] and not receipt['target_abi_observed']
    for pin in receipt['artifacts']:
        assert (root/pin['name']).stat().st_size==pin['size']
        assert sha((root/pin['name']).read_bytes())==pin['sha256']
    duplicate=native.assemble_native_collection(root,packages,identity=PLATFORM,
        include_prefixes=('usr/bin','usr/lib','usr/share/postgresql'),manifest_name='assembled.json')
    assert duplicate==receipt
    spec=NativeBundleSpec.from_wire(receipt['spec'])
    cap=prepare_native_bundle(root,spec.manifest_name,spec.manifest_sha256,inventory=receipt['artifacts'])
    assert cap.current() and not list(root.glob('.rkb-native-build-*'))


def test_python312_zstd_path_requires_exact_preinstalled_decoder_pin(monkeypatch):
    import importlib.metadata
    monkeypatch.setattr(native.sys,'version_info',(3,12))
    monkeypatch.setattr(importlib.metadata,'version',lambda name:'unverified')
    with pytest.raises(NativeBundleError,match='zstd_decoder_unverified'):
        native._zstd_reader(None)
