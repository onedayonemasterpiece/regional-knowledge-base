"""A3-c real temporary filesystem round-trip; zero network, pip or provider."""
import csv
import hashlib
import sys
from pathlib import Path
import pytest
import regional_knowledge.kaggle_stage0 as stage0
import regional_knowledge.kaggle_stage0_preinstalled as pre
from regional_knowledge.kaggle_stage0 import Stage0Spec,Stage0Download,Stage0Error,prepare_stage0


def runtime():
    return dict(python_version='3.13.15',cache_tag='cpython-313',machine='x86_64',
        libc_name='glibc',libc_version='2.39',os_id='ubuntu',os_version_id='24.04')


def installed_fixture(tmp_path):
    site=tmp_path/'site';site.mkdir()
    (site/'rkbfixture.py').write_text('ANSWER = 42\n')
    info=site/'rkbfixture-1.2.3.dist-info';info.mkdir()
    (info/'METADATA').write_text('Metadata-Version: 2.1\nName: rkbfixture\nVersion: 1.2.3\n')
    with (info/'RECORD').open('w',newline='') as dest:
        writer=csv.writer(dest)
        for path in ('rkbfixture.py','rkbfixture-1.2.3.dist-info/METADATA',
                     'rkbfixture-1.2.3.dist-info/RECORD'):
            writer.writerow((path,'',''))
    return site


@pytest.fixture
def bundle(tmp_path,monkeypatch):
    monkeypatch.setattr(pre,'environment',runtime)
    site=installed_fixture(tmp_path)
    archive=tmp_path/'python-preinstalled.tar.gz'
    result=pre.build_archive(site,{'rkbfixture':'1.2.3'},archive,
        artifact_name='deps/python-preinstalled.tar.gz')
    return site,archive,result


def plan(root,archive,result,*,url=None):
    content=archive.read_bytes()
    item=Stage0Download('deps/python-preinstalled.tar.gz',
        url or 'kaggle-dataset://owner/dependency-snapshot/17/python-preinstalled.tar.gz',
        len(content),hashlib.sha256(content).hexdigest())
    pinned=result['preinstalled']
    spec=Stage0Spec((item,),(),{'rkbfixture':'1.2.3'},
        install_bytes=pinned['unpacked_bytes']+1024,preinstalled=pinned)
    prepared=dict(root=str(root),
        entries=[dict(name=item.name,size=item.size,sha256=item.sha256,role='code')],
        max_total_bytes=item.size+1024)
    return spec,prepared


def test_package_is_deterministic_and_records_cpu_abi(bundle,tmp_path):
    site,archive,result=bundle
    second=tmp_path/'second.tar.gz'
    repeat=pre.build_archive(site,{'rkbfixture':'1.2.3'},second,
        artifact_name='deps/python-preinstalled.tar.gz',observed=runtime())
    assert archive.read_bytes()==second.read_bytes()
    assert result['sha256']==repeat['sha256']
    assert result['preinstalled']['file_count']==3
    assert result['preinstalled']['accelerator']=='cpu'


def test_versioned_dataset_archive_activates_without_pip_or_https(bundle,tmp_path,monkeypatch):
    _,archive,result=bundle
    working=tmp_path/'kaggle-working';working.mkdir()
    inputs=tmp_path/'kaggle-input'
    mount=inputs/'dependency-snapshot';mount.mkdir(parents=True)
    (mount/'python-preinstalled.tar.gz').write_bytes(archive.read_bytes())
    monkeypatch.setattr(stage0,'TRUSTED_ROOT',working)
    monkeypatch.setattr(stage0,'TRUSTED_INPUT_ROOT',inputs)
    def denied(*a,**kw):raise AssertionError('no network or pip permitted')
    monkeypatch.setattr(stage0.urllib.request,'build_opener',denied)
    monkeypatch.setattr(stage0.subprocess,'run',denied)
    spec,prepared=plan(working,archive,result)
    assert Stage0Spec.from_wire(spec.wire())==spec
    with pytest.raises(TypeError):
        spec.preinstalled['environment']['python_version']='0.0.0'
    previous=list(sys.path)
    try:
        report=prepare_stage0(prepared,spec.wire())
        assert report['installed'] is False
        assert report['downloaded_bytes']==0 and report['dataset_copied_files']==1
        assert (Path(sys.path[0])/'rkbfixture.py').read_text()=='ANSWER = 42\n'
        assert __import__('rkbfixture').ANSWER==42
    finally:
        sys.path[:]=previous
        sys.modules.pop('rkbfixture',None)


def test_abi_change_fails_closed_without_pip_fallback(bundle,tmp_path,monkeypatch):
    _,archive,result=bundle
    working=tmp_path/'kaggle-working';working.mkdir()
    inputs=tmp_path/'kaggle-input'
    mount=inputs/'dependency-snapshot';mount.mkdir(parents=True)
    (mount/'python-preinstalled.tar.gz').write_bytes(archive.read_bytes())
    monkeypatch.setattr(stage0,'TRUSTED_ROOT',working)
    monkeypatch.setattr(stage0,'TRUSTED_INPUT_ROOT',inputs)
    monkeypatch.setattr(stage0.urllib.request,'build_opener',lambda *a:None)
    monkeypatch.setattr(stage0.subprocess,'run',lambda *a,**kw:
        pytest.fail('no pip fallback permitted'))
    spec,prepared=plan(working,archive,result)
    monkeypatch.setattr(pre,'environment',lambda:dict(runtime(),libc_version='2.40'))
    with pytest.raises(Stage0Error,match='preinstalled_runtime_abi_mismatch'):
        prepare_stage0(prepared,spec.wire())
    assert not tuple(working.rglob('rkbfixture.py'))


def test_missing_mount_or_corrupt_pinned_bytes_fail_without_network(bundle,tmp_path,monkeypatch):
    _,archive,result=bundle
    working=tmp_path/'kaggle-working';working.mkdir()
    inputs=tmp_path/'kaggle-input'
    mount=inputs/'dependency-snapshot';mount.mkdir(parents=True)
    monkeypatch.setattr(stage0,'TRUSTED_ROOT',working)
    monkeypatch.setattr(stage0,'TRUSTED_INPUT_ROOT',inputs)
    monkeypatch.setattr(stage0.urllib.request,'build_opener',lambda *a:
        pytest.fail('no http fallback'))
    monkeypatch.setattr(stage0.subprocess,'run',lambda *a,**kw:
        pytest.fail('no pip fallback'))
    spec,prepared=plan(working,archive,result)
    with pytest.raises(Stage0Error,match='stage0_dataset_unavailable'):
        prepare_stage0(prepared,spec.wire())
    (mount/'python-preinstalled.tar.gz').write_bytes(b'corrupt')
    with pytest.raises(Stage0Error,match='stage0_dataset_file_invalid'):
        prepare_stage0(prepared,spec.wire())


def test_bad_manifest_accelerator_and_unversioned_sources_refused(bundle,tmp_path):
    _,archive,result=bundle
    working=tmp_path/'kaggle-working';working.mkdir()
    spec,_=plan(working,archive,result)
    for values,reason in [
        (dict(result['preinstalled'],accelerator='cuda'),'preinstalled_package_pin_invalid'),
        (dict(result['preinstalled'],packages_sha256='0'*64),'preinstalled_package_pin_invalid')]:
        with pytest.raises(ValueError,match=reason):
            Stage0Spec(spec.downloads,(),spec.packages,spec.install_bytes,preinstalled=values)
    with pytest.raises(ValueError,match='preinstalled_archive_replaces_wheels'):
        Stage0Spec(spec.downloads,('other.whl',),spec.packages,spec.install_bytes,
            preinstalled=result['preinstalled'])
    # A mutable public URL cannot masquerade as an immutable Dataset version.
    with pytest.raises(ValueError,match='preinstalled_requires_versioned_dataset'):
        plan(working,archive,result,url='https://public.example/archive')


def test_build_refuses_dependency_symlink_and_gpu_wheel(bundle,tmp_path):
    site,_,_=bundle
    (site/'rkbfixture.py').unlink()
    (site/'rkbfixture.py').symlink_to('/etc/hosts')
    with pytest.raises(ValueError,match='preinstalled_unsafe_file'):
        pre.build_archive(site,{'rkbfixture':'1.2.3'},tmp_path/'bad.tar.gz',
            artifact_name='deps/x.tar.gz')
    with pytest.raises(ValueError,match='preinstalled_cpu_or_path'):
        pre.build_archive(site,{'torch':'2.11.0'},tmp_path/'bad2.tar.gz',
            artifact_name='deps/x.tar.gz')
