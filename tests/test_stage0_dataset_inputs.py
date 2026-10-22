"""Real mount/cache I/O with pinned fixture bytes; no provider/network/install."""
from dataclasses import replace
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest

import regional_knowledge.kaggle_stage0 as stage0


@pytest.fixture
def dataset_sandbox(tmp_path,monkeypatch):
    working=tmp_path/'working';working.mkdir()
    inputs=tmp_path/'input';inputs.mkdir()
    mounted=inputs/'rkb-bootstrap';mounted.mkdir()
    monkeypatch.setattr(stage0,'TRUSTED_ROOT',working)
    monkeypatch.setattr(stage0,'TRUSTED_INPUT_ROOT',inputs)
    monkeypatch.setattr(stage0,'_versions',lambda packages:dict(packages))
    requests=[]
    class NoNetwork:
        def open(self,*args,**kwargs):
            requests.append(args)
            raise AssertionError('Dataset input must not fall back to a network request')
    monkeypatch.setattr(stage0.urllib.request,'build_opener',lambda *args:NoNetwork())
    def no_install(*args,**kwargs):raise AssertionError('unexpected installer')
    monkeypatch.setattr(stage0.subprocess,'run',no_install)
    return working,inputs,mounted,requests


def pinned(name,data,source_name=None,version=7):
    return stage0.Stage0Download(name,
        f'kaggle-dataset://fixture-owner/rkb-bootstrap/{version}/{source_name or name}',
        len(data),hashlib.sha256(data).hexdigest())


def plan_and_inventory(working,*items,wheels=()):
    plan=stage0.Stage0Spec(tuple(items),tuple(wheels),{'fixture-package':'1.2.3'},0)
    inventory=dict(root=str(working),entries=[dict(name=i.name,size=i.size,sha256=i.sha256,role='code')
        for i in items],max_total_bytes=sum(i.size for i in items))
    return plan,inventory


def test_dataset_native_and_wheel_copy_then_warm_reuse_without_mount(dataset_sandbox):
    working,inputs,mounted,requests=dataset_sandbox
    native=b'native fixture'*(stage0.CHUNK_BYTES//7)
    wheel=b'pinned wheel fixture'
    (mounted/'native-packages.tar').write_bytes(native);(mounted/'native-packages.tar').chmod(0o400)
    (mounted/'pyaes.whl').write_bytes(wheel);(mounted/'pyaes.whl').chmod(0o400)
    items=(replace(pinned('native-packages.tar',native),executable=True),
        pinned('wheels/pyaes.whl',wheel,source_name='pyaes.whl'))
    plan,inventory=plan_and_inventory(working,*items,wheels=('wheels/pyaes.whl',))
    assert plan.dataset_sources==('fixture-owner/rkb-bootstrap/7',)
    assert stage0.Stage0Spec.from_wire(plan.wire())==plan
    receipt=stage0.prepare_stage0(inventory,plan.wire())
    assert receipt['downloaded_bytes']==0 and receipt['dataset_copied_bytes']==len(native)+len(wheel)
    assert receipt['dataset_copied_files']==2 and receipt['reused_files']==0
    assert (working/'native-packages.tar').read_bytes()==native
    assert (working/'native-packages.tar').stat().st_mode&0o777==0o500
    assert (working/'wheels/pyaes.whl').read_bytes()==wheel
    assert (working/'wheels/pyaes.whl').stat().st_mode&0o777==0o400
    (mounted/'native-packages.tar').unlink();(mounted/'pyaes.whl').unlink();mounted.rmdir();inputs.rmdir()
    reused=stage0.prepare_stage0(inventory,plan.wire())
    assert reused['downloaded_bytes']==reused['dataset_copied_bytes']==0 and reused['reused_files']==2
    assert not requests and not list(working.rglob('.rkb-part-*'))


def test_same_slug_with_wrong_version_bytes_is_rejected_by_content_pin(dataset_sandbox):
    working,_,mounted,requests=dataset_sandbox
    item=pinned('native-packages.tar',b'approved version')
    (mounted/'native-packages.tar').write_bytes(b'previous version');(mounted/'native-packages.tar').chmod(0o400)
    plan,inventory=plan_and_inventory(working,item)
    with pytest.raises(stage0.Stage0Error,match='stage0_download_hash_mismatch'):
        stage0.prepare_stage0(inventory,plan.wire())
    assert not (working/item.name).exists() and not requests
    assert not list(working.rglob('.rkb-part-*'))


@pytest.mark.parametrize('kind',['missing','short','parent_link','leaf_link','fifo','writable'])
def test_unavailable_or_unsafe_dataset_never_uses_network_or_publishes(dataset_sandbox,kind):
    working,_,mounted,requests=dataset_sandbox
    data=b'pinned bytes'
    external=working.parent/'external';external.mkdir();(external/'asset').write_bytes(data)
    item=pinned('native/asset',data)
    if kind=='parent_link':(mounted/'native').symlink_to(external,target_is_directory=True)
    else:
        parent=mounted/'native';parent.mkdir()
        if kind=='leaf_link':(parent/'asset').symlink_to(external/'asset')
        elif kind=='fifo':stage0.os.mkfifo(parent/'asset')
        elif kind=='short':(parent/'asset').write_bytes(b'short')
        elif kind=='writable':
            (parent/'asset').write_bytes(data);(parent/'asset').chmod(0o666)
    plan,inventory=plan_and_inventory(working,item)
    with pytest.raises(stage0.Stage0Error):stage0.prepare_stage0(inventory,plan.wire())
    assert not requests and not (working/item.name).exists()
    assert (external/'asset').read_bytes()==data and not list(working.rglob('.rkb-part-*'))


def test_input_root_symlink_is_not_trusted(dataset_sandbox,monkeypatch):
    working,inputs,mounted,requests=dataset_sandbox
    data=b'pin';(mounted/'asset').write_bytes(data)
    alias=inputs.parent/'input-alias';alias.symlink_to(inputs,target_is_directory=True)
    monkeypatch.setattr(stage0,'TRUSTED_INPUT_ROOT',alias)
    plan,inventory=plan_and_inventory(working,pinned('asset',data))
    with pytest.raises(stage0.Stage0Error,match='stage0_dataset_mount_invalid'):
        stage0.prepare_stage0(inventory,plan.wire())
    assert not requests and not (working/'asset').exists()


def test_dataset_copy_reserves_full_missing_bytes_before_creating_output(dataset_sandbox,monkeypatch):
    working,_,mounted,requests=dataset_sandbox
    data=b'pin';(mounted/'asset').write_bytes(data)
    plan,inventory=plan_and_inventory(working,pinned('nested/asset',data,source_name='asset'))
    monkeypatch.setattr(stage0.shutil,'disk_usage',lambda path:SimpleNamespace(free=len(data)-1))
    with pytest.raises(stage0.Stage0Error,match='stage0_insufficient_disk'):
        stage0.prepare_stage0(inventory,plan.wire())
    assert not requests and not (working/'nested').exists()


def test_existing_wrong_cache_is_preserved_and_never_overwritten(dataset_sandbox):
    working,_,mounted,requests=dataset_sandbox
    data=b'pin';(mounted/'asset').write_bytes(data);(working/'asset').write_bytes(b'old')
    plan,inventory=plan_and_inventory(working,pinned('asset',data))
    with pytest.raises(stage0.Stage0Error,match='stage0_existing_file_mismatch'):
        stage0.prepare_stage0(inventory,plan.wire())
    assert (working/'asset').read_bytes()==b'old' and not requests


@pytest.mark.parametrize('url',[
    'kaggle-dataset://fixture-owner/rkb-bootstrap/0/asset',
    'kaggle-dataset://fixture-owner/rkb-bootstrap/01/asset',
    'kaggle-dataset://fixture-owner/rkb-bootstrap/latest/asset',
    'kaggle-dataset://fixture-owner/rkb-bootstrap/7/../secret',
    'kaggle-dataset://fixture-owner/rkb-bootstrap/7/%2e%2e/secret',
    'kaggle-dataset://fixture-owner/rkb-bootstrap/7/nested//asset',
    'kaggle-dataset://fixture-owner/rkb-bootstrap/7/asset?token=secret',
    'kaggle-dataset://fixture-owner/rkb-bootstrap/7/asset#fragment',
    'kaggle-dataset://user:secret@fixture-owner/rkb-bootstrap/7/asset',
    'file:///home/dev/private',
])
def test_dataset_source_is_versioned_and_cannot_escape_mount(url):
    with pytest.raises(ValueError):replace(pinned('asset',b'pin'),url=url)


@pytest.mark.parametrize('other',[
    'kaggle-dataset://fixture-owner/rkb-bootstrap/8/second',
    'kaggle-dataset://different-owner/rkb-bootstrap/7/second',
])
def test_ambiguous_mount_versions_and_owners_are_not_attachable(other):
    first=pinned('first',b'pin');second=replace(pinned('second',b'pin'),url=other)
    with pytest.raises(ValueError,match='stage0_dataset_mount_ambiguous'):
        stage0.Stage0Spec((first,second),(),{'fixture-package':'1.2.3'},0)
