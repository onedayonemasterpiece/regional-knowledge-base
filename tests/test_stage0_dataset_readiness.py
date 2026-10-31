"""Existing publisher/client classes, synthetic provider responses, no effects."""
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_stage0 import Stage0Download,Stage0Spec
from regional_knowledge.kaggle_stage0_dataset import probe_stage0_datasets


class Api:
    def __init__(self):
        self.versions=[7,7];self.statuses=['ready','ready'];self.calls=[]
        self.pages=[dict(files=[dict(name='native.tar',total_bytes=3)],next_page_token=None)]
        self.private=True
    @contextmanager
    def build_kaggle_client(self):
        yield SimpleNamespace(datasets=SimpleNamespace(dataset_api_client=self))
    def get_dataset(self,request):
        self.calls.append(('get',request.owner_slug,request.dataset_slug))
        return dict(current_version_number=self.versions.pop(0),is_private=self.private)
    def dataset_status(self,ref):
        self.calls.append(('status',ref));return self.statuses.pop(0)
    def dataset_list_files(self,ref,*,page_token,page_size):
        self.calls.append(('files',ref,page_token,page_size));return self.pages.pop(0)


def plan(*,url='kaggle-dataset://fixture-owner/rkb-bootstrap/7/native.tar'):
    return Stage0Spec((Stage0Download('native.tar',url,3,'0'*64),),(),{'fixture':'1.0'},0)


def test_ready_preflight_uses_selected_version_without_mutating_provider():
    api=Api();result=probe_stage0_datasets(api,plan())
    assert result.ready and result.reason=='ready'
    assert result.dataset_count==result.file_count==1
    assert api.calls==[('get','fixture-owner','rkb-bootstrap'),
        ('status','fixture-owner/rkb-bootstrap'),('files','fixture-owner/rkb-bootstrap/7',None,100),
        ('get','fixture-owner','rkb-bootstrap'),('status','fixture-owner/rkb-bootstrap')]


def test_https_only_plan_needs_no_provider_observation():
    api=Api();result=probe_stage0_datasets(api,plan(url='https://fixture.example/native.tar'))
    assert result.ready and result.dataset_count==result.file_count==0 and not api.calls


def test_additive_latest_keeps_selected_version_and_mount_metadata_compatible():
    api=Api();api.versions=[9,9]
    api.pages.append(dict(files=[dict(name='native.tar',total_bytes=3),
        dict(name='native084.tar',total_bytes=4)]))
    assert probe_stage0_datasets(api,plan()).ready
    assert ('files','fixture-owner/rkb-bootstrap/7',None,100) in api.calls
    assert ('files','fixture-owner/rkb-bootstrap/9',None,100) in api.calls


@pytest.mark.parametrize('rows,reason',[
    ([dict(name='native.tar',total_bytes=4)],'dataset_mount_file_metadata_differs'),
    ([dict(name='replacement.tar',total_bytes=3)],'dataset_mount_files_pending'),
])
def test_selected_ready_version_cannot_hide_incompatible_latest_mount(rows,reason):
    api=Api();api.versions=[9];api.pages.append(dict(files=rows))
    result=probe_stage0_datasets(api,plan())
    assert not result.ready and result.reason==reason
    assert len([c for c in api.calls if c[0]=='files'])==2


@pytest.mark.parametrize('versions,statuses,reason',[
    ([None],[],'dataset_version_pending'),([6],[],'dataset_version_pending'),
    ([7],['preparing'],'dataset_warming'),([7,8],['ready'],'dataset_changed_during_probe'),
    ([7,7],['ready','preparing'],'dataset_changed_during_probe'),
])
def test_pending_versions_and_warmup_do_not_become_ready(versions,statuses,reason):
    api=Api();api.versions=versions;api.statuses=statuses
    # Existing observe returns None only on 404; simulate that actual boundary.
    if versions==[None]:
        def not_found(request):raise SimpleProviderError(404)
        api.get_dataset=not_found
    result=probe_stage0_datasets(api,plan())
    assert not result.ready and result.reason==reason


class SimpleProviderError(RuntimeError):
    def __init__(self,status_code):self.status_code=status_code


@pytest.mark.parametrize('page,reason',[
    (dict(files=[],next_page_token=None),'dataset_files_pending'),
    (dict(files=[dict(name='native.tar',total_bytes=4)]),'dataset_file_metadata_differs'),
    (dict(files=[dict(name='native.tar',total_bytes=True)]),'dataset_file_metadata_differs'),
    (dict(files=[dict(name='native.tar',total_bytes=3)]*2),'dataset_file_metadata_differs'),
    (dict(files=[],error_message='provider private detail'),'dataset_files_unavailable'),
])
def test_file_metadata_gates_are_specific_and_sanitized(page,reason):
    api=Api();api.pages=[page]
    result=probe_stage0_datasets(api,plan())
    assert not result.ready and result.reason==reason
    assert 'private' not in repr(result)


def test_required_file_on_later_page_is_found_with_native_pagination():
    api=Api();api.pages.insert(0,dict(files=[dict(name='unrelated',total_bytes=20)],next_page_token='next'))
    assert probe_stage0_datasets(api,plan()).ready
    assert ('files','fixture-owner/rkb-bootstrap/7','next',100) in api.calls


def test_repeated_cursor_has_a_bounded_pending_result():
    api=Api();api.pages=[dict(files=[],next_page_token='same')]*2
    result=probe_stage0_datasets(api,plan())
    assert not result.ready and result.reason=='dataset_file_cursor_invalid'
    assert len([c for c in api.calls if c[0]=='files'])==2


def test_public_input_is_ready_but_saved_publisher_still_requires_private():
    from regional_knowledge.kaggle_dataset_publisher import KaggleDatasetPublisher,DatasetPublicationError
    api=Api();api.private=False
    def owner_only(ref):raise AssertionError('Public input must not call owner-only processing status')
    api.dataset_status=owner_only
    result=probe_stage0_datasets(api,plan())
    assert result.ready and result.reason=='ready'
    api.versions=[7]
    publisher=KaggleDatasetPublisher(api,provider_ref='fixture-owner/rkb-bootstrap',owner='fixture-owner')
    with pytest.raises(DatasetPublicationError,match='dataset_private_version_unproven'):
        publisher.observe()


def test_same_source_name_with_conflicting_pins_is_refused_before_provider_read():
    api=Api();first=plan().downloads[0]
    second=Stage0Download('second.tar',first.url,first.size,'1'*64)
    conflict=Stage0Spec((first,second),(),{'fixture':'1.0'},0)
    with pytest.raises(ValueError,match='stage0_dataset_file_pin_ambiguous'):
        probe_stage0_datasets(api,conflict)
    assert not api.calls
