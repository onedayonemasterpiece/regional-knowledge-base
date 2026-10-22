from contextlib import contextmanager
from types import SimpleNamespace as NS

import pytest

from regional_knowledge.kaggle_dataset_publisher import (
    DatasetPublicationError, DatasetUpload, KaggleDatasetPublisher)


class FakeApi:
    def __init__(self):
        self.version = None
        self.description = ''
        self.private = True
        self.status = 'ready'
        self.files = []
        self.mutations = []
        self.lost_ack = False
        self.datasets = NS(dataset_api_client=self)
        self.blobs = NS(blob_api_client=self)

    @contextmanager
    def build_kaggle_client(self):
        yield self

    def get_dataset(self, request):
        if self.version is None:
            exc = RuntimeError('not found')
            exc.status_code = 404
            raise exc
        return NS(current_version_number=self.version, is_private=self.private)

    def get_dataset_metadata(self, request):
        return NS(info=NS(description=self.description))

    def dataset_status(self, ref):
        return self.status

    def dataset_list_files(self, ref, **kwargs):
        assert ref.endswith('/' + str(self.version))
        return NS(files=self.files, next_page_token=None)

    def start_blob_upload(self, request):
        self.mutations.append('reserve')
        return NS(token='private-token', create_url='https://storage.googleapis.com/blob?secret=private')

    def create_dataset(self, request):
        assert request.is_private is True
        self.version = 1
        self.description = request.description
        self.mutations.append('create')
        if self.lost_ack:
            raise TimeoutError('lost acknowledgment')

    def create_dataset_version(self, request):
        assert request.body.delete_old_versions is False
        self.version += 1
        self.description = request.body.description
        self.mutations.append('version')


def setup():
    api = FakeApi()
    publisher = KaggleDatasetPublisher(api, provider_ref='owner/rkb-pg', owner='owner')
    upload = publisher.reserve(name='base.tar', size=123, sha256='a' * 64, now=100)
    api.files = [NS(name='base.tar', total_bytes=123)]
    return api, publisher, (upload,)


def test_direct_grant_then_lost_create_ack_reconciles_without_duplicate():
    api, p, files = setup()
    api.lost_ack = True
    assert p.finalize(files=files, operation='checkpoint-7', previous_version=None) == 1
    assert p.finalize(files=files, operation='checkpoint-7', previous_version=None) == 1
    assert api.mutations == ['reserve', 'create']
    assert 'private-token' not in repr(files[0]) and 'secret=' not in repr(files[0])


def test_new_version_keeps_previous_and_requires_exact_package():
    api, p, files = setup()
    p.finalize(files=files, operation='checkpoint-7', previous_version=None)
    assert p.finalize(files=files, operation='checkpoint-8', previous_version=1) == 2
    api.files[0].total_bytes = 122
    assert not p.reconcile(files=files, operation='checkpoint-8', expected_version=2)
    with pytest.raises(DatasetPublicationError, match='predecessor_changed'):
        p.finalize(files=files, operation='checkpoint-8', previous_version=1)
    assert api.mutations == ['reserve', 'create', 'version']


def test_provider_ready_alone_and_wrong_operation_do_not_prove_publication():
    api, p, files = setup()
    p.finalize(files=files, operation='checkpoint-7', previous_version=None)
    assert not p.reconcile(files=files, operation='another-command', expected_version=1)
    api.status = 'processing'
    assert not p.reconcile(files=files, operation='checkpoint-7', expected_version=1)
    api.private = False
    with pytest.raises(DatasetPublicationError, match='private_version_unproven'):
        p.reconcile(files=files, operation='checkpoint-7', expected_version=1)


def test_no_effect_for_foreign_owner_or_invalid_blob():
    api = FakeApi()
    with pytest.raises(ValueError):
        KaggleDatasetPublisher(api, provider_ref='foreign/rkb-pg', owner='owner')
    p = KaggleDatasetPublisher(api, provider_ref='owner/rkb-pg', owner='owner')
    with pytest.raises(DatasetPublicationError):
        p.reserve(name='../base.tar', size=1, sha256='a' * 64, now=100)
    assert not api.mutations
    with pytest.raises(DatasetPublicationError):
        DatasetUpload('base.tar', 1, 'a' * 64, 'token', 'https://attacker.example/upload')


def test_real_kaggle_json_upload_origin_is_supported():
    value=DatasetUpload('pg.blob',56_422_400,'a'*64,'opaque-token',
        'https://www.googleapis.com/upload/storage/v1/b/owned/o?upload_id=opaque')
    assert value.size==56_422_400


def test_handler_link_requires_owned_private_exact_handler_version(monkeypatch):
    from dataclasses import replace
    from regional_knowledge.kaggle_dataset_base import DatasetFile
    api=FakeApi();api.version=7
    p=KaggleDatasetPublisher(api,provider_ref='owner/rkb-read-handler-artifacts',owner='owner')
    file=DatasetFile('handler-'+('a'*64)+'.blob',123,'a'*64);calls=[]
    def link(self,file,*,version):
        calls.append((file,version));return 'native-only-url','native-etag'
    monkeypatch.setattr(KaggleDatasetPublisher,'_native_download_link',link)
    assert p.handler_download_link(file,version=7)==('native-only-url','native-etag')
    with pytest.raises(DatasetPublicationError,match='unavailable'):
        p.handler_download_link(file,version=8)
    api.private=False
    with pytest.raises(DatasetPublicationError,match='private_version_unproven'):
        p.handler_download_link(file,version=7)
    for bad in (replace(file,name='sql-'+('a'*64)+'.blob'),replace(file,size=512*1024+1)):
        with pytest.raises(DatasetPublicationError,match='scope'):
            p.handler_download_link(bad,version=7)
    primary=KaggleDatasetPublisher(api,provider_ref='owner/rkb-pg-checkpoints',owner='owner')
    with pytest.raises(DatasetPublicationError,match='scope'):
        primary.handler_download_link(file,version=7)
    assert calls==[(file,7)] and api.mutations==[]


def test_403_requires_independent_owned_inventory_absence():
    api, p, files = setup()
    def denied(request):
        exc = RuntimeError('denied')
        exc.status_code = 403
        raise exc
    api.get_dataset = denied
    api.dataset_list_with_response = lambda **kwargs: NS(datasets=[], next_page_token=None)
    assert p.observe() is None
    api.dataset_list_with_response = lambda **kwargs: NS(datasets=[NS(ref=p.ref)], next_page_token=None)
    with pytest.raises(DatasetPublicationError, match='metadata_denied'):
        p.observe()
    assert api.mutations == ['reserve']
