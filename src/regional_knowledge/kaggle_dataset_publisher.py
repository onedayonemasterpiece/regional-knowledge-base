"""Controller metadata operations for direct native PG → Kaggle Dataset upload.

Uses the same official SDK blob/create/version calls as my-data-hub's brokered
checkpoint adapter. Archive bytes and provider credentials never cross WSS.
The caller persists the operation and opaque blob grant in its private queue
before returning a ticket, and persists publication intent before finalize().
No mutation is retried here; reconcile() is the read-only recovery path.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import re
import os
from pathlib import Path
import shlex

from .native_snapshot_transfer import origin


class DatasetPublicationError(RuntimeError):
    pass


def authenticated_service_api():
    """Reuse the controller's existing service credential source on DevCoveer."""
    for line in Path('/home/dev/.env').read_text().splitlines():
        if line.startswith(('KAGGLE_USERNAME=', 'KAGGLE_KEY=')):
            key, value = line.split('=', 1)
            os.environ[key] = shlex.split(value)[0]
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()
    return api


def _field(value, name, default=None):
    return value.get(name, default) if isinstance(value, dict) else getattr(value, name, default)


@dataclass(frozen=True)
class DatasetUpload:
    name: str
    size: int
    sha256: str
    token: str = field(repr=False)
    url: str = field(repr=False)

    def __post_init__(self):
        if (not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,199}', self.name)
                or type(self.size) is not int or not 1 <= self.size <= 8 * 1024**3
                or not re.fullmatch(r'[0-9a-f]{64}', self.sha256)
                or not self.token or len(self.token) > 8192):
            raise DatasetPublicationError('dataset_blob_identity_invalid')
        # Grants come from the authenticated Kaggle SDK, never a caller URL.
        if origin(self.url) not in {'https://storage.googleapis.com','https://www.googleapis.com'}:
            raise DatasetPublicationError('dataset_blob_origin_invalid')

    @property
    def description(self):
        return 'rkb.sha256:' + self.sha256


class KaggleDatasetPublisher:
    def __init__(self, api, *, provider_ref, owner, title='RKB PostgreSQL checkpoints'):
        if (not re.fullmatch(r'[A-Za-z0-9_-]+/[A-Za-z0-9_-]+', provider_ref)
                or provider_ref.split('/')[0] != owner):
            raise ValueError('owned Kaggle Dataset required')
        self.api, self.ref, self.title = api, provider_ref, title
        self.owner, self.slug = provider_ref.split('/')

    def reserve(self, *, name, size, sha256, now):
        from kagglesdk.blobs.types.blob_api_service import ApiBlobType, ApiStartBlobUploadRequest
        # Validate metadata before the provider effect.
        DatasetUpload(name, size, sha256, 'validation', 'https://storage.googleapis.com/validation')
        request = ApiStartBlobUploadRequest()
        request.type = ApiBlobType.DATASET
        request.name, request.content_length = name, size
        request.content_type = 'application/octet-stream'
        request.last_modified_epoch_seconds = int(now)
        try:
            with self.api.build_kaggle_client() as client:
                response = client.blobs.blob_api_client.start_blob_upload(request)
            return DatasetUpload(name, size, sha256, _field(response, 'token'), _field(response, 'create_url'))
        except Exception:
            # Provider exceptions may contain signed URLs or opaque tokens.
            raise DatasetPublicationError('dataset_blob_reservation_unknown') from None

    @staticmethod
    def _description(files, operation):
        if (type(operation) is not str or not 1 <= len(operation.encode()) <= 256
                or not files or len(files) > 16
                or any(type(f) is not DatasetUpload for f in files)
                or len({f.name for f in files}) != len(files)):
            raise DatasetPublicationError('dataset_publication_identity_invalid')
        return json.dumps(dict(codec='rkb.pg-dataset.v1', operation_sha256=hashlib.sha256(operation.encode()).hexdigest(),
            files=sorted([dict(name=f.name, size=f.size, sha256=f.sha256) for f in files], key=lambda f:f['name'])),
            sort_keys=True, separators=(',', ':'))

    def observe(self):
        """Owned/private current metadata, or None only for an explicit 404."""
        from kagglesdk.datasets.types.dataset_api_service import ApiGetDatasetRequest
        request = ApiGetDatasetRequest()
        request.owner_slug, request.dataset_slug = self.owner, self.slug
        try:
            with self.api.build_kaggle_client() as client:
                value = client.datasets.dataset_api_client.get_dataset(request)
        except Exception as exc:
            status = getattr(exc, 'status_code', None) or getattr(getattr(exc, 'response', None), 'status_code', None)
            if status == 404:
                return None
            if status == 403:
                # Kaggle returns 403 for a new private slug too. Prove absence
                # with authenticated own inventory; never treat denial alone
                # as permission to overwrite/create an existing Dataset.
                cursor, seen = None, set()
                for _ in range(20):
                    try:
                        page = self.api.dataset_list_with_response(mine=True, search=self.slug,
                            sort_by='updated', page_size=100, page_token=cursor)
                    except Exception:
                        raise DatasetPublicationError('dataset_inventory_unavailable') from None
                    if any(_field(row, 'ref') == self.ref for row in _field(page, 'datasets', []) or []):
                        raise DatasetPublicationError('dataset_metadata_denied') from None
                    cursor = _field(page, 'next_page_token')
                    if not cursor:
                        return None
                    if cursor in seen:
                        break
                    seen.add(cursor)
            raise DatasetPublicationError('dataset_metadata_unavailable') from None
        version = _field(value, 'current_version_number')
        if (_field(value, 'is_private') is not True or type(version) is not int or version < 1):
            raise DatasetPublicationError('dataset_private_version_unproven')
        return version

    def version_created_at(self, version):
        """Provider timestamp of the exact verified Saved version, not latest."""
        from datetime import datetime, timezone
        from kagglesdk.datasets.types.dataset_api_service import ApiGetDatasetRequest
        if type(version) is not int or version < 1:
            raise ValueError('exact_saved_dataset_version_required')
        request = ApiGetDatasetRequest()
        request.owner_slug, request.dataset_slug = self.owner, self.slug
        try:
            with self.api.build_kaggle_client() as client:
                value = client.datasets.dataset_api_client.get_dataset(request)
            matches = [v for v in _field(value, 'versions', []) or []
                       if _field(v, 'version_number') == version]
            if len(matches) != 1:
                raise ValueError
            created = _field(matches[0], 'creation_date')
            if not isinstance(created, datetime):
                raise ValueError
            # The installed native SDK returns naive UTC datetimes.
            return created.replace(tzinfo=timezone.utc).timestamp() if created.tzinfo is None else created.timestamp()
        except Exception:
            raise DatasetPublicationError('dataset_saved_version_time_unavailable') from None

    def read_control(self, file, *, version):
        """Read only the fixed bounded checkpoint metadata; never a PG archive."""
        from kagglesdk.datasets.types.dataset_api_service import ApiDownloadDatasetRequest
        if (file.name not in {'base-manifest.json','source-control.json',
                'journal-control.json'} or file.size>64*1024**2
                or type(version) is not int or version<1):
            raise DatasetPublicationError('dataset_control_read_scope')
        request=ApiDownloadDatasetRequest()
        request.owner_slug,request.dataset_slug=self.owner,self.slug
        request.file_name,request.dataset_version_number,request.raw=file.name,version,True
        response=None
        try:
            with self.api.build_kaggle_client() as client:
                response=client.datasets.dataset_api_client.download_dataset(request)
                if response.status_code!=200:
                    raise DatasetPublicationError('dataset_control_read_unavailable')
                data=bytearray()
                for chunk in response.iter_content(chunk_size=65536):
                    data.extend(chunk)
                    if len(data)>file.size:
                        raise DatasetPublicationError('dataset_control_read_bytes_changed')
            if len(data)!=file.size or hashlib.sha256(data).hexdigest()!=file.sha256:
                raise DatasetPublicationError('dataset_control_read_bytes_changed')
            return bytes(data)
        except DatasetPublicationError:
            raise
        except Exception:
            raise DatasetPublicationError('dataset_control_read_unavailable') from None
        finally:
            if response is not None:response.close()

    def snapshot_download_link(self,file,*,version):
        """Resolve a native-only download capability; consume no archive body."""
        from kagglesdk.datasets.types.dataset_api_service import ApiDownloadDatasetRequest
        if (not file.name.startswith('pg-') or not file.name.endswith('.blob')
                or type(version) is not int or version<1):
            raise DatasetPublicationError('dataset_archive_read_scope')
        request=ApiDownloadDatasetRequest()
        request.owner_slug,request.dataset_slug=self.owner,self.slug
        request.file_name,request.dataset_version_number,request.raw=file.name,version,True
        response=None
        try:
            with self.api.build_kaggle_client() as client:
                # Official SDK uses stream=True for HttpRedirect/FileDownload.
                response=client.datasets.dataset_api_client.download_dataset(request)
                if (response.status_code!=200 or int(response.headers.get('Content-Length','-1'))!=file.size
                        or origin(response.url)!='https://storage.googleapis.com'
                        or not response.headers.get('ETag')):
                    raise DatasetPublicationError('dataset_archive_link_unverified')
                return response.url,response.headers['ETag']
        except DatasetPublicationError:raise
        except Exception:raise DatasetPublicationError('dataset_archive_link_unavailable') from None
        finally:
            if response is not None:response.close()

    def reconcile(self, *, files, operation, expected_version):
        description = self._description(files, operation)
        if self.observe() != expected_version:
            return False
        from kagglesdk.datasets.types.dataset_api_service import ApiGetDatasetMetadataRequest
        request = ApiGetDatasetMetadataRequest()
        request.owner_slug, request.dataset_slug = self.owner, self.slug
        with self.api.build_kaggle_client() as client:
            response = client.datasets.dataset_api_client.get_dataset_metadata(request)
        if _field(_field(response, 'info'), 'description') != description:
            return False
        # Status and version are separate provider observations.
        if self.api.dataset_status(self.ref) != 'ready':
            return False
        observed, cursor, seen = [], None, set()
        for _ in range(10):
            response = self.api.dataset_list_files(f'{self.ref}/{expected_version}', page_token=cursor, page_size=100)
            if _field(response, 'error_message'):
                raise DatasetPublicationError('dataset_file_metadata_unavailable')
            for item in _field(response, 'files', []) or []:
                observed.append((_field(item, 'name'), _field(item, 'total_bytes')))
            cursor = _field(response, 'next_page_token')
            if not cursor:
                break
            if cursor in seen:
                raise DatasetPublicationError('dataset_file_cursor_repeated')
            seen.add(cursor)
        else:
            raise DatasetPublicationError('dataset_file_listing_unbounded')
        return (sorted(observed) == sorted((f.name, f.size) for f in files)
                and self.observe() == expected_version)

    def finalize(self, *, files, operation, previous_version):
        from kagglesdk.datasets.types.dataset_api_service import (
            ApiCreateDatasetRequest, ApiCreateDatasetVersionRequest,
            ApiCreateDatasetVersionRequestBody, ApiDatasetNewFile)
        description = self._description(files, operation)
        if previous_version is not None and (type(previous_version) is not int or previous_version < 1):
            raise DatasetPublicationError('dataset_previous_version_invalid')
        expected = 1 if previous_version is None else previous_version + 1
        if self.reconcile(files=files, operation=operation, expected_version=expected):
            return expected
        if self.observe() != previous_version:
            raise DatasetPublicationError('dataset_publication_predecessor_changed')
        uploaded = []
        for file in files:
            item = ApiDatasetNewFile()
            item.token, item.description = file.token, file.description
            uploaded.append(item)
        try:
            with self.api.build_kaggle_client() as client:
                if previous_version is None:
                    request = ApiCreateDatasetRequest()
                    request.owner_slug, request.slug = self.owner, self.slug
                    request.title, request.license_name = self.title, 'CC0-1.0'
                    request.is_private, request.description, request.files = True, description, uploaded
                    client.datasets.dataset_api_client.create_dataset(request)
                else:
                    body = ApiCreateDatasetVersionRequestBody()
                    body.version_notes, body.description, body.files = operation, description, uploaded
                    body.delete_old_versions = False
                    request = ApiCreateDatasetVersionRequest()
                    request.owner_slug, request.dataset_slug, request.body = self.owner, self.slug, body
                    client.datasets.dataset_api_client.create_dataset_version(request)
        except Exception:
            # Read back even after a lost provider response; never resubmit here.
            pass
        if self.reconcile(files=files, operation=operation, expected_version=expected):
            return expected
        raise DatasetPublicationError('dataset_publication_pending_readback')
