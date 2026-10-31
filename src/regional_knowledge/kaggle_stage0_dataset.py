"""One read-only controller preflight for versioned Stage0 Dataset inputs.

Uses the existing authenticated client. No sleeps, credentials,
uploads, launch claims or notebook effects. Call from a bounded controller tick
before claiming a launch; an unready result leaves the launch pending.
"""
from __future__ import annotations

from dataclasses import dataclass

from .kaggle_stage0 import Stage0Spec, _dataset_locator


@dataclass(frozen=True)
class Stage0DatasetReadiness:
    ready: bool
    reason: str
    dataset_count: int
    file_count: int


def _field(value,name,default=None):
    return value.get(name,default) if isinstance(value,dict) else getattr(value,name,default)


def _observation(api,owner,slug):
    from kagglesdk.datasets.types.dataset_api_service import ApiGetDatasetRequest
    # Input dependencies may be public. Saved publication keeps its separate
    # private-Dataset invariant in KaggleDatasetPublisher.observe.
    request=ApiGetDatasetRequest();request.owner_slug,request.dataset_slug=owner,slug
    try:
        with api.build_kaggle_client() as client:
            value=client.datasets.dataset_api_client.get_dataset(request)
    except Exception as exc:
        if (getattr(exc,'status_code',None) or getattr(getattr(exc,'response',None),'status_code',None))==404:return None
        raise
    version=_field(value,'current_version_number')
    if type(version) is not int or version<1:raise ValueError('dataset_version_unproven')
    # A public published version is available to readers; the processing
    # status endpoint is owner-only (404 for a readable public dependency).
    return version,_field(value,'is_private') is True


def _file_readiness(api,ref,version,files):
    cursor=None;seen_cursors=set();found=set()
    for _ in range(4):
        page=api.dataset_list_files(f'{ref}/{version}',page_token=cursor,page_size=100)
        if _field(page,'error_message'):return 'dataset_files_unavailable'
        rows=_field(page,'files',[]) or []
        if not isinstance(rows,(list,tuple)) or len(rows)>100:
            return 'dataset_file_listing_invalid'
        for row in rows:
            name=_field(row,'name')
            if type(name) is not str or name not in files:continue
            size=_field(row,'total_bytes')
            if type(size) is not int or size!=files[name][0] or name in found:
                return 'dataset_file_metadata_differs'
            found.add(name)
        if found==files.keys():return None
        cursor=_field(page,'next_page_token')
        if not cursor:return 'dataset_files_pending'
        if type(cursor) is not str or cursor in seen_cursors:
            return 'dataset_file_cursor_invalid'
        seen_cursors.add(cursor)
    return 'dataset_file_listing_bounded'


def probe_stage0_datasets(api,stage0):
    """Check selected and mount-compatible latest metadata; worker checks SHA.

    Notebook save may discard the requested Dataset version suffix. Latest
    must therefore retain the selected filenames/sizes too; an additive new
    version is valid. Neither listing substitutes for native byte verification.
    """
    if type(stage0) is not Stage0Spec:
        raise ValueError('stage0_typed_plan_required')
    required={}
    for item in stage0.downloads:
        locator=_dataset_locator(item.url)
        if locator is None:continue
        owner,slug,version,name=locator
        files=required.setdefault((owner,slug,version),{})
        pin=(item.size,item.sha256)
        if name in files and files[name]!=pin:
            raise ValueError('stage0_dataset_file_pin_ambiguous')
        files[name]=pin
    count=len(required);file_count=sum(map(len,required.values()))
    def result(ready,reason):return Stage0DatasetReadiness(ready,reason,count,file_count)
    for (owner,slug,version),files in sorted(required.items()):
        ref=owner+'/'+slug
        try:
            before=_observation(api,owner,slug)
            if before is None or before[0]<version:
                return result(False,'dataset_version_pending')
            if before[1] and api.dataset_status(ref)!='ready':
                return result(False,'dataset_warming')
            for candidate in dict.fromkeys((version,before[0])):
                reason=_file_readiness(api,ref,candidate,files)
                if reason:
                    if candidate!=version:reason='dataset_mount_'+reason.removeprefix('dataset_')
                    return result(False,reason)
            # A concurrent version transition is a reason to try on a later
            # tick, never to make a launch claim while still warming.
            if _observation(api,owner,slug)!=before or before[1] and api.dataset_status(ref)!='ready':
                return result(False,'dataset_changed_during_probe')
        except Exception:
            # SDK errors may contain signed URLs, private refs or credentials.
            return result(False,'dataset_observation_unavailable')
    return result(True,'ready')
