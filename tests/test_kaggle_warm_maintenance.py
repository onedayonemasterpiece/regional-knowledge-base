"""Inline reviewed Python uses the same warm slot and retained resources."""
import hashlib
from pathlib import Path
import pytest

from regional_knowledge.kaggle_warm_maintenance import (
    WarmHandlerMaintenance, WarmHandlerPatchApproval, WarmMaintenanceRejected,
    inline_handler_bytes,
)
from regional_knowledge.kaggle_warm_read import ReadHandlerSlot
from test_kaggle_warm_read import old, context, staged_module, good_probe, good_stage


def args(source, **extra):
    return dict(handler_source=source,handler_sha256=hashlib.sha256(source.encode()).hexdigest(),
        reviewed_commit='b'*40,expected_loaded_sha256='a'*64,expected_generation=0,**extra)


def owner_for(tmp_path, *, self_test=good_stage, probe=good_probe, current=lambda _:None):
    root=tmp_path/'private';root.mkdir(mode=0o700)
    slot=ReadHandlerSlot(old,loaded_sha256='a'*64,compatibility=context())
    async def no_download(*a,**k):raise AssertionError('inline source must not use storage')
    owner=WarmHandlerMaintenance(slot=slot,
        workspace=lambda op,k:root/(op+'-'+k),constructor={'marker':'new'},
        self_test=self_test,probe=probe,current=current)
    return owner,slot,root


@pytest.mark.asyncio
async def test_inline_stage_activate_and_rollback_without_storage_or_new_resources(tmp_path):
    _,path,digest=staged_module(tmp_path)
    owner,slot,root=owner_for(tmp_path)
    staged=await owner('read_handler_stage',args(path.read_text()),'stage',{})
    assert staged['staged_sha256']==digest and staged['loaded_sha256']=='a'*64
    assert (root/'stage-read_handler'/'handler.py').stat().st_mode&0o777==0o600
    assert (await slot({},encode=None))['version']=='old'
    receipt=await owner('read_handler_activate',dict(staged_sha256=digest,
        expected_loaded_sha256='a'*64,expected_generation=0),'activate',{})
    assert receipt['loaded_sha256']==digest and receipt['generation']==1
    assert (await slot({},encode=None))['version']=='new'
    receipt=await owner('read_handler_rollback',dict(expected_loaded_sha256=digest,
        expected_generation=1),'rollback',{})
    assert receipt['loaded_sha256']=='a'*64 and receipt['generation']==2


@pytest.mark.parametrize('replacement,reason',[
    ({'handler_sha256':'f'*64},'bytes_mismatch'),
    ({'reviewed_commit':'bad'},'approval_invalid'),
    ({'handler_source':'x'*(512*1024+1)},'artifact_budget'),
    ({'handler_source':'\ud800'},'approval_invalid'),
    ({'module_path':'/untrusted.py'},'arguments'),
    ({'expected_generation':3},'stale_operation'),
])
@pytest.mark.asyncio
async def test_invalid_inline_candidate_never_executes(tmp_path,replacement,reason):
    owner,slot,root=owner_for(tmp_path)
    wire=args("raise AssertionError('must not execute')")|replacement
    with pytest.raises(WarmMaintenanceRejected,match=reason):
        await owner('read_handler_stage',wire,'bad',{})
    assert not list(root.iterdir()) and slot.receipt()['loaded_sha256']=='a'*64


@pytest.mark.asyncio
async def test_inline_source_rejects_legacy_ticket(tmp_path):
    owner,slot,root=owner_for(tmp_path)
    with pytest.raises(WarmMaintenanceRejected,match='arguments'):
        await owner('read_handler_stage',args('pass'),'ticket',{'download_ticket':{}})
    assert not list(root.iterdir())


@pytest.mark.asyncio
async def test_revoked_attachment_during_selftest_cleans_candidate(tmp_path):
    _,path,_=staged_module(tmp_path);allowed=[True]
    def current(_):
        if not allowed[0]:raise PermissionError('revoked')
    async def selftest(candidate):
        await good_stage(candidate);allowed[0]=False
    owner,slot,root=owner_for(tmp_path,self_test=selftest,current=current)
    with pytest.raises(PermissionError,match='revoked'):
        await owner('read_handler_stage',args(path.read_text()),'revoked',{})
    assert not list(root.iterdir()) and slot.receipt()['staged_sha256'] is None
    assert (await slot({},encode=None))['version']=='old'


@pytest.mark.asyncio
async def test_revoked_attachment_during_activation_retains_old_handler(tmp_path):
    _,path,digest=staged_module(tmp_path);allowed=[True]
    def current(_):
        if not allowed[0]:raise PermissionError('revoked')
    async def probe(candidate):
        await good_probe(candidate);allowed[0]=False
    owner,slot,_=owner_for(tmp_path,probe=probe,current=current)
    await owner('read_handler_stage',args(path.read_text()),'stage',{})
    with pytest.raises(PermissionError,match='revoked'):
        await owner('read_handler_activate',dict(staged_sha256=digest,
            expected_loaded_sha256='a'*64,expected_generation=0),'activate',{})
    assert slot.receipt()['loaded_sha256']=='a'*64
    assert slot.receipt()['generation']==0 and not slot.receipt()['switching']


def test_inline_budget_is_utf8_bytes_and_digest_is_exact():
    source='я'*(256*1024)
    assert len(inline_handler_bytes(source,hashlib.sha256(source.encode()).hexdigest(),'c'*40))==512*1024
    with pytest.raises(WarmMaintenanceRejected,match='budget'):
        inline_handler_bytes(source+'я','a'*64,'c'*40)


def test_schema_bundle_approval_remains_compatible():
    from regional_knowledge.kaggle_pg_snapshot_native import BlobRef
    blob=BlobRef('approved/snapshots/blobs/source',1024,'a'*64,'"etag"')
    approval=WarmHandlerPatchApproval(blob,'c'*40)
    assert WarmHandlerPatchApproval.from_wire(approval.wire())==approval
    with pytest.raises(WarmMaintenanceRejected):
        WarmHandlerPatchApproval.from_wire(dict(approval.wire(),path='/untrusted.py'))
