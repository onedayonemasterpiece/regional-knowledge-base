"""Reviewed immutable warm patch staged through existing signed transfer, never WSS bytes."""
import hashlib
from pathlib import Path
import pytest

from regional_knowledge.kaggle_pg_snapshot_native import BlobRef
from regional_knowledge.kaggle_warm_maintenance import (
    WarmHandlerMaintenance, WarmHandlerPatchApproval, WarmMaintenanceRejected,
)
from regional_knowledge.kaggle_warm_read import ReadHandlerSlot
from test_kaggle_warm_read import old, context, staged_module, good_probe, good_stage


def proof(path, *, size=None, sha=None):
    content=path.read_bytes()
    return BlobRef('rkb/snapshots/blobs/approved/'+hashlib.sha256(content).hexdigest(),
        size if size is not None else len(content),
        sha if sha is not None else hashlib.sha256(content).hexdigest(),
        '"trusted-immutable-etag"')


@pytest.mark.asyncio
async def test_reviewed_artifact_can_stage_apply_readback_and_rollback_without_new_resources(tmp_path):
    trusted,path,digest=staged_module(tmp_path)
    blob=proof(path)
    transfer=[];work_root=tmp_path/'private-transfer';work_root.mkdir(mode=0o700)
    def workspace(operation,kind):
        result=work_root/(hashlib.sha256(operation.encode()).hexdigest()+'-'+kind)
        if kind=='read_handler':
            return result
        return result
    async def download(item,operation,*,ticket,transfer_operation):
        transfer.append((item,operation,ticket,transfer_operation))
        assert item==blob and transfer_operation==operation
        target=workspace(operation,'download')
        target.write_bytes(path.read_bytes());target.chmod(0o600)
        return target
    slot=ReadHandlerSlot(old,loaded_sha256='a'*64,compatibility=context())
    owner=WarmHandlerMaintenance(slot=slot,download=download,workspace=workspace,
        constructor={'marker':'new'},self_test=good_stage,probe=good_probe,
        current=lambda frame:None)
    approved=WarmHandlerPatchApproval(blob=blob,reviewed_commit='b'*40)
    args=dict(artifact=approved.wire(),expected_loaded_sha256='a'*64,expected_generation=0)
    stage=await owner('read_handler_stage',args,'stage-once',
        {'download_ticket':{'trusted':'fixture'}})
    assert stage['loaded_sha256']=='a'*64 and stage['staged_sha256']==digest
    assert stage['generation']==0 and len(transfer)==1
    with pytest.raises(WarmMaintenanceRejected,match='arguments'):
        await owner('read_handler_stage',dict(args,module_path='/tmp/attacker.py'),'evil',
            {'download_ticket':{'trusted':'fixture'}})
    applied=await owner('read_handler_activate',dict(staged_sha256=digest,
        expected_loaded_sha256='a'*64,expected_generation=0),'activate',
        {})
    assert applied['loaded_sha256']==digest and applied['generation']==1
    assert (await slot({'hello':True},encode=None))['version']=='new'
    assert len(transfer)==1
    rolled=await owner('read_handler_rollback',
        dict(expected_loaded_sha256=digest,expected_generation=1),'rollback',{})
    assert rolled['loaded_sha256']=='a'*64 and rolled['generation']==2
    assert (await slot({'hello':True},encode=None))['version']=='old'


@pytest.mark.asyncio
async def test_warm_patch_fails_closed_before_download_on_bad_scope(tmp_path):
    _,path,digest=staged_module(tmp_path)
    transfer=[]
    async def forbidden(*args,**kwargs):
        transfer.append(True)
        raise AssertionError('must not download')
    slot=ReadHandlerSlot(old,loaded_sha256='a'*64,compatibility=context())
    owner=WarmHandlerMaintenance(slot=slot,download=forbidden,
        workspace=lambda op,k:tmp_path,constructor={'marker':'new'},
        self_test=good_stage,probe=good_probe,current=lambda frame:None)
    ref=proof(path)
    approval=WarmHandlerPatchApproval(ref,'b'*40)
    args=dict(artifact=approval.wire(),expected_loaded_sha256='a'*64,expected_generation=0)
    with pytest.raises(WarmMaintenanceRejected,match='transfer'):
        await owner('read_handler_stage',args,'no-ticket',{})
    with pytest.raises(WarmMaintenanceRejected,match='stale'):
        await owner('read_handler_stage',dict(args,expected_generation=99),'stale',
            {'download_ticket':{}})
    oversized=proof(path,size=512*1024+1)
    with pytest.raises(WarmMaintenanceRejected,match='artifact'):
        await owner('read_handler_stage',
            dict(args,artifact=WarmHandlerPatchApproval(oversized,'b'*40).wire()),
            'oversize',{'download_ticket':{}})
    with pytest.raises(WarmMaintenanceRejected,match='no_verified_stage'):
        await owner('read_handler_activate',dict(staged_sha256=digest,
            expected_loaded_sha256='a'*64,expected_generation=0),'not-staged',{})
    assert transfer==[] and slot.receipt()['loaded_sha256']=='a'*64




@pytest.mark.asyncio
async def test_lost_wss_attachment_after_download_cannot_stage_and_cleans_bytes(tmp_path):
    _,source,digest=staged_module(tmp_path)
    private=tmp_path/'private';private.mkdir(mode=0o700)
    permitted=[True]; downloaded=[]
    def current(frame):
        if not permitted[0]:raise PermissionError('synthetic detached WSS generation')
    def workspace(op,kind):return private/(kind+'-'+op)
    async def download(blob,op,*,ticket,transfer_operation):
        file=workspace(op,'download')
        file.write_bytes(source.read_bytes());file.chmod(0o600)
        downloaded.append(file)
        permitted[0]=False
        return file
    slot=ReadHandlerSlot(old,loaded_sha256='a'*64,compatibility=context())
    owner=WarmHandlerMaintenance(slot=slot,download=download,workspace=workspace,
        constructor={'marker':'new'},self_test=good_stage,probe=good_probe,current=current)
    approval=WarmHandlerPatchApproval(proof(source),'b'*40)
    with pytest.raises(PermissionError,match='detached'):
        await owner('read_handler_stage',dict(artifact=approval.wire(),
            expected_loaded_sha256='a'*64,expected_generation=0),'one',
            {'download_ticket':{'signed':'fixture'}})
    assert not downloaded[0].exists()
    assert slot.receipt()['staged_sha256'] is None
    assert slot.receipt()['loaded_sha256']=='a'*64


@pytest.mark.asyncio
async def test_attachment_revoked_during_activation_probe_rolls_back_to_old_handler(tmp_path):
    root,path,digest=staged_module(tmp_path)
    allowed=[True]
    def current(frame):
        if not allowed[0]:raise PermissionError('synthetic generation revoked')
    def workspace(op,kind):return tmp_path/(op+'-'+kind)
    async def download(blob,op,*,ticket,transfer_operation):
        destination=workspace(op,'download')
        destination.write_bytes(path.read_bytes());destination.chmod(0o600)
        return destination
    async def revoking_probe(handler):
        assert (await handler({'probe':True},encode=None))['version']=='new'
        allowed[0]=False
    slot=ReadHandlerSlot(old,loaded_sha256='a'*64,compatibility=context())
    owner=WarmHandlerMaintenance(slot=slot,download=download,workspace=workspace,
        constructor={'marker':'new'},self_test=good_stage,probe=revoking_probe,current=current)
    blob=proof(path)
    await owner('read_handler_stage',dict(artifact=WarmHandlerPatchApproval(blob,'b'*40).wire(),
        expected_loaded_sha256='a'*64,expected_generation=0),'stage',
        {'download_ticket':{'scoped':'fixture'}})
    with pytest.raises(PermissionError,match='revoked'):
        await owner('read_handler_activate',dict(staged_sha256=digest,
            expected_loaded_sha256='a'*64,expected_generation=0),'activate',{})
    assert slot.receipt()['loaded_sha256']=='a'*64
    assert slot.receipt()['generation']==0 and not slot.receipt()['switching']
    assert (await slot({'last':True},encode=None))['version']=='old'


def test_reviewed_approval_enforces_exact_blob_and_commit():
    blob=BlobRef('approved/snapshots/blobs/source',1024,'a'*64,'"etag"')
    approval=WarmHandlerPatchApproval(blob,'c'*40)
    assert WarmHandlerPatchApproval.from_wire(approval.wire())==approval
    with pytest.raises(WarmMaintenanceRejected):
        WarmHandlerPatchApproval(blob,'not-a-reviewed-commit')
    with pytest.raises(WarmMaintenanceRejected):
        WarmHandlerPatchApproval.from_wire(dict(approval.wire(),path='/tmp/source.py'))
