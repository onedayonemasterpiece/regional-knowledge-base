import pytest
from regional_knowledge.kaggle_source_commands import decode_command,receipt_wire
from regional_knowledge.kaggle_pg_snapshot_native import SnapshotError
from test_first_book_restore import scenario,outcome
from test_control_spool import env
from test_participants import people


@pytest.mark.asyncio
async def test_wrong_origin_closure_rejected_before_prepare(env):
    s=await scenario(env)
    s.history.origin_closure=dict(s.history.origin_closure,acl_sha256='f'*64)
    with pytest.raises(SnapshotError,match='snapshot_source_control_binding'):
        await s.physical.recover(s.manifest,s.binding,database=s.database,expected_versions=s.versions,
            expected_final_catalog_sha256=s.history.catalog_hash,
            identity=('fixture-run','fixture-nonce','fixture-incarnation'),operation_key='bad-origin')
    assert not s.events and not s.native.finished


@pytest.mark.asyncio
async def test_rejected_last_transition_cannot_advance_final_closure(env):
    s=await scenario(env)
    original=s.native.apply
    async def rejected(binding,handle,**args):
        command=decode_command(args['command_bytes'])
        if command.sequence!=26:return await original(binding,handle,**args)
        return receipt_wire(outcome(command,s.physical.registry,rejected=True))
    s.native.apply=rejected
    with pytest.raises(SnapshotError,match='snapshot_final_closure_transition_required'):
        await s.physical.recover(s.manifest,s.binding,database=s.database,expected_versions=s.versions,
            expected_final_catalog_sha256=s.history.catalog_hash,
            identity=('fixture-run','fixture-nonce','fixture-incarnation'),operation_key='rejected-upgrade')
    assert not s.native.finished
    assert s.store.latest_checkpoint.dataset_version==2
