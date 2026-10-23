"""Retained native history / exact accepted suffix and unknown-route classification.

Uses the existing control journal and NativeSourceHistoryObserver fixture. It
does not open a provider, a real PostgreSQL instance or a new recovery service.
"""
from dataclasses import replace
import base64

import pytest

from regional_knowledge.control_spool import SpoolConflict
from regional_knowledge.kaggle_apply import ApplyReceipt, ApplyConflict
from regional_knowledge.kaggle_source_commands import canonical, digest
from regional_knowledge.recovery_journal import JournalCorrupt, StoredObject
from test_control_spool import accept, env, receipt
from test_source_history_wss import pair
from test_source_read_gate import binding
from test_participants import people


async def applied_first(e, p):
    await p.v.establish_genesis(p.b)
    seal=await accept(e)
    outcome=receipt(e.cmd)
    p.pg.applied_seq=1
    p.pg.receipts=[(1,e.cmd.operation_key,digest(e.cmd.encode()),e.registry.resolve(e.cmd).fingerprint,
        seal.manifest.sha256,outcome.status,outcome.outcome_sha256,digest(outcome.outcome),
        len(outcome.outcome),'db-source',digest(e.cmd.encode()))]
    await e.journal.record_outcome(seal,command=e.cmd,database_generation='db-source',
        outcome=outcome.outcome,writer_generation='writer-1')
    proof=await p.v.extend(p.b)
    assert proof.applied_sequence==1 and proof.accepted_sequence==1 and not proof.pending
    return proof,seal


@pytest.mark.asyncio
async def test_expired_retained_prefix_uses_only_small_valid_suffix_not_lifetime_scan(env,monkeypatch):
    p=pair(env)
    try:
        prior,old_seal=await applied_first(env,p)
        next_command=replace(env.cmd,sequence=2,operation_key='new-accepted-tail',request_id='new-tail')
        new_seal=await env.journal.seal([next_command],predecessor=await env.journal.head(),
            writer_generation='writer-1')
        # The verified old+new bytes exceed the configured *incremental*
        # budget; the new tail alone fits, so a genesis scan must not occur.
        p.v.max_history_bytes=len(next_command.encode())+20
        assert len(env.cmd.encode())+len(next_command.encode())>p.v.max_history_bytes
        actual_read=env.journal.read_segment
        traversed=[]
        async def only_suffix(ref):
            traversed.append(ref)
            assert ref!=old_seal.manifest, 'already-verified history was decoded again'
            return await actual_read(ref)
        monkeypatch.setattr(env.journal,'read_segment',only_suffix)
        p.v.clock=lambda:prior.deadline+1  # startup calls restore once read proof expires
        renewed=await p.v.restore(p.b)
        assert renewed.current() and renewed.accepted_sequence==2
        assert renewed.applied_sequence==1 and len(renewed.pending)==1
        # Journal head readback and exact suffix verification may each inspect
        # the same new segment; neither may traverse the verified old prefix.
        assert 1<=len(traversed)<=2 and set(traversed)=={new_seal.manifest}
        assert p.calls[-3]['mode']=='delta'
        assert p.v._native_prefix is not None
    finally:await p.n.aclose()


@pytest.mark.asyncio
async def test_corrupt_new_accepted_tail_is_not_hidden_by_verified_old_prefix(env):
    p=pair(env)
    try:
        await applied_first(env,p)
        command=replace(env.cmd,sequence=2,operation_key='corrupt-tail',request_id='new-tail')
        seal=await env.journal.seal([command],predecessor=await env.journal.head(),
            writer_generation='writer-1')
        original=env.store.objects[seal.manifest.key]
        env.store.objects[seal.manifest.key]=StoredObject(b'not-the-accepted-manifest',original.token)
        with pytest.raises((JournalCorrupt,SpoolConflict)):
            await p.v.restore(p.b)
        assert p.v._coverage is None or not p.v._coverage.current()
    finally:await p.n.aclose()


@pytest.mark.asyncio
async def test_wrong_native_binding_does_not_inherit_a_verified_prefix(env):
    p=pair(env)
    try:
        await applied_first(env,p)
        with pytest.raises(SpoolConflict):
            await p.v.restore(binding())
        assert p.v._coverage is None or not p.v._coverage.current()
    finally:await p.n.aclose()


def _old_opaque_receipt(command,handler_hash):
    result=b'{"known_result":true}'
    body=canonical(dict(codec='rkb.applied-outcome.v1',operation_key=command.operation_key,
        command_sha256=digest(command.encode()),cursor=command.sequence,status='applied',
        ids=dict(command.ids),request_id=command.request_id,person_id=command.person_id,
        route=command.route,logical_time=command.logical_time,
        before=[command.schema_revision,command.function_revision,command.executor_revision],
        after=[command.schema_revision,command.function_revision,command.executor_revision],
        handler_sha256=handler_hash,result_b64=base64.b64encode(result).decode(),
        result_sha256=digest(result)))
    return ApplyReceipt('db-source',command.operation_key,command.sequence,digest(command.encode()),
        'applied',body,digest(body),False)


@pytest.mark.asyncio
async def test_unknown_previously_accepted_route_stays_global_without_public_head_or_execution(env):
    p=pair(env)
    try:
        await p.v.establish_genesis(p.b)
        # Exact sealed legacy bytes; neither current registry nor current
        # admission knows this route, and classification must not grant it one.
        unknown=replace(env.cmd,route='legacy.unknown.v99',operation_key='accepted-unknown',
            request_id='unknown-source',payload=b'{}')
        seal=await env.journal.seal([unknown],predecessor=await env.journal.head(),
            writer_generation='writer-1')
        handler_hash='f'*64
        receipt=_old_opaque_receipt(unknown,handler_hash)
        p.pg.applied_seq=1
        p.pg.receipts=[(1,unknown.operation_key,digest(unknown.encode()),handler_hash,
            seal.manifest.sha256,'applied',receipt.outcome_sha256,digest(receipt.outcome),
            len(receipt.outcome),'db-source',digest(unknown.encode()))]
        await env.journal.record_outcome(seal,command=unknown,database_generation='db-source',
            outcome=receipt.outcome,writer_generation='writer-1')
        with pytest.raises(ApplyConflict):
            env.spool._validate(unknown,'db-source')  # no permission to execute it
        proof=await p.v.extend(p.b)
        assert proof.applied_sequence==proof.accepted_sequence==1
        assert proof.current() and not proof.pending
        with env.spool._db() as db:
            assert db.execute('SELECT count(*) FROM rkb_control_source_heads').fetchone()[0]==0
        assert not any('source_command' in str(call) for call in p.calls)
    finally:await p.n.aclose()
