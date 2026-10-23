"""A retained native Dataset capability replaces lifetime history verification."""
import json
from dataclasses import replace

import pytest

from regional_knowledge.control_spool import SpoolConflict
from regional_knowledge.kaggle_source_acl import NativeHistoryError
from regional_knowledge.kaggle_restored_database import VerifiedInitializedDatabase
from regional_knowledge.pg_snapshot_recovery import ControllerRecoveryProof
from regional_knowledge.kaggle_source_history_native import canonical,digest
from test_control_spool import env,people,accept,withdraw,receipt,SOURCE
from test_source_history_wss import pair


async def restored(e):
    p=pair(e)
    seal=await accept(e);head=await e.journal.head();p.pg.applied_seq=1
    control=canonical(dict(codec='rkb.source-base-control.v1',stream=e.journal.stream,
        database_generation='db-source',sequence=1,journal_head=head.reference.wire(),
        schema_version=p.v.schema_version,registry_sha256=p.v.registry_hash,routes=sorted(p.v.routes),
        heads=[[SOURCE,1,digest(e.cmd.encode()),1,0,0]]))
    class Owner:
        def __init__(self):self.retained=set();self._capabilities=self.retained;self.checks=0
        async def verify_initialized(self,cap,database,identity):
            if cap not in self.retained or database!=p.v.database or identity!=('run','nonce','inc'):
                raise SpoolConflict('unknown retained recovery')
            self.checks+=1
            if not p.n.valid(('run','inc',1)):raise SpoolConflict('native attachment changed')
            if p.pg.applied_seq!=cap.applied_sequence:raise SpoolConflict('native cursor changed')
            return {'host':'synthetic-owned-socket'}
    native_owner=Owner();controller_owner=Owner()
    cap=VerifiedInitializedDatabase(native_owner,None,p.v.database,('run','nonce','inc'),
        None,head,1,control,object());native_owner.retained.add(cap)
    proof=ControllerRecoveryProof(controller_owner,'native-reference',p.v.database,('run','nonce','inc'),
        seal.manifest,head,1,control,p.b);controller_owner.retained.add(proof)
    p.n._restore_baseline(cap)
    return p,proof,native_owner,controller_owner


@pytest.mark.asyncio
async def test_verified_base_adopts_without_genesis_or_old_receipt_scan_then_verifies_only_delta(env,monkeypatch):
    p,proof,_,_=await restored(env)
    try:
        with monkeypatch.context() as patch:
            async def no_segments(*args,**kwargs):pytest.fail('re-reading lifetime accepted history')
            patch.setattr(env.journal,'read_segment',no_segments)
            coverage=await p.v.adopt_recovered(proof,p.b)
        assert coverage.current() and coverage.applied_sequence==coverage.accepted_sequence==1
        assert not coverage.pending and not env.spool._native_history_unsettled
        assert not p.calls  # Reuse the existing adoption path; no new WSS action.
        assert not p.pg.sql  # Neither native receipt pagination nor origin scans.
        # Renewal uses the retained baseline, not mode=full or genesis.
        renewed=await p.v.renew(p.b)
        assert renewed.current() and p.calls[-3]['mode']=='renew'
        # A later accepted command is the only journal segment/receipt checked.
        cmd=withdraw(env.p.owner.subject,sequence=2)
        seal=await accept(env,cmd,predecessor=proof.journal_head)
        native=receipt(cmd);p.pg.applied_seq=2
        p.pg.receipts=[(2,cmd.operation_key,digest(cmd.encode()),env.registry.resolve(cmd).fingerprint,
            seal.manifest.sha256,native.status,native.outcome_sha256,digest(native.outcome),len(native.outcome),
            'db-source',digest(cmd.encode()))]
        await env.journal.record_outcome(seal,command=cmd,database_generation='db-source',
            outcome=native.outcome,writer_generation='writer-1')
        delta=await p.v.extend(p.b)
        assert delta.current() and delta.applied_sequence==2 and not delta.pending
        assert p.calls[-3]['mode']=='delta'
        assert not any(x.get('mode')=='full' for x in p.calls)
    finally:await p.n.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('change',['copied_controller','json_native','copied_native','cursor','control_heads','binding'])
async def test_restore_baseline_rejects_forged_stale_or_different_retained_authority(env,change):
    p,proof,native_owner,_=await restored(env)
    try:
        if change=='copied_controller':proof=replace(proof)
        elif change in ('json_native','copied_native'):
            cap=next(iter(native_owner.retained))
            with pytest.raises(NativeHistoryError):
                p.n._restore_baseline(json.loads(proof.control) if change=='json_native' else replace(cap))
            assert p.v._coverage is None
            return
        elif change=='cursor':p.pg.applied_seq=2
        elif change=='control_heads':
            control=json.loads(proof.control);control['heads'][0][4]=1
            object.__setattr__(proof,'control',canonical(control))
        else:p.n.detached(1)
        with pytest.raises((SpoolConflict,PermissionError,NativeHistoryError)):
            await p.v.adopt_recovered(proof,p.b)
        assert p.v._coverage is None and env.spool._source_anchor is None
        assert p.v._native_prefix is None
    finally:await p.n.aclose()
