"""Actual accepted native stage must be coverable before public activation."""
import json
from types import SimpleNamespace
import pytest
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.control_spool import ControlSpool,SpoolConflict
from regional_knowledge.kaggle_current_registry import current_registry
from regional_knowledge.kaggle_native_source_bindings import resolved_native_stage_command
from regional_knowledge.recovery_journal import RecoveryJournal
from regional_knowledge.source_history_verifier import NativeSourceHistoryVerifier
from regional_knowledge.source_stage import STAGE_ROUTE,SEAL_ROUTE,canonical,digest
from regional_knowledge.source_activation import resolved_activation_command,resolved_materialize_command,MATERIALIZE_CODEC
from test_participants import people
from test_source_stage import SOURCE,REQUEST,batch,seal
from test_source_activation import activation_payload,PERSON
from test_recovery_journal import ConditionalMemoryStore
from test_control_spool import receipt
from test_source_history_wss import pair


def commands(person=PERSON):
    doc=batch(count=1);head=digest(canonical(doc));activation=activation_payload([doc],head)
    args=dict(sequence=1,operation_key='native-history-stage',request_id=REQUEST,person_id=person,
        logical_time='2026-10-23T00:00:00Z',source_id=SOURCE)
    return [resolved_native_stage_command(route=STAGE_ROUTE,payload=doc,**args),
        resolved_native_stage_command(route=SEAL_ROUTE,payload=seal(1,head),**args),
        resolved_materialize_command(payload={**activation,'codec':MATERIALIZE_CODEC,'ordinal':0},**args),
        resolved_activation_command(payload=activation,**args)]


@pytest.mark.parametrize('index',range(4))
def test_all_native_ingestion_routes_are_classified_under_existing_import(index):
    command=commands()[index]
    verifier=object.__new__(NativeSourceHistoryVerifier)
    verifier.spool=SimpleNamespace(registry=current_registry());verifier.routes={'source.import':object()}
    actual,handler,_=verifier._classify(command.encode(),None,historical=True)
    assert actual==command and handler.dependency.scope_class=='public_source'
    verifier.routes={}
    with pytest.raises(SpoolConflict):verifier._classify(command.encode(),None,historical=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('index',range(4))
async def test_applied_native_command_covers_inactive_data_and_only_activation_requires_origin(people,tmp_path,index):
    q=BgeQueue(tmp_path/'queue.sqlite');store=ConditionalMemoryStore()
    journal=RecoveryJournal(store,stream='native-history',max_outcome_bytes=8192)
    registry=current_registry();spool=ControlSpool(q,people.registry,journal,registry)
    e=SimpleNamespace(q=q,spool=spool,registry=registry,journal=journal)
    p=pair(e,extra_routes=('source.import',))
    try:
        assert (await p.v.establish_genesis(p.b)).current()
        command=commands(people.owner.subject)[index]
        # Same trusted builder/validated spool path used by NativeSourceIntake.
        frozen,data,handler=spool._validate(command,'db-source')
        await spool._write(spool._stage,frozen,data,handler,'db-source')
        accepted=await spool.seal(people.admin,command.operation_key,writer_generation='writer-1',predecessor=None)
        native=receipt(command);p.pg.applied_seq=1
        p.pg.receipts=[(1,command.operation_key,digest(command.encode()),handler.fingerprint,
            accepted.manifest.sha256,native.status,native.outcome_sha256,digest(native.outcome),len(native.outcome),
            'db-source',digest(command.encode()))]
        await spool.record_apply(command.operation_key,native)
        proof=await p.v.extend(p.b)
        assert proof.current() and proof.applied_sequence==1 and len(proof.pending)==1
        assert p.calls[-1]['action']=='close' and p.calls[-1]['origin_ids']==([] if index<3 else [SOURCE])
        with q.connect(write=False) as db:
            assert db.execute('select count(*) from rkb_control_source_heads').fetchone()[0]==(0 if index<3 else 1)
        # Native apply is still distinct from remote durability.
        assert (await spool.get(command.operation_key)).outcome_state=='NOT_SENT'
    finally:
        await p.n.aclose();spool.close()
