"""Resolved source intake and opt-in native atomic activation; synthetic data only."""
import copy
import base64
from contextlib import asynccontextmanager
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import pytest_asyncio

from regional_knowledge.bge_contract import REVISION,SPACE,TOKENIZER_SHA256
from regional_knowledge.kaggle_apply import Revisions,HandlerRegistry,apply_accepted,initialize_database,ApplyExecutionError
from regional_knowledge.source_import import (
    SOURCE_IMPORT,IMPORT_DEPENDENCY,IMPORT_PRECONDITIONS,MAX_IMPORT_BYTES,
    resolved_import_command,validate_import_command,validate_import_payload,source_import_handler,
)
from test_kaggle_apply import pinned
from test_kaggle_pg_driver import database
from test_kaggle_query import PG
from test_recovery_journal import fixture as journal_fixture

ROOT=Path(__file__).resolve().parents[1]
SOURCE='00000000-0000-0000-0000-000000000002'
PASSAGE='10000000-0000-0000-0000-000000000002'
FRONTIER='30000000-0000-0000-0000-000000000002'
VERSIONS=Revisions('source-import-s1','source-import-f1','source-import-e1')


def payload():
    return dict(codec='rkb.source-import.v1',bibliography='Exact accepted fixture bibliography',revision=1,
        work_id='40000000-0000-0000-0000-000000000002',edition_id='50000000-0000-0000-0000-000000000002',
        original=dict(sha256='a'*64,size=1234,ref='archive:fixture-original',receipt_sha256='b'*64),
        model=dict(revision=REVISION,space=SPACE,tokenizer_sha256=TOKENIZER_SHA256,dimension=1024,max_tokens=512),
        passages=[dict(passage_id=PASSAGE,ordinal=0,locator='page 2 exact region',
            body='Resolved semantic harbour source text.',language='simple',embedding=[1.]+[0.]*1023,input_tokens=12)],
        research=dict(frontier_id=FRONTIER,state='pending',continuation=dict(next='resolved-assertions',cursor='page-2')))


def resolved(data=None,**changes):
    values=dict(sequence=1,operation_key='source-import-fixture-1',request_id='request-import-1',
        person_id='60000000-0000-0000-0000-000000000002',logical_time='2026-10-20T15:00:00Z',
        revisions=VERSIONS,source_id=SOURCE,payload=data or payload())
    values.update(changes)
    return resolved_import_command(**values)


def test_resolved_import_preserves_exact_bibliography_original_vectors_and_continuation():
    data=payload();command=resolved(data)
    assert command.route==SOURCE_IMPORT and command.expected_revisions=={'absent_source':'true'}
    assert validate_import_command(command)==data
    data['passages'][0]['body']='mutated caller memory'
    assert validate_import_command(command)['passages'][0]['body']=='Resolved semantic harbour source text.'
    registry=HandlerRegistry([source_import_handler(revisions=VERSIONS,check_sha256='1'*64,apply_sha256='2'*64)])
    handler=registry.resolve(command)
    assert handler.dependency==IMPORT_DEPENDENCY and handler.precondition_keys==IMPORT_PRECONDITIONS
    assert handler.check.signature=='rkb_v4.check_source_import_v1(bytea,jsonb,jsonb,text)'


@pytest.mark.parametrize('case',[
    'extra_authority','wrong_model','wrong_tokenizer','oversized','no_original_receipt','signed_original',
    'empty_research','no_research','duplicate_passage','ordinal_gap','truncate_tokens','bool_vector',
    'non_fp32','nonfinite','zero_vector','oversized_body','alternate_revision','unknown_language'])
def test_invalid_resolved_material_never_forms_accepted_command(case):
    data=payload()
    if case=='extra_authority':data['owner_grant']=True
    if case=='wrong_model':data['model']['space']='other'
    if case=='wrong_tokenizer':data['model']['tokenizer_sha256']='f'*64
    if case=='oversized':data['research']['continuation']={'private':'x'*MAX_IMPORT_BYTES}
    if case=='no_original_receipt':del data['original']['receipt_sha256']
    if case=='signed_original':data['original']['ref']='https://fixture.invalid/file?secret=value'
    if case=='empty_research':data['research']['continuation']={}
    if case=='no_research':del data['research']
    if case=='duplicate_passage':data['passages'].append(copy.deepcopy(data['passages'][0]))
    if case=='ordinal_gap':data['passages'][0]['ordinal']=1
    if case=='truncate_tokens':data['passages'][0]['input_tokens']=513
    if case=='bool_vector':data['passages'][0]['embedding'][0]=True
    if case=='non_fp32':data['passages'][0]['embedding'][1]=.0000001
    if case=='nonfinite':data['passages'][0]['embedding'][0]=float('inf')
    if case=='zero_vector':data['passages'][0]['embedding']=[0.]*1024
    if case=='oversized_body':data['passages'][0]['body']='x'*16001
    if case=='alternate_revision':data['revision']=2
    if case=='unknown_language':data['passages'][0]['language']='unknown'
    with pytest.raises(ValueError):resolved(data)


def test_payload_requires_canonical_bytes_and_exact_controller_ids():
    command=resolved()
    with pytest.raises(ValueError):validate_import_payload(json.dumps(payload()).encode())
    with pytest.raises(ValueError):validate_import_command(replace(command,expected_revisions={}))
    with pytest.raises(ValueError):validate_import_command(replace(command,ids=dict(command.ids,grant='injected')))


@pytest.mark.asyncio
async def test_remote_acceptance_retains_exact_resolved_outcome_without_model_call():
    store,journal=journal_fixture();command=resolved()
    seal=await journal.seal([command],predecessor=None,writer_generation='writer-1')
    assert await journal.verify_accepted(seal,command)
    _,commands=await journal.read_segment(seal.manifest)
    value=json.loads(commands[0])
    assert value['ids']==dict(source_id=SOURCE)
    assert await journal.locate_accepted(command)==seal
    assert await journal.verify_accepted(seal,command)==seal.command_hashes[0]


@pytest_asyncio.fixture
async def native_import(database):
    connection=database
    async with connection.transaction():
        await connection.execute((ROOT/'sql/kaggle/002_recovery.sql').read_text(),prepare=False)
    await initialize_database(connection,database_generation='source-import-fixture',writer_generation='writer-1',revisions=VERSIONS)
    async with connection.transaction():
        for name in ('003_search_scope.sql','004_read_fences.sql','005_source_withdraw.sql','006_source_import.sql'):
            await connection.execute((ROOT/'sql/kaggle'/name).read_text(),prepare=False)
        await connection.execute("SELECT rkb_v4.publish_read_frontier('source-import-fixture','writer-1',0,0,true,true)")
    check=await pinned(connection,'check_source_import_v1');apply=await pinned(connection,'apply_source_import_v1')
    registry=HandlerRegistry([source_import_handler(revisions=VERSIONS,
        check_sha256=check.definition_sha256,apply_sha256=apply.definition_sha256)])
    _,journal=journal_fixture()
    return dict(connection=connection,journal=journal,registry=registry)


async def apply_native(env,command):
    journal=env['journal'];head=await journal.head()
    seal=await journal.locate_accepted(command)
    if seal is None:seal=await journal.seal([command],predecessor=head,writer_generation='writer-1')
    return await apply_accepted(env['connection'],journal=journal,seal=seal,command=command,
        database_generation='source-import-fixture',writer_generation='writer-1',registry=env['registry'])


@PG
@pytest.mark.asyncio
async def test_native_activation_receipt_research_frontier_and_exact_duplicate(native_import):
    env=native_import;command=resolved();receipt=await apply_native(env,command)
    assert receipt.status=='applied' and not receipt.duplicate
    connection=env['connection']
    async with connection.transaction():
        row=await (await connection.execute('''SELECT s.accepted,s.withdrawn,s.bibliography,a.revision,p.body,
            o.original_sha256,o.original_receipt_sha256,r.frontier_id::text,r.state,r.continuation
            FROM rkb_v4.sources s JOIN rkb_v4.active_revision a USING(source_id)
            JOIN rkb_v4.passage_search p USING(source_id,revision)
            JOIN rkb_v4.source_import_origins o USING(source_id,revision)
            JOIN rkb_v4.source_research_frontier r USING(source_id,revision) WHERE s.source_id=%s''',(SOURCE,))).fetchone()
        assert row==(True,False,payload()['bibliography'],1,payload()['passages'][0]['body'],
            'a'*64,'b'*64,FRONTIER,'pending',payload()['research']['continuation'])
        assert await (await connection.execute('''SELECT s.applied_seq,f.projection_seq,f.text_ready,f.dense_ready,
            f.pending_seq FROM rkb_v4.recovery_state s CROSS JOIN rkb_v4.read_frontier f''')).fetchone()==(1,1,True,True,None)
    duplicate=await apply_native(env,command)
    assert duplicate.duplicate and duplicate.outcome==receipt.outcome
    seal=await env['journal'].locate_accepted(command)
    remote=await env['journal'].record_outcome(seal,command=command,database_generation='source-import-fixture',
        outcome=receipt.outcome,writer_generation='writer-1')
    assert remote.outcome_sha256==receipt.outcome_sha256


@PG
@pytest.mark.asyncio
async def test_native_duplicate_source_is_recorded_rejection_without_overwrite(native_import):
    env=native_import;await apply_native(env,resolved())
    changed=payload();changed['bibliography']='Rejected overwrite'
    receipt=await apply_native(env,resolved(changed,sequence=2,operation_key='another-import-attempt',request_id='request-2'))
    assert receipt.status=='rejected_precondition'
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT bibliography FROM rkb_v4.sources WHERE source_id=%s',(SOURCE,))).fetchone()==(payload()['bibliography'],)
        assert await (await env['connection'].execute('SELECT projection_seq,text_ready,dense_ready FROM rkb_v4.read_frontier')).fetchone()==(2,True,True)


@PG
@pytest.mark.asyncio
async def test_native_mid_import_failure_rolls_back_every_projection_and_retries_exact_command(native_import):
    env=native_import;connection=env['connection'];command=resolved()
    async with connection.transaction():
        await connection.execute('''CREATE FUNCTION rkb_v4.synthetic_import_failure() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic failure'; END $$;
            CREATE TRIGGER synthetic_import_failure BEFORE INSERT ON rkb_v4.passage_search
            FOR EACH ROW EXECUTE FUNCTION rkb_v4.synthetic_import_failure()''',prepare=False)
    with pytest.raises(ApplyExecutionError):await apply_native(env,command)
    async with connection.transaction():
        for table in ('sources','source_import_origins','source_research_frontier'):
            assert await (await connection.execute('SELECT 1 FROM rkb_v4.'+table+' WHERE source_id=%s',(SOURCE,))).fetchone() is None
        assert await (await connection.execute('SELECT applied_seq FROM rkb_v4.recovery_state')).fetchone()==(0,)
        assert await (await connection.execute('SELECT projection_seq,text_ready,dense_ready FROM rkb_v4.read_frontier')).fetchone()==(0,True,True)
        await connection.execute('DROP TRIGGER synthetic_import_failure ON rkb_v4.passage_search')
    assert (await apply_native(env,command)).status=='applied'


@PG
@pytest.mark.asyncio
async def test_worker_accepted_command_applies_native_pg_and_replays_exact_receipt(native_import):
    from regional_knowledge.kaggle_source_commands import (
        WorkerSourceExecutor, seal_wire, receipt_wire, decode_receipt,
    )
    env = native_import
    command = resolved()
    seal = await env['journal'].seal([command], predecessor=None, writer_generation='writer-1')
    command_hash = await env['journal'].verify_accepted(seal, command)
    @asynccontextmanager
    async def acquire():
        yield env['connection']
    database = SimpleNamespace(database_generation='source-import-fixture', writer_generation='writer-1')
    worker = WorkerSourceExecutor(acquire=acquire, registry=env['registry'], database=database,
        run_id='synthetic-run', launch_nonce='synthetic-nonce', runtime_incarnation='synthetic-incarnation')
    frame = dict(type='DISPATCH', protocol_version=2, connection_generation=1, id='accepted-job',
        claim='durable-claim', kind='source_command', run_id='synthetic-run', launch_nonce='synthetic-nonce',
        runtime_incarnation='synthetic-incarnation', database_generation=database.database_generation,
        writer_generation=database.writer_generation, command_sha256=command_hash,
        handler_sha256=env['registry'].resolve(command).fingerprint,
        command_b64=base64.b64encode(command.encode()).decode(), accepted=seal_wire(seal),
        compute_claim='actual-resource-claim', dispatch_generation=1)
    first, first_outcome = decode_receipt(await worker(frame))
    second, second_outcome = decode_receipt(await worker(frame))
    assert first.status == 'applied' and not first.duplicate
    assert second.duplicate and second.outcome == first.outcome
    assert first_outcome == second_outcome and first_outcome['cursor'] == 1
    assert first.command_sha256 == command_hash
    remote = await env['journal'].record_outcome(seal, command=command,
        database_generation=database.database_generation, writer_generation='writer-1', outcome=first.outcome)
    assert remote.outcome_sha256 == first.outcome_sha256
    assert receipt_wire(second)['duplicate'] is True
    with pytest.raises(PermissionError):
        await worker(dict(frame, runtime_incarnation='another-incarnation'))
    with pytest.raises(PermissionError):
        await worker(dict(frame, handler_sha256='0'*64))
    async with env['connection'].transaction():
        assert await (await env['connection'].execute('SELECT count(*) FROM rkb_v4.applied_ops')).fetchone() == (1,)
