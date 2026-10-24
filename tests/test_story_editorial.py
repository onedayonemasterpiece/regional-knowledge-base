"""Pure exact command checks; no native authorization is inferred from builders."""
from dataclasses import replace
import json
import pytest
from regional_knowledge.story_editorial import *
from regional_knowledge.story_contracts import UpsertVariant
from test_bge_admission import state,auth,people

PERSON='60000000-0000-0000-0000-000000000002'
SOURCE='61000000-0000-0000-0000-000000000001'
STORY='73000000-0000-0000-0000-000000000001'
VARIANT='74000000-0000-0000-0000-000000000001'
FINDING='72000000-0000-0000-0000-000000000001'


def operation(**changes):
    return UpsertVariant(**dict(dict(op='upsert_variant',variant_id=VARIANT,expected_variant_revision=1,
        audience='general',format='story',body='The exact revised narrative remains saved.',
        blocks=[dict(text='The exact revised narrative remains saved.',assertion_id=FINDING,assertion_revision=1)]),**changes))


def command(op=None):
    return resolved_story_edit_command(sequence=2,operation_key='story-edit-first',request_id='request-edit',
        person_id=PERSON,logical_time='2026-10-20T18:00:00Z',source_id=SOURCE,story_id=STORY,
        expected_revision=1,source_revision=1,eligibility_revision=2,operation=op or operation(),
        embedding=[1.]+[0.]*1023,input_tokens=12)


def test_exact_body_and_old_variant_reference_are_replayable_without_model():
    c=command();doc=validate_story_edit_command(c)
    assert doc['operation']['body']==operation().body and doc['resolved_variant_id']==VARIANT
    assert command().encode()==c.encode()
    assert c.ids['actor_id']==c.person_id and c.expected_revisions['variant_revision']=='1'
    assert 'publish_ready' not in c.payload.decode()


def test_new_variant_id_is_resolved_once_and_keeps_exact_zero_expected_revision():
    c=command(operation(variant_id=None,expected_variant_revision=None))
    assert c.expected_revisions['variant_revision']=='0'
    assert validate_story_edit_command(c)['resolved_variant_id']==validate_story_edit_command(command(
        operation(variant_id=None,expected_variant_revision=None)))['resolved_variant_id']


@pytest.mark.parametrize('case',['actor','variant','source_revision','schema','input_tokens','fp32','assertion'])
def test_forged_or_ambiguous_editorial_bytes_cannot_enter_the_native_route(case):
    c=command();doc=validate_story_edit_command(c)
    if case=='actor':c=replace(c,person_id='60000000-0000-0000-0000-000000000003')
    if case=='variant':doc['resolved_variant_id']=STORY
    if case=='source_revision':c=replace(c,expected_revisions={**c.expected_revisions,'source_revision':'01'})
    if case=='schema':c=replace(c,schema_revision='rkb-v4-sql001-008-v1')
    if case=='input_tokens':doc['input_tokens']=513
    if case=='fp32':doc['embedding'][1]=.00000000001
    if case=='assertion':doc['operation']['blocks'][0]['assertion_revision']=None
    if case in ('variant','input_tokens','fp32','assertion'):c=replace(c,payload=json.dumps(doc,sort_keys=True,separators=(',',':')).encode())
    with pytest.raises((ValueError,TypeError)):validate_story_edit_command(c)

@pytest.mark.asyncio
async def test_personal_editor_accepted_command_cannot_admit_unrelated_document_work(state):
    from regional_knowledge.control_spool import ControlSpool,SpoolDenied
    from regional_knowledge.kaggle_apply import HandlerRegistry
    from regional_knowledge.kaggle_source_commands import AcceptedSourceDispatch
    from regional_knowledge.participants import RequestAuthority
    from test_recovery_journal import fixture as journal_fixture
    s=state;_,journal=journal_fixture()
    handler=editorial_handler(route=STORY_EDIT,check_sha256='a'*64,apply_sha256='b'*64)
    spool=ControlSpool(s.q,s.auth.p.registry,journal,HandlerRegistry([handler]))
    bridge=AcceptedSourceDispatch(spool,binding_current=lambda **binding:True)
    actor=s.auth.p.other.subject
    c=replace(command(),sequence=1,person_id=actor,ids={**command().ids,'actor_id':actor})
    try:
        with pytest.raises(SpoolDenied):
            await spool.stage_source_import(s.auth.p.personal,c,database_generation='synthetic-db')
        staged=await spool.stage_story_editorial(s.auth.p.personal,c,database_generation='synthetic-db')
        assert staged.command.encode()==c.encode()
        await spool.seal(s.auth.p.personal,c.operation_key,predecessor=None,writer_generation='writer-1')
        # An arbitrary ordinary document precedes the accepted editorial command
        # for this same person. It must neither execute nor hide the command.
        rogue=s.q.enqueue(actor,'not-authorized-editorial-document',['Unrelated corpus material'],kind='document')
        job=await bridge.enqueue(s.auth.p.personal,c.operation_key,writer_generation='writer-1')
        work=await s.a.next_channel_work(s.run,s.token,s.generation)
        assert work.document_payload['id']==job and work.document_payload['kind']=='source_command'
        assert actor in s.a._commands and actor not in s.a._documents
        assert s.q.result(actor,rogue)['state']=='pending'
        with pytest.raises(Exception):await s.a.register_document_actor(s.auth.p.personal)
        group=replace(s.auth.p.personal,public_only=True)
        with pytest.raises(SpoolDenied):await spool.authorize_command(group,c)
        with pytest.raises(SpoolDenied):await spool.authorize_command(s.auth.p.admin,c)
    finally:
        await bridge.aclose();spool.close()

@pytest.mark.asyncio
async def test_independent_case_pending_keeps_global_order_without_book_pending(state):
    from regional_knowledge.control_spool import ControlSpool
    from regional_knowledge.kaggle_apply import DependencySpec,HandlerRegistry
    from test_recovery_journal import fixture as journal_fixture
    s=state;_,journal=journal_fixture()
    # Derived scope bookkeeping only; no uninstalled domain intake is enabled.
    base=editorial_handler(route=STORY_EDIT,check_sha256='a'*64,apply_sha256='b'*64)
    dep=DependencySpec('synthetic.case.v1','case',frozenset({'case_id'}),'shared')
    h=replace(base,route=dep.operation,dependency=dep)
    c=replace(command(),sequence=1,route=dep.operation,ids={'case_id':STORY})
    spool=ControlSpool(s.q,s.auth.p.registry,journal,HandlerRegistry([h]))
    try:
        spool._validate=lambda cmd,generation:(cmd,cmd.encode(),h)
        spool._stage(c,c.encode(),h,'synthetic-db')
        assert (await spool.scope_state())['pending']
        facts=await spool.read_facts(None)
        assert not facts.pending and not facts.unknown and facts.scope_sequence==0
        with spool._db(write=True) as db:
            db.execute("UPDATE rkb_control_operations SET seal_state='SENT_UNKNOWN' WHERE operation_key=?",(c.operation_key,))
        assert (await spool.scope_state())['unknown']
        facts=await spool.read_facts(None)
        assert not facts.pending and not facts.unknown
    finally:spool.close()
