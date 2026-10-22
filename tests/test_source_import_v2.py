"""Resolved narrative bytes/evidence, independent of model and native lifecycle."""
import copy
import hashlib
import json

import pytest

from regional_knowledge.source_import_v2 import (STORY_REVISIONS,resolved_import_v2_command,validate_import_v2_command)
from regional_knowledge.story_contracts import StageStoryCandidateInput
from test_source_import import payload as source_payload,SOURCE,PASSAGE
from test_participants import people

REGION='71000000-0000-0000-0000-000000000001'
FINDING='72000000-0000-0000-0000-000000000001'
STORY='73000000-0000-0000-0000-000000000001'
VARIANT='74000000-0000-0000-0000-000000000001'


def payload():
    value=source_payload();value['codec']='rkb.source-import.v2'
    text='In 1910 the fictional council changed the road and retained the gate.'
    body='The fictional council changed the road in 1910. The gate remained.'
    value['passages'][0]['body']=text
    value['semantic']={'regions':[{'region_id':REGION,'page_id':'75000000-0000-0000-0000-000000000001',
        'source_text':text,'source_text_sha256':hashlib.sha256(text.encode()).hexdigest()}],
        'findings':[{'assertion_id':FINDING,'candidate_key':'gate','proposition':text,'account_kind':'other',
            'attributed_to':None,'reported_by':None,'evidence':[{'region_id':REGION,'passage_id':PASSAGE,
                'start':0,'end':len(text),'original_excerpt':text}],
            'event_dates':[],'observations':[],'entity_keys':[]}],
        'stories':[{'story_id':STORY,'finding_ids':[FINDING],'title':'The road around the gate',
            'summary':'A distinct short editorial summary.','material_type':'event_narrative',
            'default_variant':{'variant_id':VARIANT,'revision':1,'audience':'general','format':'story',
                'language':'en','body':body,'blocks':[{'start':0,'end':len(body),'assertion_ids':[FINDING]}],
                'embedding':[1.]+[0.]*1023,'input_tokens':19}}], 'pending':[]}
    return value


def command(value=None,revisions=STORY_REVISIONS):
    return resolved_import_v2_command(sequence=1,operation_key='source-story-import-1',request_id='request-1',
        person_id='60000000-0000-0000-0000-000000000002',logical_time='2026-10-20T15:00:00Z',
        revisions=revisions,source_id=SOURCE,payload=payload() if value is None else value)


def test_model_authored_body_default_ref_and_fact_survive_exact_payload():
    value=payload();accepted=command(value)
    read=validate_import_v2_command(accepted)
    assert read==value and read['semantic']['stories'][0]['default_variant']['body']!='A distinct short editorial summary.'
    value['semantic']['stories'][0]['default_variant']['body']='caller changed memory'
    assert validate_import_v2_command(accepted)==read
    assert command(read).encode()==accepted.encode()


@pytest.mark.parametrize('case',['quote','hash','missing_finding','body_map_gap','stale_ref','unreachable_pending','model','revision','vector'])
def test_semantic_mismatch_never_forms_accepted_bytes(case):
    value=payload();semantic=value['semantic'];variant=semantic['stories'][0]['default_variant']
    if case=='quote':semantic['findings'][0]['evidence'][0]['original_excerpt']='invented quote'
    if case=='hash':semantic['regions'][0]['source_text_sha256']='0'*64
    if case=='missing_finding':semantic['findings']=[]
    if case=='body_map_gap':variant['blocks'][0]['start']=1
    if case=='stale_ref':variant['blocks'][0]['assertion_ids']=['other']
    if case=='unreachable_pending':
        semantic['stories']=[];semantic['pending']=[{'candidate_key':'gate','finding_ids':[FINDING],'reason':'needs_context'}]
        value['research']['state']='complete'
    if case=='model':value['model']['space']='changed'
    if case=='revision':variant['revision']=2
    if case=='vector':variant['embedding'][0]=True
    with pytest.raises(ValueError):command(value)


def test_facts_only_and_needs_context_preserve_findings_without_invented_story():
    value=payload();value['semantic']['stories']=[]
    assert len(validate_import_v2_command(command(value))['semantic']['findings'])==1
    value['semantic']['pending']=[{'candidate_key':'gate','finding_ids':[FINDING],'reason':'needs_context'}]
    assert validate_import_v2_command(command(value))['semantic']['stories']==[]


def test_v1_revisions_cannot_admit_new_story_semantics():
    from test_source_import import VERSIONS
    with pytest.raises(ValueError,match='revision barrier'):command(revisions=VERSIONS)


def test_stage_explicit_narrative_requires_complete_evidence_map():
    raw={'candidate_key':'gate','title':'A gate','summary':'Separate summary','proposition':'The gate remained.',
        'evidence_refs':[{'page_id':'75000000-0000-0000-0000-000000000001','region_key':'gate','original_excerpt':'The gate remained.'}],
        'basic_variant':{'body':'The gate remained.','blocks':[{'start':0,'end':18,'evidence_indexes':[0]}]}}
    assert StageStoryCandidateInput.model_validate(raw).basic_variant.body=='The gate remained.'
    raw['basic_variant']['blocks'][0]['evidence_indexes']=[1]
    with pytest.raises(ValueError):StageStoryCandidateInput.model_validate(raw)


@pytest.mark.asyncio
async def test_v2_uses_same_authorized_spool_remote_seal_and_captured_dispatch_proof(people,tmp_path):
    from dataclasses import replace
    from types import SimpleNamespace
    from regional_knowledge.bge_queue import BgeQueue
    from regional_knowledge.control_spool import ControlSpool,SpoolDenied
    from regional_knowledge.kaggle_source_commands import fixed_registry,decode_command,ControllerAcceptedProof,digest
    from test_recovery_journal import fixture as journal_fixture
    queue=BgeQueue(tmp_path/'queue.sqlite');_,journal=journal_fixture()
    database=SimpleNamespace(schema_revision=STORY_REVISIONS.schema,function_revision=STORY_REVISIONS.function,
        executor_revision=STORY_REVISIONS.executor)
    registry=fixed_registry(database,[{'route':'source.import.v2','check_sha256':'1'*64,'apply_sha256':'2'*64,'revisions':list(STORY_REVISIONS.wire())}])
    spool=ControlSpool(queue,people.registry,journal,registry)
    c=replace(command(),person_id=people.owner.subject)
    try:
        assert decode_command(c.encode())==c
        with pytest.raises(SpoolDenied):await spool.stage_source_import(people.personal,c,database_generation='db-story')
        entry=await spool.stage_source_import(people.admin,c,database_generation='db-story')
        assert entry.command_bytes==c.encode()
        accepted=await spool.seal(people.admin,c.operation_key,predecessor=None,writer_generation='writer-1')
        assert await journal.verify_accepted(accepted,c)==digest(c.encode())
        proof=ControllerAcceptedProof(c.encode(),accepted,digest(c.encode()))
        assert await proof.verify_accepted(accepted,c)==digest(c.encode())
        with pytest.raises(SpoolDenied):await spool.stage_source_withdraw(people.admin,c,database_generation='db-story')
    finally:spool.close()
