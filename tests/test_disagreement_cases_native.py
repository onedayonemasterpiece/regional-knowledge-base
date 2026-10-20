"""Existing explicit PG fixture only; actual accepted source9→ordered10→cases."""
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
from uuid import UUID

import pytest
import pytest_asyncio
from regional_knowledge.kaggle_apply import (HandlerRegistry,HandlerSpec,MigrationTransition,apply_accepted,ApplyExecutionError)
from regional_knowledge.recovery_journal import ExactCommand
from regional_knowledge.disagreement_native import *
from regional_knowledge.disagreement_contracts import *
from regional_knowledge.story_editorial import EDITORIAL_REVISIONS
from regional_knowledge.source_import_v4 import source_import_v4_handler,resolved_import_v4_command
from test_story_editorial_native import initialize
from test_source_story_native import connect,apply
from test_source_manifest import manifest_payload as payload, BBOX
from test_source_import import SOURCE
from test_kaggle_apply import pinned
from test_recovery_journal import fixture as journal_fixture
from test_disagreement_native import CASE,OBJECT,PAGE,REGION,ACTOR,create

OTHER='00000000-0000-0000-0000-000000000007'
DRAFT='00000000-0000-0000-0000-000000000008'
ROOT=Path(__file__).resolve().parents[1]
pytestmark=pytest.mark.skipif(not os.getenv('RKB_STORY_FIXTURE_SOCKET'),reason='explicit existing bounded native Case fixture required')


def source_command():
    return resolved_import_v4_command(sequence=1,operation_key='cases-source-first',request_id='source-1',person_id=ACTOR,
        logical_time='2026-10-20T18:00:00Z',source_id=SOURCE,payload=payload())


async def prepare(conn,generation):
    await initialize(conn,generation)
    artifact=(ROOT/'sql/kaggle/010_disagreement_cases.sql').read_text()
    sha=hashlib.sha256(artifact.encode()).hexdigest()
    # This fixture migration function contains only the exact checked-in DDL;
    # it does not execute SQL supplied in a command payload.
    async with conn.transaction():
        await conn.execute("""CREATE FUNCTION rkb_v4.fixture_check_cases_migration(p bytea,ids jsonb,expected jsonb,logical_time text)
          RETURNS boolean LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
          SELECT convert_from(p,'UTF8')::jsonb->>'migration_id'='fixture-cases-010' AND expected->>'schema'=schema_revision
           AND expected->>'function'=function_revision AND expected->>'executor'=executor_revision
           AND expected->>'migration'=coalesce(last_migration_id,'') FROM rkb_v4.recovery_state WHERE singleton $$""",prepare=False)
        await conn.execute('CREATE FUNCTION rkb_v4.fixture_apply_cases_migration(p bytea,ids jsonb,expected jsonb,logical_time text) '
            'RETURNS bytea LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $case_migration$ BEGIN\n'
            +artifact+"\nRETURN convert_to('{\"migration\":\"010_disagreement_cases.sql\"}','UTF8');END $case_migration$",prepare=False)
    c=await pinned(conn,'check_source_import_v4');a=await pinned(conn,'apply_source_import_v4')
    source=source_import_v4_handler(check_sha256=c.definition_sha256,apply_sha256=a.definition_sha256)
    c=await pinned(conn,'fixture_check_cases_migration');a=await pinned(conn,'fixture_apply_cases_migration')
    transition=HandlerSpec('schema_transition','fixture.cases.migrate',EDITORIAL_REVISIONS,c,a,
        frozenset({'schema','function','executor','migration'}),MigrationTransition('fixture-cases-010',sha,None,CASE_REVISIONS))
    _,journal=journal_fixture()
    n=dict(connection=conn,generation=generation,registry=HandlerRegistry([source,transition]),journal=journal)
    assert (await apply(n,source_command())).status=='applied'
    migration=ExactCommand(sequence=2,operation_key='cases-migration-010',request_id='migrate-2',person_id=ACTOR,
        route='fixture.cases.migrate',logical_time='2026-10-20T18:01:00Z',kind='schema_transition',
        schema_revision=EDITORIAL_REVISIONS.schema,function_revision=EDITORIAL_REVISIONS.function,executor_revision=EDITORIAL_REVISIONS.executor,
        ids={'migration_id':'fixture-cases-010'},expected_revisions=dict(schema=EDITORIAL_REVISIONS.schema,
            function=EDITORIAL_REVISIONS.function,executor=EDITORIAL_REVISIONS.executor,migration=''),
        payload=json.dumps(dict(migration_id='fixture-cases-010',artifact_sha256=sha,predecessor=None),
            sort_keys=True,separators=(',',':')).encode())
    assert (await apply(n,migration)).status=='applied'
    c=await pinned(conn,'check_disagreement_v1');a=await pinned(conn,'apply_disagreement_v1')
    n['registry']=HandlerRegistry([source,transition]+[case_handler(route,check_sha256=c.definition_sha256,
        apply_sha256=a.definition_sha256) for route in DEPENDENCIES])
    return n


@pytest_asyncio.fixture
async def native():
    conn,fd=await connect()
    try:yield await prepare(conn,'case-fixture-one')
    finally:
        await conn.rollback()
        async with conn.transaction():await conn.execute('DROP SCHEMA IF EXISTS rkb_v4 CASCADE')
        await conn.close();os.close(fd)


async def refs(n):
    async with n['connection'].transaction():
        return (await(await n['connection'].execute('SELECT rkb_v4.case_sources_v1(%s)',(CASE,))).fetchone())[0]


async def next_command(n,action,value,*,actor=ACTOR,object_id=OBJECT,case_revision=0,object_version=0,dependencies=None):
    async with n['connection'].transaction():seq=(await(await n['connection'].execute('SELECT applied_seq+1 FROM rkb_v4.recovery_state')).fetchone())[0]
    return resolved_case_command(sequence=seq,operation_key='case-native-'+str(seq),request_id='case-request-'+str(seq),person_id=actor,
        logical_time='2026-10-20T18:02:00Z',case_id=CASE,object_id=CASE if action=='create' else object_id,
        action=action,command=value,source_dependencies=(await refs(n)) if dependencies is None else dependencies,
        attribution=dict(person_id=actor,recorded_by=actor,route='personal',link_id=None,link_revision=None,human_reading_confirmed=False),
        case_revision=case_revision,object_version=object_version)


async def read(n,*,actor=ACTOR,section='case',after_seq=0,take=20,revision=None,draft_id=None,version=None):
    async with n['connection'].transaction():
        assert (await(await n['connection'].execute('SELECT rkb_v4.verify_disagreement_origins_v1(%s::uuid[])',([CASE],))).fetchone())[0]
        return (await(await n['connection'].execute('SELECT rkb_v4.read_disagreement_v1(%s,%s,%s,%s,%s,%s,%s,%s,NULL)',
            (CASE,actor,section,after_seq,take,revision,draft_id,version))).fetchone())[0]


async def create_case(n):
    c=await next_command(n,'create',create(),dependencies=[])
    assert (await apply(n,c)).status=='applied'
    return c


async def grant(n,actor=OTHER,role='contributor'):
    revision=(await read(n))['case_revision']
    c=await next_command(n,'grant',CaseGrant(grantee=UUID(actor),capability=role,reason='Synthetic case access'),case_revision=revision)
    assert (await apply(n,c)).status=='applied'
    return c


async def evidence(n):
    doc=payload();region=doc['semantic']['regions'][0]
    value=EvidenceAdd(document_id=UUID(SOURCE),source_revision=1,source_sha256=doc['original']['sha256'],
        page_id=UUID(region['page_id']),region_id=UUID(region['region_id']),text_start=0,
        quote=region['source_text'],context='Exact synthetic observed source context.',position_ids=['phase-one'],
        relation='context',speaker='Synthetic book author',attribution='direct_statement')
    async with n['connection'].transaction():eligibility=(await(await n['connection'].execute('SELECT eligibility_revision FROM rkb_v4.source_visibility WHERE source_id=%s',(SOURCE,))).fetchone())[0]
    c=await next_command(n,'add_evidence',value,dependencies=[dict(source_id=SOURCE,content_revision=1,eligibility_revision=eligibility)])
    return c


@pytest.mark.asyncio
async def test_exact_native_source_quote_current_withdrawal_and_receipt(native):
    n=native;await create_case(n);c=await evidence(n)
    first=await apply(n,c);assert first.status=='applied' and (await apply(n,c)).duplicate
    value=await read(n,section='evidence')
    item=value['items'][0]['payload']
    assert item['quote']==json.loads(c.payload)['command']['quote'] and item['verification']=='accepted_text_match'
    assert item['physical_page_index']==1 and item['visual_confirmation'] is None
    assert item['page_provenance']['printed_page_number']=='105'
    assert item['region_provenance']['bbox']==BBOX
    assert item['region_provenance']['native_block_index']==7
    assert len(value['source_dependencies'])==1
    async with n['connection'].transaction():
        await n['connection'].execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s',(SOURCE,))
    assert await read(n) is None and await read(n,section='history') is None
    async with n['connection'].transaction():
        assert (await(await n['connection'].execute('SELECT rkb_v4.read_disagreement_operation_v1(%s,%s,%s)',(CASE,ACTOR,c.operation_key))).fetchone())[0] is None


@pytest.mark.asyncio
async def test_case_grant_and_own_correction_are_independent_of_global_owner(native):
    n=native;await create_case(n);await grant(n,role='viewer')
    denied=await next_command(n,'contribute',ContributionAdd(kind='opinion',original_text='Own exact opinion.'),actor=OTHER)
    assert (await apply(n,denied)).status=='rejected_precondition'
    await grant(n)
    c=await next_command(n,'contribute',ContributionAdd(kind='opinion',original_text='Own exact opinion.'),actor=OTHER)
    assert (await apply(n,c)).status=='applied'
    wrong=await next_command(n,'correct_contribution',ContributionCorrect(original_text='Manager cannot rewrite another author.',reason='Denied'),object_version=1)
    assert (await apply(n,wrong)).status=='rejected_precondition'
    correct=await next_command(n,'correct_contribution',ContributionCorrect(original_text='Own corrected wording.',reason='Exact correction'),actor=OTHER,object_version=1)
    assert (await apply(n,correct)).status=='applied'
    rows=(await read(n,section='contributions'))['items']
    assert [r['payload']['original_text'] for r in rows]==['Own exact opinion.','Own corrected wording.']
    assert rows[1]['actor_id']==OTHER and rows[1]['payload']['human_reading_confirmed'] is False
    await grant(n,role=None)
    assert await read(n,actor=OTHER) is None


@pytest.mark.asyncio
async def test_author_private_draft_version2_selected_publish_is_atomic_and_keeps_unselected_text_private(native):
    n=native;await create_case(n);await grant(n,role='manager')
    text='Private preface. Only this exact selected conclusion. Private remaining analysis.'
    first=await next_command(n,'draft_save',PersonalDraft(text=text,based_on_seq=1),object_id=DRAFT)
    assert (await apply(n,first)).status=='applied'
    second=await next_command(n,'draft_save',PersonalDraft(text=text+' Revision two.',based_on_seq=1),object_id=DRAFT,object_version=1)
    assert (await apply(n,second)).status=='applied'
    assert (await read(n,actor=OTHER,section='draft_list'))['items']==[]
    assert await read(n,actor=OTHER,section='draft_get',draft_id=DRAFT) is None
    assert len((await read(n,section='history'))['items'])==2 # create+grant only
    start=text.index('Only');end=text.index(' Private remaining')
    c=await next_command(n,'draft_publish',DraftPublication(text_start=start,text_end=end),object_id=DRAFT,object_version=2)
    assert (await apply(n,c)).status=='applied'
    rows=(await read(n,actor=OTHER,section='contributions'))['items']
    assert rows[0]['payload']['original_text']==text[start:end]
    assert 'Private preface' not in json.dumps(rows) and 'Private remaining' not in json.dumps(rows)
    own=await read(n,section='draft_get',draft_id=DRAFT,version=2)
    assert own['payload']['text']==text+' Revision two.' and len(own['publication_ids'])==1
    assert (await apply(n,c)).duplicate and len((await read(n,section='contributions'))['items'])==1
