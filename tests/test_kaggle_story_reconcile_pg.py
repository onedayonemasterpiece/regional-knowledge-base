"""REAL PostgreSQL 17 source regression in the repository's EXISTING CI PG fixture.

Only the preconfigured GitHub Actions RKB_KAGGLE_TEST_DSN is allowed. No Dev PG,
no Docker launch, no credentials inferred from the environment, no changes to
CI, no real data. The disposable SQL001–012 + proposed SQL016 transaction is
rolled back. This is NOT ordered 013–016 migration, activation or live acceptance.
"""
from __future__ import annotations

import base64
from dataclasses import replace
import os
from pathlib import Path
from uuid import UUID

import pytest

from regional_knowledge.kaggle_story_reconcile import ReconcileIntent, _digest
from regional_knowledge.kaggle_story_reconcile_native import (
    canonical, case_child_key, intent_bytes, native_case_receipt_key,
    resolved_reconcile_command, sha,
)
from test_kaggle_story_reconcile import ACTOR, A, B, CASE, comparison, snapshot

ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.asyncio, pytest.mark.skipif(
    not os.environ.get('RKB_KAGGLE_TEST_DSN'),
    reason='Only the existing GitHub-hosted PostgreSQL fixture may execute SQL016')]


def _validated_ci_dsn():
    from psycopg.conninfo import conninfo_to_dict
    dsn = os.environ['RKB_KAGGLE_TEST_DSN']
    assert os.environ.get('GITHUB_ACTIONS') == 'true'
    assert os.environ.get('GITHUB_REPOSITORY') == 'onedayonemasterpiece/regional-knowledge-base'
    assert conninfo_to_dict(dsn) == {
        'host': '127.0.0.1', 'port': '54329', 'dbname': 'rkb_graph_test',
        'user': 'postgres', 'password': 'graph_fixture_only',
    }
    return dsn


class FixtureRollback(Exception):
    """Abort the entire disposable fixture transaction, even after assertions."""


async def _source(conn, source):
    p = source.proofs[0]
    await conn.execute('INSERT INTO rkb_v4.sources VALUES (%s,%s,true,false)',
                       (source.source_id,'synthetic bibliography'))
    await conn.execute('INSERT INTO rkb_v4.revisions VALUES (%s,1)',(source.source_id,))
    await conn.execute('INSERT INTO rkb_v4.active_revision VALUES (%s,1)',(source.source_id,))
    # SQL004 visibility trigger fires on both source and active-revision insert.
    visible=(await(await conn.execute('SELECT eligibility_revision FROM rkb_v4.source_visibility '
                                     'WHERE source_id=%s',(source.source_id,))).fetchone())[0]
    assert type(visible) is int and visible >= 1
    from psycopg.types.json import Jsonb
    await conn.execute('''INSERT INTO rkb_v4.source_import_origins
        VALUES (%s,1,%s,%s,%s,1,'synthetic://original',%s,%s,'model-test',
                'embedding-test','tokenizer-test','time-test')''',
        (source.source_id,'70000000-0000-0000-0000-000000000001',
         '70000000-0000-0000-0000-000000000002',p.source_sha256,'b'*64,'c'*64))
    await conn.execute('''INSERT INTO rkb_v4.source_research_frontier
        VALUES (%s,1,%s,'complete',%s)''',
        (source.source_id,source.frontier_id,Jsonb({})))
    await conn.execute('''INSERT INTO rkb_v4.source_regions
        VALUES (%s,1,%s,%s,%s,%s)''',
        (source.source_id,p.region_id,p.page_id,'abcd'+p.quote,
         sha(('abcd'+p.quote).encode())))
    await conn.execute('''INSERT INTO rkb_v4.story_assertion_versions
        VALUES (%s,1,%s,1,%s,'Synthetic factual assertion',%s)''',
        (p.assertion_id,source.source_id,'candidate-'+source.source_id[-1],Jsonb(dict(
            evidence=[dict(region_id=p.region_id,page_id=p.page_id,
                original_excerpt=p.quote,start=4,end=4+len(p.quote))],
            event_dates=[],observations=[],entity_keys=[]))))
    return replace(source,eligibility_revision=visible)


async def _case(conn,case_id):
    from psycopg.types.json import Jsonb
    await conn.execute('''INSERT INTO rkb_v4.disagreement_cases
       VALUES (%s,%s,1,1,0,'open',NULL,%s)''',
       (case_id,ACTOR,Jsonb(dict(positions=[dict(id='left',text='Synthetic witness')]))))


async def _intent(conn, a, b, *, action='add_evidence', comparison_key=None):
    from psycopg.types.json import Jsonb
    cmp=comparison(a,b,action=action)
    if comparison_key is not None:
        cmp=cmp.model_copy(update={'comparison_key':comparison_key})
    parent='story-reconcile:'+_digest([ACTOR,a.frontier_id,cmp.comparison_key])
    intent=ReconcileIntent(ACTOR,parent,_digest(cmp.model_dump(mode='json')),a,b,cmp)
    raw=intent_bytes(intent)
    await conn.execute('''INSERT INTO rkb_v4.story_reconciliation_intents
      (operation_key,actor_id,source_id,source_revision,source_eligibility_revision,source_frontier_id,
       peer_source_id,peer_source_revision,peer_eligibility_revision,assertion_id,peer_assertion_id,
       comparison_key,request_sha256,intent_sha256,intent_bytes,immutable_request,
       current_step,attempted,case_id,progress_version)
       VALUES (%s,%s,%s,1,%s,%s,%s,1,%s,%s,%s,%s,%s,%s,%s,%s,0,true,%s,2)''',
       (parent,ACTOR,A,a.eligibility_revision,a.frontier_id,B,b.eligibility_revision,
        a.proofs[0].assertion_id,b.proofs[0].assertion_id,cmp.comparison_key,
        intent.request_sha256,sha(raw),raw,Jsonb(__import__('json').loads(raw)),
        CASE if action=='add_evidence' else None))
    return intent


async def _accepted_case_receipt(conn,intent,selected_case,*,sequence=1):
    """Seed a synthetic, exact Case-shaped accepted receipt for SQL predicate testing.

    This is fixture evidence, not a real accepted user operation or live permit.
    """
    from regional_knowledge.disagreement_native import resolved_case_command
    from psycopg.types.json import Jsonb
    plan=intent.comparison.case_plan
    action='create' if plan.action=='create' else 'add_evidence'
    command_model=plan.create if action=='create' else plan.evidence[0]
    key=native_case_receipt_key(ACTOR,intent.operation_key,0)
    command=resolved_case_command(sequence=sequence,operation_key=key,request_id='fixture-case',
        person_id=ACTOR,logical_time='2026-10-22T00:00:00Z',case_id=selected_case,
        object_id=(selected_case if action=='create' else '80000000-0000-0000-0000-000000000001'),
        action=action, command=command_model,
        source_dependencies=([] if action=='create' else [dict(source_id=A,
            content_revision=1,eligibility_revision=intent.anchor_source.eligibility_revision)]),
        attribution=dict(person_id=ACTOR,recorded_by=ACTOR,route='owner',link_id=None,
                         link_revision=None,human_reading_confirmed=False))
    result=canonical(dict(case_id=selected_case,action=action))
    command_bytes=command.encode()
    outcome=canonical(dict(codec='rkb.applied-outcome.v1',status='applied',
        result_b64=base64.b64encode(result).decode()))
    await conn.execute('''INSERT INTO rkb_v4.applied_ops VALUES
        (%s,%s,%s,%s,%s,'fixture-generation','fixture-writer','applied',%s,%s,%s)''',
        (key,sequence,command_bytes,sha(command_bytes),'a'*64,outcome,sha(outcome),'b'*64))
    # SQL010 native trigger must capture a verifiable Case receipt.
    assert (await(await conn.execute('''SELECT case_id::text FROM rkb_v4.disagreement_receipts
        WHERE operation_key=%s''',(key,))).fetchone())[0] == selected_case


async def test_sql016_compiles_and_rejects_case_a_plan_case_b_receipt_on_real_pg():
    import psycopg
    from psycopg.types.json import Jsonb
    dsn = _validated_ci_dsn()
    connection=await psycopg.AsyncConnection.connect(dsn,connect_timeout=2,sslmode='disable')
    try:
        try:
            async with connection.transaction():
                assert (await(await connection.execute('SHOW server_version_num')).fetchone())[0].startswith('17')
                assert (await(await connection.execute("SELECT to_regnamespace('rkb_v4')")).fetchone())[0] is None
                await connection.execute('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public')
                for ordinal in range(1,13):
                    paths=list((ROOT/'sql/kaggle').glob(f'{ordinal:03d}_*.sql'))
                    assert len(paths)==1, f'exact SQL{ordinal:03d} fixture missing'
                    await connection.execute(paths[0].read_text(),prepare=False)
                # SQL016 contains narrow grants for two installed roles. This
                # disposable CI cluster has no deployment roles: create only
                # NOLOGIN fixture identities in the SAME rolled-back transaction.
                existing=await(await connection.execute("""SELECT rolname FROM pg_roles
                    WHERE rolname IN ('rkb_writer','rkb_reader')""")).fetchall()
                assert existing==[], 'fixture must never change a pre-existing role'
                await connection.execute('CREATE ROLE rkb_reader NOLOGIN')
                await connection.execute('CREATE ROLE rkb_writer NOLOGIN')
                # SQL013–015 are not yet approved. This PG probe is NOT an ordered upgrade.
                await connection.execute((ROOT/'sql/kaggle/016_story_reconcile.sql').read_text(),prepare=False)
                assert (await(await connection.execute("SELECT to_regprocedure("
                    "'rkb_v4.story_reconcile_source_current_v1(uuid,bigint,bigint,uuid,uuid,text,uuid,uuid,integer,text)')"))
                    .fetchone())[0] is not None, 'Missing handler is a FAIL, never a success/skip'
                await connection.execute("INSERT INTO rkb_v4.writer_generations VALUES ('fixture-writer',NULL,false)")
                await connection.execute('''INSERT INTO rkb_v4.recovery_state
                    (singleton,database_generation,writer_generation,applied_seq,
                     schema_revision,function_revision,executor_revision)
                    VALUES (true,'fixture-generation','fixture-writer',0,
                     'fixture-sql016','fixture-function016','fixture-python')''')
                a=await _source(connection,snapshot(A))
                b=await _source(connection,snapshot(B))
                await _case(connection,CASE)
                case_b='20000000-0000-0000-0000-000000000002'
                await _case(connection,case_b)
                value=await _intent(connection,a,b)
                p=a.proofs[0]
                valid=(await(await connection.execute('''SELECT rkb_v4.story_reconcile_source_current_v1(
                    %s::uuid,1,%s,%s::uuid,%s::uuid,%s,%s::uuid,%s::uuid,%s,%s)''',
                    (A,a.eligibility_revision,a.frontier_id,p.assertion_id,
                     p.source_sha256,p.page_id,p.region_id,4,p.quote))).fetchone())[0]
                assert valid is True, 'actual SQL revision parameter and source proof must resolve'
                # Verify both sources and the complete model-authored intent
                # are CURRENT; otherwise a false check could be a false positive.
                import json
                assert (await(await connection.execute('''SELECT rkb_v4.story_reconcile_intent_current_v1(%s,%s::uuid)''',
                    (Jsonb(json.loads(intent_bytes(value))),ACTOR))).fetchone())[0] is True
                await _accepted_case_receipt(connection,value,case_b)
                plan=Jsonb(value.comparison.case_plan.model_dump(mode='json'))
                assert (await(await connection.execute('''SELECT rkb_v4.story_reconcile_case_receipt_v1(
                    %s,%s::uuid,0,'applied',%s::uuid,%s)''',
                    (value.operation_key,ACTOR,case_b,plan))).fetchone())[0] is True
                command_b=resolved_reconcile_command(action='advance',intent=value,
                    sequence=2,request_id='test-case-B',logical_time='test',version=2,step=0,
                    case_id=case_b)
                check_b=(await(await connection.execute('''SELECT rkb_v4.check_story_reconcile_v1(
                    %s,%s,%s,%s)''',(command_b.payload,Jsonb(dict(command_b.ids)),
                    Jsonb(dict(command_b.expected_revisions)),command_b.logical_time))).fetchone())[0]
                assert check_b is False, 'valid CaseB receipt MUST NOT replace CaseA plan'
                command_a=resolved_reconcile_command(action='advance',intent=value,
                    sequence=2,request_id='test-case-A',logical_time='test',version=2,step=0,
                    case_id=CASE)
                check_a=(await(await connection.execute('''SELECT rkb_v4.check_story_reconcile_v1(
                    %s,%s,%s,%s)''',(command_a.payload,Jsonb(dict(command_a.ids)),
                    Jsonb(dict(command_a.expected_revisions)),command_a.logical_time))).fetchone())[0]
                assert check_a is False, 'native receipt is for CaseB, not plan CaseA'
                state=(await(await connection.execute('''SELECT progress_version,current_step,case_id::text
                    FROM rkb_v4.story_reconciliation_intents WHERE operation_key=%s''',
                    (value.operation_key,))).fetchone())
                assert state==(2,0,CASE), 'a rejected transition cannot mutate native PG progress'

                # Positive: first native CREATE is the ONLY legal case-ID
                # assignment from null; later add_evidence cannot replace it.
                first=await _intent(connection,a,b,action='create',comparison_key='first-create-case')
                assert (await(await connection.execute('''SELECT rkb_v4.story_reconcile_intent_current_v1(%s,%s::uuid)''',
                    (Jsonb(json.loads(intent_bytes(first))),ACTOR))).fetchone())[0] is True
                new_case='20000000-0000-0000-0000-000000000003'
                await _case(connection,new_case)
                await _accepted_case_receipt(connection,first,new_case,sequence=2)
                create_cmd=resolved_reconcile_command(action='advance',intent=first,
                    sequence=3,request_id='first-create',logical_time='test',version=2,step=0,
                    case_id=new_case)
                args=(create_cmd.payload,Jsonb(dict(create_cmd.ids)),
                      Jsonb(dict(create_cmd.expected_revisions)),create_cmd.logical_time)
                assert (await(await connection.execute('''SELECT rkb_v4.check_story_reconcile_v1(%s,%s,%s,%s)''',args)).fetchone())[0] is True
                native=(await(await connection.execute('''SELECT rkb_v4.apply_story_reconcile_v1(%s,%s,%s,%s)''',args)).fetchone())[0]
                assert json.loads(native)['case_id']==new_case
                assert (await(await connection.execute('''SELECT progress_version,current_step,case_id::text
                    FROM rkb_v4.story_reconciliation_intents WHERE operation_key=%s''',
                    (first.operation_key,))).fetchone()) == (3,1,new_case)
                raise FixtureRollback()
        except FixtureRollback:
            pass
        assert (await(await connection.execute("SELECT to_regnamespace('rkb_v4')")).fetchone())[0] is None
    finally:
        await connection.close()
