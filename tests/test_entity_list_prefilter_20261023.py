"""SQL022 entity_list selectors: one native page, exact Unicode and source receipts.

Real SQL scenarios run only in the already authorized, isolated PG17 fixture.
They do not start a Kaggle worker, apply SQL to live PG or stage content.
"""
from contextlib import asynccontextmanager
from pathlib import Path
import os

import psycopg
import pytest
import pytest_asyncio
from psycopg.rows import tuple_row

from regional_knowledge.entity_graph import GraphBundle
from regional_knowledge.kaggle_apply import HandlerRegistry, HandlerSpec
from regional_knowledge.kaggle_domain_bindings import DOMAIN_REVISIONS
from regional_knowledge.kaggle_source_graph_read import (
    SourceGraphReadRequest, execute_source_graph_read,
)
from regional_knowledge.source_graph import resolve_source_graph, resolved_graph_import_command
from regional_knowledge.source_import import IMPORT_PRECONDITIONS
from regional_knowledge.source_import_v4 import IMPORT_V4_DEPENDENCY
from regional_knowledge.kaggle_apply import PgFunction
from pg_schema_metadata import initialize_fixture_database
from test_source_graph import material, PERSON
from test_source_import import SOURCE
from test_source_manifest import manifest_payload
from test_source_story_native import apply
from test_kaggle_public_illustration_native_read import _validated_ci_dsn
from test_kaggle_apply import pinned
from test_recovery_journal import fixture as journal_fixture
from test_kaggle_source_graph_read import request as old_request, row as original_row
from test_kaggle_fact_read import Native

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    not os.getenv('RKB_KAGGLE_TEST_DSN'),
    reason='existing authorized GitHub Actions PG17 service only',
)


def selected(**updates):
    """Existing public list schema and actor: no widened inputs or scope."""
    args=dict(request_id='entity-list-sql022',connection_generation=2,turn_generation=3,
        action='list',source_id=SOURCE,entity_id=None,limit=2,min_data_seq=0,
        deadline_ms=2000,compute_claim='entity-list-sql022-read',
        actor=dict(person_id=PERSON,audience='personal',auth_epoch=1,visibility_epoch=4))
    args.update(updates)
    return SourceGraphReadRequest.model_validate(args)


def long_graph_command():
    """42 actual model-authored entities, all with real staged evidence."""
    doc,graph=material()
    prototype=graph.entity_candidates[0].entities[0]
    noise=[prototype.model_copy(update=dict(
        key=f'noise-{index:02d}',kind='person',
        canonical_label=f'Unrelated entry {index:02d}',aliases=[]))
        for index in range(36)]
    aliases=prototype.aliases
    chosen=[prototype.model_copy(update=dict(
        key=f'zz-case-{index}',kind='organization',
        canonical_label=f'Straße number {index}',
        aliases=[aliases[0].model_copy(update={'value':'İSTANBUL'})] if index==1 else []))
        for index in range(3)]
    graph=graph.model_copy(update={'entity_candidates':[
        graph.entity_candidates[0],GraphBundle(entities=noise[:30]),
        GraphBundle(entities=[*noise[30:],*chosen])]})
    payload=manifest_payload()
    payload['semantic']=doc['semantic']
    payload['source_graph']=resolve_source_graph(
        graph,source_id=SOURCE,person_id=PERSON,semantic=payload['semantic'])
    assert len(payload['source_graph']['entities'])==44
    return resolved_graph_import_command(
        sequence=1,operation_key='entity-list-prefilter-real-pg',
        request_id='entity-list-prefilter-real-pg',
        person_id=PERSON,logical_time='2026-10-23T00:00:00Z',
        source_id=SOURCE,payload=payload)


@pytest_asyncio.fixture
async def graph_sql22():
    """Full ordinary SQL001..022 prefix on a fresh authorized fixture DB."""
    connection=await psycopg.AsyncConnection.connect(
        _validated_ci_dsn(),autocommit=True,row_factory=tuple_row)
    try:
        scripts=sorted((ROOT/'sql/kaggle').glob('[0-9][0-9][0-9]_*.sql'))
        assert [int(path.name[:3]) for path in scripts]==list(range(1,23))
        async with connection.transaction():
            assert await (await connection.execute('SELECT current_database(),current_user')).fetchone()==('rkb_graph_test','postgres')
            assert (await (await connection.execute('SHOW server_version_num')).fetchone())[0].startswith('17')
            assert await (await connection.execute("SELECT to_regnamespace('rkb_v4')")).fetchone()==(None,)
            await connection.execute('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public')
            assert await (await connection.execute(
                "SELECT extversion FROM pg_extension WHERE extname='vector'")).fetchone()==('0.8.2',)
            for path in scripts[:2]:
                await connection.execute(path.read_text(),prepare=False)
        await initialize_fixture_database(connection, database_generation='sql22-entity-fixture',
            writer_generation='writer-1',revisions=DOMAIN_REVISIONS,schema_version=22)
        async with connection.transaction():
            for path in scripts[2:]:
                await connection.execute(path.read_text(),prepare=False)
            await connection.execute(
                "SELECT rkb_v4.publish_read_frontier('sql22-entity-fixture','writer-1',0,0,true,true)")
        check=await pinned(connection,'check_source_import_graph_v1')
        effect=await pinned(connection,'apply_source_import_graph_v1')
        registry=HandlerRegistry([HandlerSpec('domain_command','source.import.v4',DOMAIN_REVISIONS,
            check,effect,IMPORT_PRECONDITIONS,dependency=IMPORT_V4_DEPENDENCY)])
        _,journal=journal_fixture()
        native=dict(connection=connection,registry=registry,journal=journal,generation='sql22-entity-fixture')
        yield native
    finally:
        await connection.rollback()
        async with connection.transaction():
            await connection.execute('DROP SCHEMA IF EXISTS rkb_v4 CASCADE')
        await connection.close()


@pytest.mark.asyncio
async def test_real_pg17_sparse_kinds_query_before_limit_unicode_and_withdrawal(graph_sql22):
    native=graph_sql22
    receipt=await apply(native,long_graph_command())
    assert receipt.status=='applied'
    connection=native['connection']
    async with connection.transaction():
        # Legacy 8x(1+1) cannot reach the three rare entities at keys zz-*.
        after_source=after_key=None
        for scan in range(8):
            old=await (await connection.execute(
                "SELECT * FROM rkb_v4.read_source_graph_v1(NULL,%s::uuid,'list',%s::uuid,%s,2)",
                (SOURCE,after_source,after_key))).fetchall()
            assert len(old)==2 and not any('Straße' in row[4]['canonical_label'] for row in old)
            after_source,after_key=old[-1][0],old[-1][3]
        filtered=await (await connection.execute(
            "SELECT * FROM rkb_v4.read_source_graph_filtered_v1(%s::uuid,NULL,NULL,%s::text[],%s,4)",
            (SOURCE,['organization'],'strasse'))).fetchall()
        assert len(filtered)==3
        assert [r[4]['canonical_label'] for r in filtered]==[
            'Straße number 0','Straße number 1','Straße number 2']
        assert all(r[2]>=1 for r in filtered)
        assert all('entity_id' in r[4] and r[5] and r[6] for r in filtered)
        by_alias=await (await connection.execute(
            "SELECT record_id FROM rkb_v4.read_source_graph_filtered_v1(%s::uuid,NULL,NULL,NULL,%s,4)",
            (SOURCE,'i\u0307stanbul'))).fetchall()
        assert [r[0] for r in by_alias]==['zz-case-1']
        none=await (await connection.execute(
            "SELECT * FROM rkb_v4.read_source_graph_filtered_v1(%s::uuid,NULL,NULL,%s::text[],%s,4)",
            (SOURCE,['poi_ref'],'strasse'))).fetchall()
        assert none==[]
        await connection.execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s',(SOURCE,))
        assert await (await connection.execute(
            "SELECT * FROM rkb_v4.read_source_graph_filtered_v1(%s::uuid,NULL,NULL,NULL,%s,4)",
            (SOURCE,'strasse'))).fetchall()==[]


@pytest.mark.asyncio
async def test_real_pg17_source_filtered_cursor_returns_every_rare_entity_and_no_false_tail(graph_sql22):
    native=graph_sql22
    assert (await apply(native,long_graph_command())).status=='applied'
    connection=native['connection']
    @asynccontextmanager
    async def acquire():
        yield connection
    async with connection.transaction():
        # Restore an accurate read frontier after the accepted domain command.
        await connection.execute(
            "SELECT rkb_v4.publish_read_frontier('sql22-entity-fixture','writer-1',1,1,true,true)")
    pages=[]
    cursor=None
    for index in range(3):
        read=await execute_source_graph_read(
            selected(limit=1,kinds=['organization'],query='STRASSE',cursor=cursor),acquire=acquire)
        assert read.status=='ok',read
        assert len(read.records)==1
        assert read.evidence[0].dependencies.source_ids==(SOURCE,)
        pages.append(read.records[0]['data']['canonical_label'])
        cursor=read.next_cursor
        if index<2:
            assert cursor
        else:
            assert cursor is None
    assert pages==['Straße number 0','Straße number 1','Straße number 2']
    missing=await execute_source_graph_read(
        selected(query='not-in-source-7355',kinds=['organization']),acquire=acquire)
    assert missing.status=='ok' and not missing.records and missing.next_cursor is None
    async with connection.transaction():
        await connection.execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s',(SOURCE,))
    withdrawn=await execute_source_graph_read(
        selected(query='STRASSE',kinds=['organization']),acquire=acquire)
    assert withdrawn.status=='ok' and not withdrawn.records and withdrawn.next_cursor is None


@pytest.mark.asyncio
async def test_real_pg17_default_unicode_fold_matches_python_codepoint_rules(graph_sql22):
    connection=graph_sql22['connection']
    examples=['STRASSE','Straße','ẞ','ﬃ','İ','I','ı','ς','Σ','ΟΣ','ŉ','K','Œ',
        'КЁНИГСБЕРГ','ὈΔΥΣΣΕΎΣ','𝔄','Ⅳ','\u0000'.replace('\u0000','')]
    examples=[x for x in examples if x]
    for value in examples:
        row=await (await connection.execute(
            'SELECT rkb_v4.source_graph_casefold_v1(%s)',(value,))).fetchone()
        assert row[0]==value.casefold(),(value,row[0],value.casefold())


@pytest.mark.asyncio
async def test_new_sql22_single_pg_page_dispatch_keeps_existing_list_contract():
    original=original_row(2)
    n=Native([original],revision=('rkb-v4-sql001-022-v1','current'))
    result=await execute_source_graph_read(selected(query='  ReCoRdEd aLiAs  ',
        kinds=['person'],limit=1),acquire=n.acquire)
    assert result.status=='ok' and len(result.records)==1 and result.next_cursor is None
    calls=[(s,p) for s,p in n.calls if 'read_source_graph_' in s]
    assert len(calls)==1 and 'read_source_graph_filtered_v1' in calls[0][0]
    assert calls[0][1]==(SOURCE,None,None,['person'],'recorded alias',2)
    assert not any('read_source_graph_v1' in s for s,p in calls)
    # Absence of optional selectors preserves original native read and lookahead.
    n.calls.clear()
    without=await execute_source_graph_read(selected(query='',kinds=None),acquire=n.acquire)
    assert without.status=='ok'
    assert len([s for s,p in n.calls if 'read_source_graph_v1' in s])==1


@pytest.mark.asyncio
async def test_pre022_version_uses_old_keyset_without_calling_missing_function():
    n=Native([original_row(2)],revision=('rkb-v4-sql001-021-v1','current'))
    result=await execute_source_graph_read(selected(query='recorded alias',kinds=['person']),
        acquire=n.acquire)
    assert result.status=='ok'
    calls=[s for s,p in n.calls if 'read_source_graph_' in s]
    assert calls and not any('filtered_v1' in statement for statement in calls)
    assert len(calls)<=8


def test_query_source_cursor_shape_and_empty_kinds_stay_current_contract():
    with pytest.raises(ValueError):
        selected(kinds=[])
    with pytest.raises(ValueError):
        selected(source_id=None)
    with pytest.raises(ValueError):
        selected(action='mentions',kinds=['person'])
