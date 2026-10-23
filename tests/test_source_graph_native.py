"""Opt-in existing bounded native17 fixture; never starts services/models."""
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path

import pytest
import pytest_asyncio

from regional_knowledge.kaggle_apply import HandlerRegistry, HandlerSpec, PgFunction, ApplyExecutionError
from pg_schema_metadata import initialize_fixture_database as initialize_database
from regional_knowledge.kaggle_domain_bindings import DOMAIN_REVISIONS
from regional_knowledge.source_import import IMPORT_PRECONDITIONS
from regional_knowledge.source_import_v4 import IMPORT_V4_DEPENDENCY
from regional_knowledge.source_graph import resolved_graph_import_command
from test_source_graph import payload,PERSON
from test_source_import import SOURCE
from test_source_story_native import connect,apply
from test_kaggle_apply import pinned
from test_recovery_journal import fixture as journal_fixture

ROOT=Path(__file__).resolve().parents[1]
pytestmark=pytest.mark.skipif(not os.getenv('RKB_STORY_FIXTURE_SOCKET'),reason='explicit existing bounded native fixture required')


@pytest_asyncio.fixture
async def native():
    connection,fd=await connect()
    try:
        files=sorted(p for p in (ROOT/'sql/kaggle').glob('[0-9][0-9][0-9]_*.sql') if int(p.name[:3])<=11)
        assert [int(p.name[:3]) for p in files]==list(range(1,12))
        async with connection.transaction():
            assert await(await connection.execute('SELECT current_database(),current_user')).fetchone()==('rkb','rkb_bootstrap')
            assert (await(await connection.execute('SHOW server_version_num')).fetchone())[0].startswith('17')
            await connection.execute('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public')
            assert await(await connection.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")).fetchone()==('0.8.2',)
            assert await(await connection.execute("SELECT to_regnamespace('rkb_v4')")).fetchone()==(None,)
            for path in files[:2]:await connection.execute(path.read_text(),prepare=False)
        await initialize_database(connection,database_generation='graph-native-fixture',writer_generation='writer-1',revisions=DOMAIN_REVISIONS)
        async with connection.transaction():
            for path in files[2:]:await connection.execute(path.read_text(),prepare=False)
            await connection.execute("SELECT rkb_v4.publish_read_frontier('graph-native-fixture','writer-1',0,0,true,true)")
        check=await pinned(connection,'check_source_import_graph_v1');effect=await pinned(connection,'apply_source_import_graph_v1')
        registry=HandlerRegistry([HandlerSpec('domain_command','source.import.v4',DOMAIN_REVISIONS,
            check,effect,IMPORT_PRECONDITIONS,dependency=IMPORT_V4_DEPENDENCY)])
        _,journal=journal_fixture()
        yield dict(connection=connection,registry=registry,journal=journal,generation='graph-native-fixture')
    finally:
        await connection.rollback()
        async with connection.transaction():await connection.execute('DROP SCHEMA IF EXISTS rkb_v4 CASCADE')
        await connection.close();os.close(fd)


def command():
    value,_=payload()
    # Independent graph claims survive with no narrative Story.
    value['semantic']['stories']=[];value['semantic']['pending']=[]
    return resolved_graph_import_command(sequence=1,operation_key='graph-import-fixture',request_id='graph-request',
        person_id=PERSON,logical_time='2026-10-20T19:00:00Z',source_id=SOURCE,payload=value)


@pytest.mark.asyncio
async def test_native_atomic_source_graph_five_kinds_exact_full_hash_retry_public_dependency_and_withdrawal(native):
    c=command();receipt=await apply(native,c);assert receipt.status=='applied'
    assert (await apply(native,c)).duplicate
    connection=native['connection']
    async with connection.transaction():
        assert await(await connection.execute('SELECT count(*) FROM rkb_v4.source_entity_ids')).fetchone()==(5,)
        assert await(await connection.execute('SELECT count(*) FROM rkb_v4.source_entity_mentions')).fetchone()==(5,)
        assert await(await connection.execute('SELECT count(*) FROM rkb_v4.source_entity_finding_links')).fetchone()==(2,)
        assert await(await connection.execute('SELECT count(*) FROM rkb_v4.stories')).fetchone()==(0,)
        assert await(await connection.execute('SELECT payload_sha256 FROM rkb_v4.source_import_origins')).fetchone()==(hashlib.sha256(c.payload).hexdigest(),)
        assert await(await connection.execute('SELECT rkb_v4.verify_source_graph_origins_v1(%s::uuid[])',([SOURCE],))).fetchone()==(True,)
        assert await(await connection.execute('SELECT rkb_v4.verify_source_story_origins_v3(%s::uuid[])',([SOURCE],))).fetchone()==(True,)
        graph=json.loads(c.payload)['source_graph'];person=next(e['entity_id'] for e in graph['entities'] if e['kind']=='person')
        rows=await(await connection.execute("SELECT * FROM rkb_v4.read_source_graph_v1(%s,NULL,'entity',NULL,NULL,3)",(person,))).fetchall()
        assert len(rows)==1 and str(rows[0][0])==SOURCE and rows[0][4]['kind']=='person'
        relations=await(await connection.execute("SELECT data FROM rkb_v4.read_source_graph_v1(%s,NULL,'relations',NULL,NULL,3)",(person,))).fetchall()
        assert relations[0][0]['kind']=='founded_by' and relations[0][0]['target_id']==person
        await connection.execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s',(SOURCE,))
        assert await(await connection.execute("SELECT * FROM rkb_v4.read_source_graph_v1(%s,NULL,'entity',NULL,NULL,3)",(person,))).fetchall()==[]


@pytest.mark.asyncio
async def test_native_graph_projection_failure_rolls_back_source_and_corruption_breaks_origin_proof(native):
    connection=native['connection'];c=command()
    async with connection.transaction():
        await connection.execute("""CREATE FUNCTION rkb_v4.graph_fixture_failure() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'synthetic graph projection failure';END $$;
         CREATE TRIGGER graph_fixture_failure BEFORE INSERT ON rkb_v4.source_entity_relations
         FOR EACH ROW EXECUTE FUNCTION rkb_v4.graph_fixture_failure()""",prepare=False)
    with pytest.raises(ApplyExecutionError):await apply(native,c)
    async with connection.transaction():
        for table in ('sources','source_graph_documents','source_entity_ids','source_entity_mentions','applied_ops'):
            assert await(await connection.execute('SELECT count(*) FROM rkb_v4.'+table)).fetchone()==(0,)
        assert await(await connection.execute('SELECT applied_seq FROM rkb_v4.recovery_state')).fetchone()==(0,)
        await connection.execute('DROP TRIGGER graph_fixture_failure ON rkb_v4.source_entity_relations')
    assert (await apply(native,c)).status=='applied'
    async with connection.transaction():
        doc=json.loads(c.payload)
        for e in doc['source_graph']['entities']:e['mode']='reuse'
        assert await(await connection.execute('SELECT rkb_v4.check_source_graph_v1(%s::jsonb,%s)',(json.dumps(doc),SOURCE))).fetchone()==(True,)
        doc['source_graph']['owner_person_id']='60000000-0000-0000-0000-000000000099'
        assert await(await connection.execute('SELECT rkb_v4.check_source_graph_v1(%s::jsonb,%s)',(json.dumps(doc),SOURCE))).fetchone()==(False,)
        await connection.execute('ALTER TABLE rkb_v4.source_entity_relations DISABLE TRIGGER USER')
        await connection.execute("UPDATE rkb_v4.source_entity_relations SET data=jsonb_set(data,'{kind}','\"occurred_at\"')")
        assert await(await connection.execute('SELECT rkb_v4.verify_source_graph_origins_v1(%s::uuid[])',([SOURCE],))).fetchone()==(False,)
