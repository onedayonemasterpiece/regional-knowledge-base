"""Explicit isolated PG17/pgvector fixture: atomic Story body and exact replay."""
import base64
from contextlib import asynccontextmanager
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

import psycopg
from psycopg.rows import tuple_row
import pytest
import pytest_asyncio

from regional_knowledge.kaggle_apply import HandlerRegistry,apply_accepted,initialize_database,ApplyExecutionError
from regional_knowledge.source_import_v2 import STORY_REVISIONS,source_import_v2_handler
from test_kaggle_apply import pinned
from test_recovery_journal import fixture as journal_fixture
from test_source_import_v2 import payload,command,STORY,VARIANT,FINDING
from test_source_import import SOURCE

ROOT=Path(__file__).resolve().parents[1]
SOCKET=os.getenv('RKB_STORY_FIXTURE_SOCKET')
CONTAINER=os.getenv('RKB_STORY_FIXTURE_CONTAINER')
pytestmark=pytest.mark.skipif(not SOCKET,reason='explicit isolated native Story fixture required')


async def connect():
    socket=Path(SOCKET)
    assert socket.name=='socket' and socket.parent.name=='pg'
    assert socket.resolve().is_relative_to('/home/dev/artifacts/regional-knowledge-base')
    metadata=json.loads((socket.parent.parent/'.artifact.json').read_text())
    assert metadata['project']=='regional-knowledge-base'
    assert re.fullmatch('rkb-v4-story-fixture-[a-z0-9-]{1,32}',CONTAINER)
    info=json.loads(subprocess.run(['docker','inspect',CONTAINER],capture_output=True,text=True,check=True,timeout=3).stdout)[0]
    assert info['State']['Running'] and info['HostConfig']['NetworkMode']=='none'
    assert info['Config']['Labels']['rkb.data_class']=='test_fixture'
    assert 0<info['HostConfig']['Memory']<=256*1024**2
    assert any(m['Source']==str(socket) and m['Destination']=='/fixture-socket' for m in info['Mounts'])
    fd=os.open(socket,os.O_RDONLY|os.O_DIRECTORY)
    try:
        connection=await psycopg.AsyncConnection.connect(host='/proc/self/fd/'+str(fd),dbname='rkb',
            user='rkb_bootstrap',password='',sslmode='disable',connect_timeout=2,row_factory=tuple_row)
    except BaseException:os.close(fd);raise
    return connection,fd


async def initialize(connection,generation):
    async with connection.transaction():
        assert await(await connection.execute('SELECT current_database(),current_user')).fetchone()==('rkb','rkb_bootstrap')
        assert (await(await connection.execute('SHOW server_version_num')).fetchone())[0].startswith('17')
        await connection.execute('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public')
        assert await(await connection.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")).fetchone()==('0.8.2',)
        assert await(await connection.execute("SELECT to_regnamespace('rkb_v4')")).fetchone()==(None,)
        for name in ('001_search.sql','002_recovery.sql'):
            await connection.execute((ROOT/'sql/kaggle'/name).read_text(),prepare=False)
    await initialize_database(connection,database_generation=generation,writer_generation='writer-1',revisions=STORY_REVISIONS)
    async with connection.transaction():
        for name in ('003_search_scope.sql','004_read_fences.sql','005_source_withdraw.sql','006_source_import.sql','007_source_stories.sql'):
            await connection.execute((ROOT/'sql/kaggle'/name).read_text(),prepare=False)
        await connection.execute('SELECT rkb_v4.publish_read_frontier(%s,%s,0,0,true,true)',(generation,'writer-1'))


@pytest_asyncio.fixture
async def native():
    connection,fd=await connect()
    try:
        await initialize(connection,'story-fixture-one')
        check=await pinned(connection,'check_source_import_v2');apply=await pinned(connection,'apply_source_import_v2')
        registry=HandlerRegistry([source_import_v2_handler(revisions=STORY_REVISIONS,
            check_sha256=check.definition_sha256,apply_sha256=apply.definition_sha256)])
        _,journal=journal_fixture()
        yield {'connection':connection,'registry':registry,'journal':journal,'generation':'story-fixture-one'}
    finally:
        await connection.rollback()
        async with connection.transaction():await connection.execute('DROP SCHEMA IF EXISTS rkb_v4 CASCADE')
        await connection.close();os.close(fd)


async def apply(native,c):
    journal=native['journal'];seal=await journal.locate_accepted(c)
    if seal is None:seal=await journal.seal([c],predecessor=await journal.head(),writer_generation='writer-1')
    return await apply_accepted(native['connection'],journal=journal,seal=seal,command=c,
        database_generation=native['generation'],writer_generation='writer-1',registry=native['registry'])


async def read(connection,view='get'):
    async with connection.transaction():
        return (await(await connection.execute("SELECT rkb_v4.read_story_v1(%s,%s,NULL,0,10)",(STORY,view))).fetchone())[0]


@pytest.mark.asyncio
async def test_native_body_default_reference_retry_reconnect_and_withdrawal(native):
    c=command();receipt=await apply(native,c);assert receipt.status=='applied'
    actual=await read(native['connection']);variant=payload()['semantic']['stories'][0]['default_variant']
    assert actual['body']==variant['body'] and actual['body']!=actual['summary']
    assert actual['body_sha256']==hashlib.sha256(variant['body'].encode()).hexdigest()
    assert actual['default_variant_ref']=={'variant_id':VARIANT,'revision':1}
    assert actual['finding_ids']==[FINDING] and actual['narrative_availability']=='available'
    async with native['connection'].transaction():
        lexical=await(await native['connection'].execute("SELECT story_id FROM rkb_v4.story_lexical_candidates_v1('fictional gate',5,NULL)")).fetchall()
        vector='['+','.join(map(str,variant['embedding']))+']'
        dense=await(await native['connection'].execute('SELECT story_id FROM rkb_v4.story_dense_candidates_v1(%s::public.vector,5,NULL)',(vector,))).fetchall()
        hydrated=await(await native['connection'].execute('SELECT record FROM rkb_v4.story_hydrate_v1(%s::uuid[],NULL)',([STORY],))).fetchone()
        assert [str(x[0]) for x in lexical]==[STORY] and [str(x[0]) for x in dense]==[STORY]
        assert hydrated[0]['body']==actual['body']
    assert (await apply(native,c)).outcome==receipt.outcome and (await apply(native,c)).duplicate
    reopened,fd=await connect()
    try:assert (await read(reopened))['body']==actual['body']
    finally:await reopened.close();os.close(fd)
    assert (await read(native['connection'],'history'))['entries'][0]['action']=='source_import'
    assert (await read(native['connection'],'variants'))['entries'][0]['body']==actual['body']
    async with native['connection'].transaction():
        await native['connection'].execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s',(SOURCE,))
    assert await read(native['connection']) is None
    assert await read(native['connection'],'history') is None
    assert await read(native['connection'],'variants') is None


@pytest.mark.asyncio
async def test_mid_story_failure_rolls_back_source_frontier_and_receipt(native):
    conn=native['connection'];c=command()
    async with conn.transaction():
        await conn.execute("""CREATE FUNCTION rkb_v4.synthetic_story_failure() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'synthetic story failure';END $$;
            CREATE TRIGGER synthetic_story_failure BEFORE INSERT ON rkb_v4.story_search
            FOR EACH ROW EXECUTE FUNCTION rkb_v4.synthetic_story_failure()""",prepare=False)
    with pytest.raises(ApplyExecutionError):await apply(native,c)
    async with conn.transaction():
        for table in ('sources','source_regions','story_assertion_versions','stories','story_versions','story_variant_versions','story_search','source_import_origins','source_research_frontier','applied_ops'):
            assert await(await conn.execute('SELECT count(*) FROM rkb_v4.'+table)).fetchone()==(0,)
        assert await(await conn.execute('SELECT applied_seq FROM rkb_v4.recovery_state')).fetchone()==(0,)
        assert await(await conn.execute('SELECT projection_seq,pending_seq FROM rkb_v4.read_frontier')).fetchone()==(0,None)
        await conn.execute('DROP TRIGGER synthetic_story_failure ON rkb_v4.story_search')
    assert (await apply(native,c)).status=='applied'
    assert (await read(conn))['body']==payload()['semantic']['stories'][0]['default_variant']['body']


@pytest.mark.asyncio
async def test_accepted_body_replay_in_fresh_generation_is_exact(native):
    c=command();first=await apply(native,c);actual=await read(native['connection'])
    async with native['connection'].transaction():await native['connection'].execute('DROP SCHEMA rkb_v4 CASCADE')
    await initialize(native['connection'],'story-fixture-two');native['generation']='story-fixture-two'
    replayed=await apply(native,c)
    assert not replayed.duplicate and replayed.outcome==first.outcome
    assert await read(native['connection'])==actual


@pytest.mark.asyncio
async def test_native_function_rejects_false_quote_before_source_activation(native):
    value=payload();value['semantic']['findings'][0]['evidence'][0]['original_excerpt']='invented claim'
    c=replace(command(),payload=json.dumps(value,sort_keys=True,separators=(',',':')).encode())
    receipt=await apply(native,c)
    assert receipt.status=='rejected_precondition' and await read(native['connection']) is None
    async with native['connection'].transaction():
        assert await(await native['connection'].execute('SELECT count(*) FROM rkb_v4.sources')).fetchone()==(0,)


@pytest.mark.asyncio
async def test_native_story_catalog_keeps_full_internal_trigger_identifiers(native):
    from regional_knowledge.kaggle_database_probe import NativeDatabaseProbe
    @asynccontextmanager
    async def acquire():yield native['connection']
    raw,fields=await NativeDatabaseProbe()(acquire=acquire)
    records=json.loads(raw)['records']
    keys=[(row[0],row[1]) for row in records]
    assert len(keys)==len(set(keys)) and 0<len(keys)<=512 and len(raw)<=1024*1024
    assert any(section=='triggers' and len(key)>63 for section,key in keys)
    assert fields['schema_revision']==STORY_REVISIONS.schema
