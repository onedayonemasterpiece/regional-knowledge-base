"""Real SQL001–048 + title-only historical replay on the existing disposable PG17 CI service."""
import os
from pathlib import Path

import psycopg
import pytest

from regional_knowledge.kaggle_apply import HandlerRegistry, initialize_database
from regional_knowledge.kaggle_schema_migrate import migrate
from regional_knowledge.kaggle_domain_bindings import DOMAIN_REVISIONS
from regional_knowledge.source_import_v4 import source_import_v4_handler
from regional_knowledge.story_editorial import editorial_handler
from regional_knowledge.story_contracts import NativeStoryTitle
from regional_knowledge.story_title_command import title_handler, resolved_native_story_title_command
from test_story_editorial_native import imported, grant, edit, eligibility, read, EDITOR
from test_source_story_native import apply
from test_source_import_v2 import STORY, VARIANT
from test_kaggle_schema_migrate import files
from test_kaggle_public_illustration_native_read import _validated_ci_dsn
from test_kaggle_apply import pinned
from test_recovery_journal import fixture as journal_fixture

pytestmark=pytest.mark.skipif(not os.environ.get('RKB_KAGGLE_TEST_DSN'),
    reason='existing PG17 CI service only; no Dev database')


@pytest.mark.asyncio
async def test_native_title_pg_migration_body_vector_cas_history_saved_and_exact_replay():
    conn=await psycopg.AsyncConnection.connect(_validated_ci_dsn(),autocommit=True)
    owned=False
    try:
        assert (await(await conn.execute("SELECT to_regnamespace('rkb_v4')")).fetchone())==(None,)
        assert await(await conn.execute("SELECT rolname FROM pg_roles WHERE rolname IN ('rkb_reader','rkb_writer')")).fetchall()==[]
        owned=True
        await conn.execute('CREATE ROLE rkb_reader NOLOGIN')
        await conn.execute('CREATE ROLE rkb_writer NOLOGIN')
        await conn.execute('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public')
        artifacts=files()
        assert len(artifacts)>=48
        generation='native-title-ci-one'

        async def setup():
            assert await migrate(conn,artifacts)==tuple(range(1,len(artifacts)+1))
            await initialize_database(conn,database_generation=generation,writer_generation='writer-1')
            await conn.execute('SELECT rkb_v4.publish_read_frontier(%s,%s,0,0,true,true)',(generation,'writer-1'))

        await setup()
        handlers=[]
        for route,check_name,apply_name in (
            ('source.import.v4','check_source_import_v4','apply_source_import_v4'),
            ('story.access.v1','check_story_access_v1','apply_story_access_v1'),
            ('story.edit.v1','check_story_edit_v1','apply_story_edit_v1'),
            ('story.title.v1','check_story_title_v1','apply_story_title_v1'),
        ):
            check=await pinned(conn,check_name);apply_fn=await pinned(conn,apply_name)
            factory=source_import_v4_handler if route=='source.import.v4' else (
                title_handler if route=='story.title.v1' else lambda **kw:editorial_handler(route=route,**kw))
            handlers.append(factory(check_sha256=check.definition_sha256,apply_sha256=apply_fn.definition_sha256))
        _,journal=journal_fixture()
        n=dict(connection=conn,registry=HandlerRegistry(handlers),journal=journal,generation=generation)
        commands=[]
        async def accepted(c,status='applied'):
            commands.append(c)
            receipt=await apply(n,c)
            assert receipt.status==status
            return receipt

        await accepted(imported())
        initial=await read(n)
        assert await(await conn.execute('SELECT rkb_v4.verify_editorial_story_origins_v2(%s::uuid[])',([initial['source']['source_id']],))).fetchone()==(True,)
        await accepted(await grant(n,2,1))
        assert await(await conn.execute('SELECT rkb_v4.verify_editorial_story_origins_v2(%s::uuid[])',([initial['source']['source_id']],))).fetchone()==(True,)
        baseline=await read(n)
        body=baseline['body'];ref=baseline['default_variant_ref']
        original_title=baseline['title']
        variant_before=await(await conn.execute(
            'SELECT body,body_sha256,embedding::text,input_tokens FROM rkb_v4.editorial_variant_versions WHERE variant_id=%s AND revision=1',
            (VARIANT,))).fetchone()
        projection_before=await(await conn.execute(
            'SELECT body,embedding::text FROM rkb_v4.editorial_story_search WHERE story_id=%s',
            (STORY,))).fetchall()
        title=NativeStoryTitle(op='set_metadata',title='Литаврная собака')
        def change(seq,revision,actor=EDITOR,key='title-ci'):
            return resolved_native_story_title_command(sequence=seq,operation_key=key+str(seq),request_id='title-'+str(seq),
                person_id=actor,logical_time='2026-10-31T12:00:00Z',source_id=baseline['source']['source_id'],
                story_id=STORY,expected_revision=revision,source_revision=1,
                eligibility_revision=baseline['source']['eligibility_revision'],operation=title)
        command=change(3,2)
        saved=await accepted(command)
        assert (await apply(n,command)).duplicate and (await apply(n,command)).outcome==saved.outcome
        updated=await read(n)
        assert updated['title']=='Литаврная собака' and updated['revision']==3
        assert updated['body']==body and updated['default_variant_ref']==ref
        assert (await read(n,revision=2))['title']==original_title
        assert (await(await conn.execute(
            'SELECT body,body_sha256,embedding::text,input_tokens FROM rkb_v4.editorial_variant_versions WHERE variant_id=%s AND revision=1',
            (VARIANT,))).fetchone())==variant_before
        assert (await(await conn.execute(
            'SELECT body,embedding::text FROM rkb_v4.editorial_story_search WHERE story_id=%s',
            (STORY,))).fetchall())==projection_before
        assert await(await conn.execute(
            'SELECT count(*) FROM rkb_v4.editorial_variant_versions WHERE story_id=%s',(STORY,))).fetchone()==(1,)
        verified=await(await conn.execute(
            'SELECT rkb_v4.verify_editorial_story_origins_v2(%s::uuid[])',
            ([baseline['source']['source_id']],))).fetchone()
        if verified!=(True,):
            diagnostic=await(await conn.execute("""SELECT jsonb_build_object(
             'title_matches_snapshot',(v.data=p.data||jsonb_build_object('title','Литаврная собака')),
             'action',v.action,'actor_matches',v.actor_id=r.person_id,'receipt_seq',r.sequence,
             'checksum_matches',r.command_sha256=encode(sha256(a.command_bytes),'hex'),
             'role',rkb_v4.story_editor_role_v1(v.story_id,r.person_id),
             'command',convert_from(a.command_bytes,'UTF8')::jsonb)
             FROM rkb_v4.editorial_story_versions v
             JOIN rkb_v4.editorial_story_versions p ON p.story_id=v.story_id AND p.revision=2
             JOIN rkb_v4.editorial_story_receipts r ON r.story_id=v.story_id AND r.revision=v.revision
             JOIN rkb_v4.applied_ops a ON a.operation_key=r.operation_key
             WHERE v.story_id=%s AND v.revision=3""",(STORY,))).fetchone()
            raise AssertionError(f'editorial verifier rejected title: {diagnostic!r}')
        assert await(await conn.execute(
            'SELECT rkb_v4.verify_source_story_origins_v4(%s::uuid[])',
            ([baseline['source']['source_id']],))).fetchone()==(True,)

        await accepted(change(4,2,key='stale-'),'rejected_precondition')
        await accepted(change(5,3,actor='60000000-0000-0000-0000-000000000004',key='viewer-'),'rejected_precondition')
        assert (await read(n))['revision']==3
        await accepted(await edit(n,6,3,variant_revision=1))
        after_variant=await read(n)
        assert after_variant['title']=='Литаврная собака' and after_variant['revision']==4
        assert [r['action'] for r in (await read(n,view='history'))['entries']]==[
            'source_import','access_change','set_metadata','upsert_variant']
        assert await(await conn.execute(
            'SELECT rkb_v4.verify_editorial_story_origins_v2(%s::uuid[])',
            ([baseline['source']['source_id']],))).fetchone()==(True,)

        receipts=[await apply(n,c) for c in commands]
        assert all(r.duplicate for r in receipts)
        original_history=await read(n,view='history')
        await conn.execute('DROP SCHEMA rkb_v4 CASCADE')
        generation='native-title-ci-two';n['generation']=generation
        await setup()
        for c,r in zip(commands,receipts):
            replay=await apply(n,c)
            assert not replay.duplicate and replay.outcome==r.outcome
        assert await read(n,view='history')==original_history
        assert (await read(n))['title']=='Литаврная собака'
        assert await(await conn.execute(
            'SELECT rkb_v4.verify_editorial_story_origins_v2(%s::uuid[])',
            ([baseline['source']['source_id']],))).fetchone()==(True,)
    finally:
        if owned:
            await conn.execute('DROP SCHEMA IF EXISTS rkb_v4 CASCADE')
            await conn.execute('DROP ROLE IF EXISTS rkb_reader')
            await conn.execute('DROP ROLE IF EXISTS rkb_writer')
        await conn.close()
