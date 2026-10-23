"""Run the actual Story evidence lookup under the production read role."""
import pytest
import psycopg

from regional_knowledge.kaggle_editorial_read import _accepted_story_passages
from regional_knowledge.kaggle_owned_pg import configure_native_roles
from test_kaggle_apply import PG
from test_kaggle_pg_driver import database
from test_source_activation_pg import image_pg_sql18, accepted, stage_command, ROOT
from test_source_batch_compiler import passage_link_packet
from test_source_batch_pg import native, packet


@PG
@pytest.mark.asyncio
async def test_reader_gets_story_passage_page_ids_but_never_raw_page_payload(image_pg_sql18):
    env = image_pg_sql18
    admin = env['connection']
    doc = passage_link_packet()
    assert (await accepted(env, stage_command(doc, 1))).status == 'applied'
    prepare, _ = native(env)
    for component in ('text', 'semantic'):
        command = packet(env, doc, component)
        await prepare(command)
        assert (await accepted(env, command)).status == 'applied'
    bootstrap_created = False
    try:
        async with admin.transaction():
            if not await (await admin.execute(
                    "SELECT 1 FROM pg_roles WHERE rolname='rkb_bootstrap'"
            )).fetchone():
                await admin.execute('CREATE ROLE rkb_bootstrap NOLOGIN')
                bootstrap_created = True
            selected = [p.name for p in sorted((ROOT/'sql/kaggle').glob('[0-9][0-9][0-9]_*.sql'))]
            await configure_native_roles(admin, selected_sql=selected,
                dbname=admin.info.dbname, create_roles=False)
            story_id = (await (await admin.execute(
                'SELECT story_id FROM rkb_v4.stories LIMIT 1')).fetchone())[0]
            async with admin.cursor() as cursor:
                await cursor.execute('SET LOCAL ROLE rkb_reader')
                await cursor.execute(
                    "SELECT rkb_v4.read_editorial_story_v1(%s,NULL,'get',0,1,NULL)", (story_id,))
                record = (await cursor.fetchone())[0]
                async def execute(statement, args=()):
                    await cursor.execute(statement, args)
                result = await _accepted_story_passages(record, execute, cursor)
                assert result['body'] == doc['model_payload']['story_candidates'][0]['basic_variant']['body']
                assert len(result['passage_refs']) == 1
                assert result['passage_refs'][0]['page_id'] == doc['model_payload']['pages'][0]['page_id']
                for column in ('source_id', 'page_id', 'physical_page_index'):
                    await cursor.execute("SELECT has_column_privilege(current_user,'rkb_v4.source_stage_pages',%s,'SELECT')", (column,))
                    assert (await cursor.fetchone())[0] is True
                for column in ('model_page', 'image_receipt', 'image_receipt_sha256'):
                    await cursor.execute("SELECT has_column_privilege(current_user,'rkb_v4.source_stage_pages',%s,'SELECT')", (column,))
                    assert (await cursor.fetchone())[0] is False
        for column in ('model_page', 'image_receipt'):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                async with admin.transaction():
                    await admin.execute('SET LOCAL ROLE rkb_reader')
                    await admin.execute('SELECT '+column+' FROM rkb_v4.source_stage_pages LIMIT 1')
    finally:
        if bootstrap_created:
            async with admin.transaction():
                await admin.execute('DROP OWNED BY rkb_bootstrap')
                await admin.execute('DROP ROLE rkb_bootstrap')
