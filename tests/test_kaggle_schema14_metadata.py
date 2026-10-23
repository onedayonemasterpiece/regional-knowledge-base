"""Explicit image-source profile: catalog, role and history-reader contracts.

Synthetic tests never open PostgreSQL or execute an encoder/provider operation.
The opt-in native measurement uses the existing isolated fixture guard only.
"""
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import re
from types import SimpleNamespace as NS

import pytest

from regional_knowledge import kaggle_database_probe as probe
from regional_knowledge import kaggle_domain_schema as domain
from regional_knowledge import kaggle_source_history_native as history
from regional_knowledge.kaggle_owned_pg import (
    SQL_ARTIFACTS, DOMAIN_SQL_ARTIFACTS, IMAGE_SOURCE_SQL_ARTIFACTS, NATIVE_SOURCE_SQL_ARTIFACTS,
    configure_native_roles,
)
from test_kaggle_database_probe import Connection


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = domain.IMAGE_SOURCE_SCHEMA
FUNCTION = 'rkb-v4-native-source-activation-v1'
EXECUTOR = 'rkb-python-apply-native-source-v1'



@pytest.mark.asyncio
async def test_readiness_uses_ledger_instead_of_legacy_profile_or_catalog_inventory():
    connection=Connection()
    connection.state.update(schema_version=14,schema_revision=SCHEMA,function_revision=FUNCTION,executor_revision=EXECUTOR)
    encoded,fields=await probe.native_probe_for_schema(SCHEMA)(acquire=connection.acquire)
    assert encoded==b'{}' and fields['schema_version']==14
    connection.state['schema_revision']='obsolete-legacy-string'
    assert (await probe.native_probe_for_schema('another-legacy-string')(acquire=connection.acquire))[1]['schema_version']==14


@pytest.mark.asyncio
async def test_schema14_reader_gets_projections_not_private_stage_or_mutations():
    statements = []
    async def execute(statement, *args, **kwargs):
        statements.append(str(statement))
    await configure_native_roles(NS(execute=execute), selected_sql=IMAGE_SOURCE_SQL_ARTIFACTS, dbname='rkb')
    reader = '\n'.join(s for s in statements if 'TO rkb_reader' in s and 'TO rkb_writer,rkb_reader' not in s)
    assert 'find_source_catalog_v1' in reader and 'source_passage_metadata_v1' in reader
    assert 'read_editorial_story_v1' in reader and 'read_source_graph_v1' in reader
    for table in domain.TABLES_13 | domain.TABLES_14:
        assert table not in reader
    for action in ('prepare_source_activation', 'install_source_activation', 'apply_source_', 'check_source_'):
        assert action not in reader


@pytest.mark.asyncio
@pytest.mark.parametrize('schema,story,graph', [
    ('rkb-v4-sql001-011-v1', 'verify_source_story_origins_v3', 'verify_source_graph_origins_v1'),
    (SCHEMA, 'verify_source_story_origins_v4', 'verify_source_graph_origins_v2'),
])
async def test_history_selects_matching_provenance_without_changing_old_profile(monkeypatch, schema, story, graph):
    statements = []
    nonce = 'n'*32
    class Cursor:
        def __init__(self, result):self.result = result
        async def fetchone(self):return self.result
    async def execute(statement, params=()):
        statements.append(statement)
        return Cursor((False,) if 'SELECT EXISTS' in statement else (nonce,) if statement=='SELECT %s::text' else (True,))
    async def inspect_acl(connection):return b'acl'
    async def read_probe(**kwargs):return b'catalog', dict(schema_version=16,applied_seq=7, database_generation='db')
    released = []
    async def release(session, *, abort):released.append(abort)
    monkeypatch.setattr(history, 'inspect_acl', inspect_acl)
    observer = object.__new__(history.NativeSourceHistoryObserver)
    observer.database = NS(schema_version=int(schema.split('-')[3]),database_generation='db')
    observer.max_commands = 100; observer.valid = lambda binding:True
    observer._release = release; observer.probe = read_probe; observer.acquire = None; observer._prefixes = {}
    session = NS(nonce=nonce, exhausted=True, mode='full', connection=NS(execute=execute),
        binding=('run','inc',1), sequence=7, closure=[16])
    result = await observer._close(session, dict(action='close', session='session', nonce=nonce, origin_ids=[]))
    assert result['applied_sequence'] == 7 and released == [False]
    assert any(story+'(' in sql for sql in statements)
    assert any(graph+'(' in sql for sql in statements)
    assert not any(('verify_source_story_origins_v3' if schema==SCHEMA else 'verify_source_story_origins_v4')+'(' in sql for sql in statements)


@pytest.mark.parametrize('layer', [13, 14])
def test_inventory_names_match_exact_sql_artifact(layer):
    name = IMAGE_SOURCE_SQL_ARTIFACTS[layer-1]
    sql = (ROOT/'sql/kaggle'/name).read_text()
    functions = {}
    for match in re.finditer(r'CREATE FUNCTION rkb_v4\.([a-z0-9_]+)\((.*?)\)\s*RETURNS\s+'
            r'(TABLE\s*\([^)]*\)|[a-z]+)', sql, re.S):
        name, arguments, result = match.groups()
        arguments = ','.join(arg.strip().split()[-1] for arg in arguments.split(',')) if arguments.strip() else ''
        functions['rkb_v4.'+name+'('+arguments+')'] = (
            'record' if result.startswith('TABLE') else result, result.startswith('TABLE'))
    assert functions == getattr(domain, f'FUNCTIONS_{layer}')
    assert set(re.findall(r'CREATE TABLE rkb_v4\.([a-z0-9_]+)', sql)) == getattr(domain, f'TABLES_{layer}')
    assert set(re.findall(r'CREATE TRIGGER ([a-z0-9_]+)', sql)) == {name for _,name in getattr(domain, f'TRIGGERS_{layer}')}


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv('RKB_STORY_FIXTURE_SOCKET'), reason='explicit existing isolated PG17 fixture only')
async def test_native_schema14_integer_readiness_and_origin_verifiers():
    # The existing guard verifies the named fixture, network=none, <=256MiB,
    # private socket and rkb_bootstrap. This test starts no service or model.
    from test_source_story_native import connect
    from pg_schema_metadata import initialize_fixture_database as initialize_database
    connection, fd = await connect()
    created = False
    try:
        async with connection.transaction():
            assert await(await connection.execute('SELECT current_database(),current_user')).fetchone() == ('rkb','rkb_bootstrap')
            assert await(await connection.execute("SELECT to_regnamespace('rkb_v4')")).fetchone() == (None,)
            await connection.execute('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public')
            assert await(await connection.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")).fetchone() == ('0.8.2',)
            for name in IMAGE_SOURCE_SQL_ARTIFACTS[:2]:
                await connection.execute((ROOT/'sql/kaggle'/name).read_text(), prepare=False)
        created = True
        await initialize_database(connection, database_generation='image-metadata-fixture',
            writer_generation='writer-1',schema_version=14)
        async with connection.transaction():
            for name in IMAGE_SOURCE_SQL_ARTIFACTS[2:]:
                await connection.execute((ROOT/'sql/kaggle'/name).read_text(), prepare=False)
            await connection.execute("SELECT rkb_v4.publish_read_frontier('image-metadata-fixture','writer-1',0,0,true,true)")
        @asynccontextmanager
        async def acquire():yield connection
        encoded, fields = await probe.NativeDatabaseProbe()(acquire=acquire)
        assert encoded==b'{}'
        assert fields['applied_seq'] == 0
        assert fields['text_search_ready'] and fields['dense_search_ready']
        assert fields['schema_version']==14
        # Both new and legacy-only origin verifiers handle the empty fixture.
        async with connection.transaction():
            for name in ('verify_source_image_origins_v1','verify_source_story_origins_v4',
                         'verify_source_graph_origins_v2','verify_editorial_story_origins_v2'):
                assert await(await connection.execute('SELECT rkb_v4.'+name+'(%s::uuid[])', ([],))).fetchone() == (True,)
    finally:
        await connection.rollback()
        if created:
            async with connection.transaction():
                await connection.execute('DROP SCHEMA rkb_v4 CASCADE')
        await connection.close()
        os.close(fd)
