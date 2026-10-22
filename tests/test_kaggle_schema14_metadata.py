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
    configure_native_roles, sql_artifacts,
)
from test_kaggle_database_probe import Connection, catalog


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = domain.IMAGE_SOURCE_SCHEMA
FUNCTION = 'rkb-v4-native-source-activation-v1'
EXECUTOR = 'rkb-python-apply-native-source-v1'


def image_catalog():
    observed = catalog()
    signatures = {}
    for name in probe.STORY_FUNCTIONS | probe.FACT_FUNCTIONS:
        stem = name.split('.', 1)[1].split('(', 1)[0]
        is_set = stem.startswith(('story_lexical_', 'story_dense_', 'story_hydrate_',
            'read_source_calendar_', 'read_source_observations_', 'read_source_assertions_'))
        result = ('record' if is_set else 'boolean' if stem.startswith(('check_', 'verify_'))
            else 'bytea' if stem.startswith('apply_') else 'jsonb' if stem in ('read_story_v1', 'source_fact_proofs_v1')
            else 'trigger')
        signatures[name] = (result, is_set)
    tables = probe.STORY_TABLES | probe.FACT_TABLES
    triggers = probe.STORY_TRIGGERS | probe.FACT_TRIGGERS
    for layer in (9, 10, 11, 13, 14):
        tables |= getattr(domain, f'TABLES_{layer}')
        triggers |= getattr(domain, f'TRIGGERS_{layer}')
        signatures.update(getattr(domain, f'FUNCTIONS_{layer}'))
    signatures.update(domain.FUNCTIONS_12)
    observed['tables'].update({name:dict(kind='r') for name in tables})
    observed['functions'].update({name:dict(kind='f', returns_set=is_set, result=result,
        security_definer=False, definition='CREATE FUNCTION synthetic schema14')
        for name,(result,is_set) in signatures.items()})
    observed['triggers'].update({table+':'+name:dict(table=table, name=name, internal=False, enabled='O')
        for table,name in triggers})
    for name,table,method in (
        ('story_search_dense','story_search','hnsw'), ('story_search_fts','story_search','gin'),
        ('source_event_day','source_event_dates','btree'), ('source_event_source','source_event_dates','btree'),
        ('source_observation_metric','source_observations','btree'), ('source_observation_source','source_observations','btree')):
        observed['indexes'][name] = dict(table=table, method=method, valid=True, ready=True, live=True)
    return observed


def test_explicit_profile_keeps_old_prefixes_and_hard_budgets():
    assert sql_artifacts('rkb-v4-sql001-011-v1') == DOMAIN_SQL_ARTIFACTS
    assert sql_artifacts('unknown-future-schema') == SQL_ARTIFACTS
    assert NATIVE_SOURCE_SQL_ARTIFACTS == IMAGE_SOURCE_SQL_ARTIFACTS
    assert sql_artifacts(SCHEMA) == DOMAIN_SQL_ARTIFACTS + (
        '012_geo_dependency_bound.sql', '013_source_stage.sql', '014_source_stage_activation.sql')
    assert probe.native_probe_for_schema('rkb-v4-sql001-011-v1').max_catalog_rows == 1376
    assert probe.native_probe_for_schema('rkb-v4-sql001-008-v1').max_catalog_rows == 576
    assert probe.native_probe_for_schema('unknown-future-schema').max_catalog_rows == 512
    selected = probe.native_probe_for_schema(SCHEMA)
    assert selected.max_catalog_rows <= 2048
    assert selected.max_catalog_bytes == 1024*1024 and selected.max_definition_bytes == 65536


@pytest.mark.asyncio
async def test_complete_schema14_catalog_and_wrong_profile_refusal():
    connection = Connection()
    connection.catalog = image_catalog()
    connection.state.update(schema_revision=SCHEMA, function_revision=FUNCTION, executor_revision=EXECUTOR)
    encoded, fields = await probe.native_probe_for_schema(SCHEMA)(acquire=connection.acquire)
    assert fields['schema_revision'] == SCHEMA
    assert json.loads(encoded)['format'] == 'rkb-native-catalog-v1'
    connection.state['schema_revision'] = 'rkb-v4-sql001-011-v1'
    with pytest.raises(probe.DatabaseProbeUnavailable, match='image_source_revision_mismatch'):
        await probe.native_probe_for_schema(SCHEMA)(acquire=connection.acquire)
    connection.state['schema_revision'] = SCHEMA
    for name in domain.TABLES_13:
        del connection.catalog['tables'][name]
    with pytest.raises(probe.DatabaseProbeUnavailable, match='image_source_revision_mismatch'):
        await probe.native_probe_for_schema(SCHEMA)(acquire=connection.acquire)


@pytest.mark.parametrize('signature', sorted(domain.FUNCTIONS_12 | domain.FUNCTIONS_13 | domain.FUNCTIONS_14))
def test_each_fixed_stage_activation_function_is_required(signature):
    observed = image_catalog()
    probe.NativeDatabaseProbe._validate_catalog(observed)
    del observed['functions'][signature]
    with pytest.raises(probe.DatabaseProbeUnavailable, match='functions_incomplete'):
        probe.NativeDatabaseProbe._validate_catalog(observed)


@pytest.mark.parametrize('layer', [13, 14])
def test_new_immutable_tables_and_triggers_are_not_optional(layer):
    observed = image_catalog()
    table = sorted(getattr(domain, f'TABLES_{layer}'))[0]
    del observed['tables'][table]
    with pytest.raises(probe.DatabaseProbeUnavailable, match='schema_incomplete'):
        probe.NativeDatabaseProbe._validate_catalog(observed)
    observed = image_catalog()
    table,name = sorted(getattr(domain, f'TRIGGERS_{layer}'))[0]
    observed['triggers'][table+':'+name]['enabled'] = 'D'
    with pytest.raises(probe.DatabaseProbeUnavailable, match='triggers_not_ready'):
        probe.NativeDatabaseProbe._validate_catalog(observed)


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
    async def read_probe(**kwargs):return b'catalog', dict(applied_seq=7, database_generation='db')
    released = []
    async def release(session, *, abort):released.append(abort)
    monkeypatch.setattr(history, 'inspect_acl', inspect_acl)
    observer = object.__new__(history.NativeSourceHistoryObserver)
    observer.database = NS(schema_revision=schema, database_generation='db')
    observer.max_commands = 100; observer.valid = lambda binding:True
    observer._release = release; observer.probe = read_probe; observer.acquire = None; observer._prefixes = {}
    session = NS(nonce=nonce, exhausted=True, mode='full', connection=NS(execute=execute),
        binding=('run','inc',1), sequence=7, closure=[history.digest(b'catalog'), history.digest(b'acl')])
    result = await observer._close(session, dict(action='close', session='session', nonce=nonce, origin_ids=[]))
    assert result['applied_sequence'] == 7 and released == [False]
    assert any(story+'(' in sql for sql in statements)
    assert any(graph+'(' in sql for sql in statements)
    assert not any(('verify_source_story_origins_v3' if schema==SCHEMA else 'verify_source_story_origins_v4')+'(' in sql for sql in statements)


def test_schema14_readers_accept_exact_profile_not_a_future_range():
    from regional_knowledge.kaggle_editorial_read import EDITORIAL_SCHEMAS
    from regional_knowledge.kaggle_fact_read import FACT_READ_REVISIONS
    from regional_knowledge.kaggle_source_graph_read import SOURCE_GRAPH_READ_REVISIONS
    assert SCHEMA in EDITORIAL_SCHEMAS
    assert (SCHEMA,FUNCTION) in FACT_READ_REVISIONS and (SCHEMA,FUNCTION) in SOURCE_GRAPH_READ_REVISIONS
    assert 'rkb-v4-sql001-015-v1' not in EDITORIAL_SCHEMAS
    assert (SCHEMA,'wrong-function') not in FACT_READ_REVISIONS | SOURCE_GRAPH_READ_REVISIONS


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
async def test_native_schema14_complete_catalog_measurement():
    # The existing guard verifies the named fixture, network=none, <=256MiB,
    # private socket and rkb_bootstrap. This test starts no service or model.
    from test_source_story_native import connect
    from regional_knowledge.kaggle_apply import initialize_database, Revisions
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
            writer_generation='writer-1', revisions=Revisions(SCHEMA,FUNCTION,EXECUTOR))
        async with connection.transaction():
            for name in IMAGE_SOURCE_SQL_ARTIFACTS[2:]:
                await connection.execute((ROOT/'sql/kaggle'/name).read_text(), prepare=False)
            await connection.execute("SELECT rkb_v4.publish_read_frontier('image-metadata-fixture','writer-1',0,0,true,true)")
        @asynccontextmanager
        async def acquire():yield connection
        # Default remains the production readiness budget. A bounded structural
        # measurement may opt in separately; it is not a latency-gate PASS.
        timeout = float(os.getenv("RKB_CATALOG_FIXTURE_TIMEOUT_SECONDS", "2"))
        encoded, fields = await probe.native_probe_for_schema(SCHEMA, timeout_seconds=timeout)(acquire=acquire)
        records = json.loads(encoded)['records']
        assert fields['schema_revision'] == SCHEMA and fields['applied_seq'] == 0
        assert fields['text_search_ready'] and fields['dense_search_ready']
        assert len(records) <= 2048 and len(encoded) <= 1024*1024
        print('schema14_native_catalog_records=', len(records), 'bytes=', len(encoded))
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
