"""Opt-in existing bounded native fixture; never starts PG/model/provider itself."""
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path

import pytest
import pytest_asyncio

from regional_knowledge.kaggle_apply import HandlerRegistry,initialize_database,ApplyExecutionError
from regional_knowledge.kaggle_database_probe import NativeDatabaseProbe
from regional_knowledge.kaggle_fact_read import execute_fact_read
from regional_knowledge.source_import_v3 import FACT_REVISIONS,source_import_v3_handler
from test_source_story_native import connect,apply
from test_kaggle_apply import pinned
from test_recovery_journal import fixture as journal_fixture
from test_source_import_v3 import facts_payload,command
from test_kaggle_fact_read import request,date
from test_source_import import SOURCE
from test_source_import_v2 import FINDING

ROOT=Path(__file__).resolve().parents[1]
pytestmark=pytest.mark.skipif(not os.getenv('RKB_STORY_FIXTURE_SOCKET'),
    reason='explicit existing bounded native fixture required')


@pytest_asyncio.fixture
async def native():
    connection,fd=await connect()
    try:
        async with connection.transaction():
            assert await(await connection.execute('SELECT current_database(),current_user')).fetchone()==('rkb','rkb_bootstrap')
            assert (await(await connection.execute('SHOW server_version_num')).fetchone())[0].startswith('17')
            await connection.execute('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public')
            assert await(await connection.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")).fetchone()==('0.8.2',)
            assert await(await connection.execute("SELECT to_regnamespace('rkb_v4')")).fetchone()==(None,)
            for name in ('001_search.sql','002_recovery.sql'):
                await connection.execute((ROOT/'sql/kaggle'/name).read_text(),prepare=False)
        await initialize_database(connection,database_generation='facts-native-fixture',
            writer_generation='writer-1',revisions=FACT_REVISIONS)
        async with connection.transaction():
            for name in ('003_search_scope.sql','004_read_fences.sql','005_source_withdraw.sql',
                         '006_source_import.sql','007_source_stories.sql','008_source_facts.sql'):
                await connection.execute((ROOT/'sql/kaggle'/name).read_text(),prepare=False)
            await connection.execute("SELECT rkb_v4.publish_read_frontier('facts-native-fixture','writer-1',0,0,true,true)")
        check=await pinned(connection,'check_source_import_v3');effect=await pinned(connection,'apply_source_import_v3')
        registry=HandlerRegistry([source_import_v3_handler(revisions=FACT_REVISIONS,
            check_sha256=check.definition_sha256,apply_sha256=effect.definition_sha256)])
        _,journal=journal_fixture()
        yield dict(connection=connection,registry=registry,journal=journal,generation='facts-native-fixture')
    finally:
        await connection.rollback()
        async with connection.transaction():await connection.execute('DROP SCHEMA IF EXISTS rkb_v4 CASCADE')
        await connection.close();os.close(fd)


@pytest.mark.asyncio
async def test_native_facts_without_story_exact_calendar_decimal_retry_and_withdrawal(native):
    value=facts_payload();finding=value['semantic']['findings'][0]
    finding['event_dates'] += [date(precision='approximate_day'),date(date_role='publication'),
        date(precision='year',year=1900,month=None,day=None,original_date_text='1900')]
    c=command(value);receipt=await apply(native,c)
    assert receipt.status=='applied' and (await apply(native,c)).duplicate
    assert (await apply(native,c)).outcome==receipt.outcome
    connection=native['connection']
    @asynccontextmanager
    async def acquire():yield connection
    queried=await execute_fact_read(request(min_data_seq=1),acquire=acquire)
    assert queried.status=='ok' and len(queried.records)==1
    assert queried.records[0]['fact']['calendar']=='julian'
    assert queried.records[0]['calendar_conversion']=='none'
    empty=await execute_fact_read(request(min_data_seq=1,calendar='gregorian'),acquire=acquire)
    assert empty.status=='ok' and not empty.records
    observed=await execute_fact_read(request(action='observations',month=None,day=None,calendar=None,
        metric_key='population_count',min_data_seq=1),acquire=acquire)
    assert observed.records[0]['fact']['value']=='12.50'
    async with connection.transaction():
        assert await(await connection.execute('SELECT count(*) FROM rkb_v4.stories')).fetchone()==(0,)
        assert await(await connection.execute('SELECT count(*) FROM rkb_v4.applied_ops')).fetchone()==(1,)
        assert await(await connection.execute('SELECT rkb_v4.verify_source_story_origins_v2(%s::uuid[])',([SOURCE],))).fetchone()==(True,)
        await connection.execute('UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s',(SOURCE,))
        assert await(await connection.execute("SELECT count(*) FROM rkb_v4.read_source_calendar_v1('julian',2,29,NULL,0,%s,-1,10)",
            ('00000000-0000-0000-0000-000000000000',))).fetchone()==(0,)


@pytest.mark.asyncio
async def test_native_mid_projection_failure_rolls_back_activation_cursor_and_all_facts(native):
    connection=native['connection'];c=command()
    async with connection.transaction():
        await connection.execute("""CREATE FUNCTION rkb_v4.synthetic_fact_failure() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'synthetic fact projection failure';END $$;
            CREATE TRIGGER synthetic_fact_failure BEFORE INSERT ON rkb_v4.source_observations
            FOR EACH ROW EXECUTE FUNCTION rkb_v4.synthetic_fact_failure()""",prepare=False)
    with pytest.raises(ApplyExecutionError):await apply(native,c)
    async with connection.transaction():
        for table in ('sources','source_import_origins','story_assertion_versions','source_event_dates','source_observations','applied_ops'):
            assert await(await connection.execute('SELECT count(*) FROM rkb_v4.'+table)).fetchone()==(0,)
        assert await(await connection.execute('SELECT applied_seq FROM rkb_v4.recovery_state')).fetchone()==(0,)
        await connection.execute('DROP TRIGGER synthetic_fact_failure ON rkb_v4.source_observations')
    assert (await apply(native,c)).status=='applied'


@pytest.mark.asyncio
async def test_native_pure_fact_preconditions_and_bounded_catalog_measurement(native):
    connection=native['connection'];finding=facts_payload()['semantic']['findings'][0]
    async with connection.transaction():
        assert await(await connection.execute('SELECT rkb_v4.check_source_facts_v1(%s::jsonb)',(json.dumps(finding),))).fetchone()==(True,)
        finding['observations'][0]['value']='1E+8'
        assert await(await connection.execute('SELECT rkb_v4.check_source_facts_v1(%s::jsonb)',(json.dumps(finding),))).fetchone()==(True,)
        finding['event_dates'][0]['calendar']='gregorian' # Gregorian1900 has no29February.
        assert await(await connection.execute('SELECT rkb_v4.check_source_facts_v1(%s::jsonb)',(json.dumps(finding),))).fetchone()==(False,)
    @asynccontextmanager
    async def acquire():yield connection
    # Measurement only, using already supported explicit metadata row budget.
    catalog,fields=await NativeDatabaseProbe(max_catalog_rows=768)(acquire=acquire)
    count=len(json.loads(catalog)['records'])
    print('fresh8_catalog_records='+str(count)+' bytes='+str(len(catalog)))
    assert fields['schema_revision']==FACT_REVISIONS.schema and count<=768
