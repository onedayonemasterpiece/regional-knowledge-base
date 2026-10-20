"""Opt-in owned PG17 fixture: metadata only, no live export or PG launcher."""
import base64
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path

import pytest
import pytest_asyncio

from regional_knowledge.geo_manifest_commands import GEO_REVISIONS,geo_manifest_handler,plan_geo_manifest
from regional_knowledge.geo_migration import canonical,sha
from regional_knowledge.geo_whitelist import build_geo_whitelist
from regional_knowledge.kaggle_apply import HandlerRegistry,initialize_database
from test_geo_migration import snapshot,uid
from test_kaggle_apply import pinned
from test_recovery_journal import fixture as journal_fixture
from test_source_story_native import connect,apply

ROOT=Path(__file__).resolve().parents[1]
pytestmark=pytest.mark.skipif(not os.getenv('RKB_STORY_FIXTURE_SOCKET'),
    reason='explicit existing bounded native fixture required; this test starts no service')


@pytest_asyncio.fixture
async def native():
    connection,fd=await connect()
    try:
        files=sorted((ROOT/'sql/kaggle').glob('[0-9][0-9][0-9]_*.sql'))
        files=[p for p in files if int(p.name[:3])<=11]
        # A partial local 9/10 checkout is not a final11 composition proof.
        assert [int(p.name[:3]) for p in files]==list(range(1,12))
        async with connection.transaction():
            assert await(await connection.execute('SELECT current_database(),current_user')).fetchone()==('rkb','rkb_bootstrap')
            assert (await(await connection.execute('SHOW server_version_num')).fetchone())[0].startswith('17')
            await connection.execute('CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public')
            assert await(await connection.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")).fetchone()==('0.8.2',)
            assert await(await connection.execute("SELECT to_regnamespace('rkb_v4')")).fetchone()==(None,)
            for path in files[:2]:await connection.execute(path.read_text(),prepare=False)
        await initialize_database(connection,database_generation='geo-native-fixture',writer_generation='writer-1',revisions=GEO_REVISIONS)
        async with connection.transaction():
            for path in files[2:]:await connection.execute(path.read_text(),prepare=False)
            await connection.execute("SELECT rkb_v4.publish_read_frontier('geo-native-fixture','writer-1',0,0,true,true)")
        check=await pinned(connection,'check_geo_manifest_install_v1');effect=await pinned(connection,'apply_geo_manifest_install_v1')
        registry=HandlerRegistry([geo_manifest_handler(revisions=GEO_REVISIONS,
            check_sha256=check.definition_sha256,apply_sha256=effect.definition_sha256)])
        _,journal=journal_fixture()
        yield dict(connection=connection,registry=registry,journal=journal,generation='geo-native-fixture')
    finally:
        await connection.rollback()
        async with connection.transaction():await connection.execute('DROP SCHEMA IF EXISTS rkb_v4 CASCADE')
        await connection.close();os.close(fd)


def commands():
    manifest=build_geo_whitelist(**snapshot()).require_ready()
    plan=plan_geo_manifest(manifest)
    return manifest,plan.commands(first_sequence=1,operation_key='geo-fixture',request_id='geo-request',
        person_id=uid(1),logical_time='2026-10-20T18:00:00Z')


@pytest.mark.asyncio
async def test_native_whole_manifest_atomic_activation_exact_retry_and_private_stable_reads(native):
    manifest,resolved=commands();connection=native['connection']
    for command in resolved[:-1]:assert (await apply(native,command)).status=='applied'
    # Staged immutable metadata never enters public/private output before receipt.
    sql='SELECT * FROM rkb_v4.read_geo_identity_v1(%s,%s,NULL,NULL,NULL,NULL,NULL,10)'
    assert await(await connection.execute(sql,(uid(1),uid(10)))).fetchall()==[]
    final=await apply(native,resolved[-1]);assert final.status=='applied'
    duplicate=await apply(native,resolved[-1]);assert duplicate.duplicate and duplicate.outcome==final.outcome
    row=(await(await connection.execute(sql,(uid(1),uid(10)))).fetchall())[0]
    assert str(row[0])==uid(10) and row[4]==manifest.sha256 and row[5]==len(resolved)
    assert json.loads(bytes(row[2]))['payload']['historical_geometry']=='unresolved_with_owner'
    assert await(await connection.execute(sql,(uid(99),uid(10)))).fetchall()==[]
    by_poi=await(await connection.execute('SELECT entity_id FROM rkb_v4.read_geo_identity_v1(%s,NULL,%s,NULL,NULL,NULL,NULL,10)',
        (uid(1),'streetstory://poi/poi_ss_'+'a'*24))).fetchall()
    assert [str(x[0]) for x in by_poi]==[uid(10)]
    refs=await(await connection.execute("SELECT wire FROM rkb_v4.read_geo_members_v1(%s,%s,'owner_reference',NULL,20)",(uid(1),uid(10)))).fetchall()
    assert {json.loads(bytes(x[0]))['payload']['owner'] for x in refs}=={'street_story','cartography'}
    assert await(await connection.execute('SELECT projection_seq,text_ready,dense_ready FROM rkb_v4.read_frontier')).fetchone()==(len(resolved),True,True)
    assert await(await connection.execute('SELECT count(*) FROM rkb_v4.sources')).fetchone()==(0,)


@pytest.mark.asyncio
async def test_native_partial_counts_and_missing_json_fields_refuse_activation(native):
    manifest,resolved=commands();connection=native['connection']
    # Exercise native validation independently from trusted Python intake.
    header=json.loads(resolved[0].payload)
    empty=json.loads(base64.b64decode(header['header_prefix_b64'])+base64.b64decode(header['header_suffix_b64']))
    empty['inventory_counts']['geo_places']=3
    raw=canonical(empty);at=raw.index(b'"items":[')+len(b'"items":[')
    header.update(header_prefix_b64=base64.b64encode(raw[:at]).decode(),header_suffix_b64=base64.b64encode(raw[at:]).decode())
    full=manifest.wire();full['inventory_counts']['geo_places']=3;wrong_manifest=sha(full)
    altered=[replace(command,ids={'manifest_id':wrong_manifest},payload=canonical(header) if index==0 else command.payload)
        for index,command in enumerate(resolved)]
    for command in altered[:-1]:assert (await apply(native,command)).status=='applied'
    async with connection.transaction():
        last=altered[-1]
        assert await(await connection.execute('SELECT rkb_v4.check_geo_manifest_install_v1(%s,%s::jsonb,%s::jsonb,%s)',
            (last.payload,json.dumps(last.ids),json.dumps(last.expected_revisions),last.logical_time))).fetchone()==(False,)
        bad=json.loads(altered[1].payload);entry=bad['items'][0];body=json.loads(base64.b64decode(entry['wire_b64']));body.pop('status')
        exact=canonical(body);entry.update(wire_b64=base64.b64encode(exact).decode(),sha256=hashlib.sha256(exact).hexdigest())
        assert await(await connection.execute('SELECT rkb_v4.check_geo_manifest_install_v1(%s,%s::jsonb,%s::jsonb,%s)',
            (canonical(bad),json.dumps(last.ids),json.dumps(last.expected_revisions),last.logical_time))).fetchone()==(False,)
        assert await(await connection.execute('SELECT count(*) FROM rkb_v4.geo_manifest_receipts')).fetchone()==(0,)
