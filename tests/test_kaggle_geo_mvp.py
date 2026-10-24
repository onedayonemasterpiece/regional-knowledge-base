"""Source-only GEO MVP contract and bounded native read regressions; never live PG."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import hashlib
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from mcp.server.mcpserver.exceptions import ToolError
from regional_knowledge.geo_migration import exact_uuid
from regional_knowledge.kaggle_current_registry import current_registry
from regional_knowledge.kaggle_source_commands import decode_command
from regional_knowledge.kaggle_source_geo_mvp import (
    ROUTE, canonical, geo_resolve, resolved_command, validate_command,
)
from regional_knowledge.kaggle_source_graph_read import SourceGraphReadRequest,execute_source_graph_read
from regional_knowledge.kaggle_protocol import decode_envelope,encode_envelope
from test_kaggle_fact_read import Native
from test_source_graph import payload
from test_source_import import SOURCE

M = '10000000-0000-0000-0000-000000000001'
PERSON = '20000000-0000-0000-0000-000000000001'
WHEN = '2026-10-23T20:01:00Z'
OWNER_POI = 'streetstory://poi/30000000-0000-0000-0000-000000000001'


def args(**changes):
    value=dict(sequence=21, operation_key='geo-resolve-idempotent-21', person_id=PERSON,
        logical_time=WHEN, source_id=SOURCE, mention_id=M, source_revision=1, status='candidate',
        poi_ref=OWNER_POI, url='https://example.org/history',
        checked_at=WHEN, reason='Verified physical POI against this external page')
    value.update(changes)
    return value


def geo_request(**changes):
    value=dict(request_id='geo-read-1', connection_generation=2,turn_generation=3,
        action='geo_queue',limit=5,min_data_seq=9,deadline_ms=2000,compute_claim='actual-read-claim',
        actor=dict(person_id='verified-person',audience='personal',auth_epoch=1,visibility_epoch=4))
    value.update(changes)
    return SourceGraphReadRequest.model_validate(value)


def geo_row(source, revision=1):
    source=exact_uuid(source)
    value,_=payload()
    entity=value['source_graph']['entities'][0]
    mention=dict(entity['mention'])
    mention['mention_id']=M
    mention['entity_id']=entity['entity_id']
    proof=mention['evidence']
    return UUID(source),revision,7,M,mention,proof['exact_quote'],'Exact accepted source citation'


def test_sql35_scoped_table_latest_revision_and_accepted_command_boundary():
    sql=(Path(__file__).parents[1]/'sql/kaggle/035_geo_links.sql').read_text()
    assert 'CREATE TABLE rkb_v4.geo_links' in sql
    assert 'PRIMARY KEY(source_id,mention_id,accepted_sequence)' in sql
    assert 'REFERENCES rkb_v4.applied_ops(sequence) DEFERRABLE INITIALLY DEFERRED' in sql
    assert 'FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.revisions' in sql
    assert 'CREATE VIEW rkb_v4.geo_links_latest WITH (security_invoker=true)' in sql
    assert 'ORDER BY source_id,mention_id,accepted_sequence DESC' in sql
    assert 'REFERENCES rkb_v4.source_entity_mentions(source_id,mention_id)' in sql
    assert "JOIN rkb_v4.source_entity_ids e ON e.entity_id=m.entity_id AND e.kind='poi_ref'" in sql
    assert 'a.revision=g.source_revision' in sql and 'a.revision=want_revision' in sql
    assert 'WHERE s.accepted AND NOT s.withdrawn' in sql
    assert 'LEFT JOIN LATERAL (' in sql and 'latest.status IN' in sql
    assert 'LIMIT least(greatest(coalesce(take,1),1),5)' in sql
    assert 'CREATE FUNCTION rkb_v4.check_geo_resolve_v1' in sql
    assert 'CREATE FUNCTION rkb_v4.apply_geo_resolve_v1' in sql
    assert 'SELECT applied_seq+1 INTO STRICT seq FROM rkb_v4.recovery_state WHERE singleton FOR UPDATE' in sql
    assert 'INSERT INTO rkb_v4.geo_links' in sql
    assert 'UPDATE rkb_v4.geo_links' not in sql and 'ON CONFLICT' not in sql
    assert 'CREATE TRIGGER immutable_geo_link' not in sql
    assert 'NOT EXISTS' not in sql and 'jsonb_typeof' not in sql
    assert '?&' not in sql and 'd-ARRAY' not in sql and 'a.revision=1' not in sql
    assert 'CREATE TABLE rkb_v4.geo_intents' not in sql and 'CREATE TABLE rkb_v4.geo_attempts' not in sql


def test_exact_native_handler_and_canonical_accepted_bytes():
    from regional_knowledge.kaggle_native_source_bindings import NATIVE_ROUTES
    from regional_knowledge.source_catalog_command import ROUTE as CATALOG_ROUTE
    assert {ROUTE, CATALOG_ROUTE} <= NATIVE_ROUTES
    command=resolved_command(**args())
    assert validate_command(command)['mention_id']==M
    assert decode_command(command.encode()).encode()==command.encode()
    handler=current_registry().resolve(command)
    assert handler.check.name=='check_geo_resolve_v1'
    assert handler.apply.name=='apply_geo_resolve_v1'
    assert handler.dependency.resource_keys==frozenset({'source_id'})
    assert handler.precondition_keys==frozenset({'source_revision'})
    assert resolved_command(**args()).encode()==command.encode()
    assert resolved_command(**args(source_revision=2)).expected_revisions=={'source_revision':'2'}
    assert validate_command(resolved_command(**args(source_revision=2)))['mention_id']==M
    changed=resolved_command(**args(reason='Different decision under same key'))
    assert changed.encode()!=command.encode()
    with pytest.raises(ValueError):
        validate_command(replace(command, expected_revisions={'source_revision':'01'}))
    with pytest.raises(ValueError):
        validate_command(replace(command, expected_revisions={'source_revision':'0'}))
    with pytest.raises(ValueError):
        validate_command(replace(command, expected_revisions={'source_revision':'1','fake':'x'}))
    with pytest.raises(ValueError):
        validate_command(replace(command, payload=canonical(dict(validate_command(command), fake='x'))))
    with pytest.raises(ValueError):
        validate_command(replace(command, payload=canonical(dict(validate_command(command), reason=7))))
    with pytest.raises(ValueError):
        validate_command(replace(command, payload=canonical(dict(validate_command(command), status='bad'))))  # Existing ControlSpool/applied_ops conflict rather than overwrite.


@pytest.mark.parametrize('changes', [
    {'status':'not_found'},{'poi_ref':'another-owner'}, {'source_id':'non-uuid'},
    {'mention_id':'non-uuid'}, {'checked_at':None},{'url':None},
    {'status':'unresolved', 'poi_ref':OWNER_POI},{'url':'file:///forbidden'},
    {'reason':''}, {'checked_at':'2026-10-23'}, {'reason':'z'*1025},
    {'source_revision':0},{'source_revision':True},{'source_revision':'2'},
    {'source_revision':9223372036854775808},
])
def test_resolve_strict_invalid_inputs(changes):
    with pytest.raises(ValueError):
        resolved_command(**args(**changes))


def test_valid_unresolved_and_ambiguous_have_no_fabricated_poi():
    for status in ('unresolved','ambiguous'):
        command=resolved_command(**args(status=status,poi_ref=None,url=None,checked_at=None))
        doc=validate_command(command)
        assert doc['poi_ref'] is None and doc['status']==status


@pytest.mark.asyncio
async def test_geo_queue_uses_one_bounded_read_and_source_scoped_equal_mention_ids():
    # Two sources may have the SAME mention UUID: pair identity is required.
    sources=[str(UUID(int=i+1)) for i in range(5)]
    rows=[geo_row(source,2 if n==1 else 1) for n,source in enumerate(sources)]
    native=Native(rows,revision=('rkb-v4-sql001-035-v1','new-source'))
    result=await execute_source_graph_read(geo_request(),acquire=native.acquire)
    assert result.status=='ok' and len(result.records)==5 and len(result.evidence)==5
    assert {r['source_id'] for r in result.records}==set(sources)
    assert {r['source_revision'] for r in result.records}=={1,2}
    assert {r['id'] for r in result.records}=={source+':'+M for source in sources}
    assert all(e.kind=='geo' and len(e.dependencies.source_ids)==1 for e in result.evidence)
    sql=[(text,params) for text,params in native.calls if 'read_geo_queue_v1' in text]
    assert len(sql)==1 and sql[0][1]==(5,)
    assert result.next_cursor is None
    assert type(decode_envelope(encode_envelope(result))) is type(result)


@pytest.mark.asyncio
async def test_geo_queue_gated_on_schema35_no_sql_when_missing():
    native=Native([],revision=('rkb-v4-sql001-034-v1','previous'))
    result=await execute_source_graph_read(geo_request(),acquire=native.acquire)
    assert result.reason=='not_ready' and not any('read_geo_queue_v1' in q for q,_ in native.calls)


@pytest.mark.parametrize('change', [{'limit':6},{'entity_id':SOURCE},{'source_id':SOURCE},
                                     {'cursor':'foreign-cursor'},{'query':'all'}])
def test_geo_queue_rejects_unsafe_selectors(change):
    with pytest.raises(ValueError):
        geo_request(**change)


class ExistingAccepted:
    """Read-only fake for exact same-key replay: must never stage a second write."""
    def __init__(self, command):
        self.command=command
        self.outcome_state='DURABLE'
        self.apply_receipt=SimpleNamespace(status='applied')
        self.accepted=object()


class SpoolReplay:
    def __init__(self,command):
        self._lock=asyncio.Lock()
        self.entry=ExistingAccepted(command)
        self.gets=0

    async def get(self,key):
        self.gets+=1
        assert key==self.entry.command.operation_key
        return self.entry

    def __getattr__(self,name):
        raise AssertionError('existing durable replay cannot invoke spool write: '+name)


class ImportReplay:
    def __init__(self,spool):
        self.spool=spool
        self.access=SimpleNamespace(authority=SimpleNamespace(person_id=PERSON),context=object(),binding=object())

    @asynccontextmanager
    async def _access(self,principal):
        assert principal=='verified-owner'
        yield self.access

    async def _check(self,*args):
        return None


@pytest.mark.asyncio
async def test_replay_exact_operation_key_has_no_second_write_or_owner_effect():
    command=resolved_command(**args())
    spool=SpoolReplay(command)
    startup=SimpleNamespace(importer=ImportReplay(spool),source_dispatch=object())
    p={key:value for key,value in args().items() if key not in ('sequence','person_id','logical_time')}
    first=await geo_resolve(startup,'verified-owner',**p)
    second=await geo_resolve(startup,'verified-owner',**p)
    assert first==second and first['status']=='applied' and first['duplicate']
    assert spool.gets==2
    with pytest.raises(ToolError,match='operation_key_has_different_accepted_bytes'):
        await geo_resolve(startup,'verified-owner',**(p|{'reason':'changed accepted bytes'}))
    with pytest.raises(ToolError,match='operation_key_has_different_accepted_bytes'):
        await geo_resolve(startup,'verified-owner',**(p|{'source_revision':2}))
    assert spool.gets==4


@pytest.mark.asyncio
async def test_missing_intake_cannot_bypass_owner_authorization():
    with pytest.raises(ToolError,match='native_source_intake_unavailable'):
        await geo_resolve(SimpleNamespace(importer=None,source_dispatch=None),'caller',
            **{key:value for key,value in args().items() if key not in ('sequence','person_id','logical_time')})


from test_kaggle_apply import PG
from test_kaggle_pg_driver import database
import json
import psycopg


@PG
@pytest.mark.asyncio
async def test_disposable_pg17_sql35_append_only_corrections_current_revision_and_integrity(database):
    """Real disposable PG17: accepted ledger, latest projection and current source proof; never remote PG."""
    connection=database  # SQL001 and the first accepted source are installed by the disposable fixture.
    sources=[str(UUID(int=i)) for i in range(1,6)]
    entity=str(UUID(int=101)); nonpoi=str(UUID(int=102)); other_mention=str(UUID(int=103))
    sql_path=Path(__file__).parents[1]/'sql/kaggle'
    async with connection.transaction():
        await connection.execute((sql_path/'002_recovery.sql').read_text(),prepare=False)
        await connection.execute("INSERT INTO rkb_v4.writer_generations(generation) VALUES('fixture-writer')")
        await connection.execute("""INSERT INTO rkb_v4.recovery_state(
            singleton,database_generation,writer_generation,applied_seq,
            schema_revision,function_revision,executor_revision)
            VALUES(true,'fixture-generation','fixture-writer',0,'schema','function','executor')""")
        await connection.execute("""
            CREATE TABLE rkb_v4.source_graph_documents(
                source_id uuid PRIMARY KEY REFERENCES rkb_v4.sources,
                source_revision bigint NOT NULL,
                FOREIGN KEY(source_id,source_revision) REFERENCES rkb_v4.revisions(source_id,revision));
            CREATE TABLE rkb_v4.source_entity_ids(entity_id uuid PRIMARY KEY,kind text NOT NULL);
            CREATE TABLE rkb_v4.source_entity_mentions(
                source_id uuid NOT NULL REFERENCES rkb_v4.source_graph_documents,
                mention_id uuid NOT NULL,entity_id uuid NOT NULL,data jsonb NOT NULL,
                PRIMARY KEY(source_id,mention_id));
            CREATE TABLE rkb_v4.source_visibility(source_id uuid PRIMARY KEY,eligibility_revision bigint NOT NULL);
            CREATE TABLE rkb_v4.source_graph_receipts(
                source_id uuid PRIMARY KEY REFERENCES rkb_v4.source_graph_documents);
        """,prepare=False)
        await connection.execute(
            'INSERT INTO rkb_v4.source_entity_ids VALUES(%s,%s),(%s,%s)',
            (entity,'poi_ref',nonpoi,'person'))
        # The disposable minimal graph table models a future accepted revision2;
        # production SQL011 is still revision1-only and is NOT modified by GEO.
        for i,source in enumerate(sources):
            rev=2 if i==1 else 1
            if i:
                await connection.execute('INSERT INTO rkb_v4.sources VALUES(%s,%s,%s,%s)',
                    (source,'fixture source',i!=2,i==3))
                await connection.execute('INSERT INTO rkb_v4.revisions VALUES(%s,1)',(source,))
                if rev==2:
                    await connection.execute('INSERT INTO rkb_v4.revisions VALUES(%s,2)',(source,))
                await connection.execute('INSERT INTO rkb_v4.active_revision VALUES(%s,%s)',(source,rev))
            await connection.execute('INSERT INTO rkb_v4.source_graph_documents VALUES(%s,%s)',(source,rev))
            await connection.execute('INSERT INTO rkb_v4.source_visibility VALUES(%s,7)',(source,))
            if i!=4:  # An accepted source without a graph receipt is not eligible.
                await connection.execute('INSERT INTO rkb_v4.source_graph_receipts VALUES(%s)',(source,))
            mention=dict(mention_id=M,exact_source_spelling='Test place',
                evidence=dict(exact_quote='Test place in printed source.'))
            await connection.execute('''INSERT INTO rkb_v4.source_entity_mentions
                VALUES(%s,%s,%s,%s::jsonb)''',(source,M,entity,json.dumps(mention)))
        await connection.execute('''INSERT INTO rkb_v4.source_entity_mentions
            VALUES(%s,%s,%s,%s::jsonb)''',(sources[0],other_mention,nonpoi,
            json.dumps(dict(mention_id=other_mention,evidence=dict(exact_quote='Not a POI')))))
        await connection.execute((sql_path/'035_geo_links.sql').read_text(),prepare=False)

    async def queue(take):
        return await (await connection.execute(
            'SELECT source_id,source_revision,data FROM rkb_v4.read_geo_queue_v1(%s)',(take,))).fetchall()

    async def check(command):
        return (await (await connection.execute(
            'SELECT rkb_v4.check_geo_resolve_v1(%s,%s::jsonb,%s::jsonb,%s)',
            (command.payload,json.dumps(dict(command.ids)),
             json.dumps(dict(command.expected_revisions)),command.logical_time))).fetchone())[0]

    async def accept(command):
        # Model the existing single accepted dispatcher transaction: apply inserts
        # correction, then the actual ledger and recovery cursor are committed together.
        assert await check(command)
        outcome=(await (await connection.execute(
            'SELECT rkb_v4.apply_geo_resolve_v1(%s,%s::jsonb,%s::jsonb,%s)',
            (command.payload,json.dumps(dict(command.ids)),
             json.dumps(dict(command.expected_revisions)),command.logical_time))).fetchone())[0]
        assert json.loads(outcome)['accepted_sequence']==command.sequence
        encoded=command.encode()
        await connection.execute("""INSERT INTO rkb_v4.applied_ops(
            operation_key,sequence,command_bytes,command_sha256,
            accepted_manifest_sha256,database_generation,writer_generation,
            status,outcome_bytes,outcome_sha256,handler_sha256)
            VALUES(%s,%s,%s,%s,%s,'fixture-generation','fixture-writer',
                   'applied',%s,%s,%s)""",
            (command.operation_key,command.sequence,encoded,hashlib.sha256(encoded).hexdigest(),
             'a'*64,outcome,hashlib.sha256(outcome).hexdigest(),'b'*64))
        await connection.execute('UPDATE rkb_v4.recovery_state SET applied_seq=%s WHERE singleton',
            (command.sequence,))
        return json.loads(outcome)

    before=await queue(5)
    assert {str(x[0]) for x in before}==set(sources[:2])
    assert {int(x[1]) for x in before}=={1,2}
    assert len(await queue(None))<=5 and len(await queue(1))==1
    cmd1=resolved_command(**args(source_id=sources[1],source_revision=2,sequence=1))
    async with connection.transaction():
        first=await accept(cmd1)
    assert first['source_revision']==2
    assert {str(x[0]) for x in await queue(5)}=={sources[0]}  # latest candidate is settled

    cmd2=resolved_command(**args(source_id=sources[1],source_revision=2,sequence=2,
        operation_key='geo-correction-unresolved',status='unresolved',
        poi_ref=None,url=None,checked_at=None,reason='Historic POI reference now uncertain'))
    async with connection.transaction():
        second=await accept(cmd2)
    assert second['accepted_sequence']==2
    pending=await queue(5)
    assert {str(x[0]) for x in pending}==set(sources[:2])
    unresolved=next(x[2] for x in pending if str(x[0])==sources[1])
    assert unresolved['latest_geo_resolution']['status']=='unresolved'
    assert unresolved['latest_geo_resolution']['accepted_sequence']==2
    assert unresolved['latest_geo_resolution']['source_revision']==2

    corrected_poi='streetstory://poi/30000000-0000-0000-0000-000000000002'
    cmd3=resolved_command(**args(source_id=sources[1],source_revision=2,sequence=3,
        operation_key='geo-correction-new-owner-ref',poi_ref=corrected_poi,
        reason='Confirmed corrected external POI'))
    async with connection.transaction():
        third=await accept(cmd3)
    assert third['status']=='candidate'
    assert {str(x[0]) for x in await queue(5)}=={sources[0]}

    history=await (await connection.execute("""
        SELECT accepted_sequence,source_revision,status,poi_ref,reason
        FROM rkb_v4.geo_links WHERE source_id=%s AND mention_id=%s
        ORDER BY accepted_sequence""",(sources[1],M))).fetchall()
    assert [x[0] for x in history]==[1,2,3]
    assert [x[2] for x in history]==['candidate','unresolved','candidate']
    assert history[0][3]==OWNER_POI and history[2][3]==corrected_poi
    latest=await (await connection.execute("""SELECT accepted_sequence,status,poi_ref
        FROM rkb_v4.geo_links_latest WHERE source_id=%s AND mention_id=%s""",
        (sources[1],M))).fetchone()
    assert latest==(3,'candidate',corrected_poi)
    assert (await (await connection.execute('SELECT count(*) FROM rkb_v4.applied_ops')).fetchone())==(3,)

    # Same accepted operation_key is returned from the unchanged applied_ops
    # ledger, never applied again. Distinct accepted keys may append corrections.
    receipt=await (await connection.execute("""SELECT sequence,command_bytes,outcome_bytes
        FROM rkb_v4.applied_ops WHERE operation_key=%s""",(cmd1.operation_key,))).fetchone()
    assert receipt[0]==1 and receipt[1]==cmd1.encode()
    assert (await (await connection.execute('SELECT count(*) FROM rkb_v4.geo_links')).fetchone())==(3,)

    for negative in [
        resolved_command(**args(source_id=sources[1],source_revision=1,sequence=4,
            operation_key='geo-wrong-revision')),
        resolved_command(**args(source_id=sources[2],sequence=4,operation_key='geo-unaccepted')),
        resolved_command(**args(source_id=sources[3],sequence=4,operation_key='geo-withdrawn')),
        resolved_command(**args(source_id=sources[4],sequence=4,operation_key='geo-no-receipt')),
        resolved_command(**args(source_id=sources[0],mention_id=other_mention,sequence=4,
            operation_key='geo-nonpoi')),
        resolved_command(**args(source_id=sources[0],mention_id=str(UUID(int=104)),sequence=4,
            operation_key='geo-unknown-mention')),
    ]:
        assert await check(negative) is False
    tampered=replace(cmd1,ids={'source_id':sources[0],'mention_id':M})
    assert await check(tampered) is False

    # Changing active revision without a matching accepted source graph must
    # not relabel the old v2 evidence as v3 or admit a stale GEO correction.
    async with connection.transaction():
        await connection.execute('INSERT INTO rkb_v4.revisions VALUES(%s,3)',(sources[1],))
        await connection.execute('UPDATE rkb_v4.active_revision SET revision=3 WHERE source_id=%s',(sources[1],))
    assert {str(x[0]) for x in await queue(5)}=={sources[0]}
    stale=resolved_command(**args(source_id=sources[1],source_revision=3,sequence=4,
        operation_key='geo-stale-proof'))
    assert await check(stale) is False
