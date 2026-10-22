"""Native media SQL014/015 snapshot: real existing authority gate, FAKE native I/O.

One fake reader pool and one READ ONLY transaction per internal snapshot.
No Dev/Kaggle PostgreSQL, Telegram, provider, client credentials or uploads.
Optional SQL-compile probe uses ONLY the preexisting GitHub PG17 CI service,
rollback-only, and does NOT assert data import/role grants/live archive readiness.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace
import hashlib
import os

import pytest
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from regional_knowledge.kaggle_public_illustration_gate import _canonical
from regional_knowledge.kaggle_public_illustration_native_read import (
    NativePublicIllustrationReader, NativeIllustrationReadUnavailable,
    SQL014_SHA256, SQL015_SHA256, READ_SQL, READ_SQL_SHA256,
    _FIELDS, _row_to_snapshot,
)
from test_control_spool import env, SOURCE
from test_read_authority import auth
from test_participants import people
from test_kaggle_public_illustration_gate import (
    snapshot, ExistingArchiveReader, media_renderer, archive_validator,
    make_gate, issue, ILLUSTRATION, READER_SHA,
)
from test_story_reconcile_ordered_013_016_pg import _validated_ci_dsn, _files


def source_row(s=None):
    value = s or snapshot()
    item = value.item
    binding = item["crop_binding"]
    row = dict(
        source_id=value.source_id, illustration_id=value.illustration_id,
        source_revision=value.source_revision,
        eligibility_revision=value.eligibility_revision,
        accepted=value.accepted, withdrawn=value.withdrawn,
        original_sha256=value.original_sha256,
        origin_original_sha256=value.origin_original_sha256,
        media_candidate_sha256=value.media_candidate_sha256,
        media_origin_sha256=value.media_origin_sha256,
        item=item, manifest_page=value.manifest_page,
        manifest_region=value.manifest_region,
        archive_binding_bytes=_canonical(binding),
        archive_operation_key="illustration:" + hashlib.sha256(_canonical(binding)).hexdigest(),
        archive_status=value.archive_state["status"],
        photo=value.archive_state["photo"],
        document=value.archive_state["document"],
        archive_chat_id=value.archive_chat_id, archive_topic_id=value.archive_topic_id,
        database_generation=value.database_generation,
        observed_data_seq=value.observed_data_seq,
        observed_projection_seq=value.observed_projection_seq,
        observed_visibility_epoch=value.observed_visibility_epoch,
    )
    return tuple(row[k] for k in _FIELDS)


class _Context:
    def __init__(self, db, name, value):
        self.db, self.name, self.value = db, name, value

    async def __aenter__(self):
        self.db.events.append("enter:" + self.name)
        if self.name == "transaction":
            self.db.transactions += 1
        return self.value

    async def __aexit__(self, exc_type, exc, tb):
        self.db.events.append("exit:" + self.name)
        return False


class _Cursor:
    def __init__(self, db):
        self.db = db

    async def execute(self, sql, params=()):
        await asyncio.sleep(0)
        self.db.events.append("execute:" + sql[:25])
        self.db.statements.append((sql, params))
        if self.db.on_execute:
            self.db.on_execute(sql)
        if self.db.raise_execute:
            raise self.db.raise_execute

    async def fetchmany(self, count):
        await asyncio.sleep(0)
        self.db.events.append("fetch")
        if self.db.on_fetch:
            self.db.on_fetch()
        assert count == 2
        return self.db.rows


class _ExistingReaderPool:
    def __init__(self, rows=None):
        self.rows = [source_row()] if rows is None else rows
        self.closed = False
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.row_factory = tuple_row
        self.statements = []
        self.events = []
        self.transactions = 0
        self.on_execute = None
        self.on_fetch = None
        self.raise_execute = None
        self.acquire_calls = 0
        self.lease = True
        self.connection = SimpleNamespace()

    def acquire(self):
        self.acquire_calls += 1
        return _Context(self, "existing-pool", self)

    def transaction(self):
        return _Context(self, "transaction", self)

    def cursor(self):
        return _Context(self, "cursor", _Cursor(self))


def reader(pool, binding, *, timeout_seconds=2):
    return NativePublicIllustrationReader(
        acquire=pool.acquire,
        binding_current=lambda current: current is binding and pool.lease,
        archive_chat_id=-100234, archive_scope="owner-owned-topic4",
        timeout_seconds=timeout_seconds,
    )


async def read_current(read, binding):
    return await read(SOURCE, ILLUSTRATION,
                      "60000000-0000-0000-0000-000000000006", binding)


@pytest.mark.asyncio
async def test_single_repeatable_read_only_transaction_parameterized_actual_media_sql():
    from test_source_read_gate import binding
    b=binding()
    pool=_ExistingReaderPool()
    read=reader(pool,b)
    result=await read_current(read,b)
    assert type(result).__name__=="NativeMediaSnapshot"
    assert result.native_reader_sha256==READ_SQL_SHA256
    assert result.item["crop_binding"]["source_id"]==SOURCE
    assert result.public_row["data"]=={
        k:v for k,v in result.item.items() if k!="crop_binding"}
    assert pool.acquire_calls==1 and pool.transactions==1
    assert pool.events.count("enter:transaction")==1
    assert pool.events.count("exit:transaction")==1
    assert pool.events[-1]=="exit:existing-pool"
    assert len(pool.statements)==3
    assert pool.statements[0][0]=="SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
    assert "set_config('statement_timeout'" in pool.statements[1][0]
    assert pool.statements[2]==(READ_SQL,(SOURCE,ILLUSTRATION,-100234,4))
    assert "LIMIT 2" in READ_SQL
    assert all(word not in READ_SQL.upper() for word in
               ("INSERT INTO ", "UPDATE RKB_V4.", "DELETE FROM ", "GRANT ", "CREATE ROLE "))
    assert all(name in READ_SQL for name in (
        "rkb_v4.source_illustrations", "rkb_v4.source_illustration_candidates",
        "rkb_v4.source_illustration_origins",
        "rkb_v4.source_illustration_availability",
        "rkb_v4.source_image_origins", "rkb_v4.source_manifests",
        "rkb_v4.source_import_origins", "rkb_v4.applied_ops",
        "rkb_v4.active_revision", "rkb_v4.source_visibility",
        "rkb_v4.recovery_state", "rkb_v4.read_frontier"))


@pytest.mark.asyncio
async def test_real_canonical_read_authority_and_source_gate_accept_exact_internal_snapshot(env,auth):
    gate,old_native,archive,verified,binding=await make_gate(env,auth)
    pool=_ExistingReaderPool()
    read=NativePublicIllustrationReader(
        acquire=pool.acquire, binding_current=gate.source_gate.binding_current,
        archive_chat_id=-100234, archive_scope="owner-owned-topic4")
    gate.native_read=read
    gate.reader_sha=READ_SQL_SHA256
    result=await issue(gate,verified,binding)
    assert result.frame.artifact.kind=="server_opaque_illustration"
    assert "crop_binding" not in result.frame.model_dump_json()
    assert "chat_id" not in result.frame.model_dump_json()
    assert gate.issuance_current(result.ticket)
    assert pool.acquire_calls==2 and pool.transactions==2
    gate.release(result.ticket)


@pytest.mark.asyncio
@pytest.mark.parametrize("change",[
    "withdrawn","not_accepted","source_revision","eligibility",
    "original","origin","media_sha","candidate_sha",
    "wrong_page","wrong_region","wrong_bbox","wrong_crop",
    "wrong_raster","wrong_receipt","wrong_binding_bytes","wrong_operation",
    "wrong_chat","wrong_topic","wrong_db","projection_lag","stale_sequence",
    "pending_archive","no_media_locator"
])
async def test_exact_source_original_manifest_crop_archive_and_frontier_negative(change):
    from test_source_read_gate import binding
    b=binding()
    row=dict(zip(_FIELDS,source_row()))
    if change=="withdrawn":row["withdrawn"]=True
    elif change=="not_accepted":row["accepted"]=False
    elif change=="source_revision":row["source_revision"]=2
    elif change=="eligibility":row["eligibility_revision"]=0
    elif change=="original":row["original_sha256"]="a"*64
    elif change=="origin":row["origin_original_sha256"]="a"*64
    elif change=="media_sha":row["media_origin_sha256"]="a"*64
    elif change=="candidate_sha":row["media_candidate_sha256"]="z"*64
    elif change=="wrong_page":row["manifest_page"]={**row["manifest_page"],"page_id":"70000000-0000-0000-0000-000000000007"}
    elif change=="wrong_region":row["manifest_region"]={**row["manifest_region"],"region_id":"70000000-0000-0000-0000-000000000007"}
    elif change=="wrong_bbox":row["manifest_region"]={**row["manifest_region"],"bbox":dict(left=5,top=5,right=10,bottom=10)}
    elif change=="wrong_crop":row["item"]={**row["item"],"crop_binding":{**row["item"]["crop_binding"],"source_id":"70000000-0000-0000-0000-000000000007"}}
    elif change=="wrong_raster":row["manifest_page"]={**row["manifest_page"],"image_sha256":"f"*64}
    elif change=="wrong_receipt":row["manifest_page"]={**row["manifest_page"],"source_image_receipt_sha256":"f"*64}
    elif change=="wrong_binding_bytes":row["archive_binding_bytes"]=b'{"source_id":"forged"}'
    elif change=="wrong_operation":row["archive_operation_key"]="illustration:"+"f"*64
    elif change=="wrong_chat":row["archive_chat_id"]=-100235
    elif change=="wrong_topic":row["archive_topic_id"]=2
    elif change=="wrong_db":row["database_generation"]="obsolete"
    elif change=="projection_lag":row["observed_projection_seq"]=1
    elif change=="stale_sequence":row["observed_data_seq"]=-1
    elif change=="pending_archive":row["archive_status"]="unknown"
    elif change=="no_media_locator":row["photo"]=None
    pool=_ExistingReaderPool(rows=[tuple(row[k] for k in _FIELDS)])
    with pytest.raises(NativeIllustrationReadUnavailable):
        await read_current(reader(pool,b),b)
    assert pool.transactions==1
    assert pool.events[-1]=="exit:existing-pool"


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[],[source_row(),source_row()]])
async def test_absent_or_ambiguous_pg_snapshot_never_available(rows):
    from test_source_read_gate import binding
    b=binding()
    pool=_ExistingReaderPool(rows=rows)
    with pytest.raises(NativeIllustrationReadUnavailable):
        await read_current(reader(pool,b),b)
    assert pool.acquire_calls==1


@pytest.mark.asyncio
@pytest.mark.parametrize("when", ["before","execute","fetch","after_cursor"])
async def test_reader_lease_changes_before_and_during_native_await(when):
    from test_source_read_gate import binding
    b=binding()
    pool=_ExistingReaderPool()
    read=reader(pool,b)
    if when=="before":pool.lease=False
    elif when=="execute":
        pool.on_execute=lambda sql: setattr(pool,"lease",False) if "FROM rkb_v4.source_illustrations" in sql else None
    elif when=="fetch":pool.on_fetch=lambda:setattr(pool,"lease",False)
    else:pool.on_fetch=lambda:setattr(pool,"lease",False)
    with pytest.raises(NativeIllustrationReadUnavailable):
        await read_current(read,b)
    if when=="before":assert pool.acquire_calls==0
    else:assert pool.acquire_calls==1


@pytest.mark.asyncio
async def test_exact_actor_and_source_selectors_are_not_self_grants():
    from test_source_read_gate import binding
    b=binding()
    pool=_ExistingReaderPool()
    read=reader(pool,b)
    for selectors in [
        (SOURCE,ILLUSTRATION,"not-an-actor"),
        ("not-a-source",ILLUSTRATION,"60000000-0000-0000-0000-000000000006"),
        (SOURCE,"bad-illustration","60000000-0000-0000-0000-000000000006")]:
        with pytest.raises(NativeIllustrationReadUnavailable):
            await read(*selectors,b)
    assert pool.acquire_calls==0


@pytest.mark.asyncio
async def test_missing_real_pg_private_select_grant_is_generic_unavailable():
    from test_source_read_gate import binding
    b=binding()
    pool=_ExistingReaderPool()
    pool.raise_execute=PermissionError("PRIVATE_RKB_PG_ROLE_DETAILS")
    with pytest.raises(NativeIllustrationReadUnavailable) as exc:
        await read_current(reader(pool,b),b)
    assert "PRIVATE_RKB" not in str(exc.value)
    assert "SQL" not in str(exc.value)


@pytest.mark.asyncio
async def test_cancel_keeps_lease_unissued_and_exits_existing_transaction():
    from test_source_read_gate import binding
    b=binding()
    pool=_ExistingReaderPool()
    entered=asyncio.Event()
    blocker=asyncio.Event()
    async def hang():
        entered.set()
        await blocker.wait()
    original=_Cursor.fetchmany
    async def waiting(self,count):
        await hang()
        return await original(self,count)
    from unittest.mock import patch
    with patch.object(_Cursor,"fetchmany",waiting):
        task=asyncio.create_task(read_current(reader(pool,b),b))
        await asyncio.wait_for(entered.wait(),2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert pool.events[-1]=="exit:existing-pool"
    assert pool.transactions==1


@pytest.mark.asyncio
async def test_deadline_is_bounded_without_any_provider_work():
    from test_source_read_gate import binding
    b=binding()
    pool=_ExistingReaderPool()
    original=_Cursor.fetchmany
    async def slow(self,n):
        await asyncio.sleep(.04)
        return await original(self,n)
    from unittest.mock import patch
    with patch.object(_Cursor,"fetchmany",slow):
        with pytest.raises(NativeIllustrationReadUnavailable,match="native_media_read_deadline"):
            await read_current(reader(pool,b,timeout_seconds=.01),b)
    assert pool.events[-1]=="exit:existing-pool"


def test_static_exact_source_sql_pins_are_immutable_in_this_leaf():
    from pathlib import Path
    root=Path(__file__).resolve().parents[1]
    assert hashlib.sha256((root/"sql/kaggle/014_source_stage_activation.sql").read_bytes()).hexdigest()==SQL014_SHA256
    assert hashlib.sha256((root/"sql/kaggle/015_source_illustrations.sql").read_bytes()).hexdigest()==SQL015_SHA256
    assert hashlib.sha256(READ_SQL.encode()).hexdigest()==READ_SQL_SHA256


class _Rollback(Exception):
    pass


@pytest.mark.asyncio
@pytest.mark.skipif(not os.environ.get("RKB_KAGGLE_TEST_DSN"),
                    reason="Only existing approved GitHub PG17 CI fixture may run SQL")
async def test_real_github_pg17_001_to_016_executes_exact_private_sql_without_rows():
    """Actual PG syntax/schema gate, not native data/role/authorization acceptance."""
    import psycopg
    from pathlib import Path
    from regional_knowledge.kaggle_public_illustration_native_read import READ_SQL
    root=Path(__file__).resolve().parents[1]
    dsn=_validated_ci_dsn()
    conn=await psycopg.AsyncConnection.connect(dsn,connect_timeout=2,sslmode="disable")
    try:
        try:
            async with conn.transaction():
                assert (await (await conn.execute("SELECT to_regnamespace('rkb_v4')")).fetchone())[0] is None
                await conn.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
                files=_files()
                for n in range(1,17):
                    if n==16:
                        roles=await (await conn.execute("""SELECT rolname FROM pg_roles
                            WHERE rolname IN ('rkb_reader','rkb_writer')""")).fetchall()
                        assert roles==[]
                        await conn.execute("CREATE ROLE rkb_reader NOLOGIN")
                        await conn.execute("CREATE ROLE rkb_writer NOLOGIN")
                    await conn.execute(files[n].read_text(),prepare=False)
                row=await (await conn.execute(READ_SQL,(
                    SOURCE,ILLUSTRATION,-100234,4))).fetchall()
                assert row==[], "unseeded PostgreSQL must not invent an authorized image"
                raise _Rollback
        except _Rollback:
            pass
        assert (await (await conn.execute("SELECT to_regnamespace('rkb_v4')")).fetchone())[0] is None
    finally:
        await conn.close()


# This real-PG gate uses deliberately synthetic SQL-shaped rows. It proves that
# the actual PostgreSQL SELECT returns a *populated* figure/crop snapshot and
# rejects changes; it does NOT simulate an accepted command, provider ACK,
# durable Dataset, real image payload or installed production reader grants.
@pytest.mark.asyncio
@pytest.mark.skipif(not os.environ.get("RKB_KAGGLE_TEST_DSN"),
                    reason="Only existing GitHub PG17 CI fixture is authorized")
@pytest.mark.parametrize("scenario", (
    "valid_pdf", "valid_djvu", "wrong_original", "wrong_media_origin",
    "wrong_figure", "wrong_crop", "missing_locator", "unknown_archive"))
async def test_real_pg17_populated_sql15_media_snapshot_and_denials(scenario):
    import base64
    import psycopg
    from psycopg.types.json import Jsonb
    from regional_knowledge.kaggle_public_illustration_native_read import _row_to_snapshot

    s=snapshot()
    source=s.source_id
    figure=s.illustration_id
    actor="60000000-0000-0000-0000-000000000006"
    ingestion="70000000-0000-0000-0000-000000000007"
    op="fixture:media-relational-read:1"
    receipt=_canonical(dict(sha256=s.original_sha256,
                            format="djvu" if scenario=="valid_djvu" else "pdf"))
    receipt_sha=hashlib.sha256(receipt).hexdigest()
    projection=_canonical(dict(codec="synthetic-activation",source_id=source))
    projection_sha=hashlib.sha256(projection).hexdigest()
    cmd=_canonical(dict(route="fixture-only-non-domain-read",
                        ids=dict(source_id=source),person_id=actor))
    cmd_sha=hashlib.sha256(cmd).hexdigest()
    knowledge=dict(passages=[],semantic=dict(stories=[]))
    knowledge_bytes=_canonical(knowledge)
    knowledge_sha=hashlib.sha256(knowledge_bytes).hexdigest()
    media=dict(codec="rkb.source-illustrations.v1",source_id=source,
               source_revision=1,illustrations=[s.item],
               passage_links=[],poi_media_links=[])
    media_bytes=_canonical(media)
    media_sha=hashlib.sha256(media_bytes).hexdigest()
    page=s.manifest_page.copy()
    region=s.manifest_region.copy()
    if scenario=="wrong_figure":
        region["bbox"]={**region["bbox"],"left":1}
    manifest=dict(codec="rkb.source-image-manifest.v1",
        source_id=source,source_revision=1,
        original_receipt_b64=base64.b64encode(receipt).decode(),
        metadata=dict(title="Fixture illustrated book",authors=[]),
        pages=[page],regions=[region],passages=[])
    crop=_canonical(s.item["crop_binding"])
    if scenario=="wrong_crop":crop=b'{"source_id":"forged"}'
    crop_op="illustration:"+hashlib.sha256(crop).hexdigest()
    photo=None if scenario=="missing_locator" else s.archive_state["photo"]
    archive_status="unknown" if scenario=="unknown_archive" else "ready"

    conn=await psycopg.AsyncConnection.connect(
        _validated_ci_dsn(),connect_timeout=2,sslmode="disable")
    class Rollback(Exception):pass
    class SavepointRollback(Exception):pass
    try:
        try:
            async with conn.transaction():
                assert (await(await conn.execute(
                    "SELECT to_regnamespace('rkb_v4')")).fetchone())[0] is None
                await conn.execute("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
                files=_files()
                for ordinal in range(1,17):
                    if ordinal==16:
                        roles=await(await conn.execute("""SELECT rolname FROM pg_roles
                            WHERE rolname IN ('rkb_reader','rkb_writer')""")).fetchall()
                        assert roles==[]
                        await conn.execute("CREATE ROLE rkb_reader NOLOGIN")
                        await conn.execute("CREATE ROLE rkb_writer NOLOGIN")
                    await conn.execute(files[ordinal].read_text(),prepare=False)

                # Inert synthetic ledger FK row: route explicitly cannot run any
                # accepted source activation trigger. No fake "Saved" asserted.
                await conn.execute("INSERT INTO rkb_v4.writer_generations(generation) VALUES(%s)",
                                   ("fixture-writer",))
                await conn.execute("""INSERT INTO rkb_v4.recovery_state
                    (singleton,database_generation,writer_generation,applied_seq,
                    schema_revision,function_revision,executor_revision)
                    VALUES(true,'db-source','fixture-writer',1,'fixture-schema',
                           'fixture-functions','fixture-executor')""")
                await conn.execute("""INSERT INTO rkb_v4.applied_ops
                    (operation_key,sequence,command_bytes,command_sha256,
                     accepted_manifest_sha256,database_generation,writer_generation,
                     status,outcome_bytes,outcome_sha256,handler_sha256)
                    VALUES(%s,1,%s,%s,%s,'db-source','fixture-writer','applied',
                           %s,%s,%s)""",
                    (op,cmd,cmd_sha,"a"*64,b'{}',hashlib.sha256(b'{}').hexdigest(),"b"*64))
                await conn.execute("INSERT INTO rkb_v4.sources VALUES(%s,%s,true,false)",
                                   (source,"Synthetic " + scenario))
                await conn.execute("INSERT INTO rkb_v4.revisions VALUES(%s,1)",(source,))
                await conn.execute("INSERT INTO rkb_v4.active_revision VALUES(%s,1)",(source,))
                await conn.execute("""INSERT INTO rkb_v4.source_import_origins
                    (source_id,revision,work_id,edition_id,original_sha256,
                     original_size,original_ref,original_receipt_sha256,
                     payload_sha256,model_revision,embedding_space,
                     tokenizer_sha256,imported_logical_time)
                    VALUES(%s,1,%s,%s,%s,42,'fixture-source',%s,%s,
                        'fixture-model','fixture-BGE','fixture-tokenizer','fixture-time')""",
                    (source,"80000000-0000-0000-0000-000000000008",
                     "90000000-0000-0000-0000-000000000009",
                     "c"*64 if scenario=="wrong_original" else s.original_sha256,
                     receipt_sha,projection_sha))
                await conn.execute("""INSERT INTO rkb_v4.source_stage_sessions
                    (source_id,ingestion_id,source_metadata,original_receipt,
                     page_count,logical_time) VALUES(%s,%s,%s,%s,1,'fixture-time')""",
                    (source,ingestion,Jsonb({}),receipt))
                await conn.execute("""INSERT INTO rkb_v4.source_activation_candidates
                    (source_id,ingestion_id,batch_count,last_batch_sha256,
                     materializer_sha256,knowledge_bytes,knowledge_sha256,
                     knowledge,prepared_database,prepared_writer)
                    VALUES(%s,%s,1,%s,%s,%s,%s,%s,'db-source','fixture-writer')""",
                    (source,ingestion,"d"*64,"e"*64,knowledge_bytes,
                     knowledge_sha,Jsonb(knowledge)))
                await conn.execute("""INSERT INTO rkb_v4.source_image_origins
                    (source_id,ingestion_id,batch_count,last_batch_sha256,
                     materializer_sha256,knowledge_sha256,
                     activation_payload_sha256,operation_key,sequence,
                     person_id,request_id,command_sha256)
                    VALUES(%s,%s,1,%s,%s,%s,%s,%s,1,%s,'fixture-request',%s)""",
                    (source,ingestion,"d"*64,"e"*64,knowledge_sha,
                     projection_sha,op,actor,cmd_sha))
                await conn.execute("""INSERT INTO rkb_v4.source_manifests
                    (source_id,source_revision,manifest,payload_sha256,
                     original_receipt) VALUES(%s,1,%s,%s,%s)""",
                    (source,Jsonb(manifest),projection_sha,receipt))
                await conn.execute("""INSERT INTO rkb_v4.source_illustration_candidates
                    (source_id,knowledge_sha256,media_bytes,media_sha256,media)
                    VALUES(%s,%s,%s,%s,%s)""",
                    (source,knowledge_sha,media_bytes,media_sha,Jsonb(media)))
                await conn.execute("""INSERT INTO rkb_v4.source_illustrations
                    (source_id,illustration_id,data) VALUES(%s,%s,%s)""",
                    (source,figure,Jsonb(s.item)))
                await conn.execute("""INSERT INTO rkb_v4.source_illustration_origins
                    (source_id,media_sha256,knowledge_sha256,operation_key,
                     sequence,command_sha256,person_id)
                    VALUES(%s,%s,%s,%s,1,%s,%s)""",
                    (source,"f"*64 if scenario=="wrong_media_origin" else media_sha,
                     knowledge_sha,op,cmd_sha,actor))
                await conn.execute("""INSERT INTO rkb_v4.source_illustration_availability
                    (source_id,illustration_id,operation_key,binding_bytes,
                     archive_chat_id,archive_topic_id,version,status,photo,document)
                    VALUES(%s,%s,%s,%s,-100234,4,3,%s,%s,NULL)""",
                    (source,figure,crop_op,crop,archive_status,
                     Jsonb(photo) if photo is not None else None))
                await conn.execute("""UPDATE rkb_v4.read_frontier SET
                    projection_seq=1,text_ready=true,dense_ready=true WHERE singleton""")
                rows=await(await conn.execute(
                    READ_SQL,(source,figure,-100234,4))).fetchall()
                absent=scenario in ("wrong_media_origin","wrong_figure","unknown_archive")
                if absent:
                    assert rows==[],scenario
                else:
                    assert len(rows)==1,scenario
                    kwargs=dict(source_id=source,illustration_id=figure,
                        reader_key=("run","inc",1,"db-source",0,"fixture"),
                        archive_chat_id=-100234,archive_topic_id=4,
                        archive_scope="synthetic-topic4")
                    if scenario in ("wrong_original","wrong_crop","missing_locator"):
                        with pytest.raises(NativeIllustrationReadUnavailable):
                            _row_to_snapshot(rows[0],**kwargs)
                    else:
                        native=_row_to_snapshot(rows[0],**kwargs)
                        assert native.item==s.item
                        assert native.manifest_page==page
                        assert native.manifest_region==region
                        assert native.observed_data_seq==native.observed_projection_seq==1
                        assert native.public_row["data"]=={
                            k:v for k,v in s.item.items() if k!="crop_binding"}
                        sql15=await(await conn.execute(
                            "SELECT data,archive_status FROM rkb_v4.read_source_illustrations_v1(%s,NULL,4)",
                            (source,))).fetchall()
                        assert sql15==[(native.public_row["data"],"ready")]

                        # SAVEPOINT mutations never escape this disposable PG
                        # transaction; actual query must close on withdrawal.
                        try:
                            async with conn.transaction():
                                await conn.execute(
                                    "UPDATE rkb_v4.sources SET withdrawn=true WHERE source_id=%s",
                                    (source,))
                                assert await(await conn.execute(
                                    READ_SQL,(source,figure,-100234,4))).fetchall()==[]
                                raise SavepointRollback
                        except SavepointRollback:
                            pass
                        assert len(await(await conn.execute(
                            READ_SQL,(source,figure,-100234,4))).fetchall())==1
                        try:
                            async with conn.transaction():
                                await conn.execute("INSERT INTO rkb_v4.revisions VALUES(%s,2)",
                                                   (source,))
                                await conn.execute("""UPDATE rkb_v4.active_revision
                                    SET revision=2 WHERE source_id=%s""",(source,))
                                assert await(await conn.execute(
                                    READ_SQL,(source,figure,-100234,4))).fetchall()==[]
                                raise SavepointRollback
                        except SavepointRollback:
                            pass
                        assert len(await(await conn.execute(
                            READ_SQL,(source,figure,-100234,4))).fetchall())==1
                raise Rollback
        except Rollback:
            pass
        assert (await(await conn.execute(
            "SELECT to_regnamespace('rkb_v4')")).fetchone())[0] is None
    finally:
        await conn.close()
