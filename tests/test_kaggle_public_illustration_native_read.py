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
