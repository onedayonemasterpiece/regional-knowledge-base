"""Local Launcher import watch: real encrypted service credential, ASGI and intake calls.

All objects/data are synthetic. No Kaggle, original PDF or live credentials.
"""
from contextlib import asynccontextmanager
import asyncio
import base64
import hashlib
import json
from types import SimpleNamespace
import time
from uuid import UUID, uuid4, uuid5

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.contracts import BookIngestOutput
from regional_knowledge.import_activity_reader import ImportActivityReader, PATH
from regional_knowledge.kaggle_importer import KaggleSourceImporter
from regional_knowledge.service_auth import new_service_token, service_scope
from test_read_authority import auth
from test_participants import people


@pytest.fixture
def watcher(tmp_path, auth):
    queue = BgeQueue(tmp_path / "queue.sqlite")
    ingestion, source, actor = str(uuid4()), str(uuid4()), auth.p.owner.subject
    now = time.time()
    with queue.connect() as db:
        db.execute("""CREATE TABLE rkb_source_intake_control (
            ingestion_id TEXT PRIMARY KEY, source_id TEXT, person_id TEXT,
            phase TEXT, created REAL, last_operation_at REAL, research_json TEXT)""")
        db.execute("""CREATE TABLE rkb_native_source_intake_state (
            ingestion_id TEXT PRIMARY KEY, source_id TEXT, person_id TEXT,
            phase TEXT, pending_operation_key TEXT, packet_mode INTEGER,
            batch_count INTEGER, published_batch_count INTEGER,
            enriched_batch_count INTEGER, rejected_batch_index INTEGER,
            read_completion_requested INTEGER)""")
        db.execute("""CREATE TABLE rkb_control_operations (
            source_id TEXT, person_id TEXT, operation_key TEXT,
            sequence INTEGER, seal_state TEXT, apply_state TEXT,
            command_bytes BLOB)""")
        db.execute(
            "INSERT INTO rkb_source_intake_control VALUES(?,?,?,?,?,?,?)",
            (ingestion, source, actor, "staging", now - 10, now - 5,
             json.dumps({"continuation":{"tool":"book_pages","arguments":{"cursor":"9"}}})),
        )
        db.execute(
            "INSERT INTO rkb_native_source_intake_state VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (ingestion, source, actor, "staging", None, 1, 1, 1, 1, None, 0),
        )
        stage = {"ingestion_id":ingestion, "batch_index":0,
                 "model_payload":{"pages":[{"physical_page_index":page} for page in (3,8)]}}
        raw=json.dumps(stage).encode()
        packet = {"ingestion_id":ingestion, "batch_index":0, "component":"text",
                  "last_batch_sha256":hashlib.sha256(raw).hexdigest()}
        for seq, key, payload in ((1,f"source-stage:{ingestion}:0:0",raw),
                                  (2,f"source-packet:{ingestion}:0:text",json.dumps(packet).encode())):
            command=json.dumps({"payload_b64":base64.b64encode(payload).decode()}).encode()
            db.execute("INSERT INTO rkb_control_operations VALUES(?,?,?,?,?,?,?)",
                (source,actor,key,seq,"ACCEPTED","APPLIED",command))
    startup = SimpleNamespace(importer=None,
        config=SimpleNamespace(queue=queue, oauth_provider=auth.provider),
        reads_current=lambda:True)
    importer = object.__new__(KaggleSourceImporter)
    importer.queue, importer.startup = queue, startup
    importer._lock = asyncio.Lock()
    startup.importer = importer
    ident, token = new_service_token()
    auth.provider.issue_service_credential(
        auth.p.admin, service_id="launcher-import-watch", token=token,
        scope={"operation":"import_activity","owner_person_id":actor}, expires_at=auth.now[0]+86400,
        reason="synthetic read-only watcher")
    receiver = ImportActivityReader(
        importer=importer, service_id="launcher-import-watch",
        credential_id=ident, credential_epoch=1, current=lambda:True)
    headers = {"Authorization":"Bearer "+token,
        "X-RKB-Service":"launcher-import-watch",
        "X-RKB-Service-Credential":ident,
        "X-RKB-Service-Epoch":"1"}
    app = Starlette(routes=[Route(PATH, receiver.__call__, methods=["GET"])])
    return SimpleNamespace(queue=queue, importer=importer, startup=startup,
        auth=auth, ingestion=ingestion, source=source, actor=actor,
        ident=ident, token=token, receiver=receiver, headers=headers, app=app)


def read_timestamp(w):
    with w.queue.connect(write=False) as db:
        return db.execute("SELECT last_operation_at FROM rkb_source_intake_control "
                          "WHERE ingestion_id=?", (w.ingestion,)).fetchone()[0]


async def get(w, *, headers=None, client="127.0.0.1", query=None):
    transport = httpx.ASGITransport(app=w.app, client=(client, 9090))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as client:
        return await client.get(PATH, params=query if query is not None else
                                {"ingestion_id":w.ingestion},
                                headers=w.headers if headers is None else headers)



def synthetic_page_records(db,w,*,batch,physical,sequence,text_state="APPLIED"):
    stage=dict(ingestion_id=w.ingestion,batch_index=batch,
        model_payload=dict(pages=[dict(physical_page_index=physical,
            synthetic_review='x'*16384)]))
    raw=json.dumps(stage,separators=(",",":")).encode()
    text=dict(ingestion_id=w.ingestion,batch_index=batch,component="text",
              last_batch_sha256=hashlib.sha256(raw).hexdigest())
    for n,kind,payload,applied in (
        (0,"stage",raw,"APPLIED"),
        (1,"packet",json.dumps(text).encode(),text_state)):
        key=(f"source-stage:{w.ingestion}:{batch}:0" if kind=="stage" else
             f"source-packet:{w.ingestion}:{batch}:text")
        command=json.dumps({"payload_b64":base64.b64encode(payload).decode()}).encode()
        db.execute("INSERT INTO rkb_control_operations VALUES(?,?,?,?,?,?,?)",
            (w.source,w.actor,key,sequence+n,"ACCEPTED",applied,command))


@pytest.mark.asyncio
async def test_activity_requires_matching_text_receipt_and_respects_unaccepted_reverse_cursor(watcher):
    w=watcher
    with w.queue.connect() as db:
        synthetic_page_records(db,w,batch=1,physical=9,sequence=3,text_state="REJECTED_PRECONDITION")
        # Native published counters must not invent accepted page 9.
        db.execute("UPDATE rkb_native_source_intake_state SET published_batch_count=100")
        db.execute("UPDATE rkb_source_intake_control SET research_json=? WHERE ingestion_id=?",
            (json.dumps({"continuation":{"arguments":{"cursor":"3"}}}),w.ingestion))
    status=(await get(w)).json()
    assert status["accepted_pages"]==2 and status["next_cursor"]=="4"
    with w.queue.connect() as db:
        db.execute("UPDATE rkb_source_intake_control SET research_json=? WHERE ingestion_id=?",
            (json.dumps({"continuation":{"arguments":{"cursor":"5"}}}),w.ingestion))
    assert (await get(w)).json()["next_cursor"]=="5", "unaccepted reverse/gap cursor is preserved"
    with w.queue.connect() as db:
        db.execute("UPDATE rkb_source_intake_control SET research_json=? WHERE ingestion_id=?",
            (json.dumps({"continuation":{"arguments":{"cursor":"9"}}}),w.ingestion))
    assert (await get(w)).json()["accepted_pages"]==2
    key=f"source-packet:{w.ingestion}:1:text"
    with w.queue.connect() as db:
        original=db.execute("SELECT command_bytes FROM rkb_control_operations WHERE operation_key=?",
            (key,)).fetchone()[0]
        command=json.loads(original)
        packet=json.loads(base64.b64decode(command['payload_b64']))
        packet['last_batch_sha256']='0'*64
        command['payload_b64']=base64.b64encode(json.dumps(packet).encode()).decode()
        db.execute("UPDATE rkb_control_operations SET apply_state='APPLIED',command_bytes=? WHERE operation_key=?",
            (json.dumps(command).encode(),key))
    assert (await get(w)).json()['accepted_pages']==2, 'wrong TEXT parent hash is not publication'
    with w.queue.connect() as db:
        db.execute("UPDATE rkb_control_operations SET command_bytes=? WHERE operation_key=?",(original,key))
    status=(await get(w)).json()
    assert status["accepted_pages"]==3 and status["next_cursor"]=="10"


@pytest.mark.asyncio
async def test_activity_199_page_journal_skips_stale_140_not_rejected_198(watcher):
    """Synthetic scale of the observed 199 APPLIED TEXT pages + rejected duplicate."""
    import time as clock
    w=watcher
    with w.queue.connect() as db:
        db.execute("DELETE FROM rkb_control_operations")
        for index in range(199):
            synthetic_page_records(db,w,batch=index,physical=index,sequence=2*index+1)
        synthetic_page_records(db,w,batch=213,physical=198,sequence=399,
                               text_state="REJECTED_PRECONDITION")
        db.execute("UPDATE rkb_native_source_intake_state SET batch_count=214,"
                   "published_batch_count=214,enriched_batch_count=214,rejected_batch_index=213")
        db.execute("UPDATE rkb_source_intake_control SET research_json=? WHERE ingestion_id=?",
            (json.dumps({"continuation":{"arguments":{"cursor":"140"}}}),w.ingestion))
    begin=clock.perf_counter()
    result=(await get(w)).json()
    elapsed_ms=round(1000*(clock.perf_counter()-begin),2)
    print(f"synthetic 199 TEXT APPLIED pages + 1 rejected duplicate: watch={elapsed_ms} ms")
    assert (result["state"],result["accepted_pages"],result["next_cursor"])==("staged",199,"199")

@pytest.mark.asyncio
async def test_verified_original_before_first_stage_is_ready_for_reader(watcher):
    w=watcher
    with w.queue.connect() as db:
        db.execute('DELETE FROM rkb_native_source_intake_state')
        db.execute('DELETE FROM rkb_control_operations')
        db.execute('UPDATE rkb_source_intake_control SET research_json=NULL')
    original=read_timestamp(w)
    result=(await get(w)).json()
    assert (result['state'],result['next_action'])==('staged','continue_pages')
    assert result['accepted_pages']==0 and result['next_cursor'] is None
    assert read_timestamp(w)==original
    with w.queue.connect() as db:
        db.execute("UPDATE rkb_source_intake_control SET phase='acquiring'")
    pending=(await get(w)).json()
    assert (pending['state'],pending['next_action'])==('processing','wait')


@pytest.mark.asyncio
async def test_authorized_activity_is_redacted_read_only_and_cold_is_honest(watcher):
    w=watcher
    original=read_timestamp(w)
    response=await get(w)
    result=response.json()
    assert response.status_code==200
    assert response.headers["cache-control"]=="no-store"
    assert set(result)=={"state","next_action","last_operation_at","accepted_pages","next_cursor"}
    assert result["state"]=="staged" and result["next_action"]=="continue_pages"
    assert result["accepted_pages"]==2 and result["next_cursor"]=="9"
    assert result["last_operation_at"].endswith("Z")
    assert w.actor not in str(result) and w.source not in str(result)
    assert read_timestamp(w)==original  # GET has zero activity side effects
    w.startup.reads_current=lambda:False
    cold=(await get(w))
    assert cold.status_code==200
    assert (cold.json()["state"],cold.json()["next_action"])==("processing","wait")
    assert cold.json()["last_operation_at"]==result["last_operation_at"]
    assert cold.json()["accepted_pages"]==2 and read_timestamp(w)==original


@pytest.mark.asyncio
async def test_scoped_service_can_read_assigned_ingestion_not_real_foreign_owner(watcher):
    w = watcher
    foreign_ingestion, foreign_source = str(uuid4()), str(uuid4())
    foreign_owner = w.auth.p.other.subject
    foreign_timestamp = time.time() - 20
    with w.queue.connect() as db:
        db.execute(
            "INSERT INTO rkb_source_intake_control VALUES(?,?,?,?,?,?,?)",
            (foreign_ingestion, foreign_source, foreign_owner, "staging",
             foreign_timestamp - 10, foreign_timestamp, None),
        )
        assert db.execute("SELECT person_id FROM rkb_source_intake_control WHERE ingestion_id=?",
                          (foreign_ingestion,)).fetchone()[0] == foreign_owner
    own_before = read_timestamp(w)
    own = await get(w)
    foreign = await get(w, query={"ingestion_id": foreign_ingestion})
    assert own.status_code == 200 and foreign.status_code == 403
    assert foreign.json() == {"error": "service_scope_required"}
    with w.queue.connect(write=False) as db:
        assert db.execute("SELECT last_operation_at FROM rkb_source_intake_control WHERE ingestion_id=?",
                          (foreign_ingestion,)).fetchone()[0] == foreign_timestamp
    assert read_timestamp(w) == own_before
    with pytest.raises(ValueError):
        service_scope({"operation":"import_activity"})
    with pytest.raises(ValueError):
        service_scope({"operation":"import_activity","owner_person_id":str(uuid4()),"extra":"read"})


@pytest.mark.asyncio
async def test_missing_foreign_remote_revoked_and_user_tokens_cannot_watch(watcher):
    w=watcher
    assert (await get(w,headers={})).status_code==403
    assert (await get(w,client="192.0.2.17")).status_code==403
    assert (await get(w,headers=dict(w.headers, Authorization="Bearer "+w.auth.tokens.access_token))).status_code==403
    assert (await get(w,headers=dict(w.headers, **{"X-RKB-Service":"projects-hub-reader"}))).status_code==403
    ident, foreign_token = new_service_token()
    w.auth.provider.issue_service_credential(
        w.auth.p.admin, service_id="projects-hub-reader", token=foreign_token,
        scope={"operation":"knowledge_search", "provider":"projects_hub",
               "provider_tenant":"ph.fixture", "workspace_ids":["ws_fixture"]},
        expires_at=w.auth.now[0]+86400, reason="foreign synthetic principal")
    assert (await get(w,headers=dict(w.headers,Authorization="Bearer "+foreign_token))).status_code==403
    assert (await get(w,query={"ingestion_id":str(uuid4())})).status_code==404
    assert (await get(w,query={"ingestion_id":w.ingestion,"extra":"value"})).status_code==422
    w.auth.provider.revoke_service_credential(
        w.auth.p.admin,w.ident,expected_epoch=1,reason="synthetic revoked watcher")
    assert (await get(w)).status_code==403
    with pytest.raises(ValueError):
        service_scope({"operation":"import_activity","workspace_ids":["ws_fixture"]})


@pytest.mark.asyncio
async def test_status_preserves_reader_silence_but_pages_and_stage_refresh_it(watcher):
    w=watcher
    @asynccontextmanager
    async def access(_principal):
        yield SimpleNamespace(authority=SimpleNamespace(person_id=w.actor),
                              context=object(),binding=object())
    async def receipt(*_):return object()
    async def check(*_):pass
    async def reply(_access, result, **_):return result
    async def pages(*_, **__):
        page=SimpleNamespace(
            data=b"image", physical_page_index=0,
            page_id=str(uuid5(UUID(w.source),"page:1:0")),
            mime_type="image/jpeg",native_text_info={"preview_render":{}})
        return SimpleNamespace(pages=[page],next_cursor="1")
    async def stage(*_):
        return BookIngestOutput(ingestion_id=w.ingestion,document_id=w.source,
            state="staged",message="synthetic stage",next_action="continue_pages")
    w.importer._access=access
    w.importer._receipt=receipt
    w.importer._check=check
    w.importer._reply=reply
    w.importer.originals=SimpleNamespace(pages=pages)
    w.importer.native_intake=SimpleNamespace(stage=stage)
    w.importer._native_status=stage
    with w.queue.connect() as db:
        db.execute('UPDATE rkb_source_intake_control SET last_operation_at=? WHERE ingestion_id=?',
                   (time.time()-21*60,w.ingestion))
    before=read_timestamp(w)
    for _ in range(2):
        await w.importer.book_ingest(command='status',principal=object(),ingestion_id=w.ingestion)
        assert read_timestamp(w)==before,'an observer must not restart the 20-minute reader silence clock'
        assert time.time()-read_timestamp(w)>20*60
    await w.importer.book_pages(principal=object(),ingestion_id=w.ingestion)
    after_pages=read_timestamp(w)
    assert after_pages>before
    await w.importer.book_ingest(command="stage",principal=object(),
                                 ingestion_id=w.ingestion,payload={"pages":[]})
    after_stage=read_timestamp(w)
    assert after_stage>after_pages
    result=(await get(w)).json()
    assert result["last_operation_at"]!=None
    assert result["last_operation_at"].endswith("Z")
    assert result["accepted_pages"]==2
