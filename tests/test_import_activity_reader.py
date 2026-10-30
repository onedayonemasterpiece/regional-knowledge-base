"""Local Launcher import watch: real encrypted service credential, ASGI and intake calls.

All objects/data are synthetic. No Kaggle, original PDF or live credentials.
"""
from contextlib import asynccontextmanager
import asyncio
import base64
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
        for seq, pages in ((1, (3, 8)), (2, (8,))):
            doc = {"ingestion_id":ingestion, "batch_index":0,
                   "model_payload":{"pages":[{"physical_page_index":page} for page in pages]}}
            command = json.dumps({"payload_b64":base64.b64encode(
                json.dumps(doc).encode()).decode()}).encode()
            db.execute("INSERT INTO rkb_control_operations VALUES(?,?,?,?,?,?,?)",
                       (source, actor, f"source-stage:{ingestion}:0:{seq}",
                        seq, "ACCEPTED", "APPLIED", command))
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
        scope={"operation":"import_activity"}, expires_at=auth.now[0]+86400,
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
async def test_timestamp_moves_on_actual_book_pages_and_book_ingest_stage(watcher):
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
    before=read_timestamp(w)
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
