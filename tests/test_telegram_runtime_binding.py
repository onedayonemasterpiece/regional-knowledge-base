"""Deterministic RKB1 source-only checks. No private credentials or provider calls."""
from __future__ import annotations

import ast
import base64
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.telegram_runtime_binding import (
    Rkb1Lease, SLOT, TelegramBindingUnavailable,
    build_rkb1_downloader, check_rkb1_authorized,
)

RUN_A = "11111111-1111-4111-8111-111111111111"
RUN_B = "22222222-2222-4222-8222-222222222222"
INC_A = "runAaVeryLongIncarnation000001"
INC_B = "runBaVeryLongIncarnation000002"
CHAT = -1004368830579
PROVIDER = "zigomaro/rkb-bge-m3-cpu"
DEVICE = dict(app_version="RKB 1.0", device_model="RKB Kaggle worker",
              lang_code="ru", system_lang_code="ru-RU", system_version="CP313 Linux")
SYNTHETIC_SESSION = "TEST_SESSION_NOT_AUTHORIZED_ANYWHERE"


def _add_run(db, run_id, incarnation, *, provider_version=99, launch_nonce="nonceA"):
    db.execute("""INSERT INTO runs(
        id,status,token_hash,started,heartbeat,useful,ready_at,provider_ref,
        launch_state,provider_version,launch_nonce,runtime_incarnation,
        connection_generation,channel_active,attachment_conflict
        ) VALUES(?,'ready','synthetic',100,100,100,100,?,'dispatched',
                ?,?,?,1,1,0)""",
        (run_id, PROVIDER, provider_version, launch_nonce, incarnation))


@pytest.fixture
def seeded(tmp_path):
    now = [100.]
    queue = BgeQueue(tmp_path / "existing-private-queue.sqlite", clock=lambda: now[0])
    with queue.connect() as db:
        db.execute("""CREATE TABLE rkb_writer_control(
            id INTEGER PRIMARY KEY,run_id TEXT,active INTEGER,
            publication_json TEXT)""")
        _add_run(db, RUN_A, INC_A)
        db.execute("UPDATE control SET current_run=? WHERE id=1", (RUN_A,))
        db.execute("INSERT INTO rkb_writer_control(id,run_id,active) VALUES(1,?,1)", (RUN_A,))
    return queue, now


def _second_writer(queue):
    with queue.connect() as db:
        _add_run(db, RUN_B, INC_B, provider_version=100, launch_nonce="nonceB")
        db.execute("UPDATE control SET current_run=? WHERE id=1", (RUN_B,))
        db.execute("UPDATE rkb_writer_control SET run_id=? WHERE id=1", (RUN_B,))


def _terminal(queue, *, incarnation=INC_A, exact=True, status="terminal"):
    with queue.connect() as db:
        db.execute("UPDATE runs SET launch_state=? WHERE id=?", (status, RUN_A))
        receipt = dict(contract="kaggle-2.2.4-sdk-0.1.37-exact-session-persisted-stream-v1",
                       identity=dict(id=RUN_A, runtime_incarnation=incarnation,
                           provider_ref=PROVIDER,provider_version=99,launch_nonce="nonceA"))
        db.execute("""INSERT OR REPLACE INTO bge_provider_terminal_receipts
            (run_id,receipt_json) VALUES(?,?)""", (RUN_A, json.dumps(receipt)))


def test_native_bundle_import_has_no_dev_provider_sdk_dependency():
    # kaggle_target_launch.MODULES deliberately does not ship the Dev-only
    # native provider terminal observer; it is loaded solely by Dev lease handoff.
    source = Path(__file__).resolve().parents[1] / 'src/regional_knowledge/telegram_runtime_binding.py'
    tree = ast.parse(source.read_text(encoding='utf-8'))
    assert all(not (isinstance(node, ast.ImportFrom)
                    and node.module == 'kaggle_provider_terminal')
               for node in tree.body)


def test_claim_and_reconnect_keep_one_writer_incarnation(seeded):
    queue, _ = seeded
    lease = Rkb1Lease(queue, archive_chat_id=CHAT)
    assert lease.current(RUN_A, INC_A) is False
    assert lease.claim(RUN_A, INC_A) is True
    assert lease.current(RUN_A, INC_A) is True
    with queue.connect() as db:
        db.execute("""UPDATE runs SET connection_generation=2,channel_active=0
            WHERE id=?""", (RUN_A,))
    assert not lease.current(RUN_A, INC_A)  # suspended use, NOT released slot
    with queue.connect() as db:
        db.execute("""UPDATE runs SET connection_generation=3,channel_active=1,
            heartbeat=100 WHERE id=?""", (RUN_A,))
    assert lease.claim(RUN_A, INC_A)
    assert lease.current(RUN_A, INC_A)
    with queue.connect(write=False) as db:
        assert db.execute("SELECT count(*) FROM rkb_telegram_session_lease").fetchone()[0] == 1
        assert db.execute("SELECT incarnation FROM rkb_telegram_session_lease").fetchone()[0] == INC_A


def test_never_transfer_on_ttl_lost_heartbeat_error_or_new_incarnation(seeded):
    queue, clock = seeded
    lease = Rkb1Lease(queue, archive_chat_id=CHAT)
    lease.claim(RUN_A, INC_A)
    clock[0] = 9000
    assert not lease.current(RUN_A, INC_A)
    _second_writer(queue)
    with queue.connect() as db:
        db.execute("UPDATE runs SET heartbeat=?,useful=? WHERE id=?",
                   (clock[0],clock[0],RUN_B))
    with pytest.raises(TelegramBindingUnavailable, match="prior_execution_unknown"):
        lease.claim(RUN_B, INC_B)
    with queue.connect() as db:
        db.execute("UPDATE runs SET status='failed',channel_active=0 WHERE id=?", (RUN_A,))
        db.execute("UPDATE runs SET heartbeat=9000,useful=9000 WHERE id=?", (RUN_B,))
    with pytest.raises(TelegramBindingUnavailable, match="prior_execution_unknown"):
        lease.claim(RUN_B, INC_B)
    # Same run with a DIFFERENT incarnation cannot retake its old slot.
    with queue.connect() as db:
        db.execute("UPDATE runs SET runtime_incarnation=?,status='ready',channel_active=1,"
                   "heartbeat=9000,useful=9000 WHERE id=?", (INC_B, RUN_A))
        db.execute("UPDATE control SET current_run=? WHERE id=1", (RUN_A,))
        db.execute("UPDATE rkb_writer_control SET run_id=? WHERE id=1", (RUN_A,))
    with pytest.raises(TelegramBindingUnavailable, match="prior_execution_unknown"):
        lease.claim(RUN_A, INC_B)


def test_exact_provider_terminal_receipt_only_allows_transfer(seeded):
    queue, _ = seeded
    lease = Rkb1Lease(queue, archive_chat_id=CHAT)
    lease.claim(RUN_A, INC_A)
    _second_writer(queue)
    _terminal(queue, incarnation="WRONG-INCARNATION-0000000")
    with pytest.raises(TelegramBindingUnavailable, match="prior_execution_unknown"):
        lease.claim(RUN_B, INC_B)
    _terminal(queue, incarnation=INC_A, status="dispatched")
    with pytest.raises(TelegramBindingUnavailable, match="prior_execution_unknown"):
        lease.claim(RUN_B, INC_B)
    _terminal(queue, incarnation=INC_A, status="terminal")
    assert lease.claim(RUN_B, INC_B)
    assert lease.current(RUN_B, INC_B)
    assert not lease.current(RUN_A, INC_A)
    assert lease.claim(RUN_B, INC_B)


def test_writer_and_archive_scope_fail_closed(seeded):
    queue, _ = seeded
    for bad_chat,bad_topic in [(123,2),(CHAT,4),(True,2)]:
        with pytest.raises(TelegramBindingUnavailable):
            Rkb1Lease(queue, archive_chat_id=bad_chat,topic_id=bad_topic)
    lease=Rkb1Lease(queue,archive_chat_id=CHAT)
    with queue.connect() as db:
        db.execute("UPDATE rkb_writer_control SET active=0")
    with pytest.raises(TelegramBindingUnavailable,match="writer_binding_required"):
        lease.claim(RUN_A,INC_A)
    with queue.connect() as db:
        db.execute("UPDATE rkb_writer_control SET active=1")
    lease.claim(RUN_A,INC_A)
    changed=Rkb1Lease(queue,archive_chat_id=-1000000001234)
    with pytest.raises(TelegramBindingUnavailable,match="archive_scope_changed"):
        changed.claim(RUN_A,INC_A)


def test_already_claimed_slot_survives_process_restart_and_never_contains_credentials(seeded):
    queue,_=seeded
    Rkb1Lease(queue,archive_chat_id=CHAT).claim(RUN_A,INC_A)
    again=Rkb1Lease(BgeQueue(queue.path,clock=queue.clock),archive_chat_id=CHAT)
    assert again.current(RUN_A,INC_A)
    raw=queue.path.read_bytes()
    assert b"TEST_SESSION_NOT_AUTHORIZED_ANYWHERE" not in raw
    assert b"api_hash" not in raw
    with queue.connect(write=False) as db:
        assert [r[1] for r in db.execute("PRAGMA table_info(rkb_telegram_session_lease)")]==[
            "slot","run_id","incarnation","archive_chat_id","topic_id"]


def _bundle(data=None):
    payload=dict(session=SYNTHETIC_SESSION,**DEVICE) if data is None else data
    return base64.b64encode(json.dumps(payload).encode()).decode()


class FakeClient:
    def __init__(self):
        self.messages=0
        self.downloads=0
        self.auth=0
    async def get_messages(self,*args,**kwargs):
        self.messages+=1
    def iter_download(self,*args,**kwargs):
        self.downloads+=1
    async def is_user_authorized(self):
        self.auth+=1
        return True


def _factories():
    calls=[]
    client=FakeClient()
    def session_factory(secret):
        calls.append(("session",secret))
        return "test-session-object"
    def client_factory(session,api_id,api_hash,**kwargs):
        calls.append(("client",session,api_id,api_hash,kwargs))
        return client
    def downloader_factory(**kwargs):
        calls.append(("downloader",kwargs))
        return SimpleNamespace(**kwargs)
    return calls,client,session_factory,client_factory,downloader_factory


def _build(*, source_name=SLOT, bundle_b64=None, api_id="123456",
           api_hash="a"*32, run_id=RUN_A, runtime_incarnation=INC_A,
           archive_chat_id=CHAT, topic_id=2, current=None,
           dependency=None, session_factory=None, client_factory=None,
           downloader_factory=None):
    _,_,sf,cf,df=_factories()
    return build_rkb1_downloader(source_name=source_name,
        bundle_b64=_bundle() if bundle_b64 is None else bundle_b64,
        api_id=api_id,api_hash=api_hash,run_id=run_id,
        runtime_incarnation=runtime_incarnation,
        archive_chat_id=archive_chat_id,topic_id=topic_id,
        authenticated_lease_current=current or (lambda r,i: True),
        dependency_current=dependency or (lambda: True),
        session_factory=session_factory or sf,
        client_factory=client_factory or cf,
        downloader_factory=downloader_factory or df)


def test_rkb1_bundle_preserves_exact_five_device_metadata_without_connect():
    calls,client,sf,cf,df=_factories()
    downloader=_build(session_factory=sf,client_factory=cf,downloader_factory=df)
    assert calls[0]==("session",SYNTHETIC_SESSION)
    assert calls[1][0:4]==("client","test-session-object",123456,"a"*32)
    assert all(calls[1][4][k]==v for k,v in DEVICE.items())
    assert set(calls[1][4]) == set(DEVICE) | {
        "auto_reconnect","connection_retries","request_retries"}
    assert "session" not in calls[1][4]
    assert calls[1][4]["auto_reconnect"] is False
    assert calls[1][4]["connection_retries"]==0
    assert calls[1][4]["request_retries"]==0
    assert calls[2][0]=="downloader"
    assert downloader.client is client
    assert (downloader.chat_id,downloader.topic_id)==(CHAT,2)
    assert (client.messages,client.downloads,client.auth)==(0,0,0)
    assert SYNTHETIC_SESSION not in repr(downloader)


def test_wrong_identity_or_stale_lease_stops_before_parsing_secret_or_constructing_client():
    called=[]
    def reject(*args,**kwargs):
        called.append("called")
        raise AssertionError("unexpected")
    cases=[
       dict(source_name="TELEGRAM_E2E"),
       dict(source_name="VIBEPUBLISH_TELEGRAM_AUTH_BUNDLE"),
       dict(archive_chat_id=1234),
       dict(topic_id=4),
       dict(current=lambda r,i:False),
       dict(dependency=lambda:False),
    ]
    for options in cases:
        with pytest.raises(TelegramBindingUnavailable):
            _build(bundle_b64="not base64!",session_factory=reject,
                   client_factory=reject,downloader_factory=reject,**options)
    assert not called


@pytest.mark.parametrize("bad",[
    "not base64",
    _bundle(dict(session=SYNTHETIC_SESSION,**DEVICE,api_hash="x"*32)),
    _bundle(dict(session="",**DEVICE)),
    _bundle(dict(session=SYNTHETIC_SESSION,**{**DEVICE,"system_version":""})),
    base64.b64encode(b'{"session":"a","session":"b"}').decode(),
    _bundle(dict(session=SYNTHETIC_SESSION,**{**DEVICE,"lang_code":"ru"+chr(10)+"boom"})),
])
def test_malformed_or_extended_bundle_is_silent_and_never_constructs(bad):
    def forbidden(*args,**kwargs):
        raise AssertionError("should never construct client")
    with pytest.raises(TelegramBindingUnavailable,match="bundle_invalid") as e:
        _build(bundle_b64=bad,session_factory=forbidden,client_factory=forbidden)
    assert "TEST_SESSION" not in str(e.value)


def test_session_client_exceptions_are_redacted():
    def rejected(session,api_id,api_hash,**kwargs):
        raise RuntimeError("PROVIDER ERROR with " + SYNTHETIC_SESSION)
    with pytest.raises(TelegramBindingUnavailable,match="client_unavailable") as e:
        _build(client_factory=rejected)
    assert SYNTHETIC_SESSION not in str(e.value)
    assert e.value.__suppress_context__ is True


def test_runtime_owner_predicate_remains_dynamic_during_download():
    current=[True]
    downloader=_build(current=lambda r,i:current[0])
    assert downloader.current()
    current[0]=False
    assert not downloader.current()
    current[0]=True
    assert downloader.current()


@pytest.mark.asyncio
async def test_native_authorization_gate_on_synthetic_client_only():
    client=FakeClient()
    state=[True]
    assert await check_rkb1_authorized(client,lambda:state[0])
    assert client.auth==1
    state[0]=False
    with pytest.raises(TelegramBindingUnavailable,match="lease_not_current"):
        await check_rkb1_authorized(client,lambda:state[0])
    assert client.auth==1
    class Denied(FakeClient):
        async def is_user_authorized(self):
            self.auth+=1
            return False
    state[0]=True
    with pytest.raises(TelegramBindingUnavailable,match="authorization_unavailable"):
        await check_rkb1_authorized(Denied(),lambda:state[0])
