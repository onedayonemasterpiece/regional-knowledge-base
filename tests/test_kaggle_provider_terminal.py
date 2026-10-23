"""Synthetic provider/session responses; no credentials, network or model load."""
import contextlib
import json
import sys
from types import SimpleNamespace, ModuleType

import pytest

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge import kaggle_provider_terminal as native

REF = 'fixture-owner/stable-notebook'


class Response:
    status_code = 200
    def __init__(self, body=None, content_type='application/json'):
        self.body = json.dumps(body if body is not None else [
            {'stream_name': 'stderr', 'time': 1.0, 'data': 'fixture failed'}]).encode()
        self.headers = {'Content-Type': content_type}
        self.closed = False
    def iter_content(self, chunk_size=8192):
        yield self.body
    def close(self): self.closed = True


class API:
    def __init__(self, run):
        self.run = run
        self.calls = []
        self.phase = 'ERROR'
        self.log = Response()
        self.source_id = run['id']
        self.source_ref = run['provider_ref']
        self.source_nonce = run['launch_nonce']
        self.on_log = lambda: None
        self.http = SimpleNamespace(_init_session=lambda: None,
            _session=SimpleNamespace(send=lambda *a, **k: None))
    @contextlib.contextmanager
    def build_kaggle_client(self):
        yield SimpleNamespace(_http_client=self.http,
            kernels=SimpleNamespace(kernels_api_client=self))
    def get_kernel(self, request):
        self.calls.append(('source', request.version_label, request.user_name, request.kernel_slug))
        return SimpleNamespace(metadata=SimpleNamespace(ref=self.source_ref),
            blob=SimpleNamespace(source='RUN_CONFIG = '+repr(
                {'run_id': self.source_id, 'launch_nonce': self.source_nonce})))
    def get_kernel_session_status(self, request):
        self.calls.append(('status', request.version_label, request.user_name, request.kernel_slug))
        return SimpleNamespace(status=self.phase)
    def get_kernel_session_logs_stream(self, request):
        self.calls.append(('stream', request.version_label, request.user_name, request.kernel_slug))
        self.on_log()
        return self.log


@pytest.fixture(autouse=True)
def sdk(monkeypatch):
    module = ModuleType('kagglesdk.kernels.types.kernels_api_service')
    class Request:
        version_label = ''
        wait_for_logs_url_seconds = 0
    for name in ('ApiGetKernelRequest', 'ApiGetKernelSessionStatusRequest',
                 'ApiGetKernelSessionLogsStreamRequest'):
        setattr(module, name, Request)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(native, '_installed_versions', lambda: ('2.2.4', '0.1.37'))


@pytest.fixture
def env(tmp_path):
    now = [1000.]
    queue = BgeQueue(tmp_path/'queue.sqlite', clock=lambda: now[0])
    rid = queue.request_startup()['run_id']
    queue.launch_claim(rid, provider_ref=REF)
    queue.launch_record(rid, REF, provider_version=93)
    with queue.connect() as db:
        db.execute("update runs set status='expired' where id=?", (rid,))
    now[0] = 1200.
    def row():
        with queue.connect(write=False) as db:
            return dict(db.execute('select * from runs where id=?', (rid,)).fetchone())
    return queue, now, rid, row


def observe(api, row, now):
    return native.observe_version_terminal(api, row(), clock=lambda: now[0])


def test_exact_version_retirement_no_incarnation_idempotent_restart(env):
    q, now, rid, row = env
    api = API(row())
    proof = observe(api, row, now)
    assert proof is not None and all(call[1] == 'v93' for call in api.calls)
    assert row()['runtime_incarnation'] is None
    assert q.retire_provider_reservation(proof)
    assert row()['launch_state'] == 'terminal'
    reopened = BgeQueue(q.path, clock=lambda: now[0])
    assert reopened.retire_provider_reservation(proof)
    with q.connect(write=False) as db:
        assert db.execute('select count(*) from bge_provider_terminal_receipts').fetchone()[0] == 1


@pytest.mark.parametrize('phase',['ERROR','CANCEL_ACKNOWLEDGED'])
@pytest.mark.parametrize('body,ctype', [
    ({'error': 'denied'}, 'application/json'),
    ({'code': 500, 'message': 'failed'}, 'application/json'),
    ({'log': []}, 'application/json'),
    ([], 'application/json'),
    ([{'data': 'not a native log record'}], 'application/json'),
    ([{'stream_name': 'stdout', 'time': 1, 'data': 'END_OF_LOG'}], 'text/event-stream'),
])
def test_non_terminal_json_sse_and_empty_are_not_proof(env, body, ctype, phase):
    q, now, _, row = env
    api = API(row()); api.phase=phase; api.log = Response(body, ctype)
    assert observe(api, row, now) is None
    assert row()['launch_state'] == 'dispatched'
    assert api.log.closed


@pytest.mark.parametrize('phase', ['RUNNING', 'QUEUED', 'CANCEL_REQUESTED'])
def test_phase_alone_and_cancel_ack_not_terminal(env, phase):
    _, now, _, row = env
    api = API(row()); api.phase = phase
    assert observe(api, row, now) is None
    assert not any(c[0] == 'stream' for c in api.calls)


def test_cancel_ack_with_exact_persisted_native_log_retires_only_that_session(env):
    q,now,rid,row=env
    api=API(row());api.phase='CANCEL_ACKNOWLEDGED'
    proof=observe(api,row,now)
    assert proof is not None and proof.status=='CANCEL_ACKNOWLEDGED'
    assert [call[0]for call in api.calls]==['source','status','stream','source']
    assert all(call[1]=='v93'for call in api.calls)
    assert q.retire_provider_reservation(proof)
    assert row()['launch_state']=='terminal'


@pytest.mark.parametrize('phase',['ERROR','CANCEL_ACKNOWLEDGED'])
@pytest.mark.parametrize('field,value', [('source_id', 'wrong-run'),
    ('source_ref', 'other-owner/stable-notebook'), ('source_nonce', 'wrong-nonce')])
def test_wrong_version_or_source_binding(env, field, value, phase):
    _, now, _, row = env
    api = API(row());api.phase=phase; setattr(api, field, value)
    assert observe(api, row, now) is None


def test_source_changed_between_reads(env):
    _, now, _, row = env
    api = API(row()); api.on_log = lambda: setattr(api, 'source_id', 'another-run')
    assert observe(api, row, now) is None


def test_stale_or_changed_heartbeat_proof_cannot_retire(env):
    q, now, rid, row = env
    proof = observe(API(row()), row, now)
    now[0] += 31
    assert not q.retire_provider_reservation(proof)
    now[0] -= 31
    with q.connect() as db: db.execute('update runs set heartbeat=heartbeat+1 where id=?', (rid,))
    assert not q.retire_provider_reservation(proof)
    with pytest.raises(TypeError): q.retire_provider_reservation(True)


def test_retire_preserves_other_shared_owner_jobs_writer_and_demand(env):
    q, now, rid, row = env
    with q.connect() as db:
        other = q._new_run(db, now[0])
        db.execute('update control set current_run=?,successor=? where id=1', (other, other))
        db.execute('create table bge_shared_admission(id integer primary key,enabled integer,run_id text,incarnation text)')
        db.execute('insert into bge_shared_admission values(1,1,?,?)', (other, 'other-instance'))
        db.execute("insert into jobs values('durable','actor','same-key','source_command','[]','{}','claimed',0,0,?,'claim',9999,null,1)", (rid,))
        before = tuple(db.execute('select * from control').fetchone())
    assert q.retire_provider_reservation(observe(API(row()), row, now))
    with q.connect(write=False) as db:
        assert tuple(db.execute('select * from control').fetchone()) == before
        assert db.execute('select run_id from bge_shared_admission').fetchone()[0] == other
        job = db.execute("select * from jobs where id='durable'").fetchone()
        assert job['run_id'] == rid and job['claim'] == 'claim' and job['state'] == 'claimed'


def test_bounded_stream_invalid_json_and_oversize(env):
    _, now, _, row = env
    for body in (b'{', b' '*(native.MAX_LOG_BYTES+1)):
        api = API(row()); api.log.body = body
        assert observe(api, row, now) is None
        assert api.log.closed


def test_unmapped_10893_never_queries_or_gets_retired(env):
    q, now, rid, row = env
    with q.connect() as db:
        db.execute("update runs set launch_state='dispatching',provider_version=null where id=?", (rid,))
    api = API(row())
    assert observe(api, row, now) is None
    assert not api.calls


def test_history_cursor_crosses_101_mixed_rows_and_survives_restart(env):
    q, now, rid, row = env
    with q.connect() as db:
        for i in range(100):
            other = q._new_run(db, 0.)
            db.execute("update runs set status='expired',launch_state='dispatching',provider_ref=? where id=?", (REF if i%2 else 'other-owner/notebook', other))
    visited = []
    for _ in range(30):
        page = q.provider_reconciliation_page(limit=4)
        if not page: break
        visited.extend(r['id'] for r in page)
        for r in page: q.advance_provider_reconciliation(r['_cursor'])
        q = BgeQueue(q.path, clock=lambda: now[0])
    assert len(visited) == len(set(visited)) == 101
    assert q.provider_reconciliation_page(limit=4) == []


def controller():
    import importlib.util
    from pathlib import Path
    path = Path(__file__).resolve().parents[1]/'scripts/production/bge_kaggle_controller.py'
    spec = importlib.util.spec_from_file_location('native_terminal_controller', path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


def test_mixed_history_known_terminal_unmapped_and_different_ref(env):
    q, now, rid, row = env
    with q.connect() as db:
        unknown = q._new_run(db, 0.)
        different = q._new_run(db, 0.)
        db.execute("update runs set status='expired',launch_state='dispatching',provider_ref=? where id=?", (REF, unknown))
        db.execute("update runs set status='expired',launch_state='dispatched',provider_ref=?,provider_version=7 where id=?", ('another-owner/notebook', different))
    api = API(row())
    assert controller().reconcile_provider_history(api, q, limit=3) == 1
    with q.connect(write=False) as db:
        assert db.execute('select launch_state from runs where id=?', (unknown,)).fetchone()[0] == 'dispatching'
        assert db.execute('select launch_state from runs where id=?', (different,)).fetchone()[0] == 'dispatched'
    assert {c[1] for c in api.calls} == {'v93', 'v7'}
    assert not any(c[1] not in ('v93', 'v7') for c in api.calls)


def test_live_known_successor_is_not_probed_or_retired(env):
    q, now, rid, row = env
    with q.connect() as db:
        db.execute("update runs set status='ready',heartbeat=?,started=?,useful=? where id=?", (now[0], now[0], now[0], rid))
        successor = q._new_run(db, now[0])
        db.execute('update control set current_run=?,successor=? where id=1', (rid, successor))
    api = API(row())
    assert controller().reconcile_provider_history(api, q) == 0
    assert not api.calls
    assert q.launch_claim(successor, provider_ref=REF) is not None


def test_archived_unknown_does_not_block_current_development_after_retirement(env):
    q, now, rid, row = env
    with q.connect() as db:
        unknown = q._new_run(db, 0.)
        db.execute("update runs set status='expired',launch_state='dispatching',provider_ref=? where id=?", (REF, unknown))
        candidate = q._new_run(db, now[0])
    assert q.retire_provider_reservation(observe(API(row()), row, now))
    with q.connect() as db:db.execute('update control set current_run=? where id=1',(candidate,))
    assert q.launch_claim(candidate, provider_ref=REF) is not None
    assert q.launch_claim(candidate, provider_ref=REF) is None
    with q.connect(write=False) as db:
        assert db.execute('select launch_state from runs where id=?',(unknown,)).fetchone()[0]=='dispatching'


def test_publication_intent_and_durable_claim_survive_terminal(env):
    q, now, rid, row = env
    with q.connect() as db:
        db.execute('create table rkb_writer_control(id integer primary key,run_id text,publication_json text)')
        db.execute('insert into rkb_writer_control values(1,?,?)', (rid, '{"state":"UNKNOWN"}'))
    assert q.retire_provider_reservation(observe(API(row()), row, now))
    with q.connect(write=False) as db:
        assert db.execute('select current_run from control').fetchone()[0] == rid
        assert db.execute('select publication_json from rkb_writer_control').fetchone()[0] == '{"state":"UNKNOWN"}'


def test_same_shared_owner_cleared_only_after_native_proof(env):
    q, now, rid, row = env
    with q.connect() as db:
        db.execute('create table bge_shared_admission(id integer primary key,enabled integer,run_id text,incarnation text)')
        db.execute('insert into bge_shared_admission values(1,1,?,null)', (rid,))
    assert q.retire_provider_reservation(observe(API(row()), row, now))
    with q.connect(write=False) as db:
        assert db.execute('select run_id from bge_shared_admission').fetchone()[0] is None


def test_package_pin_and_non200_fail_closed(env, monkeypatch):
    _, now, _, row = env
    api = API(row())
    monkeypatch.setattr(native, '_installed_versions', lambda: ('2.2.5', '0.1.37'))
    assert observe(api, row, now) is None and not api.calls
    monkeypatch.setattr(native, '_installed_versions', lambda: native.PIN)
    api.log.status_code = 403
    assert observe(api, row, now) is None and api.log.closed


def test_observation_deadline_and_heartbeat_during_probe(env):
    q, now, rid, row = env
    api = API(row())
    ticks = iter([0, 13])
    assert native.observe_version_terminal(api, row(), clock=lambda: now[0], monotonic=lambda: next(ticks)) is None
    def heartbeat():
        with q.connect() as db:
            db.execute('update runs set heartbeat=? where id=?', (now[0], rid))
    api = API(row()); api.on_log = heartbeat
    proof = observe(api, row, now)
    assert proof is not None
    assert not q.retire_provider_reservation(proof)


def test_transport_bounds_before_sdk_json_decode_and_restores_send():
    response = Response(); seen = []
    def send(request, **kwargs):
        seen.append(kwargs)
        return response
    session = SimpleNamespace(send=send,get_adapter=lambda url:SimpleNamespace(send=send))
    client = SimpleNamespace(_http_client=SimpleNamespace(_init_session=lambda: None, _session=session))
    with native._bounded_transport(client, 12, lambda: 0) as budget:
        budget[0] = 1024
        actual = session.send(SimpleNamespace(url='https://fixture.invalid'))
        assert actual._content == response.body
        assert actual._content_consumed and actual.closed
    assert session.send is send
    assert seen == [{'timeout': (3, 3), 'stream': True}]
    response = Response(); response.body = b'x'*1025
    with pytest.raises(ValueError):
        with native._bounded_transport(client, 12, lambda: 0) as budget:
            budget[0] = 1024
            session.send(SimpleNamespace(url='https://fixture.invalid'))
    assert response.closed and session.send is send


# Reuse the actual existing admission fixtures for cross-process retirement.
from test_bge_admission import state, begin
from test_read_authority import auth
from test_participants import people


@pytest.mark.asyncio
async def test_persisted_receipt_releases_old_sent_ram_debt_on_new_attachment(state):
    s = state
    await begin(s, 'old-sent-read')
    work = await s.q.next_channel_work(s.run, s.token, 1)
    s.a.mark_work_sent(work)
    s.q.clock = lambda: 1200.
    with s.q.connect() as db:
        db.execute("update runs set status='expired' where id=?",(s.run,))
        old = dict(db.execute('select * from runs where id=?', (s.run,)).fetchone())
    proof = native.observe_version_terminal(API(old), old, clock=s.q.clock)
    # Simulate the standalone controller using a distinct queue object.
    other = BgeQueue(s.q.path, clock=s.q.clock)
    assert other.retire_provider_reservation(proof)
    assert s.a.status()['state'] == 'busy'  # RAM has not seen the receipt yet.
    with s.q.connect() as db:
        new = s.q._new_run(db, 1200.)
        db.execute('update control set current_run=? where id=1', (new,))
    token = (s.q.path.parent/'bge-worker-credentials'/new).read_text()
    nonce = s.q.prepare_channel(new)
    s.q.launch_claim(new, provider_ref=old['provider_ref'])
    s.q.launch_record(new, old['provider_ref'], provider_version=8)
    generation = s.q.attach_channel(new, token, nonce, 'new-instance')['connection_generation']
    s.q.heartbeat(new, token, ready=True, connection_generation=generation)
    await s.a.resource_state(new, token, generation, runtime_incarnation='new-instance', state='idle')
    assert s.a.status()['state'] == 'idle'
    assert s.a._work is None


from test_control_spool import env as spool_env, accept


@pytest.mark.asyncio
async def test_retirement_preserves_accepted_seq1_unknown_apply_and_source_history(spool_env):
    e = spool_env
    await accept(e)
    q = e.q
    initial = q.clock()
    run = q.request_startup()['run_id']
    q.launch_claim(run, provider_ref=REF)
    q.launch_record(run, REF, provider_version=93)
    with q.connect() as db:
        db.execute("update runs set status='expired' where id=?", (run,))
        db.execute("update rkb_control_operations set apply_state='SENT_UNKNOWN' where operation_key=?", (e.cmd.operation_key,))
        db.execute("insert into jobs values('source-history','actor','history-key','recovery_phase','[]',?,'claimed',0,0,?,'history-claim',9999999999,null,1)",
                   ('{"database_generation":"synthetic-db-v93","phase":"source_history"}', run))
        tables = [r[0] for r in db.execute("select name from sqlite_master where type='table' and name like 'rkb_%' order by name")]
        before = {t: [tuple(r) for r in db.execute('select * from '+t)] for t in tables}
        job = tuple(db.execute("select * from jobs where id='source-history'").fetchone())
        row = dict(db.execute('select * from runs where id=?', (run,)).fetchone())
        op = db.execute('select sequence,seal_state,apply_state,outcome_state from rkb_control_operations').fetchone()
        assert tuple(op) == (1, 'ACCEPTED', 'SENT_UNKNOWN', 'NOT_SENT')
    q.clock = lambda: initial + 200
    proof = native.observe_version_terminal(API(row), row, clock=q.clock)
    assert q.retire_provider_reservation(proof)
    with q.connect(write=False) as db:
        assert {t: [tuple(r) for r in db.execute('select * from '+t)] for t in tables} == before
        assert tuple(db.execute("select * from jobs where id='source-history'").fetchone()) == job


def test_real_requests_response_cache_remains_readable_after_bounded_close():
    import io
    requests = pytest.importorskip('requests')
    response = requests.Response()
    response.status_code = 200
    response.headers['Content-Type'] = 'application/json'
    response.raw = io.BytesIO(b'[{"stream_name":"stderr","time":1,"data":"fixture"}]')
    original = lambda *args, **kwargs: response
    session = SimpleNamespace(send=original,get_adapter=lambda url:SimpleNamespace(send=original))
    http = SimpleNamespace(_init_session=lambda: None, _session=session, _verbose=True)
    with native._bounded_transport(SimpleNamespace(_http_client=http), 12, lambda: 0) as budget:
        assert not http._verbose
        budget[0] = native.MAX_LOG_BYTES
        actual = session.send(SimpleNamespace(url='https://fixture.invalid'))
        assert actual.json()[0]['stream_name'] == 'stderr'
        assert native._persisted_log(actual, 12, lambda: 0) is not None
    assert http._verbose and session.send is original


from test_queue_writer import owner as writer_owner


@pytest.mark.asyncio
async def test_stale_ready_with_real_publication_fence_is_not_selected(writer_owner):
    import sqlite3
    s=writer_owner;s.q.clock=lambda:1200.
    with s.q.connect() as db:
        db.execute("update rkb_writer_control set publication_json=? where id=1",('{"state":"UNKNOWN"}',))
        row=dict(db.execute('select * from runs where id=?',(s.run,)).fetchone())
    assert s.q._run_expired(row,1200.)
    # Prove this fixture has the actual rejecting writer trigger.
    with pytest.raises(sqlite3.IntegrityError,match='publication unresolved'):
        with s.q.connect() as db:db.execute("update runs set status='expired' where id=?",(s.run,))
    api=API(row)
    assert controller().reconcile_provider_history(api,s.q,limit=2)==0
    assert not api.calls
    proof=native.observe_version_terminal(API(row),row,clock=s.q.clock)
    assert proof is not None and not s.q.retire_provider_reservation(proof)
    with s.q.connect(write=False) as db:
        assert db.execute('select status from runs where id=?',(s.run,)).fetchone()[0]=='ready'
        assert db.execute('select count(*) from bge_provider_terminal_receipts').fetchone()[0]==0


@pytest.mark.asyncio
async def test_real_writer_publication_survives_closure_then_native_reconcile_unblocks(writer_owner):
    import hashlib
    s=writer_owner;s.q.clock=lambda:1200.
    # Synthetic historical restore: already expired before the retained UNKNOWN
    # intent is restored. Keep all real QueueWriterAuthority triggers installed.
    intent=dict(key=s.key,data_sha256=hashlib.sha256(b'exact').hexdigest(),data_length=5,
                expected_token=None,nonce='fixture-retained-intent',state='UNKNOWN')
    encoded=json.dumps(intent,sort_keys=True,separators=(',',':'))
    with s.q.connect() as db:
        db.execute("update runs set status='expired' where id=?",(s.run,))
        db.execute('update rkb_writer_control set publication_json=? where id=1',(encoded,))
        row=dict(db.execute('select * from runs where id=?',(s.run,)).fetchone())
    proof=native.observe_version_terminal(API(row),row,clock=s.q.clock)
    assert s.q.retire_provider_reservation(proof)
    s.q.maintenance()
    with s.q.connect(write=False) as db:
        assert db.execute('select current_run from control').fetchone()[0]==s.run
        assert db.execute('select publication_json from rkb_writer_control').fetchone()[0]==encoded
        assert db.execute('select count(*) from bge_provider_terminal_receipts').fetchone()[0]==1
    # Only the existing exact native readback settles the publication.
    s.c.objects[s.key]=(b'exact','"fixture-native-token"')
    assert await s.w.reconcile(s.s)
    s.q.maintenance()
    with s.q.connect(write=False) as db:
        current=db.execute('select current_run from control').fetchone()[0]
        assert current is not None and current!=s.run
        assert db.execute('select publication_json from rkb_writer_control').fetchone()[0] is None


@pytest.mark.parametrize('kind',['source_command','recovery_phase'])
def test_finite_claim_lease_then_requeue_finalizes_closed_pointer_and_allocates(env,kind):
    q,now,rid,row=env
    with q.connect() as db:
        db.execute("insert into jobs values('finite','actor','same-operation',?,'[]','{}','claimed',0,0,?,'original-claim',?,null,1)",
                   (kind,rid,now[0]+5))
    proof=observe(API(row()),row,now)
    assert q.retire_provider_reservation(proof)
    q.maintenance()
    with q.connect(write=False) as db:
        assert db.execute('select current_run from control').fetchone()[0]==rid
    now[0]+=6
    q.maintenance()
    with q.connect(write=False) as db:
        job=db.execute("select * from jobs where id='finite'").fetchone()
        assert job['state']=='pending' and job['run_id'] is None
        assert job['idempotency']=='same-operation' and job['attempt']==1
        current=db.execute('select current_run from control').fetchone()[0]
        assert current is not None and current!=rid
        assert db.execute('select count(*) from bge_provider_terminal_receipts').fetchone()[0]==1
    assert q.retire_provider_reservation(proof)  # repeat cannot clear the new owner
    with q.connect(write=False) as db:
        assert db.execute('select current_run from control').fetchone()[0]==current


def test_automatic_reconciliation_does_not_probe_archived_unknown(env):
    q,now,rid,row=env
    with q.connect() as db:
        candidate=q._new_run(db,now[0])
        db.execute('update control set current_run=? where id=1',(candidate,))
    api=API(row())
    assert controller().reconcile_provider_history(api,q,current_only=True)==0
    assert api.calls==[]
    assert q.launch_claim(candidate,provider_ref=REF)
    assert q.launch_claim(candidate,provider_ref=REF) is None


def test_current_unknown_owner_still_fences_a_successor(env):
    q,now,rid,row=env
    with q.connect() as db:
        candidate=q._new_run(db,now[0])
        db.execute('update control set successor=? where id=1',(candidate,))
    assert q.launch_claim(candidate,provider_ref=REF) is None


def test_exact_native_terminal_can_retire_compute_without_rewriting_ready_or_writer(env):
    q,now,rid,row=env
    with q.connect() as db:
        db.execute("update runs set status='ready' where id=?",(rid,))
    proof=observe(API(row()),row,now)
    assert proof is not None and q.retire_provider_reservation(proof)
    current=row()
    assert current['status']=='ready' and current['launch_state']=='terminal'
    assert q.launch_claim(rid,provider_ref=REF) is None


def test_lease_expired_current_owner_is_retired_only_by_exact_terminal_proof(env):
    """A dead 'starting' owner no longer needs a manual status edit to be retired."""
    q, now, rid, row = env
    module = controller(); module._LEASE_TERMINAL_PROBED.clear()
    with q.connect() as db:
        db.execute("update runs set status='starting',started=0,useful=? where id=?", (now[0], rid))
        db.execute('update control set current_run=? where id=1', (rid,))
    api = API(row()); api.phase = 'RUNNING'
    # Provider still running: UNKNOWN, capacity stays owned, probes are throttled.
    assert module.reconcile_provider_history(api, q, current_only=True) == 0
    probes = len(api.calls)
    assert probes and module.reconcile_provider_history(api, q, current_only=True) == 0
    assert len(api.calls) == probes
    assert rid in q.provider_owned_run_ids()
    module._LEASE_TERMINAL_PROBED.clear(); api.phase = 'ERROR'
    assert module.reconcile_provider_history(api, q, current_only=True) == 1
    with q.connect(write=False) as db:
        assert db.execute('select launch_state from runs where id=?', (rid,)).fetchone()[0] == 'terminal'
    q.maintenance()  # the existing pointer finalization releases the slot
    with q.connect(write=False) as db:
        assert db.execute('select status from runs where id=?', (rid,)).fetchone()[0] == 'expired'
    assert rid not in q.provider_owned_run_ids()


def test_live_current_owner_with_fresh_lease_is_not_probed(env):
    q, now, rid, row = env
    module = controller(); module._LEASE_TERMINAL_PROBED.clear()
    with q.connect() as db:
        db.execute("update runs set status='ready',started=?,heartbeat=?,useful=? where id=?", (now[0], now[0], now[0], rid))
        db.execute('update control set current_run=? where id=1', (rid,))
    api = API(row())
    assert module.reconcile_provider_history(api, q, current_only=True) == 0 and not api.calls
