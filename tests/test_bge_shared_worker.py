"""Real shared transport, synthetic peer/encoder only: no provider, model or PG."""
import asyncio
from contextlib import asynccontextmanager
import json
import threading

import pytest

from test_bge_wss_worker import CONFIG, module
from regional_knowledge.bge_contract import SPACE


def query(generation=1, *, request='read-1', turn=1, claim='read-claim', text='fixture query'):
    return dict(type='QUERY', protocol_version=2, operation='knowledge_search', operation_version=2,
        request_id=request, connection_generation=generation, turn_generation=turn,
        compute_claim=claim, query=text, filters={}, min_data_seq=0, deadline_ms=1000,
        evidence_limit=3, actor=dict(person_id='fixture-person', audience='personal',
                                   auth_epoch=1, visibility_epoch=1))


def result(frame):
    return dict(type='QUERY_RESULT', protocol_version=2,
        **{key: frame[key] for key in ('request_id', 'connection_generation', 'turn_generation')},
        status='degraded', mode='hybrid', reason='bounded_retrieval', evidence=[],
        observed_data_seq=0, observed_projection_seq=0,
        observed_visibility_epoch=0, observed_database_generation='fixture-db')


def cancel(frame, **changes):
    return dict(type='CANCEL_QUERY', protocol_version=2, reason='caller_cancelled',
        **{key: frame[key] for key in ('request_id', 'connection_generation', 'turn_generation')},
        **changes)


class Peer:
    """Ordered deterministic duplex peer; waits inspect only actual wire frames."""
    def __init__(self):
        self.sent = []
        self.inputs = {}
        self.condition = asyncio.Condition()
        self.sessions = 0
        self.drop_release_once = False
        self.release_hook = None

    @asynccontextmanager
    async def connect(self):
        self.sessions += 1
        generation = self.sessions
        incoming = self.inputs[generation] = asyncio.Queue()
        peer = self

        class Socket:
            async def send(self, raw):
                frame = json.loads(raw)
                async with peer.condition:
                    peer.sent.append((generation, frame))
                    peer.condition.notify_all()
                kind = frame['type']
                if kind == 'ATTACH':
                    await peer.put(dict(type='WELCOME', provider_version='fixture-version'), generation)
                elif kind == 'READY':
                    await peer.put(dict(type='READY_ACK', status='ready'), generation)
                elif kind == 'HEARTBEAT':
                    await peer.put(dict(type='HEARTBEAT_ACK'), generation)
                elif kind == 'RESOURCE_RELEASED' and peer.drop_release_once:
                    peer.drop_release_once = False
                    raise ConnectionError('synthetic lost release acknowledgment')
                if kind == 'RESOURCE_RELEASED' and peer.release_hook is not None:
                    await peer.release_hook(frame, generation)

            async def receive(self):
                value = await incoming.get()
                if isinstance(value, BaseException):
                    raise value
                return value

        yield Socket()

    async def put(self, frame, generation=None):
        generation = generation or self.sessions
        await self.inputs[generation].put(json.dumps({'protocol_version': 2,
            'connection_generation': generation, **frame}))

    async def disconnect(self, generation=None):
        await self.inputs[generation or self.sessions].put(ConnectionError('synthetic disconnect'))

    async def wait(self, kind, *, generation=None, predicate=lambda _: True, timeout=2):
        async def found():
            async with self.condition:
                while True:
                    for session, frame in self.sent:
                        if (frame['type'] == kind and (generation is None or session == generation)
                                and predicate(frame)):
                            return frame
                    await self.condition.wait()
        return await asyncio.wait_for(found(), timeout)

    def frames(self, kind, generation=None):
        return [frame for session, frame in self.sent
                if frame['type'] == kind and (generation is None or session == generation)]


@asynccontextmanager
async def running(handler, encoder=None, *, peer=None):
    transport = module('bge_wss_transport')
    original_jitter = transport.random.uniform
    transport.random.uniform = lambda *_: .001
    stop = asyncio.Event()
    peer = peer or Peer()
    loads = []

    def load():
        loads.append('loaded')
        return encoder or (lambda _: [1.] + [0.] * 1023)

    worker = transport.BgeWssTransport(dict(CONFIG, shared_reads=True), load, lambda: {},
        query_handler=handler, connector=peer.connect, heartbeat_seconds=.01,
        maximum_seconds=5, stop=stop)
    task = asyncio.create_task(worker.run())
    try:
        await peer.wait('READY')
        yield peer, task, loads
    finally:
        stop.set()
        try:
            await asyncio.wait_for(task, 3)
        finally:
            transport.random.uniform = original_jitter


@pytest.mark.asyncio
async def test_hybrid_read_and_fresh_idle_probe_keep_legacy_payload_separate():
    calls = []

    async def handler(frame, *, encode):
        calls.append(frame['request_id'])
        assert await encode(frame['query']) == [1.] + [0.] * 1023
        return result(frame)

    async with running(handler) as (peer, task, loads):
        await peer.wait('RESOURCE_STATE', predicate=lambda f: f['state'] == 'idle')
        await peer.put(dict(type='RESOURCE_PROBE', probe_nonce='lost-before-receive'))
        proof = await peer.wait('RESOURCE_STATE', predicate=lambda f: f.get('probe_nonce') == 'lost-before-receive')
        assert proof['state'] == 'idle' and proof['connection_generation'] == 1
        frame = query()
        await peer.put(frame)
        answer = await peer.wait('QUERY_RESULT')
        assert answer['status'] == 'degraded' and answer['mode'] == 'hybrid'
        assert answer['request_id'] == frame['request_id']
        released = await peer.wait('RESOURCE_RELEASED')
        assert {k: released[k] for k in ('resource_kind', 'resource_id', 'compute_claim', 'dispatch_generation')} == {
            'resource_kind': 'read', 'resource_id': 'read-1', 'compute_claim': 'read-claim', 'dispatch_generation': 1}
        assert calls == ['read-1'] and loads == ['loaded']
        assert not peer.frames('RESULT')


@pytest.mark.asyncio
async def test_cancel_keeps_native_compute_and_sql_cleanup_occupied_with_responsive_control():
    entered, finish = threading.Event(), threading.Event()
    cleanup_entered, cleanup_finish = asyncio.Event(), asyncio.Event()
    handler_calls = []

    def native(text):
        entered.set()
        assert finish.wait(3)
        return [1.] + [0.] * 1023

    async def handler(frame, *, encode):
        handler_calls.append(frame['request_id'])
        try:
            await encode(frame['query'])
            return result(frame)
        finally:
            cleanup_entered.set()
            await cleanup_finish.wait()

    try:
        async with running(handler, native) as (peer, task, _):
            frame = query()
            await peer.put(frame)
            assert await asyncio.to_thread(entered.wait, 1)
            # Duplicate assignment cannot duplicate the native forward/handler.
            await peer.put(frame)
            # A different turn cannot cancel this current consumer.
            stale = cancel(frame)
            stale['turn_generation'] = 0
            await peer.put(stale)
            await peer.put(dict(type='RESOURCE_PROBE', probe_nonce='after-stale-cancel'))
            busy = await peer.wait('RESOURCE_STATE', predicate=lambda f: f.get('probe_nonce') == 'after-stale-cancel')
            assert busy['state'] == 'busy' and not cleanup_entered.is_set()
            await peer.put(cancel(frame))
            await asyncio.wait_for(cleanup_entered.wait(), 1)
            # Native completion alone still cannot release SQL cleanup ownership.
            finish.set()
            await peer.put(dict(type='RESOURCE_PROBE', probe_nonce='sql-cleanup-pending'))
            busy = await peer.wait('RESOURCE_STATE', predicate=lambda f: f.get('probe_nonce') == 'sql-cleanup-pending')
            assert busy['state'] == 'busy'
            assert not peer.frames('RESOURCE_RELEASED')
            assert await peer.wait('HEARTBEAT')
            cleanup_finish.set()
            await peer.wait('RESOURCE_RELEASED')
            assert handler_calls == ['read-1']
            assert not peer.frames('QUERY_RESULT')
    finally:
        finish.set()
        cleanup_finish.set()


@pytest.mark.asyncio
async def test_disconnected_read_never_replays_result_and_releases_original_dispatch_identity():
    entered, finish = threading.Event(), threading.Event()
    cleanup = asyncio.Event()

    def native(_):
        entered.set()
        assert finish.wait(3)
        return [1.] + [0.] * 1023

    async def handler(frame, *, encode):
        try:
            await encode(frame['query'])
            return result(frame)
        finally:
            cleanup.set()

    try:
        async with running(handler, native) as (peer, task, loads):
            await peer.put(query())
            assert await asyncio.to_thread(entered.wait, 1)
            await peer.disconnect()
            await peer.wait('ATTACH', generation=2)
            busy = await peer.wait('RESOURCE_STATE', generation=2, predicate=lambda f: f['state'] == 'busy')
            assert busy['dispatch_generation'] == 1 and busy['connection_generation'] == 2
            await asyncio.wait_for(cleanup.wait(), 1)
            finish.set()
            released = await peer.wait('RESOURCE_RELEASED', generation=2)
            assert released['dispatch_generation'] == 1 and released['connection_generation'] == 2
            assert released['compute_claim'] == 'read-claim'
            assert not peer.frames('QUERY_RESULT', 2)
            assert loads == ['loaded']
    finally:
        finish.set()


@pytest.mark.asyncio
async def test_release_is_retained_across_disconnect_before_idle_and_next_assignment():
    peer = Peer()
    peer.drop_release_once = True
    calls = []

    async def handler(frame, *, encode):
        calls.append(frame['request_id'])
        await encode(frame['query'])
        return result(frame)

    async with running(handler, peer=peer) as (peer, task, loads):
        await peer.put(query())
        released = await peer.wait('RESOURCE_RELEASED', generation=2)
        assert released['dispatch_generation'] == 1 and released['connection_generation'] == 2
        await peer.wait('RESOURCE_STATE', generation=2, predicate=lambda f: f['state'] == 'idle')
        kinds = [f['type'] for generation, f in peer.sent if generation == 2]
        assert kinds.index('RESOURCE_RELEASED') < kinds.index('RESOURCE_STATE')
        assert not peer.frames('QUERY_RESULT', 2)
        await peer.put(query(2, request='read-2', turn=2, claim='read-claim-2'))
        await peer.wait('QUERY_RESULT', generation=2, predicate=lambda f: f['request_id'] == 'read-2')
        assert calls == ['read-1', 'read-2'] and loads == ['loaded']


@pytest.mark.asyncio
async def test_document_ack_debt_and_read_share_one_forward_without_losing_durable_result():
    calls = []

    def native(text):
        calls.append(text)
        return [1.] + [0.] * 1023

    async def handler(frame, *, encode):
        await encode(frame['query'])
        return result(frame)

    async with running(handler, native) as (peer, task, _):
        await peer.put(dict(type='DISPATCH', id='document-job', claim='document-claim',
            kind='document', texts=['document fixture'], space=SPACE,
            compute_claim='document-compute', dispatch_generation=1))
        durable = await peer.wait('RESULT')
        await peer.wait('RESOURCE_RELEASED', predicate=lambda f: f['resource_kind'] == 'document')
        # No durable RESULT_ACK yet: compute has finished, so read may use its slot.
        await peer.put(query())
        await peer.wait('QUERY_RESULT')
        await peer.wait('RESOURCE_RELEASED', predicate=lambda f: f['resource_kind'] == 'read')
        await peer.disconnect()
        replayed = await peer.wait('RESULT', generation=2)
        assert {k: v for k, v in durable.items() if k != 'connection_generation'} == {
            k: v for k, v in replayed.items() if k != 'connection_generation'}
        await peer.put(dict(type='RESULT_ACK', job_id='document-job', claim='document-claim', status='duplicate'))
        await peer.put(dict(type='RESOURCE_PROBE', probe_nonce='durable-acked'))
        assert (await peer.wait('RESOURCE_STATE', generation=2,
            predicate=lambda f: f.get('probe_nonce') == 'durable-acked'))['state'] == 'idle'
        assert calls == ['document fixture', 'fixture query']
        assert not peer.frames('QUERY_RESULT', 2)


@pytest.mark.parametrize('handler', [None, lambda frame, **kwargs: result(frame)])
def test_shared_reads_rejects_missing_or_synchronous_handler_before_load(handler):
    transport = module('bge_wss_transport')

    def forbidden():
        raise AssertionError('invalid configuration loaded encoder')

    with pytest.raises((ValueError, TypeError)):
        transport.BgeWssTransport(dict(CONFIG, shared_reads=True), forbidden, lambda: {}, query_handler=handler)


@pytest.mark.asyncio
async def test_deadline_result_does_not_release_native_forward_debt():
    entered, finish = threading.Event(), threading.Event()

    def native(_):
        entered.set()
        assert finish.wait(3)
        return [1.] + [0.] * 1023

    async def handler(frame, *, encode):
        try:
            await asyncio.wait_for(encode(frame['query']), .02)
        except TimeoutError:
            unavailable = result(frame)
            unavailable.update(status='unavailable', mode=None, reason='deadline_exceeded')
            return unavailable
        raise AssertionError('synthetic timeout did not fire')

    try:
        async with running(handler, native) as (peer, task, _):
            await peer.put(query())
            assert await asyncio.to_thread(entered.wait, 1)
            answer = await peer.wait('QUERY_RESULT')
            assert answer['status'] == 'unavailable' and answer['reason'] == 'deadline_exceeded'
            await peer.put(dict(type='RESOURCE_PROBE', probe_nonce='after-deadline-result'))
            proof = await peer.wait('RESOURCE_STATE', predicate=lambda f: f.get('probe_nonce') == 'after-deadline-result')
            assert proof['state'] == 'busy' and not peer.frames('RESOURCE_RELEASED')
            finish.set()
            await peer.wait('RESOURCE_RELEASED')
    finally:
        finish.set()


@pytest.mark.asyncio
async def test_handler_failure_is_explicit_unavailable_without_exception_payload():
    async def handler(frame, *, encode):
        raise RuntimeError('private synthetic diagnostic must stay private')

    async with running(handler) as (peer, task, _):
        await peer.put(query())
        answer = await peer.wait('QUERY_RESULT')
        assert answer['status'] == 'unavailable' and answer['reason'] == 'internal_error'
        assert answer.get('mode') is None and not answer.get('evidence')
        await peer.wait('RESOURCE_RELEASED')
        assert 'private synthetic diagnostic' not in json.dumps(peer.sent)


@pytest.mark.asyncio
async def test_shared_control_and_probe_are_live_before_blocked_encoder_load():
    transport = module('bge_wss_transport')
    peer, stop = Peer(), asyncio.Event()
    load_entered, allow_load = threading.Event(), threading.Event()

    def load():
        assert peer.frames('RESOURCE_STATE')[0]['state'] == 'idle'
        load_entered.set()
        assert allow_load.wait(3)
        return lambda _: [1.] + [0.] * 1023

    async def handler(frame, *, encode):
        await encode(frame['query'])
        return result(frame)

    worker = transport.BgeWssTransport(dict(CONFIG, shared_reads=True), load, lambda: {},
        query_handler=handler, connector=peer.connect, heartbeat_seconds=.01,
        maximum_seconds=5, stop=stop)
    task = asyncio.create_task(worker.run())
    try:
        await peer.wait('RESOURCE_STATE', predicate=lambda f: f['state'] == 'idle')
        assert await asyncio.to_thread(load_entered.wait, 1)
        await peer.put(dict(type='RESOURCE_PROBE', probe_nonce='during-heavy-load'))
        assert (await peer.wait('RESOURCE_STATE', predicate=lambda f:
            f.get('probe_nonce') == 'during-heavy-load'))['state'] == 'idle'
        await peer.wait('HEARTBEAT')
        assert not peer.frames('READY')
        allow_load.set()
        await peer.wait('READY')
    finally:
        allow_load.set()
        stop.set()
        await asyncio.wait_for(task, 3)


@pytest.mark.asyncio
async def test_repeated_outer_cancel_drains_read_native_task_and_sql_cleanup():
    transport = module('bge_wss_transport')
    peer = Peer()
    entered, finish = threading.Event(), threading.Event()
    cleanup_entered, cleanup_finish = asyncio.Event(), asyncio.Event()

    def native(_):
        entered.set()
        assert finish.wait(3)
        return [1.] + [0.] * 1023

    async def handler(frame, *, encode):
        try:
            await encode(frame['query'])
            return result(frame)
        finally:
            cleanup_entered.set()
            await cleanup_finish.wait()

    worker = transport.BgeWssTransport(dict(CONFIG, shared_reads=True), lambda: native, lambda: {},
        query_handler=handler, connector=peer.connect, heartbeat_seconds=.01, maximum_seconds=5)
    task = asyncio.create_task(worker.run())
    try:
        await peer.wait('READY')
        await peer.put(query())
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.wait_for(cleanup_entered.wait(), 1)
        assert not task.done()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        cleanup_finish.set()
        await asyncio.sleep(0)
        assert not task.done()  # Cleanup done, actual thread still owns compute.
    finally:
        cleanup_finish.set()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 3)


@pytest.mark.asyncio
@pytest.mark.parametrize('violation', ['cancel-bool-turn', 'cancel-bool-generation', 'changed-active-payload'])
async def test_invalid_cancel_and_changed_active_identity_fail_closed_without_rerunning(violation):
    entered = asyncio.Event()
    calls = []

    async def handler(frame, *, encode):
        calls.append(frame['query'])
        entered.set()
        await asyncio.Event().wait()
        return result(frame)

    # A malformed control frame or changed payload terminates this attachment;
    # the currently blocked read must be cancelled/cleaned, never rerun.
    async with running(handler) as (peer, task, loads):
        original = query()
        await peer.put(original)
        await asyncio.wait_for(entered.wait(), 1)
        invalid = query(text='changed fixture query') if violation == 'changed-active-payload' else cancel(original)
        if violation == 'cancel-bool-turn':
            invalid['turn_generation'] = True
        elif violation == 'cancel-bool-generation':
            invalid['connection_generation'] = True
        await peer.put(invalid)
        await peer.wait('READY',generation=2)
        assert not task.done() and loads==['loaded']
        assert not peer.frames('QUERY_RESULT') and peer.sessions==2
    assert calls == ['fixture query']


@pytest.mark.asyncio
async def test_next_assignment_on_release_preserves_already_completed_result_on_same_socket():
    peer = Peer()
    second_entered, finish_second = asyncio.Event(), asyncio.Event()

    async def handler(frame, *, encode):
        if frame['request_id'] == 'read-2':
            second_entered.set()
            await finish_second.wait()
        await encode(frame['query'])
        return result(frame)

    async def dispatch_next_on_release(frame, generation):
        if frame['resource_id'] == 'read-1':
            # Simulate controller dispatch immediately upon compute release,
            # before this sender can continue to its queued old QUERY_RESULT.
            await peer.put(query(generation, request='read-2', turn=2, claim='read-claim-2'))
            await asyncio.wait_for(second_entered.wait(), 1)

    peer.release_hook = dispatch_next_on_release
    async with running(handler, peer=peer) as (peer, task, _):
        try:
            await peer.put(query())
            await asyncio.wait_for(second_entered.wait(), 1)
            first = await peer.wait('QUERY_RESULT', predicate=lambda f: f['request_id'] == 'read-1')
            assert first['connection_generation'] == 1 and first['turn_generation'] == 1
            finish_second.set()
            await peer.wait('QUERY_RESULT', predicate=lambda f: f['request_id'] == 'read-2')
        finally:
            finish_second.set()


@pytest.mark.asyncio
async def test_each_idle_probe_replays_unacknowledged_release_before_nonce_proof():
    async def handler(frame, *, encode):
        await encode(frame['query'])
        return result(frame)

    async with running(handler) as (peer, task, _):
        await peer.put(query())
        original = await peer.wait('RESOURCE_RELEASED')
        for nonce in ('first-idle-challenge', 'second-idle-challenge'):
            start = len(peer.sent)
            await peer.put(dict(type='RESOURCE_PROBE', probe_nonce=nonce))
            proof = await peer.wait('RESOURCE_STATE', predicate=lambda f: f.get('probe_nonce') == nonce)
            assert proof['state'] == 'idle'
            tail = [frame for _, frame in peer.sent[start:]]
            proof_index = next(i for i, frame in enumerate(tail) if frame.get('probe_nonce') == nonce)
            releases = [frame for frame in tail[:proof_index] if frame['type'] == 'RESOURCE_RELEASED']
            assert releases and releases[-1] == original


@pytest.mark.asyncio
async def test_native_diagnostics_only_after_wire_send_and_blocked_drain_does_not_hold_reply():
    from regional_knowledge.kaggle_bootstrap import PreparedRuntime
    from test_kaggle_target_runtime import candidate
    transport=module('bge_wss_transport');peer=Peer();stop=asyncio.Event()
    entered,release=threading.Event(),threading.Event()
    calls=[]
    async def handler(frame,*,encode):return result(frame)
    def drain(frame):
        calls.append(frame['request_id'])
        entered.set()
        release.wait(5)  # Broken optional collector must not hold either read.
        return []
    async def prepare(*args,**kwargs):return PreparedRuntime(handler,candidate(),read_event_drain=drain)
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),lambda:lambda text:[1.]+[0.]*1023,lambda:{},
        prepare_runtime=prepare,connector=peer.connect,heartbeat_seconds=.01,maximum_seconds=3,stop=stop)
    task=asyncio.create_task(worker.run())
    try:
        await peer.wait('READY');await peer.put(query(request='native-event-query'))
        value=await asyncio.wait_for(peer.wait('QUERY_RESULT'),1)
        assert value['status']=='degraded' and 'read_events' not in value
        assert len(json.dumps(value).encode())<=262144
        assert await asyncio.to_thread(entered.wait,1)
        assert calls==['native-event-query']
        # A second authorized read is still delivered while the first
        # diagnostic drain is blocked. Do not create another background drain.
        await peer.put(query(request='native-event-query-2',turn=2,claim='native-claim-2'))
        second=await asyncio.wait_for(peer.wait('QUERY_RESULT',
            predicate=lambda f:f['request_id']=='native-event-query-2'),1)
        assert second['status']=='degraded' and 'read_events' not in second
        assert not worker._read_telemetry_export.done()
        assert calls==['native-event-query']
    finally:
        release.set();stop.set();await task


@pytest.mark.asyncio
@pytest.mark.parametrize('phase',['snapshot_status','capture'])
async def test_durable_recovery_releases_compute_while_retaining_one_job(phase):
    transport=module('bge_wss_transport');peer=Peer();stop=asyncio.Event()
    started=asyncio.Event();finish=asyncio.Event();executions=[]
    async def read(frame,*,encode):return result(frame)
    async def native(frame):
        executions.append(frame['id']);started.set();await finish.wait()
        return {'phase':phase,'request_sha256':'c'*64,'value':{'status':'pending'}}
    worker=transport.BgeWssTransport(dict(CONFIG,shared_reads=True),
        lambda:lambda _: [1.]+[0.]*1023,lambda:{},query_handler=read,
        recovery_handler=native,connector=peer.connect,heartbeat_seconds=.01,maximum_seconds=8,stop=stop)
    task=asyncio.create_task(worker.run())
    dispatch=dict(type='DISPATCH',id='metadata-job',claim='metadata-claim',kind='recovery_phase',
        compute_claim='metadata-compute',dispatch_generation=1,run_id=CONFIG['run_id'],
        launch_nonce=CONFIG['launch_nonce'],runtime_incarnation=worker.incarnation,
        database_generation='fixture-db',writer_generation='fixture-writer',
        phase=phase,operation_key='metadata-operation',request_sha256='c'*64,arguments={'handle':'owned'})
    try:
        await peer.wait('READY');await peer.put(dispatch);await started.wait()
        assert worker.job['id']=='metadata-job' and not worker.encoding.done()
        released=await peer.wait('RESOURCE_RELEASED',predicate=lambda f:f['resource_id']=='metadata-job')
        assert released['compute_claim']=='metadata-compute'
        await peer.put(query(request='read-during-metadata'))
        await peer.wait('QUERY_RESULT',predicate=lambda f:f['request_id']=='read-during-metadata')
        assert not finish.is_set() and worker.job['id']=='metadata-job'
        finish.set();receipt=await peer.wait('RECOVERY_PHASE_RESULT')
        assert receipt['job_id']=='metadata-job' and receipt['claim']=='metadata-claim'
        assert executions==['metadata-job']
        await peer.put(dict(type='RESULT_ACK',job_id='metadata-job',claim='metadata-claim',status='accepted'))
    finally:
        finish.set();stop.set();await asyncio.wait_for(task,3)


@pytest.mark.asyncio
@pytest.mark.parametrize('with_encode',[False,True])
async def test_durable_document_forward_does_not_exclude_reads_or_overlap_model_calls(with_encode):
    entered=threading.Event();finish=threading.Event();calls=[];active=0;peak=0
    def native(text):
        nonlocal active,peak
        active+=1;peak=max(peak,active);calls.append(text)
        try:
            if text=='blocking document':
                entered.set();assert finish.wait(3)
            return [1.]+[0.]*1023
        finally:active-=1
    async def read(frame,*,encode):
        if with_encode:await encode(frame['query'])
        return result(frame)
    try:
        async with running(read,native) as (peer,task,loads):
            await peer.put(dict(type='DISPATCH',id='blocked-doc',claim='doc-claim',
                kind='document',texts=['blocking document'],space=SPACE,
                compute_claim='doc-compute',dispatch_generation=1))
            assert await asyncio.to_thread(entered.wait,1)
            await peer.wait('RESOURCE_RELEASED',predicate=lambda f:f['resource_kind']=='document')
            await peer.put(query())
            if with_encode:
                await asyncio.sleep(.02)
                assert calls==['blocking document'] and not peer.frames('QUERY_RESULT')
                finish.set()
            response=await peer.wait('QUERY_RESULT')
            assert response['status']=='degraded'
            await peer.wait('RESOURCE_RELEASED',predicate=lambda f:f['resource_kind']=='read')
            if not with_encode:assert not finish.is_set()
            finish.set();durable=await peer.wait('RESULT')
            assert durable['job_id']=='blocked-doc' and durable['claim']=='doc-claim'
            assert calls==(['blocking document','fixture query'] if with_encode else ['blocking document'])
            assert peak==1 and loads==['loaded']
            await peer.put(dict(type='RESULT_ACK',job_id='blocked-doc',claim='doc-claim',status='accepted'))
    finally:finish.set()


@pytest.mark.asyncio
@pytest.mark.parametrize('job_kind', ['source_command', 'recovery_phase'])
@pytest.mark.parametrize('fault', ['new_dispatch', 'wrong_incarnation', 'stale_generation',
                                  'changed_replay', 'wrong_ack', 'authority_closed'])
async def test_pending_native_result_resync_preserves_runtime_and_auth_fences(monkeypatch, job_kind, fault):
    from regional_knowledge.kaggle_bootstrap import PreparedRuntime
    from test_kaggle_target_runtime import candidate
    transport = module('bge_wss_transport')
    monkeypatch.setattr(transport.random, 'uniform', lambda *_: .001)
    peer, stop = Peer(), asyncio.Event()
    loads, preparations, executions, closes = [], [], [], []

    def load():
        loads.append('load')
        return lambda _: [1.] + [0.] * 1023

    async def read(frame, *, encode):
        return result(frame)

    async def native(frame):
        executions.append(frame['id'])
        return {'retained_native_receipt': frame['id']}

    async def prepare(*args, **kwargs):
        prepared = PreparedRuntime(read, candidate(), source_handler=native)
        preparations.append(prepared)
        return prepared

    async def close():
        closes.append('close')

    worker = transport.BgeWssTransport(dict(CONFIG, shared_reads=True), load, lambda: {},
        prepare_runtime=prepare, recovery_handler=native, close_runtime=close,
        connector=peer.connect, heartbeat_seconds=.01, maximum_seconds=5, stop=stop)
    task = asyncio.create_task(worker.run())
    dispatch = dict(type='DISPATCH', id='retained-job', claim='retained-claim', kind=job_kind,
        compute_claim='retained-compute', dispatch_generation=1, run_id=CONFIG['run_id'],
        launch_nonce=CONFIG['launch_nonce'], runtime_incarnation=worker.incarnation,
        database_generation='fixture-db', writer_generation='fixture-writer',
        command_b64='e30=', command_sha256='a' * 64, accepted={}, handler_sha256='b' * 64,
        phase='fixture-phase', operation_key='fixture-operation', request_sha256='c' * 64, arguments={})
    result_type = 'SOURCE_COMMAND_RESULT' if job_kind == 'source_command' else 'RECOVERY_PHASE_RESULT'
    try:
        await peer.wait('READY')
        await peer.put(dispatch)
        original_result = await peer.wait(result_type)
        await peer.wait('RESOURCE_RELEASED')
        original_job, original_pending = worker.job, worker.pending
        original_encoder, original_runtime = worker.encoder, worker.prepared_runtime
        conflicting = dict(dispatch, id='different-job', claim='different-claim', compute_claim='different-compute')
        if fault == 'wrong_incarnation':
            conflicting['runtime_incarnation'] = 'different-incarnation'
        elif fault == 'stale_generation':
            conflicting['connection_generation'] = 0
        elif fault == 'changed_replay':
            conflicting = dict(dispatch, command_sha256='d' * 64, request_sha256='d' * 64)
        elif fault == 'wrong_ack':
            conflicting = dict(type='RESULT_ACK', job_id='retained-job', claim='wrong', status='accepted')
        if fault == 'authority_closed':
            await peer.inputs[1].put(transport.BgeChannelStopped('channel authority or contract ended'))
        else:
            await peer.put(conflicting)
        if fault == 'authority_closed':
            with pytest.raises(transport.BgeChannelStopped):
                await asyncio.wait_for(task, 2)
            assert peer.sessions == 1 and closes == ['close']
            assert worker.job is original_job and worker.pending is original_pending
            return
        # This is the real run() reconnect path, not a swallowed reader error.
        replayed = await peer.wait(result_type, generation=2)
        assert not task.done() and not closes
        assert worker.job is original_job and worker.pending is original_pending
        assert worker.encoder is original_encoder and worker.prepared_runtime is original_runtime
        assert loads == ['load'] and preparations == [original_runtime] and executions == ['retained-job']
        assert {k: v for k, v in original_result.items() if k != 'connection_generation'} == {
            k: v for k, v in replayed.items() if k != 'connection_generation'}
        assert {f['runtime_incarnation'] for f in peer.frames('ATTACH')} == {worker.incarnation}
        if job_kind == 'source_command':
            await peer.put(dict(type='RESULT_ACK', job_id='retained-job', claim='retained-claim', status='apply_received'))
            await peer.put(dict(type='RESOURCE_PROBE', probe_nonce='local-receipt-only'))
            await peer.wait('RESOURCE_STATE', predicate=lambda f: f.get('probe_nonce') == 'local-receipt-only')
            assert worker.pending is original_pending and worker.job is original_job
        await peer.put(dict(type='RESULT_ACK', job_id='retained-job', claim='retained-claim', status='duplicate'))
        await peer.put(dict(type='RESOURCE_PROBE', probe_nonce='terminal-receipt'))
        await peer.wait('RESOURCE_STATE', predicate=lambda f: f.get('probe_nonce') == 'terminal-receipt')
        assert worker.pending is None and worker.job is None and not closes
        assert executions == ['retained-job'] and peer.sessions == 2
    finally:
        stop.set()
        await asyncio.gather(task, return_exceptions=True)
    assert closes == ['close']
