"""Single run-authenticated WSS attachment for existing BGE work.

This carries internal QUERY and encoding assignments, not public identity or
source-output authority. The queue owns run authentication, attachment
generations and durable claims; accepted/Saved recovery remains separate.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import secrets
import time
from itertools import count

import anyio
from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocketDisconnect

from .bge_contract import REVISION, SPACE
from .bge_error_facts import failure_facts
from .bge_queue import StaleClaim, StaleAttachment
from .kaggle_protocol import RESULT_MAX_BYTES, decode_envelope, QueryResult, PassageReadResult, StoryReadResult
from .kaggle_protocol import SqlHealthResult
from .kaggle_fact_contracts import FactReadRequest, FactReadResult
from .kaggle_source_read import SourceReadRequest, SourceReadResult
from .kaggle_case_contracts import CaseReadRequest, CaseReadResult
from .kaggle_geo_contracts import GeoReadRequest, GeoReadResult
from .kaggle_editorial_contracts import EditorialReadRequest, EditorialReadResult
from .kaggle_source_graph_read import SourceGraphReadRequest, SourceGraphReadResult
from .kaggle_acceptance_contracts import AcceptanceReadResult


def _failure_site(error):
    trace=error.__traceback__
    while trace is not None and trace.tb_next is not None:trace=trace.tb_next
    return (trace.tb_frame.f_code.co_filename.rsplit('/',1)[-1]+':'+str(trace.tb_lineno)) if trace else 'unknown'

MAX_FRAME_BYTES = 65536
MAX_DISPATCH_BYTES = 256 * 1024
MAX_SOURCE_DISPATCH_BYTES = 2 * 1024 * 1024
MAX_SOURCE_RESULT_BYTES = 16 * 1024
MAX_RECOVERY_DISPATCH_BYTES = 4 * 1024 * 1024
MAX_RECOVERY_RESULT_BYTES = 128 * 1024
MAX_BOOTSTRAP_BYTES = 1536 * 1024
SEND_QUEUE_FRAMES = 8
RECONCILE_SECONDS = .25
SEND_TIMEOUT_SECONDS = 5
logger = logging.getLogger(__name__)


def _decode(message):
    raw = message.get('bytes')
    if raw is None:
        raw = message.get('text', '').encode('utf-8')
    if len(raw) > MAX_BOOTSTRAP_BYTES:
        raise ValueError('frame_size')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate_field')
            result[key] = value
        return result
    def nonfinite(_):
        raise ValueError('nonfinite')
    frame = json.loads(raw, object_pairs_hook=unique, parse_constant=nonfinite)
    if not isinstance(frame, dict) or type(frame.get('protocol_version')) is not int or frame['protocol_version'] != 2:
        raise ValueError('protocol')
    if frame.get('type')!='BOOTSTRAP_OBSERVATION' and len(raw)>RESULT_MAX_BYTES:
        raise ValueError('frame_size')
    if frame.get('type') not in ('QUERY_RESULT','PASSAGE_READ_RESULT','STORY_READ_RESULT','FACT_READ_RESULT','SOURCE_READ_RESULT','CASE_READ_RESULT','GEO_READ_RESULT','EDITORIAL_READ_RESULT','SOURCE_GRAPH_READ_RESULT','ACCEPTANCE_READ_RESULT','RECOVERY_PHASE_RESULT','BOOTSTRAP_OBSERVATION') and len(raw) > MAX_FRAME_BYTES:
        raise ValueError('frame_size')
    if frame.get('type')=='SOURCE_COMMAND_RESULT' and len(raw)>MAX_SOURCE_RESULT_BYTES:
        raise ValueError('source_receipt_size')
    if frame.get('type')=='RECOVERY_PHASE_RESULT' and len(raw)>MAX_RECOVERY_RESULT_BYTES:
        raise ValueError('recovery_receipt_size')
    return frame


def _fields(frame, required, optional=()):
    if set(frame) - set(required) - set(optional) or set(required) - set(frame):
        raise ValueError('fields')


def _identifier(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 256:
        raise ValueError('identifier')
    return value


def channel_route(queue, *, read_telemetry=None, before_stop=None):
    """Return a native route for the existing MCP ASGI application."""
    async def endpoint(socket):
        # Explicit startup configuration only. Legacy encoding stays on its
        # existing path; attaching a socket never enables the target mode.
        shared = getattr(queue, '_shared_reads', None)
        source_dispatch = getattr(queue, '_source_commands', None)
        recovery_dispatch = getattr(queue, '_recovery_phases', None)
        generation = run_id = None
        tasks = []
        token = ''
        async def queue_call(method, *args, **kwargs):
            return await asyncio.to_thread(method, *args, **kwargs)
        try:
            authorization = socket.headers.get('authorization', '')
            if not authorization.startswith('Bearer ') or not authorization[7:]:
                await socket.close(code=4403)
                return
            token = authorization[7:]
            await socket.accept()
            message = await asyncio.wait_for(socket.receive(), 10)
            if message['type'] == 'websocket.disconnect':
                return
            hello = _decode(message)
            _fields(hello, ('type', 'protocol_version', 'run_id', 'launch_nonce', 'runtime_incarnation'),
                    ('environment',))
            if hello['type'] != 'ATTACH':
                raise ValueError('hello_required')
            environment = None
            if 'environment' in hello:
                from .kaggle_environment import validate_environment
                environment = validate_environment(hello['environment'])
            run_id = _identifier(hello['run_id'])
            attachment = await queue_call(queue.attach_channel, run_id, token,
                                          _identifier(hello['launch_nonce']),
                                          _identifier(hello['runtime_incarnation']))
            generation = attachment['connection_generation']
            logger.info(json.dumps({'event': 'bge_channel_attached', 'run_id': run_id,
                                    'connection_generation': generation,
                                    'provider_bound': attachment['provider_version'] is not None}))
            if environment is not None:
                logger.info(json.dumps({'event': 'kaggle_bootstrap_environment', 'run_id': run_id,
                    'runtime_incarnation': hello['runtime_incarnation'],
                    'connection_generation': generation, 'environment': environment,
                    'readiness_verified': False}))
            outgoing = asyncio.PriorityQueue(maxsize=SEND_QUEUE_FRAMES)
            service_frames = asyncio.Queue()
            sequence = count()
            probe_nonce = None
            last_work = None
            cancelled_work = None
            ready = False
            promoted = False
            ready_ack_sent = False
            telegram_inflight = False
            telegram_challenge = None
            pins_announced = False
            runtime_readiness = None
            runtime_blocked = None
            promotion_lock = asyncio.Lock()

            async def send(frame_type, *, priority=0, work=None, **payload):
                frame = {'type': frame_type, 'protocol_version': 2,
                         'connection_generation': generation, **payload}
                encoded = json.dumps(frame, allow_nan=False, ensure_ascii=False, separators=(',', ':'))
                limit = MAX_DISPATCH_BYTES if frame_type == 'DISPATCH' else MAX_FRAME_BYTES
                if frame_type=='DISPATCH' and payload.get('kind')=='source_command':limit=MAX_SOURCE_DISPATCH_BYTES
                if frame_type=='DISPATCH' and payload.get('kind')=='recovery_phase':limit=MAX_RECOVERY_DISPATCH_BYTES
                if len(encoded.encode('utf-8')) > limit:
                    raise ValueError('outgoing_size')
                await asyncio.wait_for(outgoing.put((priority, next(sequence), frame, encoded, work)), SEND_TIMEOUT_SECONDS)

            async def sender():
                nonlocal ready_ack_sent, telegram_inflight, telegram_challenge
                while True:
                    priority, _, frame, encoded, work = await outgoing.get()
                    if work is not None:
                        await status()  # Revalidate the authenticated attachment.
                        try:
                            shared.mark_work_sent(work)
                        except StaleAttachment:
                            # No network attempt happened. A cancelled read may
                            # have expired behind a control frame in this queue.
                            try:
                                shared.discard_unsent(work)
                            except StaleAttachment:
                                pass
                            continue
                    elif priority == 1:
                        # Drop a completed, expired or superseded assignment
                        # while it waited behind higher-priority control ACKs.
                        current = await queue_call(queue.resume_channel_claim, run_id, token, generation)
                        if current is None or any(frame[key] != current[key]
                                                  for key in ('id', 'claim', 'kind', 'texts', 'space')):
                            logger.info(json.dumps({'event': 'bge_channel_dispatch_discarded',
                                                    'run_id': run_id, 'connection_generation': generation,
                                                    'reason': 'claim_changed'}))
                            continue
                    if frame.get('type')=='RKB1_BIND_GRANT':
                        observed=await status()
                        owner=getattr(queue,'_telegram_runtime_source',None)
                        permitted=(ready_ack_sent and owner is not None
                            and all(observed.get(k) for k in ('ready','active','runtime_verified'))
                            and observed.get('provider_version') is not None
                            and await queue_call(owner.lease.current,run_id,hello['runtime_incarnation'])
                            and owner.current(run_id,hello['runtime_incarnation'],generation))
                        if not permitted:
                            # Current socket gets an explicit retryable answer, never a silent drop.
                            frame.clear()
                            frame.update(type='RKB1_BIND_PENDING',protocol_version=2,connection_generation=generation)
                            encoded=json.dumps(frame,separators=(',',':'))
                        else:
                            # Publish correlation before send awaits: a fast valid RESULT is allowed.
                            telegram_challenge=secrets.token_urlsafe(24)
                            frame['challenge']=telegram_challenge
                            encoded=json.dumps(frame,allow_nan=False,separators=(',',':'))
                    if frame.get('type')=='RKB1_BIND_PENDING':
                        telegram_inflight=False;telegram_challenge=None
                    write_started=time.monotonic_ns()
                    try:
                        await asyncio.wait_for(socket.send_text(encoded), SEND_TIMEOUT_SECONDS)
                    finally:
                        if frame.get('type')=='RKB1_BIND_GRANT':
                            for key in ('bundle_b64','api_id','api_hash'):frame.pop(key,None)
                        encoded=None
                    if work is not None and work.query is not None:
                        observe_boundary('write',frame,write_started,time.monotonic_ns())
                    if frame.get('type')=='READY_ACK' and frame.get('status')=='ready':
                        ready_ack_sent=True

            async def status():
                return await queue_call(queue.channel_status, run_id, token, generation)

            def observe_boundary(phase,frame,started,finished,**durations):
                observe=getattr(read_telemetry,'channel_timing',None)
                if observe is None:return
                try:
                    from .query_telemetry import ReadRuntime
                    observe(phase,request_id=frame['request_id'],
                        runtime=ReadRuntime(run_id,hello['runtime_incarnation']),
                        connection_generation=generation,turn_generation=frame['turn_generation'],
                        started_ns=started,finished_ns=finished,**durations)
                except Exception:pass  # Optional observation cannot affect a frame/result.

            stop_sent = False
            stop_checkpoint = None

            async def maybe_stop():
                nonlocal stop_sent, stop_checkpoint
                if stop_sent:
                    return True
                predicate = getattr(queue, 'stop_eligible', None)
                if predicate is None or not await queue_call(predicate, run_id, token, generation):
                    return False
                if shared is not None:
                    state = shared.status()
                    resource = state['resource']
                    if (state['document_result_pending'] or
                            resource is not None and resource.run_id == run_id):
                        return False
                # Only an authenticated, idle run reaches this point. Await
                # the existing target checkpoint owner before sending STOP;
                # success AND explicit failure are terminal for this wait.
                # The target callback enforces the original nine-hour limit.
                if before_stop is not None:
                    if stop_checkpoint is None:
                        logger.info(json.dumps({'event':'bge_stop_checkpoint',
                            'run_id':run_id,'connection_generation':generation,'outcome':'started'}))
                        stop_checkpoint = asyncio.create_task(before_stop(run_id,generation))
                        tasks.append(stop_checkpoint)
                        return False
                    if not stop_checkpoint.done():
                        return False  # Dispatch and receive must complete the checkpoint RPCs.
                    try:
                        stop_checkpoint.result()
                        logger.info(json.dumps({'event':'bge_stop_checkpoint',
                            'run_id':run_id,'connection_generation':generation,'outcome':'settled'}))
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        logger.warning(json.dumps({'event':'bge_stop_checkpoint',
                            'run_id':run_id,'outcome':'error','error_type':type(error).__name__}))
                # Same authenticated WSS, exact run/incarnation, no new frame.
                # A delivered STOP is not native provider terminal evidence.
                stop_sent = True
                await send('STOP', run_id=run_id, launch_nonce=hello['launch_nonce'],
                           runtime_incarnation=hello['runtime_incarnation'])
                return True

            def bootstrap_idle_current():
                # An idle baseline is control evidence only. Reauthenticate and
                # bind it to the persisted finite plan on every reception.
                from .kaggle_bootstrap_pins import BootstrapRuntimeExpectation
                with queue.connect(write=False) as db:
                    run = queue._authorize(db, run_id, token, queue.clock())
                    queue._dispatch_authorize(run, generation)
                    expected = queue._runtime_row(db, run_id)
                    if expected is None:
                        raise ValueError('bootstrap_control_unavailable')
                    row = db.execute('SELECT * FROM bge_runtime_bootstrap WHERE run_id=?', (run_id,)).fetchone()
                    if row is not None and row['finalized']:
                        proof=queue.bootstrap_pins_current(run_id,token,generation)
                        if proof is None:raise ValueError('bootstrap_control_unavailable')
                        return proof
                    plan = BootstrapRuntimeExpectation.from_wire(json.loads(expected['expected_json']))
                    if (row is None or row['deadline'] <= queue.clock()
                            or run['status'] != 'starting' or queue._run_expired(run, queue.clock())
                            or json.loads(row['plan_json']) != plan.wire()):
                        raise ValueError('bootstrap_control_unavailable')

            async def promote(observed):
                nonlocal promoted, runtime_blocked, pins_announced
                if observed.get('runtime_required') and not pins_announced:
                    proof=await queue_call(queue.bootstrap_pins_current,run_id,token,generation)
                    if proof is not None:
                        await send('BOOTSTRAP_ACK',status='pins_finalized',**proof)
                        pins_announced=True
                async with promotion_lock:
                    if ready and not promoted and observed['provider_version'] is not None:
                        if observed.get('runtime_required'):
                            if shared is None:
                                issues = ('shared_admission_required',)
                            elif runtime_readiness is None:
                                issues = ('runtime_readiness_required',)
                            else:
                                evaluated = runtime_readiness
                                proof = await queue_call(queue.accept_search_runtime, run_id, token,
                                                         generation, evaluated)
                                if runtime_readiness is not evaluated:
                                    # A newer READY replaced this observation while it
                                    # was checked; its own promote reports the outcome.
                                    # Never answer it with a stale runtime_not_ready.
                                    return
                                issues = proof.issues
                            if issues:
                                if runtime_blocked != issues:
                                    await send('READY_ACK', status='runtime_not_ready',
                                        provider_version=str(observed['provider_version']), reasons=list(issues))
                                    runtime_blocked = issues
                                return
                        await queue_call(queue.heartbeat, run_id, token, ready=True,
                                         connection_generation=generation)
                        promoted = True
                        logger.info(json.dumps({'event': 'bge_channel_ready', 'run_id': run_id,
                                                'connection_generation': generation}))
                        await send('READY_ACK', status='ready', provider_version=str(observed['provider_version']))

            async def dispatch():
                nonlocal last_work, cancelled_work
                resume_pending = True
                while True:
                    if not promoted or shared is None or probe_nonce is not None:
                        observed = await status()
                        await promote(observed)
                    # Inspect the original run before admission for a successor
                    # can fence this channel. Never stop merely on draining.
                    if promoted and await maybe_stop():
                        await asyncio.Event().wait()  # Sender waits for the worker's ordinary close.
                    if shared is not None:
                        if promoted and source_dispatch is not None:
                            settled=await source_dispatch.poll_settlement(run_id=run_id,token=token,generation=generation)
                            if settled is not None:
                                await send('RESULT_ACK',**settled)
                        if (last_work is not None and last_work.query is not None
                                and last_work.query.connection_generation == generation
                                and shared.status()['resource'] == last_work.resource
                                and cancelled_work != last_work.resource
                                and not shared.read_consumer_active(last_work)):
                            # Consumer cancellation is independent from actual
                            # forward/SQL cleanup and never releases capacity.
                            await send('CANCEL_QUERY', request_id=last_work.query.request_id,
                                       turn_generation=last_work.query.turn_generation,
                                       reason='caller_cancelled')
                            cancelled_work = last_work.resource
                        if probe_nonce is None and (promoted or getattr(queue,'_recovery_phases',None) is not None):
                            work = await shared.next_channel_work(run_id, token, generation)
                            if work is not None:
                                last_work = work
                                try:
                                    if work.query is not None:
                                        payload = work.query.model_dump(mode='json')
                                        read_type=payload.pop('type')
                                        payload.pop('protocol_version')
                                        await send(read_type, priority=1, work=work, **payload)
                                    else:
                                        await send('DISPATCH', priority=1, work=work,
                                            **dict(work.document_payload),
                                            compute_claim=work.compute_claim,
                                            dispatch_generation=work.dispatch_generation)
                                except BaseException:
                                    try:
                                        shared.discard_unsent(work)
                                    except StaleAttachment:
                                        pass  # An uncertain send retains its debt.
                                    raise
                        await shared.wait_channel_work(RECONCILE_SECONDS)
                        continue
                    if promoted:
                        job = None
                        if resume_pending:
                            job = await queue_call(queue.resume_channel_claim, run_id, token, generation)
                            resume_pending = False
                            if job:
                                logger.info(json.dumps({'event': 'bge_channel_claim_resumed',
                                                        'run_id': run_id, 'connection_generation': generation}))
                        if job is None:
                            job = await queue_call(queue.claim, run_id, token,
                                                   connection_generation=generation)
                        if job:
                            await send('DISPATCH', priority=1, **job)
                    await asyncio.sleep(RECONCILE_SECONDS)

            async def receiver(service=False):
                nonlocal ready, promoted, probe_nonce, runtime_readiness, runtime_blocked, telegram_inflight, telegram_challenge
                while True:
                    message = await (service_frames.get() if service else socket.receive())
                    received_at=time.monotonic_ns()
                    if message['type'] == 'websocket.disconnect':
                        return
                    frame = message if service else _decode(message)
                    if not service and frame['type'] in ('RECOVERY_PHASE_RESULT', 'SNAPSHOT_UPLOAD_INTENT', 'HEARTBEAT',
                                                         'RKB1_BIND_REQUEST', 'SOURCE_COMMAND_RESULT'):
                        service_frames.put_nowait(frame)
                        continue
                    if type(frame.get('connection_generation')) is not int or frame['connection_generation'] != generation:
                        raise StaleAttachment('generation')
                    status_started=time.monotonic_ns()
                    observed = await status()
                    status_ms=(time.monotonic_ns()-status_started)/1e6
                    common = ('type', 'protocol_version', 'connection_generation')
                    if shared is None and frame['type'] == 'RESOURCE_STATE':
                        _fields(frame, (*common, 'state'))
                        if frame['state'] != 'idle':
                            raise ValueError('bootstrap_idle_required')
                        await queue_call(bootstrap_idle_current)
                    elif shared is not None and frame['type'] == 'RESOURCE_STATE':
                        _fields(frame, (*common, 'state'),
                                ('resource_kind', 'resource_id', 'compute_claim',
                                 'dispatch_generation', 'probe_nonce'))
                        # An already queued idle baseline is weaker than the
                        # outstanding fresh ordered probe on a reconnect.
                        if probe_nonce is not None and 'probe_nonce' not in frame:
                            continue
                        if 'probe_nonce' in frame and frame['probe_nonce'] != probe_nonce:
                            continue  # An earlier exact-slot challenge was superseded.
                        await shared.resource_state(run_id, token, generation,
                            runtime_incarnation=hello['runtime_incarnation'],
                            **{key:value for key,value in frame.items() if key not in common})
                        if 'probe_nonce' in frame:
                            probe_nonce = None
                    elif shared is not None and frame['type'] == 'RESOURCE_RELEASED':
                        _fields(frame, (*common, 'resource_kind', 'resource_id',
                                        'compute_claim', 'dispatch_generation'))
                        changed = await shared.resource_released(run_id, token, generation,
                            runtime_incarnation=hello['runtime_incarnation'],
                            **{key:value for key,value in frame.items() if key not in common})
                        if probe_nonce is not None and changed:
                            # A retained release changes the slot version. A
                            # challenge issued before it cannot establish idle.
                            probe_nonce = await shared.begin_resource_probe(run_id, token, generation)
                            await send('RESOURCE_PROBE', probe_nonce=probe_nonce)
                    elif shared is not None and frame['type'] in ('QUERY_RESULT','SQL_HEALTH_RESULT','PASSAGE_READ_RESULT','STORY_READ_RESULT','FACT_READ_RESULT','SOURCE_READ_RESULT','CASE_READ_RESULT','GEO_READ_RESULT','EDITORIAL_READ_RESULT','SOURCE_GRAPH_READ_RESULT','ACCEPTANCE_READ_RESULT'):
                        events=frame.pop('read_events',None)
                        result = decode_envelope(json.dumps(frame, allow_nan=False,
                            ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
                        if not isinstance(result, (QueryResult,SqlHealthResult,PassageReadResult,StoryReadResult,FactReadResult,SourceReadResult,CaseReadResult,GeoReadResult,EditorialReadResult,SourceGraphReadResult,AcceptanceReadResult)):
                            raise ValueError('query_result_required')
                        if not ready or observed['provider_version'] is None:
                            raise ValueError('not_ready')
                        settle_started=time.monotonic_ns()
                        await shared.finish_read(run_id, token, generation, result)
                        finished=time.monotonic_ns()
                        observe_boundary('receive',frame,received_at,finished,status_ms=status_ms,
                            settle_ms=(finished-settle_started)/1e6)
                        if events is not None and read_telemetry is not None:
                            read_telemetry.native_events(events,request_id=result.request_id,run_id=run_id,
                                runtime_incarnation=hello['runtime_incarnation'],connection_generation=generation,
                                turn_generation=result.turn_generation)
                        # Ephemeral response: no durable ACK, receipt, replay or
                        # release of actual compute is implied by this frame.
                    elif frame['type']=='BOOTSTRAP_OBSERVATION':
                        _fields(frame,(*common,'observation'))
                        if not observed.get('runtime_required') or promoted:
                            raise ValueError('unexpected_bootstrap_observation')
                        receipt=await queue_call(queue.record_bootstrap_observation,run_id,token,generation,frame['observation'])
                        runtime_readiness=frame['observation']['runtime_readiness']
                        # Candidate exists, but promote() cannot grant serving
                        # until trusted local finalization replaces only the
                        # initial bootstrap expectation in one CAS.
                        ready=True;runtime_blocked=None
                        await send('BOOTSTRAP_ACK',observation_sha256=receipt,status='observed_not_ready')
                        await promote(observed)
                    elif frame['type']=='RKB1_BIND_REQUEST':
                        _fields(frame,(*common,'retained_client'))
                        if type(frame['retained_client']) is not bool:raise ValueError('telegram_request_fields')
                        if telegram_inflight:raise ValueError('telegram_request_already_pending')
                        telegram_inflight=True
                        owner=getattr(queue,'_telegram_runtime_source',None)
                        grant=None
                        if (ready_ack_sent and owner is not None
                                and all(observed.get(k) for k in ('ready','active','runtime_verified'))
                                and observed.get('provider_version') is not None):
                            try:
                                async with asyncio.timeout(5):
                                    grant=await owner.grant(run_id,hello['runtime_incarnation'],generation,
                                        retained_client=frame['retained_client'])
                            except Exception:
                                grant=None
                        if grant is None:await send('RKB1_BIND_PENDING')
                        else:await send('RKB1_BIND_GRANT',**grant)
                    elif frame['type']=='RKB1_BIND_RESULT':
                        _fields(frame,(*common,'status','challenge'))
                        if (not telegram_inflight or telegram_challenge is None
                                or type(frame['challenge']) is not str
                                or not secrets.compare_digest(frame['challenge'],telegram_challenge)
                                or frame['status'] not in ('authorized','unavailable')):
                            raise ValueError('unexpected_telegram_binding_result')
                        telegram_inflight=False;telegram_challenge=None
                        owner=getattr(queue,'_telegram_runtime_source',None)
                        confirmed=False
                        if owner is not None:
                            confirmed=await owner.confirm_archive_auth(run_id,hello['runtime_incarnation'],generation,
                                frame['status']=='authorized')
                        logger.info(json.dumps({'event':'rkb1_archive_authorization',
                            'run_id':run_id,'connection_generation':generation,
                            'status':frame['status'],'confirmed':bool(confirmed)}))
                        if frame['status']=='authorized' and not confirmed:
                            await send('RKB1_BIND_PENDING')
                    elif frame['type'] == 'READY':
                        _fields(frame, (*common, 'space', 'model_revision'), ('runtime_readiness',))
                        if frame['space'] != SPACE or frame['model_revision'] != REVISION:
                            raise ValueError('model_contract')
                        if (promoted and observed.get('runtime_required')
                                and frame.get('runtime_readiness') != runtime_readiness):
                            ready = promoted = False
                            # In-place revision changes require a separate
                            # drain/update barrier, not an unsolicited READY.
                            raise StaleAttachment('target readiness changed after promotion')
                        if 'runtime_readiness' in frame:
                            if not observed.get('runtime_required'):
                                raise ValueError('unexpected_runtime_readiness')
                            if not isinstance(frame['runtime_readiness'], dict):
                                raise ValueError('runtime_readiness')
                            runtime_readiness = frame['runtime_readiness']
                            runtime_blocked = None
                        ready = True
                        if observed['provider_version'] is None:
                            await send('READY_ACK', status='provider_pending', provider_version=None)
                        else:
                            await promote(observed)
                    elif frame['type'] == 'HEARTBEAT':
                        _fields(frame, common, ('diagnostics',))
                        await queue_call(queue.heartbeat, run_id, token,
                                         diagnostics=frame.get('diagnostics'),
                                         connection_generation=generation)
                        if not await maybe_stop():
                            await send('HEARTBEAT_ACK', provider_version=(str(observed['provider_version'])
                                       if observed['provider_version'] is not None else None))
                    elif frame['type']=='SNAPSHOT_UPLOAD_INTENT':
                        _fields(frame,(*common,'request_id','job_id','claim','run_id','launch_nonce',
                            'runtime_incarnation','compute_claim','dispatch_generation','operation_key','size','sha256'))
                        recovery_dispatch=getattr(queue,'_recovery_phases',None)
                        if recovery_dispatch is None or shared is None:
                            raise ValueError('snapshot_transfer_unavailable')
                        for key in ('request_id','job_id','claim','operation_key'):_identifier(frame[key])
                        from .kaggle_dataset_publisher import DatasetPublicationError
                        try:
                            ticket=await recovery_dispatch.issue_upload(frame,run_id=run_id,token=token,generation=generation)
                        except DatasetPublicationError:
                            # This grant failed before delivery. End only the
                            # capture request; keep the authenticated PG/model
                            # owner available for diagnosis and recovery.
                            ticket={'error':'snapshot_upload_grant_unavailable'}
                        await send('SNAPSHOT_UPLOAD_TICKET',request_id=frame['request_id'],ticket=ticket)
                    elif frame['type']=='RECOVERY_PHASE_RESULT':
                        recovery_dispatch=getattr(queue,'_recovery_phases',None)
                        _fields(frame,(*common,'job_id','claim','receipt','run_id','launch_nonce',
                            'runtime_incarnation','compute_claim','dispatch_generation'))
                        _identifier(frame['job_id']);_identifier(frame['claim'])
                        if shared is None:
                            raise ValueError('recovery_executor_unavailable')
                        if recovery_dispatch is None:
                            # A retained result can arrive immediately after
                            # READY on API restart, before periodic startup has
                            # rebuilt its maintenance owner. Keep this exact
                            # receipt until that owner can authenticate/settle it.
                            logger.info(json.dumps({'event':'bge_recovery_owner_wait',
                                'run_id':run_id,'connection_generation':generation}))
                            async with asyncio.timeout(2*SEND_TIMEOUT_SECONDS):
                                while recovery_dispatch is None:
                                    await asyncio.sleep(RECONCILE_SECONDS)
                                    recovery_dispatch=getattr(queue,'_recovery_phases',None)
                        try:
                            settled=await recovery_dispatch.receive(frame,run_id=run_id,token=token,generation=generation)
                        except (StaleClaim,LookupError):
                            settled=dict(job_id=frame['job_id'],claim=frame['claim'],status='stale')
                        shared.durable_result_settled(frame['job_id'],frame['claim'],settled['status'])
                        await send('RESULT_ACK',**settled)
                    elif frame['type']=='SOURCE_COMMAND_RESULT':
                        _fields(frame,(*common,'job_id','claim','receipt','run_id','launch_nonce',
                            'runtime_incarnation','compute_claim','dispatch_generation'))
                        if source_dispatch is None or shared is None or not ready or observed['provider_version'] is None:
                            raise ValueError('source_executor_unavailable')
                        _identifier(frame['job_id']);_identifier(frame['claim'])
                        try:
                            settled=await source_dispatch.receive(frame,run_id=run_id,token=token,generation=generation)
                        except (StaleClaim,LookupError):
                            settled=dict(job_id=frame['job_id'],claim=frame['claim'],status='stale')
                        if settled is None:
                            await send('RESULT_ACK',job_id=frame['job_id'],claim=frame['claim'],status='apply_received')
                        else:
                            shared.durable_result_settled(frame['job_id'],frame['claim'],settled['status'])
                            await send('RESULT_ACK',**settled)
                    elif frame['type'] == 'RESULT':
                        _fields(frame, (*common, 'job_id', 'claim', 'space', 'vectors'), ('timings',))
                        if (not ready or observed['provider_version'] is None
                                or (observed.get('runtime_required') and not promoted)):
                            raise ValueError('not_ready')
                        job_id = _identifier(frame['job_id'])
                        _identifier(frame['claim'])
                        if not isinstance(frame.get('timings', {}), dict):
                            raise ValueError('timings')
                        timings = frame.get('timings', {})
                        if set(timings) - {'encoder_seconds'} or any(
                            type(v) not in (int, float) or not math.isfinite(v) or v < 0
                            for v in timings.values()
                        ):
                            raise ValueError('timings')
                        vectors = frame['vectors']
                        if (not isinstance(vectors, list) or len(vectors) != 1
                                or not isinstance(vectors[0], list)
                                or any(type(v) not in (int, float) for v in vectors[0])):
                            raise ValueError('vectors')
                        try:
                            outcome = await queue_call(queue.complete, run_id, token, job_id,
                                frame['claim'], frame['space'], vectors, frame.get('timings', {}),
                                connection_generation=generation)
                        except StaleClaim:
                            outcome = 'stale'
                        if shared is not None:
                            shared.durable_result_settled(job_id, frame['claim'], outcome)
                        logger.info(json.dumps({'event': 'bge_channel_result', 'run_id': run_id,
                                                'connection_generation': generation, 'status': outcome}))
                        await send('RESULT_ACK', job_id=job_id, claim=frame['claim'], status=outcome)
                    else:
                        raise ValueError('unsupported_frame')

            await send('WELCOME', provider_version=(str(attachment['provider_version'])
                       if attachment['provider_version'] is not None else None))
            if shared is not None:
                try:
                    probe_nonce = await shared.begin_resource_probe(run_id, token, generation)
                except StaleAttachment:
                    # First attachment has no bound owned resource yet. A
                    # different unresolved run still fails at resource_state.
                    probe_nonce = None
                if probe_nonce is not None:
                    await send('RESOURCE_PROBE', probe_nonce=probe_nonce)
            tasks = [asyncio.create_task(sender()), asyncio.create_task(receiver()),
                     asyncio.create_task(dispatch()), asyncio.create_task(receiver(service=True))]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except WebSocketDisconnect:
            pass
        except asyncio.CancelledError:
            pass
        except PermissionError as error:
            from .bge_queue import StaleAttachment as CurrentStaleAttachment
            logger.info(json.dumps({'event': 'bge_channel_rejected', 'run_id': run_id,
                                    'connection_generation': generation,
                                    'reason': 'stale_attachment' if isinstance(error, CurrentStaleAttachment) else 'revoked_run',
                                    'error_type': type(error).__name__,'error_site':_failure_site(error)}))
            await socket.close(code=1012 if isinstance(error, CurrentStaleAttachment) else 4403)
        except (ValueError, KeyError, TypeError, UnicodeError, TimeoutError) as error:
            logger.info(json.dumps({'event': 'bge_channel_rejected', 'run_id': run_id,
                                    'connection_generation': generation, 'reason': 'invalid_frame',
                                    'error_type':type(error).__name__,'error_site':_failure_site(error)}))
            await socket.close(code=1012)
        except Exception as error:
            logger.warning(json.dumps({'event': 'bge_channel_failed', 'run_id': run_id,
                                       'connection_generation': generation,
                                       'error_type': type(error).__name__, **failure_facts(error)}))
            await socket.close(code=1011)
        finally:
            with anyio.move_on_after(6, shield=True):
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                if generation is not None:
                    if shared is not None:shared.channel_lost(run_id,generation)
                    try:
                        await queue_call(queue.detach_channel, run_id, token, generation)
                    except PermissionError:
                        pass
    return WebSocketRoute('/bge-worker/channel', endpoint)
