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
from itertools import count

import anyio
from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocketDisconnect

from .bge_contract import REVISION, SPACE
from .bge_queue import StaleClaim, StaleAttachment
from .kaggle_protocol import RESULT_MAX_BYTES, decode_envelope, QueryResult, PassageReadResult

MAX_FRAME_BYTES = 65536
MAX_DISPATCH_BYTES = 256 * 1024
MAX_SOURCE_DISPATCH_BYTES = 2 * 1024 * 1024
MAX_SOURCE_RESULT_BYTES = 16 * 1024
MAX_RECOVERY_DISPATCH_BYTES = 4 * 1024 * 1024
MAX_RECOVERY_RESULT_BYTES = 128 * 1024
SEND_QUEUE_FRAMES = 8
RECONCILE_SECONDS = .25
SEND_TIMEOUT_SECONDS = 5
logger = logging.getLogger(__name__)


def _decode(message):
    raw = message.get('bytes')
    if raw is None:
        raw = message.get('text', '').encode('utf-8')
    if len(raw) > RESULT_MAX_BYTES:
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
    if frame.get('type') not in ('QUERY_RESULT','PASSAGE_READ_RESULT','RECOVERY_PHASE_RESULT') and len(raw) > MAX_FRAME_BYTES:
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


def channel_route(queue):
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
            sequence = count()
            probe_nonce = None
            last_work = None
            cancelled_work = None
            ready = False
            promoted = False
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
                    await asyncio.wait_for(socket.send_text(encoded), SEND_TIMEOUT_SECONDS)

            async def status():
                return await queue_call(queue.channel_status, run_id, token, generation)

            async def promote(observed):
                nonlocal promoted, runtime_blocked
                async with promotion_lock:
                    if ready and not promoted and observed['provider_version'] is not None:
                        if observed.get('runtime_required'):
                            if runtime_readiness is None:
                                issues = ('runtime_readiness_required',)
                            else:
                                proof = await queue_call(queue.accept_search_runtime, run_id, token,
                                                         generation, runtime_readiness)
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
                    observed = await status()
                    await promote(observed)
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

            async def receiver():
                nonlocal ready, promoted, probe_nonce, runtime_readiness, runtime_blocked
                while True:
                    message = await socket.receive()
                    if message['type'] == 'websocket.disconnect':
                        return
                    frame = _decode(message)
                    if type(frame.get('connection_generation')) is not int or frame['connection_generation'] != generation:
                        raise StaleAttachment('generation')
                    observed = await status()
                    common = ('type', 'protocol_version', 'connection_generation')
                    if shared is not None and frame['type'] == 'RESOURCE_STATE':
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
                    elif shared is not None and frame['type'] in ('QUERY_RESULT','PASSAGE_READ_RESULT'):
                        result = decode_envelope(json.dumps(frame, allow_nan=False,
                            ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
                        if not isinstance(result, (QueryResult,PassageReadResult)):
                            raise ValueError('query_result_required')
                        if not ready or observed['provider_version'] is None:
                            raise ValueError('not_ready')
                        await shared.finish_read(run_id, token, generation, result)
                        # Ephemeral response: no durable ACK, receipt, replay or
                        # release of actual compute is implied by this frame.
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
                        await send('HEARTBEAT_ACK', provider_version=(str(observed['provider_version'])
                                   if observed['provider_version'] is not None else None))
                    elif frame['type']=='RECOVERY_PHASE_RESULT':
                        recovery_dispatch=getattr(queue,'_recovery_phases',None)
                        _fields(frame,(*common,'job_id','claim','receipt','run_id','launch_nonce',
                            'runtime_incarnation','compute_claim','dispatch_generation'))
                        if recovery_dispatch is None or shared is None:
                            raise ValueError('recovery_executor_unavailable')
                        _identifier(frame['job_id']);_identifier(frame['claim'])
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
                        if not ready or observed['provider_version'] is None:
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
                     asyncio.create_task(dispatch())]
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
                                    'reason': 'stale_attachment' if isinstance(error, CurrentStaleAttachment) else 'revoked_run'}))
            await socket.close(code=4409 if isinstance(error, CurrentStaleAttachment) else 4403)
        except (ValueError, KeyError, TypeError, UnicodeError, TimeoutError):
            logger.info(json.dumps({'event': 'bge_channel_rejected', 'run_id': run_id,
                                    'connection_generation': generation, 'reason': 'invalid_frame'}))
            await socket.close(code=4400)
        except Exception as error:
            logger.warning(json.dumps({'event': 'bge_channel_failed', 'run_id': run_id,
                                       'connection_generation': generation,
                                       'error_type': type(error).__name__}))
            await socket.close(code=1011)
        finally:
            with anyio.move_on_after(6, shield=True):
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                if generation is not None:
                    try:
                        await queue_call(queue.detach_channel, run_id, token, generation)
                    except PermissionError:
                        pass
    return WebSocketRoute('/bge-worker/channel', endpoint)
