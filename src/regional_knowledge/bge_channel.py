"""Single run-authenticated WSS attachment for existing durable BGE jobs.

This is an encoding transport, not public search or accepted/Saved recovery.
The queue owns authentication, attachment generations and durable claims.
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
from .bge_queue import StaleClaim

MAX_FRAME_BYTES = 65536
MAX_DISPATCH_BYTES = 256 * 1024
SEND_QUEUE_FRAMES = 8
RECONCILE_SECONDS = .25
SEND_TIMEOUT_SECONDS = 5
logger = logging.getLogger(__name__)


def _decode(message):
    raw = message.get('bytes')
    if raw is None:
        raw = message.get('text', '').encode('utf-8')
    if len(raw) > MAX_FRAME_BYTES:
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
            _fields(hello, ('type', 'protocol_version', 'run_id', 'launch_nonce', 'runtime_incarnation'))
            if hello['type'] != 'ATTACH':
                raise ValueError('hello_required')
            run_id = _identifier(hello['run_id'])
            attachment = await queue_call(queue.attach_channel, run_id, token,
                                          _identifier(hello['launch_nonce']),
                                          _identifier(hello['runtime_incarnation']))
            generation = attachment['connection_generation']
            logger.info(json.dumps({'event': 'bge_channel_attached', 'run_id': run_id,
                                    'connection_generation': generation,
                                    'provider_bound': attachment['provider_version'] is not None}))
            outgoing = asyncio.PriorityQueue(maxsize=SEND_QUEUE_FRAMES)
            sequence = count()
            ready = False
            promoted = False
            promotion_lock = asyncio.Lock()

            async def send(frame_type, *, priority=0, **payload):
                frame = {'type': frame_type, 'protocol_version': 2,
                         'connection_generation': generation, **payload}
                encoded = json.dumps(frame, allow_nan=False, ensure_ascii=False, separators=(',', ':'))
                limit = MAX_DISPATCH_BYTES if frame_type == 'DISPATCH' else MAX_FRAME_BYTES
                if len(encoded.encode('utf-8')) > limit:
                    raise ValueError('outgoing_size')
                await asyncio.wait_for(outgoing.put((priority, next(sequence), encoded)), SEND_TIMEOUT_SECONDS)

            async def sender():
                while True:
                    priority, _, encoded = await outgoing.get()
                    if priority == 1:
                        await status()  # Fence a DISPATCH delayed in the send buffer.
                    await asyncio.wait_for(socket.send_text(encoded), SEND_TIMEOUT_SECONDS)

            async def status():
                return await queue_call(queue.channel_status, run_id, token, generation)

            async def promote(observed):
                nonlocal promoted
                async with promotion_lock:
                    if ready and not promoted and observed['provider_version'] is not None:
                        await queue_call(queue.heartbeat, run_id, token, ready=True,
                                         connection_generation=generation)
                        promoted = True
                        logger.info(json.dumps({'event': 'bge_channel_ready', 'run_id': run_id,
                                                'connection_generation': generation}))
                        await send('READY_ACK', status='ready', provider_version=str(observed['provider_version']))

            async def dispatch():
                while True:
                    observed = await status()
                    await promote(observed)
                    if promoted:
                        job = await queue_call(queue.claim, run_id, token,
                                               connection_generation=generation)
                        if job:
                            await send('DISPATCH', priority=1, **job)
                    await asyncio.sleep(RECONCILE_SECONDS)

            async def receiver():
                nonlocal ready
                while True:
                    message = await socket.receive()
                    if message['type'] == 'websocket.disconnect':
                        return
                    frame = _decode(message)
                    if type(frame.get('connection_generation')) is not int or frame['connection_generation'] != generation:
                        from .bge_queue import StaleAttachment
                        raise StaleAttachment('generation')
                    observed = await status()
                    common = ('type', 'protocol_version', 'connection_generation')
                    if frame['type'] == 'READY':
                        _fields(frame, (*common, 'space', 'model_revision'))
                        if frame['space'] != SPACE or frame['model_revision'] != REVISION:
                            raise ValueError('model_contract')
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
                        logger.info(json.dumps({'event': 'bge_channel_result', 'run_id': run_id,
                                                'connection_generation': generation, 'status': outcome}))
                        await send('RESULT_ACK', job_id=job_id, claim=frame['claim'], status=outcome)
                    else:
                        raise ValueError('unsupported_frame')

            await send('WELCOME', provider_version=(str(attachment['provider_version'])
                       if attachment['provider_version'] is not None else None))
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
            # Imported lazily so the transport remains coupled only to queue API.
            from .bge_queue import StaleAttachment
            logger.info(json.dumps({'event': 'bge_channel_rejected', 'run_id': run_id,
                                    'connection_generation': generation,
                                    'reason': 'stale_attachment' if isinstance(error, StaleAttachment) else 'revoked_run'}))
            await socket.close(code=4409 if isinstance(error, StaleAttachment) else 4403)
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
