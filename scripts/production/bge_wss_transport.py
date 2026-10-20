"""Outbound durable BGE transport, embedded into the private worker by its controller.

No model, credentials, scheduler or public query authority is created here. One
encoder load and forward survive reconnect; only an unacknowledged exact result
is resent. This is not accepted/Saved or exact provider termination evidence.
"""
import asyncio
import inspect
import json
import math
import random
import secrets
import time
from urllib.parse import urlsplit


class BgeChannelStopped(RuntimeError):
    pass


def _channel_json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate channel field')
            result[key] = value
        return result

    if not isinstance(raw, (str, bytes)) or len(raw.encode() if isinstance(raw, str) else raw) > 2*1024*1024:
        raise ValueError('channel frame bound')
    frame = json.loads(raw, object_pairs_hook=unique,
                       parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite channel value')))
    if not isinstance(frame, dict) or type(frame.get('protocol_version')) is not int or frame['protocol_version'] != 2:
        raise ValueError('channel protocol')
    limit=(4*1024*1024 if frame.get('type')=='DISPATCH' and frame.get('kind')=='recovery_phase' else
        2*1024*1024 if frame.get('type')=='DISPATCH' and frame.get('kind')=='source_command' else 262144)
    if len(raw.encode() if isinstance(raw,str) else raw)>limit:raise ValueError('channel frame bound')
    return frame


class BgeWssTransport:
    """One bounded in-flight encode and result, without a second job authority."""

    def __init__(self, config, load_encoder, diagnostics, *, connector=None,
                 heartbeat_seconds=20, maximum_seconds=11 * 3600, stop=None,
                 query_handler=None, prepare_runtime=None, close_runtime=None, recovery_handler=None, snapshot_channel=None):
        url = urlsplit(config['channel'])
        if (url.scheme != 'wss' or not url.hostname or url.port not in (None, 443)
                or url.username or url.password or url.query or url.fragment):
            raise ValueError('authenticated WSS/443 channel required')
        if not math.isfinite(heartbeat_seconds) or not 0 < heartbeat_seconds <= 20:
            raise ValueError('bounded heartbeat interval required')
        if not math.isfinite(maximum_seconds) or not 0 < maximum_seconds <= 11 * 3600:
            raise ValueError('bounded worker lifetime required')
        shared = config.get('shared_reads', False)
        async_prepare = (inspect.iscoroutinefunction(prepare_runtime)
                         or inspect.iscoroutinefunction(getattr(prepare_runtime, '__call__', None)))
        if prepare_runtime is not None and (not shared or not async_prepare):
            raise ValueError('shared preparation requires an async prepare_runtime')
        if type(shared) is not bool or (shared and not async_prepare and not (
                inspect.iscoroutinefunction(query_handler)
                or inspect.iscoroutinefunction(getattr(query_handler, '__call__', None)))):
            raise ValueError('shared_reads requires an async query_handler')
        self.config, self.load_encoder, self.diagnostics = config, load_encoder, diagnostics
        self.connector = connector
        self.heartbeat_seconds, self.maximum_seconds = heartbeat_seconds, maximum_seconds
        self.stop = stop or asyncio.Event()
        self.incarnation = secrets.token_urlsafe(24)
        self.encoder = None
        self.loading = None
        self.encoding = None
        self.job = None
        self.waiting_job = None
        self.pending = None
        self.acknowledged = None
        self.changed = asyncio.Event()
        self.shared_reads, self.query_handler = shared, query_handler
        self.prepare_runtime, self.prepared_runtime = prepare_runtime, None
        self.source_handler = None
        if recovery_handler is not None and (not shared or not (inspect.iscoroutinefunction(recovery_handler)
                or inspect.iscoroutinefunction(getattr(recovery_handler,'__call__',None)))):
            raise ValueError('shared async recovery handler required')
        self.recovery_handler=recovery_handler
        if snapshot_channel is not None:
            from regional_knowledge.kaggle_snapshot_channel import SnapshotTicketChannel
            if type(snapshot_channel) is not SnapshotTicketChannel or recovery_handler is None:
                raise ValueError('retained snapshot channel requires fixed recovery handler')
            snapshot_channel.changed=self.changed
        self.snapshot_channel=snapshot_channel
        if close_runtime is not None and not inspect.iscoroutinefunction(close_runtime):
            raise ValueError('async close_runtime required')
        self.close_runtime = close_runtime
        self.forward_lock = asyncio.Lock()
        self.forwards = set()
        self.resource = None
        self.last_released = None
        self.read_task = None
        self.read_frame = None
        self.read_cancelled = False
        self.read_discard = False
        self.read_pending = None

    @staticmethod
    def _identifier(value):
        return isinstance(value, str) and 0 < len(value.encode('utf-8')) <= 256

    @staticmethod
    def _read_identity(frame):
        return tuple(frame.get(key) for key in ('request_id', 'connection_generation', 'turn_generation'))

    def _resource_state(self, probe_nonce=None):
        frame = {'type': 'RESOURCE_STATE', 'state': 'busy' if self.resource else 'idle'}
        if self.resource:
            frame.update(self.resource)
        if probe_nonce is not None:
            frame['probe_nonce'] = probe_nonce
        return frame

    def _claim_resource(self, frame, kind):
        if self.resource is not None or self.forwards:
            raise ValueError('worker resource busy')
        self.last_released = None  # New assignment implicitly acknowledges release.
        if kind == 'document':
            self.read_frame, self.read_pending = None, None
        self.resource = {'resource_kind': kind,
            'resource_id': frame['request_id'] if kind == 'read' else frame['id'],
            'compute_claim': frame['compute_claim'],
            'dispatch_generation': frame['connection_generation']}
        self.changed.set()
        return self.resource

    def _maybe_release(self):
        if not self.shared_reads or self.resource is None or self.forwards:
            return
        task = self.read_task if self.resource['resource_kind'] == 'read' else self.encoding
        if task is not None and task.done():
            self.last_released, self.resource = self.resource, None
            self.changed.set()

    async def _forward(self, text, resource=None):
        if resource is not None and self.resource is not resource:
            raise ValueError('encode consumer no longer owns resource')
        async def actual():
            async with self.forward_lock:
                return await asyncio.to_thread(self.encoder, text)
        # The Task, not its consumer, owns the lock until the real sync forward
        # returns. No consumer/session cancellation is passed to this Task.
        task = asyncio.create_task(actual())
        self.forwards.add(task)
        def finished(done):
            self.forwards.discard(done)
            if not done.cancelled():
                done.exception()
            self._maybe_release()
            self.changed.set()
        task.add_done_callback(finished)
        return await asyncio.shield(task)

    def _cancel_read(self):
        self.read_discard = True
        self.read_pending = None
        if self.read_task is not None and not self.read_task.done() and not self.read_cancelled:
            self.read_cancelled = True
            self.read_task.cancel()

    @staticmethod
    def _unavailable(frame, reason):
        exact=frame.get('type')=='PASSAGE_READ'
        return {'type': 'PASSAGE_READ_RESULT' if exact else 'QUERY_RESULT', 'protocol_version': 2,
            **{key: frame[key] for key in ('request_id', 'connection_generation', 'turn_generation')},
            'status': 'unavailable', **({} if exact else {'mode':None}), 'evidence': [],
            'observed_data_seq': None, 'observed_projection_seq': None, 'reason': reason}

    def _validate_query(self, frame):
        exact=frame.get('type')=='PASSAGE_READ'
        if exact:
            from regional_knowledge.kaggle_protocol import decode_envelope,PassageReadRequest
            if not isinstance(decode_envelope(json.dumps(frame,allow_nan=False).encode()),PassageReadRequest):
                raise ValueError('shared exact read contract')
        if (len(json.dumps(frame, allow_nan=False).encode()) > 16384
                or not all(self._identifier(frame.get(key)) for key in ('request_id', 'compute_claim'))
                or type(frame.get('turn_generation')) is not int or frame['turn_generation'] < 0
                or type(frame.get('deadline_ms')) is not int or not 0 < frame['deadline_ms'] <= 2000
                or (not exact and (not isinstance(frame.get('query'), str) or not frame['query'].strip()
                or len(frame['query']) > 1000 or len(frame['query'].encode()) > 4096))
                or self.encoder is None
                or self.prepare_runtime is not None and self.prepared_runtime is None):
            raise ValueError('shared QUERY contract')

    def _validate_query_result(self, frame, result):
        if frame.get('type')=='PASSAGE_READ':
            from regional_knowledge.kaggle_protocol import decode_envelope,PassageReadResult
            typed=decode_envelope(json.dumps(result,allow_nan=False).encode())
            if not isinstance(typed,PassageReadResult) or self._read_identity(result)!=self._read_identity(frame):
                raise ValueError('exact read result identity/type mismatch')
            return result
        if (not isinstance(result, dict) or result.get('type') != 'QUERY_RESULT'
                or type(result.get('protocol_version')) is not int or result['protocol_version'] != 2
                or self._read_identity(result) != self._read_identity(frame)
                or type(result.get('connection_generation')) is not int
                or type(result.get('turn_generation')) is not int
                or result.get('status') not in ('ok', 'degraded', 'unavailable', 'cancelled', 'invalid_query', 'denied')):
            raise ValueError('query result identity/shape')
        searched = result['status'] in ('ok', 'degraded')
        if ((result.get('mode') == 'hybrid') != searched
                or (not searched and (result.get('mode') is not None or result.get('evidence')))
                or (searched and result.get('reason') == 'dense_unavailable')
                or (result['status'] != 'ok' and not result.get('reason'))
                or len(json.dumps(result, allow_nan=False, separators=(',', ':')).encode()) > 262144):
            raise ValueError('query result bound/mode')
        return result

    def _start_read(self, frame):
        resource = self._claim_resource(frame, 'read')
        self.read_frame = frame
        self.read_pending = None
        self.read_cancelled = self.read_discard = False
        async def consume():
            async def encode(text):
                return await self._forward(text, resource)
            deadline = asyncio.get_running_loop().time() + frame['deadline_ms'] / 1000
            try:
                async with asyncio.timeout_at(deadline):
                    result = await self.query_handler(frame, encode=encode)
                if asyncio.get_running_loop().time() >= deadline:
                    result = self._unavailable(frame, 'deadline_exceeded')
                result = self._validate_query_result(frame, result)
            except asyncio.CancelledError:
                raise
            except Exception:
                reason = 'deadline_exceeded' if asyncio.get_running_loop().time() >= deadline else 'internal_error'
                result = self._unavailable(frame, reason)
            if not self.read_discard:
                self.read_pending = result
                self.changed.set()
        self.read_task = asyncio.create_task(consume())
        self.read_task.add_done_callback(lambda _: (self._maybe_release(), self.changed.set()))

    async def _load(self):
        self.encoder = await asyncio.to_thread(self.load_encoder)
        if self.prepare_runtime is not None:
            prepared = await self.prepare_runtime(self.encoder, run_id=self.config['run_id'],
                launch_nonce=self.config['launch_nonce'], runtime_incarnation=self.incarnation)
            from regional_knowledge.kaggle_bootstrap import PreparedRuntime
            if type(prepared) is not PreparedRuntime:
                raise ValueError('typed PreparedRuntime required')
            prepared.__post_init__()
            prepared.readiness.to_wire()
            self.query_handler, self.prepared_runtime = prepared.query_handler, prepared
            self.source_handler = prepared.source_handler
        self.changed.set()

    async def _encode(self, job):
        if job['kind'] in ('source_command','recovery_phase'):
            handler=self.recovery_handler if job['kind']=='recovery_phase' else self.source_handler
            if handler is None:raise ValueError('fixed native executor unavailable')
            if job['kind']=='recovery_phase' and self.snapshot_channel is not None:
                with self.snapshot_channel.assignment(job):receipt=await handler(job)
            else:receipt=await handler(job)
            self.pending=dict(job_id=job['id'],claim=job['claim'],receipt=receipt,
                **{key:job[key] for key in ('run_id','launch_nonce','runtime_incarnation',
                    'compute_claim','dispatch_generation')})
            if len(json.dumps(self.pending,allow_nan=False).encode())>(130000 if job['kind']=='recovery_phase' else 15500):
                raise ValueError('source command receipt bound')
            self.changed.set()
            return
        before = time.monotonic()
        vector = await self._forward(job['texts'][0], self.resource if self.shared_reads else None)
        self.pending = {'job_id': job['id'], 'claim': job['claim'], 'space': SPACE,
                        'vectors': [vector], 'timings': {'encoder_seconds': time.monotonic() - before}}
        # Reject non-finite/oversized model output before queueing any bytes.
        if len(json.dumps(self.pending, allow_nan=False).encode()) > 262000:
            raise ValueError('channel result bound')
        self.changed.set()

    def _start(self, job):
        if self.shared_reads:
            self._claim_resource(job, 'document')
        self.job = job
        self.encoding = asyncio.create_task(self._encode(job))
        self.encoding.add_done_callback(lambda _: (self._maybe_release(), self.changed.set()))

    @staticmethod
    def _failure(task):
        if task is not None and task.done():
            task.result()

    async def _session(self, socket):
        async def receive():
            return _channel_json(await socket.receive())

        async def send(frame):
            raw = json.dumps({'protocol_version': 2, **frame}, allow_nan=False, separators=(',', ':'))
            if len(raw.encode()) > 262144:
                raise ValueError('channel send bound')
            await socket.send(raw)

        environment = {}
        if self.config.get('target_runtime') is not None:
            from regional_knowledge.kaggle_environment import observe_environment
            environment['environment'] = observe_environment()
        await send({'type': 'ATTACH', 'run_id': self.config['run_id'],
                    'launch_nonce': self.config['launch_nonce'], 'runtime_incarnation': self.incarnation,
                    **environment})
        welcome = await asyncio.wait_for(receive(), 10)
        generation = welcome.get('connection_generation')
        if welcome.get('type') != 'WELCOME' or type(generation) is not int or generation < 1:
            raise ValueError('server attachment generation required')
        attachment_release = attachment_state = None
        if self.shared_reads:
            self._maybe_release()
            attachment_release = self.last_released
            if attachment_release is not None:
                await send({'type': 'RESOURCE_RELEASED', 'connection_generation': generation, **attachment_release})
            attachment_state = self._resource_state()
            await send({'connection_generation': generation, **attachment_state})
        if self.loading is None:
            # Minimal authenticated control is established before Torch/model imports.
            self.loading = asyncio.create_task(self._load())
            self.loading.add_done_callback(lambda _: self.changed.set())
        outbound = asyncio.PriorityQueue(maxsize=4)
        order = 0
        read_sent = None

        async def put(frame, priority=0):
            nonlocal order
            order += 1
            await outbound.put((priority, order, {'connection_generation': generation, **frame}))

        async def writer():
            while True:
                _, _, frame = await outbound.get()
                if frame.get('type') == 'QUERY_RESULT' and frame.get('connection_generation') != generation:
                    continue
                await send(frame)

        async def queue_read_result():
            nonlocal read_sent
            result = self.read_pending
            if result is not None and read_sent is not result:
                # Once queued this immutable frame belongs to this session,
                # independently of the next read consumer. Server finish_read
                # still checks its original ticket/deadline/current authority.
                read_sent = result
                await put(result, priority=1)

        async def heartbeat():
            while True:
                await asyncio.sleep(self.heartbeat_seconds)
                await put({'type': 'HEARTBEAT', 'diagnostics': await asyncio.to_thread(self.diagnostics)})

        async def states():
            ready_sent = False
            result_sent = None
            intent_sent = None
            released_sent = attachment_release
            state_sent = attachment_state
            self.changed.set()
            while True:
                await self.changed.wait()
                self.changed.clear()
                self._failure(self.loading)
                self._failure(self.encoding)
                if self.snapshot_channel is not None:
                    intent=self.snapshot_channel.pending
                    if intent is not None and intent is not intent_sent:
                        await put(intent);intent_sent=intent
                if self.shared_reads:
                    # Preserve the completed result before advertising compute
                    # release, which can immediately authorize the next read.
                    await queue_read_result()
                    self._maybe_release()
                    if self.last_released is not None and self.last_released != released_sent:
                        await put({'type': 'RESOURCE_RELEASED', **self.last_released})
                        released_sent = self.last_released
                    state = self._resource_state()
                    if state != state_sent:
                        await put(state)
                        state_sent = state
                if (self.encoder is not None and not ready_sent
                        and (self.prepare_runtime is None or self.prepared_runtime is not None)):
                    frame = {'type': 'READY', 'space': SPACE, 'model_revision': REVISION}
                    if self.prepared_runtime is not None:
                        frame['runtime_readiness'] = self.prepared_runtime.readiness.to_wire()
                    await put(frame)
                    ready_sent = True
                if self.pending is not None and result_sent != (self.pending['job_id'], self.pending['claim']):
                    result_type={'source_command':'SOURCE_COMMAND_RESULT','recovery_phase':'RECOVERY_PHASE_RESULT'}.get(self.job['kind'],'RESULT')
                    await put({'type': result_type, **self.pending}, priority=1)
                    result_sent = (self.pending['job_id'], self.pending['claim'])

        async def reader():
            while True:
                frame = await receive()
                if type(frame.get('connection_generation')) is not int or frame['connection_generation'] != generation:
                    raise BgeChannelStopped('stale channel generation')
                kind = frame.get('type')
                if self.shared_reads and kind == 'RESOURCE_PROBE':
                    if not self._identifier(frame.get('probe_nonce')):
                        raise ValueError('bounded resource probe nonce required')
                    self._maybe_release()
                    if self.last_released is not None:
                        # Idle alone cannot distinguish a never-received claim
                        # from completed work whose durable result is pending.
                        await put({'type': 'RESOURCE_RELEASED', **self.last_released})
                    await put(self._resource_state(frame['probe_nonce']))
                elif self.shared_reads and kind == 'CANCEL_QUERY':
                    if (not self._identifier(frame.get('request_id'))
                            or type(frame.get('turn_generation')) is not int or frame['turn_generation'] < 0):
                        raise ValueError('cancel QUERY identity contract')
                    if (self.read_frame is not None
                            and self._read_identity(frame) == self._read_identity(self.read_frame)):
                        self._cancel_read()
                elif self.shared_reads and kind in ('QUERY','PASSAGE_READ'):
                    self._validate_query(frame)
                    if (self.read_frame is not None
                            and frame['compute_claim'] == self.read_frame['compute_claim']
                            and frame['connection_generation'] != self.read_frame['connection_generation']):
                        raise ValueError('ephemeral QUERY cannot replay across sessions')
                    if self.read_frame is not None and self._read_identity(frame) == self._read_identity(self.read_frame):
                        if json.dumps(frame, sort_keys=True, allow_nan=False) != json.dumps(self.read_frame, sort_keys=True, allow_nan=False):
                            raise ValueError('replayed QUERY payload mismatch')
                        continue
                    await queue_read_result()
                    self._start_read(frame)
                elif kind == 'SNAPSHOT_UPLOAD_TICKET':
                    if (self.snapshot_channel is None or set(frame)!={'type','protocol_version',
                            'connection_generation','request_id','ticket'}):
                        raise ValueError('snapshot ticket response contract')
                    self.snapshot_channel.receive(frame)
                elif kind == 'DISPATCH':
                    source=frame.get('kind')=='source_command'
                    recovery=frame.get('kind')=='recovery_phase'
                    if source or recovery:
                        if (not self.shared_reads or (self.recovery_handler if recovery else self.source_handler) is None
                                or tuple(frame.get(key) for key in ('run_id','launch_nonce','runtime_incarnation'))
                                    !=(self.config['run_id'],self.config['launch_nonce'],self.incarnation)
                                or any(not self._identifier(frame.get(key)) for key in ('id','claim'))):
                            raise ValueError('source dispatch contract')
                    elif (frame.get('space') != SPACE or frame.get('kind') not in ('query', 'document')
                            or not isinstance(frame.get('texts'), list) or len(frame['texts']) != 1
                            or not isinstance(frame['texts'][0], str)
                            or not 0 < len(frame['texts'][0]) <= 40000
                            or any(not isinstance(frame.get(key), str) or not 0 < len(frame[key]) <= 256
                                   for key in ('id', 'claim')) or self.encoder is None
                            or self.prepare_runtime is not None and self.prepared_runtime is None):
                        raise ValueError('durable BGE dispatch contract')
                    if self.shared_reads and (not self._identifier(frame.get('compute_claim'))
                            or type(frame.get('dispatch_generation')) is not int
                            or frame['dispatch_generation'] < 1 or frame['kind'] not in ('document','source_command','recovery_phase')):
                        raise ValueError('shared document resource contract')
                    key = (frame['id'], frame['claim'])
                    if key == self.acknowledged:
                        continue  # A control ACK can overtake a buffered replay.
                    if self.job is not None and key == (self.job['id'], self.job['claim']):
                        if frame['kind'] != self.job['kind'] or (recovery and any(frame.get(key)!=self.job.get(key)
                                for key in ('phase','operation_key','request_sha256','arguments','database_generation','writer_generation',
                                            'run_id','launch_nonce','runtime_incarnation'))) or (source and any(frame.get(key)!=self.job.get(key)
                                for key in ('command_b64','command_sha256','accepted','handler_sha256','database_generation','writer_generation',
                                            'run_id','launch_nonce','runtime_incarnation'))
                                or not (source or recovery) and frame['texts'] != self.job['texts']):
                            raise ValueError('replayed assignment payload mismatch')
                        if self.shared_reads and any(frame.get(key) != self.job.get(key)
                                for key in ('compute_claim', 'dispatch_generation')):
                            raise ValueError('replayed document resource mismatch')
                        continue  # Existing forward/result still owns this claim.
                    if self.shared_reads and frame['dispatch_generation'] != generation:
                        raise ValueError('new dispatch requires current generation')
                    if self.waiting_job is not None and key == (self.waiting_job['id'], self.waiting_job['claim']):
                        if frame['texts'] != self.waiting_job['texts'] or frame['kind'] != self.waiting_job['kind']:
                            raise ValueError('replayed waiting assignment payload mismatch')
                        continue
                    if self.job is not None:
                        if self.shared_reads:
                            raise ValueError('durable result still awaiting acknowledgment')
                        # A lease can expire during a forward. Keep its permit until
                        # completion and hold at most one newly assigned job.
                        if self.waiting_job is not None:
                            raise ValueError('worker dispatch capacity exceeded')
                        self.waiting_job = frame
                    else:
                        if self.shared_reads:
                            await queue_read_result()
                        self._start(frame)
                elif kind == 'RESULT_ACK':
                    if (self.pending is None or frame.get('job_id') != self.pending['job_id']
                            or frame.get('claim') != self.pending['claim']
                            or frame.get('status') not in ('accepted', 'duplicate', 'stale','apply_received')):
                        raise ValueError('result acknowledgment mismatch')
                    if frame['status']=='apply_received':
                        if self.job['kind']!='source_command':raise ValueError('unexpected apply receipt acknowledgment')
                        continue  # Durable local receipt only; retain until remote outcome.
                    self.acknowledged = (self.pending['job_id'], self.pending['claim'])
                    self.pending, self.job = None, None
                    if not self.shared_reads or (self.encoding is not None and self.encoding.done()):
                        self.encoding = None
                    if self.waiting_job is not None:
                        job, self.waiting_job = self.waiting_job, None
                        self._start(job)
                elif kind not in ('READY_ACK', 'HEARTBEAT_ACK'):
                    raise ValueError('unexpected channel frame')

        tasks = [asyncio.create_task(coro()) for coro in (writer, heartbeat, states, reader)]
        tasks.append(asyncio.create_task(self.stop.wait()))
        try:
            completed, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in completed:
                task.result()
        finally:
            if self.shared_reads:
                self._cancel_read()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def run(self):
        from contextlib import asynccontextmanager
        import aiohttp

        @asynccontextmanager
        async def native_connect():
            async with aiohttp.ClientSession(trust_env=False) as client:
                async with client.ws_connect(self.config['channel'],
                        headers={'Authorization': 'Bearer ' + self.config['token']},
                        max_msg_size=4*1024*1024, heartbeat=30) as socket:
                    class Adapter:
                        async def send(self, raw):
                            await socket.send_str(raw)

                        async def receive(self):
                            message = await socket.receive()
                            if message.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                                return message.data
                            if message.type == aiohttp.WSMsgType.CLOSE and message.data in (4400, 4403, 4409):
                                raise BgeChannelStopped('channel authority or contract ended')
                            raise ConnectionError('channel disconnected')
                    yield Adapter()

        deadline = asyncio.get_running_loop().time() + self.maximum_seconds
        attempt = 0
        try:
            async with asyncio.timeout_at(deadline):
                while not self.stop.is_set():
                    self._failure(self.loading)
                    self._failure(self.encoding)
                    try:
                        async with (self.connector() if self.connector else native_connect()) as socket:
                            await self._session(socket)
                        if self.stop.is_set():
                            break
                    except BgeChannelStopped:
                        raise
                    except (ConnectionError, OSError, TimeoutError, aiohttp.ClientError):
                        attempt += 1
                        if attempt >= 10:
                            raise BgeChannelStopped('bounded channel reconnect exhausted') from None
                        # Reconnect the same process/model/nonce; never invoke push.
                        delay = random.uniform(.25, min(5.0, .5 * 2 ** attempt))
                        try:
                            await asyncio.wait_for(self.stop.wait(), delay)
                        except TimeoutError:
                            pass
        finally:
            # Thread cancellation is not forward completion. Drain any started
            # load/encode before relinquishing this worker's inference ownership.
            cancelled = False
            async def drain(task):
                nonlocal cancelled
                if task is None:
                    return
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        cancelled = True
                    except Exception:
                        break
                if not task.cancelled():
                    task.exception()
            for task in (self.loading, self.read_task, self.encoding):
                await drain(task)
            # A read's cancellation/cleanup can create its final tracked native
            # Task after shutdown began. Enumerate only after consumer cleanup.
            while self.forwards:
                for task in tuple(self.forwards):
                    await drain(task)
                    self.forwards.discard(task)
            # The pool/native owner belongs to this loop. Repeated cancellation
            # must not strand an asynchronous close after asyncio.run tears down.
            if self.close_runtime is not None:
                closing = asyncio.create_task(self.close_runtime())
                await drain(closing)
                closing.result()
            if cancelled:
                raise asyncio.CancelledError


def run_wss_worker(config, load_encoder, diagnostics, *, query_handler=None, prepare_runtime=None, close_runtime=None, recovery_handler=None, snapshot_channel=None):
    asyncio.run(BgeWssTransport(config, load_encoder, diagnostics, query_handler=query_handler,
                               prepare_runtime=prepare_runtime, close_runtime=close_runtime,
                               recovery_handler=recovery_handler,snapshot_channel=snapshot_channel).run())
