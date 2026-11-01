"""Outbound durable BGE transport, embedded into the private worker by its controller.

No model, credentials, scheduler or public query authority is created here. One
encoder load and forward survive reconnect; only an unacknowledged exact result
is resent. This is not accepted/Saved or exact provider termination evidence.
"""
import asyncio
import hashlib
import inspect
import json
import math
import random
import secrets
import time
from urllib.parse import urlsplit


class BgeChannelStopped(RuntimeError):
    pass


class BgePendingResultResync(ConnectionError):
    """Reconnect the same runtime to reconcile its retained durable result."""


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
                 heartbeat_seconds=20, maximum_seconds=11 * 3600, stop=None, reconnect_grace_seconds=600,
                 query_handler=None, prepare_runtime=None, close_runtime=None, recovery_handler=None, snapshot_channel=None):
        url = urlsplit(config['channel'])
        if (url.scheme != 'wss' or not url.hostname or url.port not in (None, 443)
                or url.username or url.password or url.query or url.fragment):
            raise ValueError('authenticated WSS/443 channel required')
        if not math.isfinite(heartbeat_seconds) or not 0 < heartbeat_seconds <= 20:
            raise ValueError('bounded heartbeat interval required')
        if not math.isfinite(maximum_seconds) or not 0 < maximum_seconds <= 11 * 3600:
            raise ValueError('bounded worker lifetime required')
        # Control-loss grace (handoff 5.4: 10 min). A Dev API/host restart must
        # not end a warm runtime; writes stay fenced while the lease is invalid.
        if not math.isfinite(reconnect_grace_seconds) or not 0 < reconnect_grace_seconds <= 3600:
            raise ValueError('bounded reconnect grace required')
        self.reconnect_grace_seconds = reconnect_grace_seconds
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
        self._retry_clock = time.monotonic
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
        self.bootstrap_only=bool(config.get('target_runtime',{}).get('bootstrap_only'))
        self.bootstrap_finalized=False
        self.bootstrap_pins_finalized=False
        self.source_handler = None
        self.source_history_handler=None
        self.original_intake_handler=None
        self.telegram_binding_task=None
        self._connection_generation=None
        if recovery_handler is not None and (not shared or not (inspect.iscoroutinefunction(recovery_handler)
                or inspect.iscoroutinefunction(getattr(recovery_handler,'__call__',None)))):
            raise ValueError('shared async recovery handler required')
        self.recovery_handler=recovery_handler
        self.recovery_attachment=None
        if recovery_handler is not None and (hasattr(recovery_handler,'attached') or hasattr(recovery_handler,'detached')):
            if not all(callable(getattr(recovery_handler,key,None)) for key in ('attached','detached')):
                raise ValueError('paired recovery attachment callbacks required')
            self.recovery_attachment=recovery_handler
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
        self.forwards = {}
        self.resource = None
        self.reads = {}
        self.released = {}

    @staticmethod
    def _identifier(value):
        return isinstance(value, str) and 0 < len(value.encode('utf-8')) <= 256

    def _bootstrap_event(self, stage, **fields):
        print(json.dumps(dict(event='worker_bootstrap',run_id=self.config['run_id'],
            stage=stage,timestamp=time.time(),**fields)),flush=True)

    @staticmethod
    def _read_identity(frame):
        return tuple(frame.get(key) for key in ('request_id', 'connection_generation', 'turn_generation'))

    def _resource_state(self, probe_nonce=None):
        frame = {'type': 'RESOURCE_STATE', 'state': 'busy' if self.resource or any(not r['released'] for r in self.reads.values()) else 'idle'}
        resource=self.resource or next((r['resource'] for r in self.reads.values() if not r['released']),None)
        if resource:frame.update(resource)
        if probe_nonce is not None:
            frame['probe_nonce'] = probe_nonce
        return frame

    def _claim_resource(self, frame, kind):
        if kind=='read':
            self.reads={k:r for k,r in self.reads.items()
                if not r['released'] or r['pending'] is not None and not r['discard']}
            if sum(not r['released'] for r in self.reads.values())>=8:raise ValueError('worker read capacity busy')
        elif self.resource is not None:raise ValueError('worker document resource busy')
        resource = {'resource_kind': kind,
            'resource_id': frame['request_id'] if kind == 'read' else frame['id'],
            'compute_claim': frame['compute_claim'],
            'dispatch_generation': frame['connection_generation']}
        if kind=='document':self.resource=resource
        self.changed.set()
        return resource

    def _released(self, resource):
        self.released[resource['compute_claim']]=resource
        while len(self.released)>9:self.released.pop(next(iter(self.released)))
        self.changed.set()

    def _maybe_release(self):
        for read in self.reads.values():
            if (not read['released'] and read['task'].done()
                    and (read['pending'] is None or read['discard'])
                    and not any(owner is read['resource'] for owner in self.forwards.values())):
                read['released']=True
                self._released(read['resource'])

    async def _forward(self, text, resource=None):
        if resource is not None and not any(r['resource'] is resource and not r['released'] for r in self.reads.values()):
            raise ValueError('encode consumer no longer owns resource')
        async def actual():
            async with self.forward_lock:
                return await asyncio.to_thread(self.encoder, text)
        # The Task, not its consumer, owns the lock until the real sync forward
        # returns. No consumer/session cancellation is passed to this Task.
        task = asyncio.create_task(actual())
        self.forwards[task]=resource
        def finished(done):
            self.forwards.pop(done,None)
            if not done.cancelled():
                done.exception()
            self._maybe_release()
            self.changed.set()
        task.add_done_callback(finished)
        return await asyncio.shield(task)

    def _cancel_read(self, identity=None):
        for key,read in self.reads.items():
            if identity is not None and key!=identity:continue
            read['discard']=True;read['pending']=None
            if not read['task'].done() and not read['cancelled']:
                read['cancelled']=True;read['task'].cancel()

    @staticmethod
    def _unavailable(frame, reason):
        if frame.get('type')=='SQL_HEALTH':
            return {'type':'SQL_HEALTH_RESULT','protocol_version':2,
                **{key:frame[key] for key in ('request_id','connection_generation','turn_generation')},
                'status':'unavailable','evidence':[],'observed_data_seq':None,
                'observed_projection_seq':None,'reason':reason}
        exact=frame.get('type') in ('PASSAGE_READ','STORY_READ','FACT_READ','SOURCE_READ','CASE_READ','GEO_READ','EDITORIAL_READ','SOURCE_GRAPH_READ','ACCEPTANCE_READ')
        return {'type': 'ACCEPTANCE_READ_RESULT' if frame.get('type')=='ACCEPTANCE_READ' else 'SOURCE_GRAPH_READ_RESULT' if frame.get('type')=='SOURCE_GRAPH_READ' else 'EDITORIAL_READ_RESULT' if frame.get('type')=='EDITORIAL_READ' else 'CASE_READ_RESULT' if frame.get('type')=='CASE_READ' else 'GEO_READ_RESULT' if frame.get('type')=='GEO_READ' else 'SOURCE_READ_RESULT' if frame.get('type')=='SOURCE_READ' else 'FACT_READ_RESULT' if frame.get('type')=='FACT_READ' else 'STORY_READ_RESULT' if frame.get('type')=='STORY_READ' else 'PASSAGE_READ_RESULT' if exact else 'QUERY_RESULT', 'protocol_version': 2,
            **{key: frame[key] for key in ('request_id', 'connection_generation', 'turn_generation')},
            'status': 'unavailable', **({} if exact and frame.get('type')!='ACCEPTANCE_READ' else {'mode':None}), 'evidence': [],
            'observed_data_seq': None, 'observed_projection_seq': None, 'reason': reason}

    def _validate_query(self, frame):
        exact=frame.get('type') in ('SQL_HEALTH','PASSAGE_READ','STORY_READ','FACT_READ','SOURCE_READ','CASE_READ','GEO_READ','EDITORIAL_READ','SOURCE_GRAPH_READ','ACCEPTANCE_READ')
        if exact:
            from regional_knowledge.kaggle_protocol import decode_envelope,PassageReadRequest,StoryReadRequest,SqlHealthRequest
            from regional_knowledge.kaggle_fact_contracts import FactReadRequest
            from regional_knowledge.kaggle_source_read import SourceReadRequest
            from regional_knowledge.kaggle_case_contracts import CaseReadRequest
            from regional_knowledge.kaggle_editorial_contracts import EditorialReadRequest
            from regional_knowledge.kaggle_source_graph_read import SourceGraphReadRequest
            from regional_knowledge.kaggle_acceptance_contracts import AcceptanceReadRequest
            from regional_knowledge.kaggle_geo_contracts import GeoReadRequest
            if not isinstance(decode_envelope(json.dumps(frame,allow_nan=False).encode()),SqlHealthRequest if frame.get('type')=='SQL_HEALTH' else AcceptanceReadRequest if frame.get('type')=='ACCEPTANCE_READ' else SourceGraphReadRequest if frame.get('type')=='SOURCE_GRAPH_READ' else EditorialReadRequest if frame.get('type')=='EDITORIAL_READ' else CaseReadRequest if frame.get('type')=='CASE_READ' else GeoReadRequest if frame.get('type')=='GEO_READ' else SourceReadRequest if frame.get('type')=='SOURCE_READ' else FactReadRequest if frame.get('type')=='FACT_READ' else StoryReadRequest if frame.get('type')=='STORY_READ' else PassageReadRequest):
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
        # Diagnostics travel beside the strict result, as on the controller.
        envelope={key:value for key,value in result.items() if key!='read_events'}
        if frame.get('type')=='SQL_HEALTH':
            from regional_knowledge.kaggle_protocol import decode_envelope,SqlHealthResult
            typed=decode_envelope(json.dumps(envelope,allow_nan=False).encode())
            if type(typed) is not SqlHealthResult or self._read_identity(result)!=self._read_identity(frame):
                raise ValueError('SQL health result identity/type mismatch')
            return result
        if frame.get('type') in ('PASSAGE_READ','STORY_READ','FACT_READ','SOURCE_READ','CASE_READ','GEO_READ','EDITORIAL_READ','SOURCE_GRAPH_READ','ACCEPTANCE_READ'):
            from regional_knowledge.kaggle_protocol import decode_envelope,PassageReadResult,StoryReadResult
            from regional_knowledge.kaggle_fact_contracts import FactReadResult
            from regional_knowledge.kaggle_source_read import SourceReadResult
            from regional_knowledge.kaggle_case_contracts import CaseReadResult
            from regional_knowledge.kaggle_editorial_contracts import EditorialReadResult
            from regional_knowledge.kaggle_source_graph_read import SourceGraphReadResult
            from regional_knowledge.kaggle_acceptance_contracts import AcceptanceReadResult
            from regional_knowledge.kaggle_geo_contracts import GeoReadResult
            typed=decode_envelope(json.dumps(envelope,allow_nan=False).encode())
            if type(typed) is not (AcceptanceReadResult if frame.get('type')=='ACCEPTANCE_READ' else SourceGraphReadResult if frame.get('type')=='SOURCE_GRAPH_READ' else EditorialReadResult if frame.get('type')=='EDITORIAL_READ' else CaseReadResult if frame.get('type')=='CASE_READ' else GeoReadResult if frame.get('type')=='GEO_READ' else SourceReadResult if frame.get('type')=='SOURCE_READ' else FactReadResult if frame.get('type')=='FACT_READ' else StoryReadResult if frame.get('type')=='STORY_READ' else PassageReadResult) or self._read_identity(result)!=self._read_identity(frame):
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
        read=dict(frame=frame,resource=resource,pending=None,cancelled=False,discard=False,released=False)
        self.reads[self._read_identity(frame)]=read
        async def consume():
            async def encode(text):
                return await self._forward(text, resource)
            deadline = math.inf if frame.get('type')=='QUERY' else asyncio.get_running_loop().time() + frame['deadline_ms'] / 1000
            try:
                async with asyncio.timeout_at(None if math.isinf(deadline) else deadline):
                    if frame.get('type')=='SQL_HEALTH':
                        def lease_current():
                            return (not read['released'] and not read['discard']
                                and self._connection_generation==frame['connection_generation'])
                        result=await self.query_handler(frame,encode=encode,lease_current=lease_current)
                    else:
                        result = await self.query_handler(frame, encode=encode)
                if asyncio.get_running_loop().time() >= deadline:
                    result = self._unavailable(frame, 'deadline_exceeded')
                result = self._validate_query_result(frame, result)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                reason = 'deadline_exceeded' if asyncio.get_running_loop().time() >= deadline else 'internal_error'
                print(json.dumps({'event':'native_read_failed','run_id':self.config['run_id'],
                    'request_ref':hashlib.sha256(frame['request_id'].encode()).hexdigest(),
                    'read_type':frame['type'],'error_class':type(error).__name__,'reason':reason}),flush=True)
                result = self._unavailable(frame, reason)
            # Read completion only publishes the bounded protocol result.
            if not read['discard']:
                read['pending'] = result
                self.changed.set()
        read['task'] = asyncio.create_task(consume())
        read['task'].add_done_callback(lambda _: (self._maybe_release(), self.changed.set()))

    async def _load(self):
        self.encoder = await asyncio.to_thread(self.load_encoder)
        self.forward_lock=getattr(self.encoder,'forward_lock',self.forward_lock)
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
            self.source_history_handler=prepared.source_history_handler
            self.original_intake_handler=prepared.original_intake_handler
            if self.source_history_handler is not None:
                self.source_history_handler.attached(self._connection_generation)
            if self.original_intake_handler is not None:
                self.original_intake_handler.attached(self._connection_generation)
        self.changed.set()

    def _native_handler(self,job):
        if job['kind']!='recovery_phase':return self.source_handler
        if job.get('phase')=='source_history':return self.source_history_handler
        if job.get('phase') in {'original_acquire','original_archive','original_verify','original_pages'}:
            return self.original_intake_handler
        return self.recovery_handler

    async def _encode(self, job):
        """Execute one durable job under the existing reconnect and ACK lifecycle."""
        try:
            if job['kind'] in ('source_command', 'recovery_phase'):
                handler = self._native_handler(job)
                if handler is None:
                    raise ValueError('fixed native executor unavailable')
                if job['kind'] == 'recovery_phase' and self.snapshot_channel is not None:
                    with self.snapshot_channel.assignment(job):
                        receipt = await handler(job)
                else:
                    receipt = await handler(job)
                pending = dict(job_id=job['id'], claim=job['claim'], receipt=receipt,
                    **{key:job[key] for key in ('run_id', 'launch_nonce', 'runtime_incarnation',
                        'compute_claim', 'dispatch_generation')})
                if len(json.dumps(pending, allow_nan=False).encode()) > (
                        130000 if job['kind'] == 'recovery_phase' else 15500):
                    raise ValueError('source command receipt bound')
            else:
                before = time.monotonic()
                vector = await self._forward(job['texts'][0])
                pending = {'job_id':job['id'], 'claim':job['claim'], 'space':SPACE,
                    'vectors':[vector], 'timings':{'encoder_seconds':time.monotonic()-before}}
                if len(json.dumps(pending, allow_nan=False).encode()) > 262000:
                    raise ValueError('channel result bound')
            # Validate completely before publishing anything to states()/writer.
            self.pending = pending
            self.changed.set()
        except (asyncio.CancelledError, PermissionError, BgeChannelStopped):
            raise  # Cancellation, authority loss and actual STOP are not UNKNOWN.
        except Exception as error:
            from regional_knowledge.recovery_journal import StaleWriter
            if isinstance(error, StaleWriter):
                raise
            if job['kind'] == 'recovery_phase':
                self.pending = dict(job_id=job['id'], claim=job['claim'],
                    receipt=dict(phase=job['phase'], request_sha256=job['request_sha256'],
                        value={'error':'native_phase_failed','retryable':True}),
                    **{key:job[key] for key in ('run_id', 'launch_nonce', 'runtime_incarnation',
                        'compute_claim', 'dispatch_generation')})
                self.changed.set()
                return
            if job['kind'] in ('source_command', 'document'):
                print(json.dumps({'event':'native_job_retry', 'run_id':self.config['run_id'],
                    'job_id':job['id'], 'kind':job['kind'], 'error_class':type(error).__name__}), flush=True)
                self.waiting_job, self.job = job, None
                raise ConnectionError('durable job requires outcome reconciliation') from error
            # Unexpected effects are not proof of failure or permission to
            # reexecute. This one bounded safe event excludes payload and text.
            print(json.dumps({'event':'native_job_outcome_unresolved',
                'job_id':job['id'], 'kind':job['kind'], 'phase':job.get('phase'),
                'error_class':type(error).__name__},
                separators=(',', ':')), flush=True)

    def _start(self, job):
        if self.shared_reads and job is not self.waiting_job:
            self._claim_resource(job, 'document')
        self.job = job
        if self.shared_reads and job is not self.waiting_job:
            # Durable job/claim/result remain singular. Actual model forwards
            # serialize in forward_lock, independently of control/PG latency.
            self._released(self.resource);self.resource=None
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
            # Match the existing API bound for metadata-only native catalogs.
            limit=1536*1024 if frame.get('type')=='BOOTSTRAP_OBSERVATION' else 262144
            if len(raw.encode()) > limit:
                raise ValueError('channel send bound')
            await socket.send(raw)

        environment = {}
        if self.config.get('target_runtime') is not None:
            self._bootstrap_event('environment_observation_start')
            from regional_knowledge.kaggle_environment import observe_environment
            environment['environment'] = observe_environment()
            self._bootstrap_event('environment_observation_complete')
        await send({'type': 'ATTACH', 'run_id': self.config['run_id'],
                    'launch_nonce': self.config['launch_nonce'], 'runtime_incarnation': self.incarnation,
                    **environment})
        self._bootstrap_event('attach_sent')
        welcome = await asyncio.wait_for(receive(), 10)
        generation = welcome.get('connection_generation')
        if welcome.get('type') != 'WELCOME' or type(generation) is not int or generation < 1:
            raise ValueError('server attachment generation required')
        self._connection_generation=generation
        self._bootstrap_event('welcome_received',connection_generation=generation)
        connected_at=self._retry_clock()
        if self.recovery_attachment is not None:
            self.recovery_attachment.attached(generation,runtime_incarnation=self.incarnation)
        if self.source_history_handler is not None:self.source_history_handler.attached(generation)
        if self.original_intake_handler is not None:self.original_intake_handler.attached(generation)
        attachment_releases=()
        attachment_state = None
        if self.shared_reads:
            self._maybe_release()
            attachment_releases=tuple(self.released.values())
            attachment_state = self._resource_state()
            for attachment_release in attachment_releases:
                await send({'type': 'RESOURCE_RELEASED', 'connection_generation': generation, **attachment_release})
            await send({'connection_generation': generation, **attachment_state})
        if self.loading is None:
            # Minimal authenticated control is established before Torch/model imports.
            self.loading = asyncio.create_task(self._load())
            self.loading.add_done_callback(lambda _: self.changed.set())
        outbound = asyncio.PriorityQueue(maxsize=4)
        order = 0
        read_sent = set()

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
            for read in tuple(self.reads.values()):
                result=read['pending'];key=read['resource']['compute_claim']
                if result is not None and key not in read_sent:
                    read_sent.add(key)
                    await put(result,priority=0)
                    if read['pending'] is result:read['pending']=None

        ready_confirmed=False
        telegram_request_pending=False
        async def request_telegram():
            nonlocal telegram_request_pending
            owner=getattr(self.original_intake_handler,'telegram_channel',None)
            if (ready_confirmed and owner is not None and not owner.current()
                    and owner.failed_generation!=generation and not telegram_request_pending
                    and (self.telegram_binding_task is None or self.telegram_binding_task.done())):
                telegram_request_pending=True
                await put({'type':'RKB1_BIND_REQUEST','retained_client':owner.retained_client})

        async def heartbeat():
            while True:
                await asyncio.sleep(self.heartbeat_seconds)
                await put({'type': 'HEARTBEAT', 'diagnostics': await asyncio.to_thread(self.diagnostics)})
                await request_telegram()

        async def states():
            ready_sent = False
            result_sent = None
            intent_sent = None
            released_sent = {r['compute_claim'] for r in attachment_releases}
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
                    for key,release in tuple(self.released.items()):
                        if key not in released_sent:
                            await put({'type':'RESOURCE_RELEASED',**release});released_sent.add(key)
                    released_sent.intersection_update(self.released)
                    read_sent.intersection_update(r['resource']['compute_claim'] for r in self.reads.values())
                    state = self._resource_state()
                    if state != state_sent:
                        await put(state)
                        state_sent = state
                if (self.encoder is not None and not ready_sent
                        and (self.prepare_runtime is None or self.prepared_runtime is not None)):
                    frame = {'type': 'READY', 'space': SPACE, 'model_revision': REVISION}
                    if self.prepared_runtime is not None:
                        frame['runtime_readiness'] = self.prepared_runtime.readiness.to_wire()
                        if self.prepared_runtime.bootstrap_observation is not None:
                            frame={'type':'BOOTSTRAP_OBSERVATION','observation':self.prepared_runtime.bootstrap_observation}
                    await put(frame)
                    ready_sent = True
                if self.pending is not None and result_sent != (self.pending['job_id'], self.pending['claim']):
                    result_type={'source_command':'SOURCE_COMMAND_RESULT','recovery_phase':'RECOVERY_PHASE_RESULT'}.get(self.job['kind'],'RESULT')
                    await put({'type': result_type, **self.pending}, priority=1)
                    result_sent = (self.pending['job_id'], self.pending['claim'])

        async def reader():
            nonlocal ready_confirmed, telegram_request_pending
            while True:
                frame = await receive()
                if type(frame.get('connection_generation')) is not int:
                    raise ValueError('integer channel generation required')
                if frame['connection_generation'] != generation:
                    raise ConnectionError('stale channel generation')
                kind = frame.get('type')
                if self.shared_reads and kind == 'RESOURCE_PROBE':
                    if not self._identifier(frame.get('probe_nonce')):
                        raise ValueError('bounded resource probe nonce required')
                    self._maybe_release()
                    for release in tuple(self.released.values()):
                        await put({'type':'RESOURCE_RELEASED',**release})
                    await put(self._resource_state(frame['probe_nonce']))
                elif self.shared_reads and kind == 'CANCEL_QUERY':
                    if (not self._identifier(frame.get('request_id'))
                            or type(frame.get('turn_generation')) is not int or frame['turn_generation'] < 0):
                        raise ValueError('cancel QUERY identity contract')
                    self._cancel_read(self._read_identity(frame))
                elif self.shared_reads and kind in ('QUERY','SQL_HEALTH','PASSAGE_READ','STORY_READ','FACT_READ','SOURCE_READ','CASE_READ','GEO_READ','EDITORIAL_READ','SOURCE_GRAPH_READ','ACCEPTANCE_READ'):
                    self._validate_query(frame)
                    previous=self.reads.get(self._read_identity(frame))
                    if previous is not None:
                        if json.dumps(frame,sort_keys=True,allow_nan=False)!=json.dumps(previous['frame'],sort_keys=True,allow_nan=False):
                            raise ValueError('replayed QUERY payload mismatch')
                        continue
                    if any(r['frame']['compute_claim']==frame['compute_claim'] for r in self.reads.values()):
                        raise ValueError('ephemeral QUERY cannot replay across sessions')
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
                        native_handler=self._native_handler(frame)
                        if (not self.shared_reads or native_handler is None
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
                            if self.pending is not None:
                                # The peer may have expired the claim before its
                                # ACK arrived. Keep the exact job/result and use
                                # the existing bounded reconnect to replay it;
                                # never execute or acknowledge different work.
                                raise BgePendingResultResync('durable result still awaiting acknowledgment')
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
                            or frame.get('status') not in ('accepted', 'duplicate', 'stale','apply_received','checkpoint_pending')):
                        raise ValueError('result acknowledgment mismatch')
                    if frame['status']=='apply_received':
                        if self.job['kind']!='source_command':raise ValueError('unexpected apply receipt acknowledgment')
                        continue  # Durable local receipt only; retain until remote outcome.
                    if frame['status']=='checkpoint_pending' and self.job['kind']!='source_command':
                        raise ValueError('unexpected pending checkpoint acknowledgment')
                    self.acknowledged = (self.pending['job_id'], self.pending['claim'])
                    self.pending, self.job = None, None
                    if not self.shared_reads or (self.encoding is not None and self.encoding.done()):
                        self.encoding = None
                    if self.waiting_job is not None:
                        job, self.waiting_job = self.waiting_job, None
                        self._start(job)
                elif kind=='BOOTSTRAP_ACK':
                    if frame.get('status')=='pins_finalized':
                        self._accept_bootstrap_pins(frame,generation,welcome.get('provider_version'))
                    elif frame.get('status')!='observed_not_ready':
                        raise ValueError('bootstrap acknowledgment status')
                elif kind=='RKB1_BIND_PENDING':
                    telegram_request_pending=False
                    owner=getattr(self.original_intake_handler,'telegram_channel',None)
                    if owner is not None:owner.authorized_generation=None
                elif kind=='RKB1_BIND_GRANT':
                    owner=getattr(self.original_intake_handler,'telegram_channel',None)
                    if not ready_confirmed or owner is None or not telegram_request_pending:
                        raise ValueError('unexpected Telegram binding')
                    if type(frame.get('challenge')) is not str or not 32<=len(frame['challenge'])<=128:
                        raise ValueError('invalid Telegram binding challenge')
                    telegram_request_pending=False
                    if self.telegram_binding_task is not None and not self.telegram_binding_task.done():
                        raise ValueError('duplicate Telegram binding')
                    async def bind_and_report(owner,grant):
                        outcome='unavailable'
                        try:
                            await owner.receive(grant)
                            if owner.current():outcome='authorized'
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            pass  # Only the fixed status travels; provider errors can contain secrets.
                        finally:
                            for key in ('bundle_b64','api_id','api_hash'):grant.pop(key,None)
                        await put({'type':'RKB1_BIND_RESULT','status':outcome,'challenge':grant['challenge']})
                    self.telegram_binding_task=asyncio.create_task(bind_and_report(owner,frame))
                    def observed_binding(task):
                        if not task.cancelled():task.exception()
                    self.telegram_binding_task.add_done_callback(observed_binding)
                elif kind=='READY_ACK':
                    if frame.get('status')=='ready':
                        ready_confirmed=True
                        if self.shared_reads and self.waiting_job is not None:
                            self._start(self.waiting_job)
                            self.waiting_job = None
                        await request_telegram()
                    if self.bootstrap_only and frame.get('status')=='ready':
                        self.bootstrap_finalized=True
                        self._lifetime_timeout.reschedule(self._overall_deadline)
                elif kind=='HEARTBEAT_ACK':
                    # Bound consecutive outage attempts, not the sum of every
                    # disconnect over an eleven-hour warmed runtime. A stable
                    # authenticated control session earns a fresh retry budget.
                    if self._retry_clock()-connected_at>=30:
                        self._reconnect_attempt=0;self._outage_started=None
                elif kind=='STOP':
                    if (set(frame)!={'type','protocol_version','connection_generation',
                            'run_id','launch_nonce','runtime_incarnation'}
                            or tuple(frame[k] for k in ('run_id','launch_nonce','runtime_incarnation'))
                                !=(self.config['run_id'],self.config['launch_nonce'],self.incarnation)):
                        raise ValueError('exact authenticated worker STOP required')
                    # An unacknowledged result, native effect, forwarded encode
                    # or queued assignment must not be interrupted by a STOP.
                    if (self.job is None and self.pending is None and self._resource_state()['state']=='idle'
                            and self.waiting_job is None
                            and not any(not task.done() for task in self.forwards)):
                        self.stop.set()
                else:
                    raise ValueError('unexpected channel frame')

        tasks = [asyncio.create_task(coro()) for coro in (writer, heartbeat, states, reader)]
        tasks.append(asyncio.create_task(self.stop.wait()))
        try:
            completed, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in completed:
                task.result()
        finally:
            if self._connection_generation==generation:self._connection_generation=None
            if self.telegram_binding_task is not None and not self.telegram_binding_task.done():
                self.telegram_binding_task.cancel()
                await asyncio.gather(self.telegram_binding_task,return_exceptions=True)
            if self.recovery_attachment is not None:self.recovery_attachment.detached(generation)
            if self.source_history_handler is not None:self.source_history_handler.detached(generation)
            if self.original_intake_handler is not None:self.original_intake_handler.detached(generation)
            if self.shared_reads:
                self._cancel_read()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def _accept_bootstrap_pins(self,frame,generation,provider_version):
        import hashlib
        prepared=self.prepared_runtime
        fields={'type','protocol_version','connection_generation','status','run_id','runtime_incarnation',
            'provider_version','observation_sha256','hashes'}
        if (set(frame)!=fields or not self.bootstrap_only or prepared is None or prepared.bootstrap_observation is None
                or frame['connection_generation']!=generation or frame['run_id']!=self.config['run_id']
                or frame['runtime_incarnation']!=self.incarnation or frame['hashes']!=prepared.readiness.to_wire()['hashes']
                or type(frame['provider_version']) is not str or not frame['provider_version'].isdigit()
                or provider_version is not None and frame['provider_version']!=str(provider_version)):
            raise ValueError('exact prepared bootstrap acknowledgment required')
        raw=json.dumps(prepared.bootstrap_observation,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
        if frame['observation_sha256']!=hashlib.sha256(raw).hexdigest():
            raise ValueError('prepared bootstrap observation differs')
        self.bootstrap_pins_finalized=True
        self._lifetime_timeout.reschedule(self._overall_deadline)

    async def run(self):
        from contextlib import asynccontextmanager
        self._bootstrap_event('aiohttp_import_start')
        import aiohttp
        self._bootstrap_event('aiohttp_import_complete')

        @asynccontextmanager
        async def native_connect():
            trace=aiohttp.TraceConfig()
            async def dns_start(*args):self._bootstrap_event('dns_start')
            async def dns_complete(*args):self._bootstrap_event('dns_complete')
            trace.on_dns_resolvehost_start.append(dns_start)
            trace.on_dns_resolvehost_end.append(dns_complete)
            async with aiohttp.ClientSession(trust_env=False,trace_configs=[trace],
                    timeout=aiohttp.ClientTimeout(total=15,connect=10,sock_connect=10)) as client:
                async with client.ws_connect(self.config['channel'],
                        headers={'Authorization': 'Bearer ' + self.config['token']},
                        max_msg_size=4*1024*1024, heartbeat=30) as socket:
                    bootstrap_event=self._bootstrap_event
                    class Adapter:
                        async def send(self, raw):
                            await socket.send_str(raw)

                        async def receive(self):
                            message = await socket.receive()
                            if message.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                                return message.data
                            code=(message.data if message.type==aiohttp.WSMsgType.CLOSE
                                  else getattr(socket,'close_code',None))
                            code=code if type(code) is int and 1000<=code<=4999 else None
                            bootstrap_event('wss_session_closed',close_code=code)
                            # The authenticated server uses 4403 when this run's
                            # authority is revoked. Retrying cannot restore it.
                            if code==4403:
                                raise BgeChannelStopped('server revoked worker authority')
                            # Other closes end this attachment; reconnect the
                            # same owned runtime with its exact pending result.
                            raise ConnectionError('channel disconnected')
                    yield Adapter()

        deadline = asyncio.get_running_loop().time() + self.maximum_seconds
        self._overall_deadline=deadline
        bootstrap_deadline=(asyncio.get_running_loop().time()+self.config['target_runtime'].get('bootstrap_seconds',900)
            if self.bootstrap_only else deadline)
        self._reconnect_attempt = 0;self._outage_started = None
        try:
            async with asyncio.timeout_at(min(deadline,bootstrap_deadline)) as lifetime:
                self._lifetime_timeout=lifetime
                while not self.stop.is_set():
                    self._failure(self.loading)
                    connected=False
                    try:
                        self._failure(self.encoding)
                        self._bootstrap_event('wss_connect_start',attempt=self._reconnect_attempt+1)
                        async with (self.connector() if self.connector else native_connect()) as socket:
                            connected=True
                            self._bootstrap_event('wss_connect_complete')
                            try:
                                await self._session(socket)
                            except (ValueError,KeyError,TypeError,UnicodeError):
                                raise ConnectionError('channel protocol validation failed') from None
                        if self.stop.is_set():
                            break
                    except BgeChannelStopped:
                        raise
                    except (ConnectionError, OSError, TimeoutError, aiohttp.ClientError) as error:
                        if self.encoding is not None and self.encoding.done() and self.job is None:
                            self.encoding = None
                        self._bootstrap_event('wss_session_failed' if connected else 'wss_connect_failed',error_type=type(error).__name__,
                            http_status=getattr(error,'status',None))
                        self._reconnect_attempt += 1
                        now = self._retry_clock()
                        if self._outage_started is None:
                            self._outage_started = now
                        if now - self._outage_started >= self.reconnect_grace_seconds:
                            raise BgeChannelStopped('bounded channel reconnect exhausted') from None
                        # Reconnect the same process/model/nonce; never invoke push.
                        delay = random.uniform(.25, min(30.0, .5 * 2 ** min(self._reconnect_attempt, 6)))
                        try:
                            await asyncio.wait_for(self.stop.wait(), delay)
                        except TimeoutError:
                            pass
        except Exception as error:
            # Cleanup can wait for an already running native load/forward.
            # Expose the failure before that drain, without exception text,
            # frames, credentials or private result payloads.
            print(json.dumps({'event':'worker_transport_failed',
                'run_id':self.config['run_id'],
                'error_type':type(error).__name__}),flush=True)
            raise
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
                        cancelled = cancelled or asyncio.current_task().cancelling()>0
                    except Exception:
                        break
                if not task.cancelled():
                    task.exception()
            if (self.encoder is not None and self.prepared_runtime is None and self.loading is not None
                    and not asyncio.current_task().cancelling()):
                self.loading.cancel()
            for task in (self.loading, self.encoding, *(r['task'] for r in self.reads.values())):
                await drain(task)
            # A read's cancellation/cleanup can create its final tracked native
            # Task after shutdown began. Enumerate only after consumer cleanup.
            while self.forwards:
                for task in tuple(self.forwards):
                    await drain(task)
                    self.forwards.pop(task,None)
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
