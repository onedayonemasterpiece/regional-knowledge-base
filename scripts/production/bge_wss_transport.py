"""Outbound durable BGE transport, embedded into the private worker by its controller.

No model, credentials, scheduler or public query authority is created here. One
encoder load and forward survive reconnect; only an unacknowledged exact result
is resent. This is not accepted/Saved or exact provider termination evidence.
"""
import asyncio
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

    if not isinstance(raw, (str, bytes)) or len(raw.encode() if isinstance(raw, str) else raw) > 262144:
        raise ValueError('channel frame bound')
    frame = json.loads(raw, object_pairs_hook=unique,
                       parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite channel value')))
    if not isinstance(frame, dict) or type(frame.get('protocol_version')) is not int or frame['protocol_version'] != 2:
        raise ValueError('channel protocol')
    return frame


class BgeWssTransport:
    """One bounded in-flight encode and result, without a second job authority."""

    def __init__(self, config, load_encoder, diagnostics, *, connector=None,
                 heartbeat_seconds=20, maximum_seconds=11 * 3600, stop=None):
        url = urlsplit(config['channel'])
        if (url.scheme != 'wss' or not url.hostname or url.port not in (None, 443)
                or url.username or url.password or url.query or url.fragment):
            raise ValueError('authenticated WSS/443 channel required')
        if not math.isfinite(heartbeat_seconds) or not 0 < heartbeat_seconds <= 20:
            raise ValueError('bounded heartbeat interval required')
        if not math.isfinite(maximum_seconds) or not 0 < maximum_seconds <= 11 * 3600:
            raise ValueError('bounded worker lifetime required')
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

    async def _load(self):
        self.encoder = await asyncio.to_thread(self.load_encoder)
        self.changed.set()

    async def _encode(self, job):
        before = time.monotonic()
        vector = await asyncio.to_thread(self.encoder, job['texts'][0])
        self.pending = {'job_id': job['id'], 'claim': job['claim'], 'space': SPACE,
                        'vectors': [vector], 'timings': {'encoder_seconds': time.monotonic() - before}}
        # Reject non-finite/oversized model output before queueing any bytes.
        if len(json.dumps(self.pending, allow_nan=False).encode()) > 262000:
            raise ValueError('channel result bound')
        self.changed.set()

    def _start(self, job):
        self.job = job
        self.encoding = asyncio.create_task(self._encode(job))
        self.encoding.add_done_callback(lambda _: self.changed.set())

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

        await send({'type': 'ATTACH', 'run_id': self.config['run_id'],
                    'launch_nonce': self.config['launch_nonce'], 'runtime_incarnation': self.incarnation})
        welcome = await asyncio.wait_for(receive(), 10)
        generation = welcome.get('connection_generation')
        if welcome.get('type') != 'WELCOME' or type(generation) is not int or generation < 1:
            raise ValueError('server attachment generation required')
        if self.loading is None:
            # Minimal authenticated control is established before Torch/model imports.
            self.loading = asyncio.create_task(self._load())
            self.loading.add_done_callback(lambda _: self.changed.set())
        outbound = asyncio.PriorityQueue(maxsize=4)
        order = 0

        async def put(frame, priority=0):
            nonlocal order
            order += 1
            await outbound.put((priority, order, {'connection_generation': generation, **frame}))

        async def writer():
            while True:
                _, _, frame = await outbound.get()
                await send(frame)

        async def heartbeat():
            while True:
                await asyncio.sleep(self.heartbeat_seconds)
                await put({'type': 'HEARTBEAT', 'diagnostics': await asyncio.to_thread(self.diagnostics)})

        async def states():
            ready_sent = False
            result_sent = None
            self.changed.set()
            while True:
                await self.changed.wait()
                self.changed.clear()
                self._failure(self.loading)
                self._failure(self.encoding)
                if self.encoder is not None and not ready_sent:
                    await put({'type': 'READY', 'space': SPACE, 'model_revision': REVISION})
                    ready_sent = True
                if self.pending is not None and result_sent != (self.pending['job_id'], self.pending['claim']):
                    await put({'type': 'RESULT', **self.pending}, priority=1)
                    result_sent = (self.pending['job_id'], self.pending['claim'])

        async def reader():
            while True:
                frame = await receive()
                if frame.get('connection_generation') != generation:
                    raise BgeChannelStopped('stale channel generation')
                kind = frame.get('type')
                if kind == 'DISPATCH':
                    if (frame.get('space') != SPACE or frame.get('kind') not in ('query', 'document')
                            or not isinstance(frame.get('texts'), list) or len(frame['texts']) != 1
                            or not isinstance(frame['texts'][0], str)
                            or not 0 < len(frame['texts'][0]) <= 40000
                            or any(not isinstance(frame.get(key), str) or not 0 < len(frame[key]) <= 256
                                   for key in ('id', 'claim')) or self.encoder is None):
                        raise ValueError('durable BGE dispatch contract')
                    key = (frame['id'], frame['claim'])
                    if key == self.acknowledged:
                        continue  # A control ACK can overtake a buffered replay.
                    if self.job is not None and key == (self.job['id'], self.job['claim']):
                        if frame['texts'] != self.job['texts'] or frame['kind'] != self.job['kind']:
                            raise ValueError('replayed assignment payload mismatch')
                        continue  # Existing forward/result still owns this claim.
                    if self.waiting_job is not None and key == (self.waiting_job['id'], self.waiting_job['claim']):
                        if frame['texts'] != self.waiting_job['texts'] or frame['kind'] != self.waiting_job['kind']:
                            raise ValueError('replayed waiting assignment payload mismatch')
                        continue
                    if self.job is not None:
                        # A lease can expire during a forward. Keep its permit until
                        # completion and hold at most one newly assigned job.
                        if self.waiting_job is not None:
                            raise ValueError('worker dispatch capacity exceeded')
                        self.waiting_job = frame
                    else:
                        self._start(frame)
                elif kind == 'RESULT_ACK':
                    if (self.pending is None or frame.get('job_id') != self.pending['job_id']
                            or frame.get('claim') != self.pending['claim']
                            or frame.get('status') not in ('accepted', 'duplicate', 'stale')):
                        raise ValueError('result acknowledgment mismatch')
                    self.acknowledged = (self.pending['job_id'], self.pending['claim'])
                    self.pending, self.job, self.encoding = None, None, None
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
                        max_msg_size=262144, heartbeat=30) as socket:
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
            for task in (self.loading, self.encoding):
                if task is None:
                    continue
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError:
                        cancelled = True
                    except Exception:
                        break
                if not task.cancelled():
                    task.exception()
            if cancelled:
                raise asyncio.CancelledError


def run_wss_worker(config, load_encoder, diagnostics):
    asyncio.run(BgeWssTransport(config, load_encoder, diagnostics).run())
