"""Full local QUERY seam: native WS/PG, synthetic identity/tokenizer/encoder.

Opt-in disposable fixture only. Loopback cleartext is a test connector override;
this proves no TLS, provider, actual BGE residency or production deployment.
"""
import asyncio
from contextlib import asynccontextmanager
import json
import socket as sockets
import time

import aiohttp
from psycopg.pq import TransactionStatus
import pytest
from starlette.applications import Starlette
import uvicorn

from regional_knowledge.bge_channel import channel_route
from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.kaggle_bootstrap import CandidateReadiness, PreparedRuntime
from regional_knowledge.kaggle_readiness import RUNTIME_HASHES, SearchRuntimeExpectation
from regional_knowledge.kaggle_protocol import Query, QueryFilters, TrustedActorContext
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from regional_knowledge.query_telemetry import ReadRuntime, bind_read_runtime
from regional_knowledge.read_telemetry import ReadTelemetry
from test_bge_wss_worker import CONFIG, module
from test_kaggle_pg_driver import database
from test_kaggle_query import env, publish, PG, tokenizer, SOURCE, VECTOR
from test_participants import people
from test_read_authority import auth


@PG
@pytest.mark.asyncio
@pytest.mark.parametrize('prepared_runtime', [False, True])
async def test_actual_shared_worker_query_sql_roundtrip_and_separate_resource_release(env, auth, tmp_path, prepared_runtime):
    await publish(env)
    queue = BgeQueue(tmp_path / 'roundtrip-control.sqlite')
    seed_text = 'synthetic durable queue seed, never a QUERY'
    seed = queue.enqueue(auth.p.owner.subject, 'roundtrip-seed', [seed_text], kind='document')
    with queue.connect(write=False) as db:
        run = db.execute('SELECT id FROM runs').fetchone()[0]
    token = (queue.path.parent / 'bge-worker-credentials' / run).read_text()
    nonce = queue.prepare_channel(run)
    proof = None
    if prepared_runtime:
        # Synthetic artifact hashes exercise the real A/B wire composition, not
        # actual loaded-model identity. Frontier values come from the real PG.
        async with env['connection'].transaction():
            row = await (await env['connection'].execute('''
                SELECT s.database_generation,(SELECT max(version) FROM rkb_v4.schema_migrations),
                    s.applied_seq,f.projection_seq,f.visibility_epoch,f.text_ready,f.dense_ready
                FROM rkb_v4.recovery_state s CROSS JOIN rkb_v4.read_frontier f
                WHERE s.singleton AND f.singleton''')).fetchone()
        fields = dict(zip(('database_generation', 'schema_version',
            'applied_seq', 'projection_seq', 'visibility_epoch', 'text_search_ready', 'dense_search_ready'), row))
        hashes = {key: 'a' * 64 for key in RUNTIME_HASHES}
        proof = CandidateReadiness(hashes=hashes, **fields)
        queue.expect_search_runtime(run, SearchRuntimeExpectation(hashes, fields['database_generation'], fields['applied_seq']))
    queue.launch_claim(run)
    queue.launch_record(run, 'fixture-owner/stable-slug', provider_version=7)
    admission = queue.configure_shared_reads(auth.resolver)
    authority = await auth.resolver.resolve_oauth(auth.tokens.access_token, destination=auth.destination)
    # No document actor is registered: this durable seed creates the owned run
    # without authorizing document computation in the shared admission fixture.
    native_records = []
    read_telemetry = ReadTelemetry(sink=lambda raw: native_records.append(json.loads(raw)))
    app = Starlette(routes=[channel_route(queue, read_telemetry=read_telemetry)])
    listener = sockets.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(5)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level='error', lifespan='off', ws='websockets-sansio'))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    stop, channel_ready, release_withheld = asyncio.Event(), asyncio.Event(), asyncio.Event()
    frames, held_control, encoder_calls, loads = [], [], [], []
    wire_ref = {}
    delay_release = {'enabled': True, 'withheld': False}
    worker_task = None
    query_text = 'semantic harbour'

    @asynccontextmanager
    async def connect():
        async with aiohttp.ClientSession() as client:
            async with client.ws_connect(f'http://127.0.0.1:{port}/bge-worker/channel',
                    headers={'Authorization': 'Bearer ' + token}) as socket:
                wire_ref['socket'] = socket

                class Adapter:
                    async def send(self, raw):
                        frame = json.loads(raw)
                        frames.append(('worker', frame))
                        # Lose only release/idle control, without blocking the
                        # sender: the completed QUERY_RESULT can still arrive.
                        # Controller capacity must remain held until exact replay.
                        if delay_release['enabled'] and frame['type'] == 'RESOURCE_RELEASED':
                            delay_release['withheld'] = True
                            held_control.append(raw)
                            release_withheld.set()
                            return
                        if (delay_release['enabled'] and delay_release['withheld']
                                and frame['type'] == 'RESOURCE_STATE' and frame['state'] == 'idle'):
                            held_control.append(raw)
                            return
                        await socket.send_str(raw)

                    async def receive(self):
                        message = await socket.receive()
                        if message.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                            raise ConnectionError('synthetic loopback channel closed')
                        frame = json.loads(message.data)
                        frames.append(('controller', frame))
                        if frame['type'] == 'READY_ACK' and frame['status'] == 'ready':
                            channel_ready.set()
                        return message.data
                yield Adapter()

    def load():
        loads.append('resident-encoder-stub-loaded')

        def encode(text):
            encoder_calls.append(text)
            return VECTOR
        return encode

    async def until(predicate):
        async with asyncio.timeout(2):
            while not predicate():
                await asyncio.sleep(.005)

    try:
        await until(lambda: server.started)
        transport = module('bge_wss_transport')
        executor = KaggleWorkerQuery(acquire=env['acquire'], tokenizer=tokenizer(),
            telemetry=asyncio.Queue(maxsize=8))
        async def handler(frame, *, encode):
            with bind_read_runtime(ReadRuntime(run, worker.incarnation)):
                return await executor(frame, encode=encode)
        preparations = []
        async def prepare(encoder, **identity):
            assert loads == ['resident-encoder-stub-loaded']
            assert identity['run_id'] == run and identity['launch_nonce'] == nonce
            preparations.append(identity)
            return PreparedRuntime(handler, proof)
        worker = transport.BgeWssTransport(dict(CONFIG, run_id=run, token=token,
                launch_nonce=nonce, shared_reads=True), load, lambda: {}, connector=connect,
            **({'prepare_runtime': prepare} if prepared_runtime else {'query_handler': handler}),
            heartbeat_seconds=.02, maximum_seconds=8, stop=stop)
        worker_task = asyncio.create_task(worker.run())
        await asyncio.wait_for(channel_ready.wait(), 2)
        generation = next(frame['connection_generation'] for direction, frame in frames
                          if direction == 'controller' and frame['type'] == 'WELCOME')
        if prepared_runtime:
            assert len(preparations) == 1
            ready_frame = next(frame for direction, frame in frames
                if direction == 'worker' and frame['type'] == 'READY')
            assert ready_frame['runtime_readiness'] == proof.to_wire()
            assert queue.channel_status(run, token, generation)['runtime_verified']
        original = Query(request_id='actual-native-query', connection_generation=generation,
            turn_generation=9, query=query_text, filters=QueryFilters(source_ids=(SOURCE,), kinds=('source',)),
            min_data_seq=5, deadline_ms=2000,
            actor=TrustedActorContext(person_id=authority.person_id, audience=authority.destination.key,
                auth_epoch=authority.auth_revision, visibility_epoch=0, context_ref=authority.context_ref))
        ticket = await admission.begin_read(authority, original, dialog_ref='native-roundtrip',
                                             deadline=time.monotonic() + 1.9)
        result = await asyncio.wait_for(asyncio.shield(ticket.result), 2)
        assert result.status == 'ok' and result.mode == 'hybrid'
        assert result.reason is None
        assert result.observed_data_seq == result.observed_projection_seq == 5
        assert result.observed_database_generation == 'query-fixture'
        assert result.evidence and len(result.evidence) <= original.evidence_limit
        source = result.evidence[0].dependencies.source_revisions[0]
        assert (source.source_id, source.content_revision, source.eligibility_revision) == (SOURCE, 1, 1)
        assert result.evidence[0].text.startswith('semantic harbour')
        assert sum(len(item.text) for item in result.evidence) <= 24000
        assert env['connection'].info.transaction_status == TransactionStatus.IDLE
        await asyncio.wait_for(release_withheld.wait(), 1)
        assert admission.status()['state'] == 'busy', 'QUERY_RESULT must not release compute capacity'
        assert loads == ['resident-encoder-stub-loaded'] and encoder_calls == [query_text]
        with queue.connect(write=False) as db:
            rows = db.execute('SELECT id,kind,texts FROM jobs').fetchall()
        assert [(row['id'], row['kind'], json.loads(row['texts'])) for row in rows] == [(seed, 'document', [seed_text])]
        assert all(query_text not in row['texts'] for row in rows)
        outgoing = [frame for direction, frame in frames if direction == 'worker']
        responses = [frame for frame in outgoing if frame['type'] == 'QUERY_RESULT']
        assert len(responses) == 1 and responses[0]['request_id'] == ticket.request_id
        events = responses[0]['read_events']
        assert len(events) == 2 and query_text not in json.dumps(events)
        def observed_native():
            return [r for r in native_records if r['event'] in (
                'kaggle_read_completed','kaggle_read_adapter_terminal')]
        await until(lambda: len(observed_native()) == 2)
        boundaries=[r for r in native_records if r['event']=='read_channel_boundary']
        assert {r['phase'] for r in boundaries}=={'write','receive'}
        assert query_text not in json.dumps(native_records)
        assert read_telemetry.failed == 0
        terminal = next(record for record in native_records if record['terminal'])
        assert terminal['status'] == 'ok'
        assert {'encode_ms', 'lexical_ms', 'dense_ms', 'hydrate_ms'} <= terminal['stages_ms'].keys()
        assert not any(frame['type'] == 'RESULT' for frame in outgoing)
        assert 'vectors' not in responses[0] and 'embedding' not in responses[0]
        assert len(json.dumps(responses[0], ensure_ascii=False).encode()) <= 256 * 1024
        dispatched = [frame for direction, frame in frames if direction == 'controller' and frame['type'] == 'QUERY']
        assert len(dispatched) == 1 and dispatched[0]['compute_claim']
        assert dispatched[0]['actor']['person_id'] == authority.person_id
        delay_release['enabled'] = False
        for raw in held_control:
            await wire_ref['socket'].send_str(raw)
        await until(lambda: admission.status()['state'] == 'idle')
        assert loads == ['resident-encoder-stub-loaded'] and encoder_calls == [query_text]
    finally:
        stop.set()
        try:
            if worker_task is not None:
                await asyncio.wait_for(worker_task, 3)
        finally:
            server.should_exit = True
            try:
                await asyncio.wait_for(serving, 3)
            finally:
                listener.close()
                admission.close()
