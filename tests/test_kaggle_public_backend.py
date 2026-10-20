"""Public source adapter with real synthetic OAuth, queue and shared admission."""
import asyncio
import json
from types import SimpleNamespace

import pytest
import pytest_asyncio
from mcp.server.mcpserver.exceptions import ToolError

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.kaggle_public_backend import KaggleSourceBackend, TargetUnavailable, current_binding
from regional_knowledge.kaggle_protocol import QueryResult
from regional_knowledge.read_authority import ReadAuthorityError
from regional_knowledge.read_issuance import capture_read_output
from regional_knowledge.read_issuance import ReadIssuanceMiddleware, bind_request_output
from starlette.requests import Request
from regional_knowledge.read_telemetry import ReadTelemetry
from test_read_authority import auth
from test_participants import people
from test_bge_runtime_readiness import expected, observation
from test_kaggle_protocol import item


class Gate:
    """Only the final source provenance gate is synthetic in these tests."""
    def __init__(self, authority):
        self.authority = authority
        self.permits, self.abandoned, self.released = [], [], []

    def catalog_supported(self):
        # This fixture retains the original source/Story read contract. Current
        # editorial/catalogue tests supply the actual native registry and gate.
        return False

    async def dispatch(self, authority, query, binding):
        await self.authority.require(authority, 'public_search')
        permit = SimpleNamespace(authority=authority, query=query, binding=binding)
        self.permits.append(permit)
        return permit

    async def finish(self, permit, result):
        await self.authority.recheck(permit.authority)
        return SimpleNamespace(ticket=permit, result=result)

    def abandon(self, permit):
        self.abandoned.append(permit)

    def release(self, ticket):
        self.released.append(ticket)

    async def refresh(self, gated):
        await self.authority.recheck(gated.ticket.authority)
        return gated

    def issuance_current(self, ticket):
        return True


@pytest_asyncio.fixture
async def target(auth, tmp_path):
    queue = BgeQueue(tmp_path / 'public-worker.sqlite', clock=lambda: 1000.)
    queue.enqueue(auth.p.owner.subject, 'document', ['synthetic passage'], kind='document')
    with queue.connect(write=False) as db:
        run = db.execute('SELECT id FROM runs').fetchone()[0]
    nonce = queue.prepare_channel(run)
    queue.expect_search_runtime(run, expected())
    token = queue.launch_claim(run)
    queue.launch_record(run, 'fixture-owner/stable-slug', provider_version=7)
    admission = queue.configure_shared_reads(auth.resolver)
    generation = queue.attach_channel(run, token, nonce, 'incarnation-a')['connection_generation']
    assert queue.accept_search_runtime(run, token, generation, observation()).ok
    queue.heartbeat(run, token, ready=True, connection_generation=generation)
    await admission.resource_state(run, token, generation, runtime_incarnation='incarnation-a', state='idle')
    records = []
    telemetry = ReadTelemetry(sink=lambda raw: records.append(json.loads(raw)))
    gate = Gate(auth.resolver)
    backend = KaggleSourceBackend(worker_queue=queue, read_authority=auth.resolver,
        source_gate=gate, destination_ref='synthetic-response', read_telemetry=telemetry)
    s = SimpleNamespace(queue=queue, admission=admission, run=run, token=token, generation=generation,
        nonce=nonce, auth=auth, gate=gate, backend=backend, telemetry=telemetry, records=records,
        principal=SimpleNamespace(access_token=auth.tokens.access_token))
    yield s
    admission.close()


async def work(s):
    for _ in range(200):
        value = await s.queue.next_channel_work(s.run, s.token, s.generation)
        if value is not None:
            assert value.kind == 'read'
            s.admission.mark_work_sent(value)
            return value
        await asyncio.sleep(.001)
    pytest.fail('read did not reach actual shared admission')


async def settle(s, value, *, status='degraded', mode='hybrid', reason='bounded_retrieval'):
    result = QueryResult(request_id=value.query.request_id, connection_generation=s.generation,
        turn_generation=value.query.turn_generation, status=status, mode=mode, reason=reason,
        observed_data_seq=5, observed_projection_seq=5,
        evidence=(item(kind='source', id='passage:/1', metadata={'title': 'Source title'}),) if mode else ())
    assert await s.admission.finish_read(s.run, s.token, s.generation, result)


async def release(s, value):
    resource = value.resource
    await s.admission.resource_released(s.run, s.token, s.generation,
        runtime_incarnation=resource.runtime_incarnation, resource_kind=resource.resource_kind,
        resource_id=resource.resource_id, compute_claim=resource.compute_claim,
        dispatch_generation=resource.dispatch_generation)


@pytest.mark.asyncio
@pytest.mark.parametrize('evidence', [False, True])
async def test_actual_queue_roundtrip_preserves_authority_until_guard_release_and_correlates(target, evidence):
    s = target
    async def read():
        with capture_read_output() as calls:
            result = await (s.backend.search_evidence('synthetic query', s.principal,
                document_ids=['00000000-0000-0000-0000-000000000001'])
                if evidence else s.backend.search('synthetic query', s.principal,
                    document_ids=['00000000-0000-0000-0000-000000000001']))
        return result, calls
    task = asyncio.create_task(read())
    dispatched = await work(s)
    assert dispatched.query.filters.source_ids == ('00000000-0000-0000-0000-000000000001',)
    assert dispatched.query.filters.kinds == ('source',)
    assert dispatched.query.min_data_seq == 5
    assert dispatched.query.actor.person_id == s.auth.p.other.subject
    await settle(s, dispatched)
    output, calls = await task
    assert output.status == 'degraded' and output.reason == 'bounded_retrieval'
    assert output.mode == 'hybrid' and output.retrieval_mode == 'bge_lexical'
    rows = output.evidence if evidence else output.results
    assert rows[0].id == 'passage:/1' and rows[0].url == 'urn:rkb:passage:passage%3A%2F1'
    if evidence:
        assert rows[0].text == 'Evidence' and len(rows[0].metadata['source_revisions']) == 2
    assert len(calls) == 1 and not s.gate.released
    authority = s.gate.permits[0].authority
    assert await s.auth.resolver.recheck(authority)
    assert s.admission.status()['state'] == 'busy'  # Result is not compute release.
    await asyncio.to_thread(s.telemetry._queue.join)
    assert not any(record.get('terminal') for record in s.records)
    calls[0].complete(output.status, output.reason)
    calls[0].release()
    with pytest.raises(ReadAuthorityError):
        await s.auth.resolver.recheck(authority)
    assert len(s.gate.released) == 1
    await asyncio.to_thread(s.telemetry._queue.join)
    terminal = next(record for record in s.records if record.get('terminal'))
    assert terminal['request_ref'] == output.read_trace.request_ref
    assert terminal['attempt_id'] == output.read_trace.attempt_id
    assert terminal['trace_id'] == output.read_trace.trace_id
    assert terminal['executed_branches'] == ['lexical', 'dense']
    assert terminal['dispatch_state'] == 'dispatched'
    await release(s, dispatched)


@pytest.mark.asyncio
async def test_missing_output_capture_fails_closed_and_releases_authority(target):
    s = target
    task = asyncio.create_task(s.backend.search('synthetic query', s.principal))
    dispatched = await work(s)
    await settle(s, dispatched)
    with pytest.raises(ToolError, match='not_ready'):
        await task
    assert len(s.gate.released) == 1
    with pytest.raises(ReadAuthorityError):
        await s.auth.resolver.recheck(s.gate.permits[0].authority)
    assert s.admission.status()['state'] == 'busy'
    await asyncio.to_thread(s.telemetry._queue.join)
    terminal = next(record for record in s.records if record.get('terminal'))
    assert terminal['status'] == 'unavailable' and terminal['reason'] == 'not_ready'
    assert terminal['executed_branches'] == ['lexical', 'dense']
    await release(s, dispatched)


@pytest.mark.asyncio
async def test_native_render_failure_is_not_mislabeled_as_bad_caller_query(target, monkeypatch):
    from regional_knowledge.contracts import SearchOutput
    s = target
    def invalid_render(*args, **kwargs):
        return SearchOutput.model_validate({'results': 'invalid native shape'})
    monkeypatch.setattr(s.backend, '_render', invalid_render)
    async def read():
        with capture_read_output():
            return await s.backend.search('synthetic query', s.principal)
    task = asyncio.create_task(read())
    dispatched = await work(s)
    await settle(s, dispatched)
    with pytest.raises(ToolError, match='not_ready'):
        await task
    await asyncio.to_thread(s.telemetry._queue.join)
    terminal = next(record for record in s.records if record.get('terminal'))
    assert terminal['status'] == 'unavailable' and terminal['reason'] == 'not_ready'
    assert terminal['executed_branches'] == ['lexical', 'dense']
    await release(s, dispatched)


@pytest.mark.asyncio
async def test_caller_cancellation_abandons_gate_permit_but_retains_actual_compute(target):
    s = target
    task = asyncio.create_task(s.backend.search('synthetic query', s.principal))
    dispatched = await work(s)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert s.gate.abandoned == s.gate.permits and not s.gate.released
    assert s.admission.status()['pending_reads'] == 0
    assert s.admission.status()['state'] == 'busy'
    with pytest.raises(ReadAuthorityError):
        await s.auth.resolver.recheck(s.gate.permits[0].authority)
    await release(s, dispatched)


@pytest.mark.asyncio
async def test_cancel_before_dispatch_never_claims_worker_execution(target):
    s = target
    task = asyncio.create_task(s.backend.search('synthetic query', s.principal))
    for _ in range(200):
        if s.admission.status()['pending_reads']:
            break
        await asyncio.sleep(.001)
    else:
        pytest.fail('read never reached admission')
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert s.admission.status()['state'] == 'idle'
    assert s.gate.abandoned == s.gate.permits
    await asyncio.to_thread(s.telemetry._queue.join)
    assert not any(record['event'] == 'read_attempt_dispatched' for record in s.records)
    terminal = next(record for record in s.records if record.get('terminal'))
    assert terminal['dispatch_state'] == 'not_dispatched' and not terminal['executed_branches']


@pytest.mark.asyncio
async def test_admission_deadline_is_typed_deadline_failure_and_compute_remains_owned(target):
    s = target
    task = asyncio.create_task(s.backend.search('synthetic query', s.principal))
    dispatched = await work(s)
    with pytest.raises(ToolError, match='deadline_exceeded'):
        await task
    assert s.gate.abandoned == s.gate.permits
    assert s.admission.status()['state'] == 'busy'
    await release(s, dispatched)


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['legacy', 'wrong-generation', 'stale-owner', 'dense-not-ready'])
async def test_binding_requires_verified_current_runtime_and_exact_shared_owner(target, change):
    s = target
    before = current_binding(s.queue)
    assert before.run_id == s.run and before.database_generation == 'synthetic-db'
    with s.queue.connect() as db:
        if change == 'legacy':
            db.execute('DELETE FROM bge_runtime_expectations')
        elif change == 'wrong-generation':
            db.execute('UPDATE bge_runtime_expectations SET accepted_generation=999')
        elif change == 'stale-owner':
            db.execute("UPDATE bge_shared_admission SET incarnation='other'")
        else:
            db.execute('UPDATE bge_runtime_expectations SET observed_json=?',
                       (json.dumps(observation(dense_search_ready=False)),))
    with pytest.raises(TargetUnavailable):
        current_binding(s.queue)
    assert not s.backend.binding_current(before)


@pytest.mark.asyncio
async def test_real_queue_to_final_asgi_issuance_releases_authority_after_send(target):
    s = target
    scope = {'type': 'http', 'method': 'POST', 'path': '/mcp', 'headers': []}
    sent = []
    async def app(scope, receive, send):
        with capture_read_output() as calls:
            output = await s.backend.search_evidence('synthetic query', s.principal)
            context = SimpleNamespace(request_context=SimpleNamespace(request=Request(scope), request_id=49))
            bind_request_output(context, calls)
        await send({'type': 'http.response.start', 'status': 200,
                    'headers': [(b'content-type', b'application/json')]})
        await send({'type': 'http.response.body', 'body': json.dumps({'jsonrpc': '2.0', 'id': 49,
            'result': {'content': [], 'structuredContent': output.model_dump(mode='json')}}).encode()})
    async def receive():
        return {'type': 'http.disconnect'}
    async def send(message):
        # The gate's canonical context remains valid through the issuance call.
        assert await s.auth.resolver.recheck(s.gate.permits[0].authority)
        assert not s.gate.released
        sent.append(message)
    task = asyncio.create_task(ReadIssuanceMiddleware(app)(scope, receive, send))
    dispatched = await work(s)
    await settle(s, dispatched)
    await task
    envelope = json.loads(sent[-1]['body'])
    assert envelope['id'] == 49 and envelope['result']['isError'] is False
    assert envelope['result']['structuredContent']['status'] == 'degraded'
    assert len(s.gate.released) == 1
    with pytest.raises(ReadAuthorityError):
        await s.auth.resolver.recheck(s.gate.permits[0].authority)
    await asyncio.to_thread(s.telemetry._queue.join)
    terminals = [record for record in s.records if record.get('terminal')]
    assert len(terminals) == 1 and terminals[0]['status'] == 'degraded'
    await release(s, dispatched)


@pytest.mark.asyncio
async def test_gate_denial_keeps_actual_executed_branches_and_final_status(target, monkeypatch):
    s = target
    finish = s.gate.finish
    async def deny(permit, result):
        gated = await finish(permit, result)
        gated.result = result.model_copy(update={'status': 'denied', 'mode': None,
                                                'reason': 'access_denied', 'evidence': ()})
        return gated
    monkeypatch.setattr(s.gate, 'finish', deny)
    async def read():
        with capture_read_output() as calls:
            output = await s.backend.search_evidence('synthetic query', s.principal)
        return output, calls
    task = asyncio.create_task(read())
    dispatched = await work(s)
    await settle(s, dispatched)
    with pytest.raises(ToolError, match='access_denied'):
        await task
    await asyncio.to_thread(s.telemetry._queue.join)
    terminal = next(record for record in s.records if record.get('terminal'))
    assert terminal['status'] == 'denied' and terminal['reason'] == 'access_denied'
    assert terminal['executed_branches'] == ['lexical', 'dense']
    await release(s, dispatched)


@pytest.mark.asyncio
@pytest.mark.parametrize('scope', [[], ['not-a-uuid'], ['AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA'],
    ['00000000-0000-0000-0000-000000000001'] * 2,
    ['00000000-0000-0000-0000-000000000001'] * 33])
async def test_invalid_explicit_empty_source_scope_never_widens_to_all_sources(target, scope):
    s = target
    with pytest.raises(ToolError, match='invalid_input'):
        await s.backend.search('synthetic query', s.principal, document_ids=scope)
    assert not s.gate.permits and s.admission.status()['pending_reads'] == 0
    assert s.admission.status()['state'] == 'idle'

@pytest.mark.asyncio
async def test_current_story_get_uses_paired_editorial_read_and_keeps_exact_saved_body(target):
    from regional_knowledge.kaggle_editorial_contracts import EditorialReadRequest,EditorialReadResult
    from regional_knowledge.kaggle_story_read import story_item
    from test_kaggle_editorial_contracts import record
    s=target;s.gate.catalog_supported=lambda:True
    r=record();r['revision']=r['current_revision']=3;r['default_variant_ref']['revision']=r['variant']['revision']=2
    r['variant'].pop('source_blocks')
    async def read():
        with capture_read_output() as calls:
            value=await s.backend.story_get(r['story_id'],s.principal,revision=3)
            return value,calls
    task=asyncio.create_task(read())
    try:
        work_item=await work(s)
        assert type(work_item.query) is EditorialReadRequest and work_item.query.revision==3
        result=EditorialReadResult(request_id=work_item.query.request_id,connection_generation=s.generation,
            turn_generation=work_item.query.turn_generation,status='ok',record=r,evidence=(story_item(r),),
            observed_data_seq=5,observed_projection_seq=5,observed_visibility_epoch=1,observed_database_generation='synthetic-db')
        assert await s.admission.finish_read(s.run,s.token,s.generation,result)
        await release(s,work_item)
        value,calls=await task
        assert value['body']==r['body'] and value['default_variant_ref']['revision']==2
        assert len(calls)==1
        calls[0].release()
    finally:
        if not task.done():task.cancel()
