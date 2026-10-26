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


@pytest.mark.parametrize('catalog, locator, expected', [
    ({'title': 'Кёнигсберг в Пруссии', 'authors': ['Фриц Гаузе'], 'publication_year': 1994},
     'pages 141', 'Фриц Гаузе. Кёнигсберг в Пруссии (1994), с. 141'),
    ({'title': 'Atlas', 'publication_year': 1880}, 'pages 12-13', 'Atlas (1880), с. 12–13'),
    ({'title': 'Учебник', 'authors': ['А. Автор']}, 'pages 141, 142',
     'А. Автор. Учебник, с. 141–142'),
    ({'title': 'Без даты', 'authors': []}, 'unknown', 'Без даты'),
    ({}, 'pages 9', 'passage-1'),
])
def test_evidence_title_uses_existing_catalog_and_page(catalog, locator, expected):
    from regional_knowledge.kaggle_public_backend import _evidence_title
    metadata = SimpleNamespace(source_provenance={'catalog': catalog}, locator=locator)
    assert _evidence_title(SimpleNamespace(id='passage-1', metadata=metadata)) == expected
    metadata.source_provenance = {'catalog': {'title': 'x' * 3000}}
    assert len(_evidence_title(SimpleNamespace(id='passage-1', metadata=metadata))) == 2048


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
    assert dispatched.query.evidence_limit == 3
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
    else:
        assert rows[0].text == 'Evidence' and rows[0].incomplete is False
        assert rows[0].next_id is None and len(rows[0].metadata['source_revisions']) == 2
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
    stages=terminal['controller_stages_ms']
    assert set(stages)=={'auth_ms','binding_ms','dispatch_gate_ms','admission_ms',
        'dispatch_to_result_ms','finish_gate_ms','result_to_terminal_ms'}
    assert all(type(value) in (int,float) and value>=0 for value in stages.values())
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
@pytest.mark.parametrize('dispatch', [False, True])
async def test_admission_deadline_is_typed_deadline_failure_and_compute_remains_owned(target, dispatch):
    s = target
    task = asyncio.create_task(s.backend.search('synthetic query', s.principal))
    dispatched = await work(s) if dispatch else None
    with pytest.raises(ToolError, match='deadline_exceeded') as error:
        await task
    payload = json.loads(str(error.value))
    await asyncio.to_thread(s.telemetry._queue.join)
    terminal = next(record for record in s.records if record.get('terminal'))
    trace = payload['read_trace']
    assert {key: trace[key] for key in ('trace_id','attempt_id','request_ref')} == {
        key: terminal[key] for key in ('trace_id','attempt_id','request_ref')}
    assert trace['backend_elapsed_ms'] >= 0
    assert terminal['dispatch_state'] == ('dispatched' if dispatch else 'not_dispatched')
    assert set(payload) == {'status','reason','read_trace'}
    assert s.gate.abandoned == s.gate.permits
    assert s.admission.status()['state'] == ('busy' if dispatch else 'idle')
    if dispatched is not None:
        await release(s, dispatched)


@pytest.mark.asyncio
async def test_exact_fetch_refusal_returns_the_same_trace_as_its_terminal_record(target):
    from regional_knowledge.kaggle_protocol import PassageReadResult
    from regional_knowledge.kaggle_public_backend import passage_fetch_id
    from test_kaggle_exact_fetch import locator
    s = target
    task = asyncio.create_task(s.backend.fetch(passage_fetch_id(locator()), s.principal))
    dispatched = await work(s)
    result = PassageReadResult(request_id=dispatched.query.request_id,
        connection_generation=s.generation, turn_generation=dispatched.query.turn_generation,
        status='unavailable', reason='evidence_withdrawn',
        observed_data_seq=None, observed_projection_seq=None)
    assert await s.admission.finish_read(s.run,s.token,s.generation,result)
    with pytest.raises(ToolError) as error:
        await task
    payload = json.loads(str(error.value))
    await asyncio.to_thread(s.telemetry._queue.join)
    terminal = next(record for record in s.records if record.get('terminal'))
    assert payload['status'] == 'unavailable' and payload['reason'] == 'evidence_withdrawn'
    assert payload['read_trace']['trace_id'] == terminal['trace_id']
    assert payload['read_trace']['request_ref'] == terminal['request_ref']
    assert set(payload) == {'status','reason','read_trace'}
    assert terminal['dispatch_state'] == 'dispatched'
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



@pytest.mark.asyncio
async def test_accepted_chunk_fetch_uses_existing_oauth_gate_admission_and_issuance(target):
    from regional_knowledge.kaggle_source_read import SourceReadRequest,SourceReadResult
    from regional_knowledge.kaggle_protocol import (
        EvidenceItem,ItemDependencies,SourceDependency)
    s=target
    source='00000000-0000-0000-0000-000000000001'
    chunk='60000000-0000-0000-0000-000000000001'
    page='40000000-0000-0000-0000-000000000001'
    region='50000000-0000-0000-0000-000000000001'

    async def read():
        with capture_read_output() as calls:
            value=await s.backend.fetch_chunks(s.principal,source_id=source,
                page_start=1,page_end=2,limit=1)
        return value,calls

    task=asyncio.create_task(read())
    dispatched=await work(s)
    assert type(dispatched.query) is SourceReadRequest
    assert dispatched.query.operation=='accepted_chunks'
    assert (dispatched.query.source_id,dispatched.query.page_start,
            dispatched.query.page_end,dispatched.query.limit)==(source,1,2,1)
    assert dispatched.query.actor.person_id==s.auth.p.other.subject
    dependency=SourceDependency(source_id=source,content_revision=1,eligibility_revision=7)
    result=SourceReadResult(request_id=dispatched.query.request_id,
        connection_generation=s.generation,turn_generation=dispatched.query.turn_generation,
        operation='accepted_chunks',status='ok',
        records=(dict(source_id=source,chunk_id=chunk,ordinal=0,text='Accepted chunk',
            pages=[dict(page_id=page,physical_page_index=1)],
            regions=[dict(page_id=page,region_id=region,start=0,end=12,role='body')]),),
        evidence=(EvidenceItem(id=chunk,kind='source',text='Accepted chunk',
            dependencies=ItemDependencies(source_ids=(source,),variant_refs=(),grant_refs=(),
                source_revisions=(dependency,))),),
        next_after_ordinal=0,observed_data_seq=5,observed_projection_seq=5,
        observed_visibility_epoch=4,observed_database_generation='db-source')
    assert await s.admission.finish_read(s.run,s.token,s.generation,result)
    output,calls=await task
    assert output['status']=='ok' and output['source_id']==source
    assert output['page_start']==1 and output['page_end']==2
    assert output['chunks'][0]['chunk_id']==chunk
    assert output['next_after_ordinal']==0 and output['has_more']
    assert len(calls)==1
    calls[0].release()
    await release(s,dispatched)


@pytest.mark.asyncio
async def test_accepted_chunk_fetch_rejects_invalid_source_or_page_range_before_queue(target):
    s=target
    with pytest.raises(ToolError,match='invalid_input'):
        await s.backend.fetch_chunks(s.principal,source_id='not-uuid',
            page_start=1,page_end=2)
    with pytest.raises(ToolError,match='invalid_input'):
        await s.backend.fetch_chunks(s.principal,
            source_id='00000000-0000-0000-0000-000000000001',
            page_start=1,page_end=5)
    assert not s.gate.permits



@pytest.mark.asyncio
@pytest.mark.parametrize("branch", ["id", "chunks", "unavailable"])
async def test_fastmcp_fetch_schema_matches_guarded_structured_wire(branch):
    """Real FastMCP tools/list and tools/call through the final issuance guard.

    FastMCP wraps union outputs itself. The guard must rerender in the same
    one-result shape *after* checking current source authorization, rather than
    undoing that wrapping or adding another level of result.
    """
    import httpx
    from jsonschema import Draft202012Validator
    from mcp.server.transport_security import TransportSecuritySettings
    from regional_knowledge.contracts import (
        FetchOutput, FetchChunksOutput, FetchChunkRecord,
        FetchChunkPage, FetchChunkRegion, ReadTrace)
    from regional_knowledge.read_issuance import retain_read_output
    from regional_knowledge.server import KnowledgeMCPServer

    server = KnowledgeMCPServer("fetch-guarded-wire-contract")
    server._current_read_output_required = True

    class SyntheticGate:
        refreshes = 0
        releases = 0

        async def refresh(self, gated):
            self.refreshes += 1
            return gated

        def issuance_current(self, ticket):
            return True

        def release(self, ticket):
            self.releases += 1

    gate = SyntheticGate()
    trace = ReadTrace(trace_id="a" * 32, attempt_id="b" * 32,
                      request_ref="c" * 64, backend_elapsed_ms=6.5)

    @server.tool(name="fetch")
    async def fetch(id: str | None = None) -> FetchOutput | FetchChunksOutput:
        if branch == "chunks":
            value = FetchChunksOutput(
                source_id="00000000-0000-0000-0000-000000000001",
                page_start=1, page_end=2,
                chunks=[FetchChunkRecord(
                    source_id="00000000-0000-0000-0000-000000000001",
                    chunk_id="60000000-0000-0000-0000-000000000001",
                    ordinal=0, text="Accepted source chunk",
                    pages=[FetchChunkPage(
                        page_id="40000000-0000-0000-0000-000000000001",
                        physical_page_index=1)],
                    regions=[FetchChunkRegion(
                        page_id="40000000-0000-0000-0000-000000000001",
                        region_id="50000000-0000-0000-0000-000000000001",
                        start=0, end=12, role="body")])],
                next_after_ordinal=0, has_more=True, status="ok",
                observed_data_seq=5, observed_projection_seq=5,
                read_trace=trace)
        else:
            value = FetchOutput(id=id or "opaque", title="Book",
                                text="Source text" if branch == "id" else "",
                                url="urn:rkb:passage:opaque",
                                status="ok" if branch == "id" else "unavailable",
                                reason=None if branch == "id" else "evidence_withdrawn",
                                read_trace=trace)
        assert retain_read_output(gate, SimpleNamespace(ticket=object(), result=value),
                                  lambda _: value)
        return value

    app = server.streamable_http_app(
        stateless_http=True, json_response=True,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=False))
    headers = {"accept": "application/json, text/event-stream"}
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://localhost") as client:
            listed = await client.post("/mcp", json={
                "jsonrpc": "2.0", "id": 1, "method": "tools/list"}, headers=headers)
            called = await client.post("/mcp", json={
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "fetch", "arguments": {"id": "opaque"}}},
                headers=headers)

    assert listed.status_code == called.status_code == 200
    tools = listed.json()["result"]["tools"]
    schema = next(t["outputSchema"] for t in tools if t["name"] == "fetch")
    output = called.json()["result"]
    payload = output["structuredContent"]
    Draft202012Validator.check_schema(schema)
    assert "result" in schema.get("required", ())
    assert json.loads(output["content"][0]["text"]) == payload
    assert gate.refreshes >= 1 and gate.releases == 1

    if branch == "unavailable":
        # Exact read failures retain existing MCP isError/status semantics.
        assert output["isError"] is True
        assert payload["status"] == "unavailable"
        assert payload["reason"] == "evidence_withdrawn"
        assert "result" not in payload
    else:
        assert not output["isError"]
        Draft202012Validator(schema).validate(payload)
        assert list(payload) == ["result"]
        assert "result" not in payload["result"]
        assert payload["result"]["status"] == "ok"
        assert payload["result"]["read_trace"]["trace_id"] == "a" * 32
        if branch == "chunks":
            assert payload["result"]["chunks"][0]["regions"][0]["role"] == "body"
            assert payload["result"]["next_after_ordinal"] == 0
            assert payload["result"]["has_more"] is True
        else:
            assert payload["result"]["id"] == "opaque"
            assert payload["result"]["text"] == "Source text"


@pytest.mark.asyncio
async def test_direct_adapter_reads_renew_idle_without_renewing_from_heartbeat(target):
    from regional_knowledge.bge_queue import IDLE_SECONDS
    s=target;wire_ids=[]
    for minute in range(1,32):
        now=1000.+60*minute
        s.queue.clock=lambda:now
        s.queue.heartbeat(s.run,s.token,ready=True,connection_generation=s.generation)
        authority=await s.auth.resolver.resolve_oauth(s.auth.tokens.access_token,
            destination=s.backend.destination)
        async def read():
            with capture_read_output() as calls:
                result=await s.backend.search_adapter('synthetic query',authority=authority,
                    request_id='same-adapter-question',turn_generation=0,min_data_seq=0,
                    deadline_monotonic=asyncio.get_running_loop().time()+2,
                    destination=s.backend.destination)
            return result,calls
        task=asyncio.create_task(read())
        dispatched=await work(s)
        wire_ids.append(dispatched.query.request_id)
        await settle(s,dispatched)
        result,calls=await task
        for call in calls:
            call.complete(result.status,result.reason);call.release()
        r=dispatched.resource
        await s.admission.resource_released(s.run,s.token,s.generation,
            runtime_incarnation=r.runtime_incarnation,resource_kind=r.resource_kind,
            resource_id=r.resource_id,compute_claim=r.compute_claim,dispatch_generation=r.dispatch_generation)
        with s.queue.connect(write=False) as db:
            assert db.execute('SELECT useful FROM runs WHERE id=?',(s.run,)).fetchone()[0]==now
    assert len(set(wire_ids))==31  # repeated logical question, fresh ephemeral reads
    # Keep heartbeats healthy after the last read without adding user demand.
    for elapsed in range(60,IDLE_SECONDS,60):
        s.queue.clock=lambda:now+elapsed
        s.queue.heartbeat(s.run,s.token,ready=True,connection_generation=s.generation)
    s.queue.clock=lambda:now+IDLE_SECONDS-1
    s.queue.heartbeat(s.run,s.token,ready=True,connection_generation=s.generation)
    with s.queue.connect(write=False) as db:
        row=db.execute('SELECT * FROM runs WHERE id=?',(s.run,)).fetchone()
        assert row['useful']==now and not s.queue._run_expired(row,now+IDLE_SECONDS-1)
        assert s.queue._run_expired(row,now+IDLE_SECONDS+1)


@pytest.mark.asyncio
async def test_revoked_direct_adapter_authority_does_not_renew_idle(target):
    s=target
    authority=await s.auth.resolver.resolve_oauth(s.auth.tokens.access_token,
        destination=s.backend.destination)
    token=await s.auth.provider.load_access_token(s.auth.tokens.access_token)
    await s.auth.provider.revoke_token(token)
    s.queue.clock=lambda:1100.
    with capture_read_output(),pytest.raises(ToolError):
        await s.backend.search_adapter('synthetic query',authority=authority,
            request_id='revoked-adapter',turn_generation=0,min_data_seq=0,
            deadline_monotonic=asyncio.get_running_loop().time()+2,destination=s.backend.destination)
    with s.queue.connect(write=False) as db:
        assert db.execute('SELECT useful FROM runs WHERE id=?',(s.run,)).fetchone()[0]==1000.
