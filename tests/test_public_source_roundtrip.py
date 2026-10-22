"""Opt-in disposable PG → canonical source history → real MCP JSON issuance.

No models/providers run: the encoder and conditional object store are synthetic.
Native SQL, history verification, OAuth, admission and final source gate are real.
"""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from psycopg import sql

from regional_knowledge.bge_queue import BgeQueue
from regional_knowledge.control_spool import ControlSpool, SOURCE_WITHDRAW, WITHDRAW_DEPENDENCY
from regional_knowledge.kaggle_apply import HandlerRegistry, HandlerSpec
from pg_schema_metadata import initialize_fixture_database as initialize_database
from regional_knowledge.kaggle_database_probe import NativeDatabaseProbe
from regional_knowledge.kaggle_protocol import QueryResult, ItemDependencies
from regional_knowledge.kaggle_public_backend import KaggleSourceBackend, current_binding, parse_passage_fetch_id
from regional_knowledge.kaggle_readiness import SearchRuntimeExpectation, RUNTIME_HASHES
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery
from regional_knowledge.read_telemetry import ReadTelemetry
from regional_knowledge.recovery_journal import RecoveryJournal
from regional_knowledge.server import KnowledgeMCPServer
from regional_knowledge.source_history_verifier import NativeSourceHistoryVerifier, inspect_acl
from regional_knowledge.source_import import resolved_import_command, source_import_handler
from regional_knowledge.source_read_gate import SourceReadGate
from mcp.server.transport_security import TransportSecuritySettings

from test_kaggle_apply import pinned
from test_kaggle_pg_driver import database, connect
from test_kaggle_query import PG, tokenizer, VECTOR
from test_participants import people
from test_read_authority import auth
from test_recovery_journal import ConditionalMemoryStore
from test_source_import import ROOT, VERSIONS, payload
from test_control_spool import withdraw

pytestmark = [PG, pytest.mark.asyncio]
A = '00000000-0000-0000-0000-000000000001'
B = '00000000-0000-0000-0000-000000000002'
GENERATION = 'public-roundtrip-fixture'


@pytest_asyncio.fixture
async def vertical(database, auth, tmp_path):
    admin = database  # Existing strict socket/container/CI guard owns lifecycle.
    roles = []
    queue = spool = None
    native = None
    try:
        async with admin.transaction():
            # Remove only the guard fixture's seed before establishing genesis.
            await admin.execute('TRUNCATE rkb_v4.passage_search,rkb_v4.active_revision,'
                                'rkb_v4.revisions,rkb_v4.sources')
            await admin.execute((ROOT / 'sql/kaggle/002_recovery.sql').read_text(), prepare=False)
            for name in ('rkb_bootstrap', 'rkb_writer', 'rkb_reader'):
                exists = await (await admin.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', (name,))).fetchone()
                if not exists:
                    await admin.execute(sql.SQL('CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB '
                        'NOCREATEROLE NOREPLICATION NOBYPASSRLS').format(sql.Identifier(name)))
                    roles.append(name)
        await initialize_database(admin, database_generation=GENERATION,
                                  writer_generation='writer-1', revisions=VERSIONS)
        async with admin.transaction():
            for name in ('003_search_scope.sql', '004_read_fences.sql',
                         '005_source_withdraw.sql', '006_source_import.sql'):
                await admin.execute((ROOT / 'sql/kaggle' / name).read_text(), prepare=False)
            await admin.execute('CREATE TABLE rkb_v4.schema_migrations(version integer PRIMARY KEY)')
            await admin.execute('INSERT INTO rkb_v4.schema_migrations VALUES(6)')
            await admin.execute('SELECT rkb_v4.publish_read_frontier(%s,%s,0,0,true,true)',
                                (GENERATION, 'writer-1'))

        registry = HandlerRegistry([
            source_import_handler(revisions=VERSIONS,
                check_sha256=(await pinned(admin, 'check_source_import_v1')).definition_sha256,
                apply_sha256=(await pinned(admin, 'apply_source_import_v1')).definition_sha256),
            HandlerSpec('domain_command', SOURCE_WITHDRAW, VERSIONS,
                await pinned(admin, 'check_source_withdraw_v1'),
                await pinned(admin, 'apply_source_withdraw_v1'),
                frozenset({'eligibility_revision'}), dependency=WITHDRAW_DEPENDENCY),
        ])
        queue = BgeQueue(tmp_path / 'public-control.sqlite', clock=lambda: 1000.)
        queue.enqueue(auth.p.owner.subject, 'fixture-document', ['synthetic'], kind='document')
        with queue.connect(write=False) as db:
            run = db.execute('SELECT id FROM runs').fetchone()[0]
        nonce = queue.prepare_channel(run)
        expected = SearchRuntimeExpectation({key: 'a' * 64 for key in RUNTIME_HASHES}, GENERATION, 0, schema_version=6)
        queue.expect_search_runtime(run, expected)
        token = queue.launch_claim(run)
        queue.launch_record(run, 'fixture-owner/stable-slug', provider_version=7)
        admission = queue.configure_shared_reads(auth.resolver)
        generation = queue.attach_channel(run, token, nonce, 'fixture-incarnation')['connection_generation']
        observed = dict(hashes=dict(expected.hashes), database_generation=GENERATION, schema_version=6, applied_seq=0, projection_seq=0, visibility_epoch=0, text_search_ready=True, dense_search_ready=True, archive_read_ready=False, proof_ready=None)
        assert queue.accept_search_runtime(run, token, generation, observed).ok
        queue.heartbeat(run, token, ready=True, connection_generation=generation)
        await admission.resource_state(run, token, generation,
            runtime_incarnation='fixture-incarnation', state='idle')

        native = await connect()
        @asynccontextmanager
        async def acquire():
            yield native
        journal = RecoveryJournal(ConditionalMemoryStore(), stream='public-roundtrip-source-history')
        spool = ControlSpool(queue, auth.p.registry, journal, registry)
        binding_current = lambda binding: current_binding(queue) == binding
        gate = SourceReadGate(auth.resolver, spool, binding_current=binding_current)
        records = []
        telemetry = ReadTelemetry(sink=lambda raw: records.append(json.loads(raw)))
        backend = KaggleSourceBackend(worker_queue=queue, read_authority=auth.resolver,
            source_gate=gate, destination_ref='fixture-http-response', read_telemetry=telemetry)
        routes = {'source.search': backend.search, 'source.fetch': backend.fetch,
                  'source.withdraw': spool.stage_source_withdraw, 'source.import': spool.stage_source_import}
        catalog, _ = await NativeDatabaseProbe()(acquire=acquire)
        async with admin.transaction():
            acl = await inspect_acl(admin)
        verifier = NativeSourceHistoryVerifier(spool, acquire=acquire, binding_current=binding_current, routes=routes, expected_schema_version=6, writer_generation='writer-1')
        binding = current_binding(queue)
        assert (await verifier.establish_genesis(binding)).current()

        for sequence, source in enumerate((A, B), 1):
            material = payload()
            suffix = source[-1]
            for key, prefix in (('work_id', '4'), ('edition_id', '5')):
                material[key] = prefix + '0000000-0000-0000-0000-00000000000' + suffix
            material['research']['frontier_id'] = '30000000-0000-0000-0000-00000000000' + suffix
            material['passages'][0]['passage_id'] = '10000000-0000-0000-0000-00000000000' + suffix
            material['passages'][0]['body'] = ('withdrawn-source-a harbour' if source == A else 'safe-source-b castle')
            material['bibliography'] = 'private withdrawn bibliography A' if source == A else 'safe bibliography B'
            command = resolved_import_command(sequence=sequence, operation_key=f'fixture-import-{sequence}',
                request_id=f'fixture-import-request-{sequence}', person_id=auth.p.owner.subject,
                logical_time='2026-10-20T15:00:00Z', revisions=VERSIONS, source_id=source, payload=material)
            await spool.stage_source_import(auth.p.admin, command, database_generation=GENERATION)
            await spool.seal(auth.p.admin, command.operation_key, writer_generation='writer-1',
                             predecessor=await journal.head())
            applied = await spool.apply(command.operation_key, admin, writer_generation='writer-1')
            assert applied.status == 'applied'
            remote = await spool.publish_outcome(command.operation_key, writer_generation='writer-1')
            assert remote.outcome_sha256 == applied.outcome_sha256
        # Rebuild coverage from exact genesis, native receipts and remote bytes.
        assert (await verifier.restore(binding)).current()
        assert not (await spool.read_facts(None)).pending
        yield SimpleNamespace(admin=admin, queue=queue, admission=admission, spool=spool,
            backend=backend, gate=gate, auth=auth, run=run, token=token, generation=generation,
            worker=KaggleWorkerQuery(acquire=acquire, tokenizer=tokenizer()),
            records=records, telemetry=telemetry, verifier=verifier)
    finally:
        if spool is not None:
            spool.close()
        if queue is not None and getattr(queue, '_shared_reads', None) is not None:
            queue.shared_reads.close()
        if native is not None:
            await native.close()
        await admin.rollback()
        async with admin.transaction():
            for name in roles:
                await admin.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(name)))


async def pending_withdrawal(s):
    async with s.admin.transaction():
        revision = (await (await s.admin.execute(
            'SELECT eligibility_revision FROM rkb_v4.source_visibility WHERE source_id=%s', (A,))).fetchone())[0]
    command = replace(withdraw(s.auth.p.owner.subject, 3), schema_revision=VERSIONS.schema,
        function_revision=VERSIONS.function, executor_revision=VERSIONS.executor,
        operation_key='fixture-withdraw-after-worker', expected_revisions={'eligibility_revision': str(revision)})
    await s.spool.stage_source_withdraw(s.auth.p.admin, command, database_generation=GENERATION)
    return command


async def request(s, *, source_ids=None, limit=3, after_result=None, composite=False):
    """Actual installed MCP JSON request plus one real shared-queue native query."""
    server = KnowledgeMCPServer('public-source-roundtrip')
    server._current_read_output_required = True
    principal = SimpleNamespace(access_token=s.auth.tokens.access_token)
    @server.tool()
    async def source_read() -> dict:
        result = await s.backend.search_evidence('castle', principal,
                                                document_ids=source_ids, max_evidence=limit)
        return result.model_dump(mode='json')
    app = server.streamable_http_app(stateless_http=True, json_response=True,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False))
    changed = False
    async def boundary(scope, receive, send):
        async def hooked_send(message):
            nonlocal changed
            if message['type'] == 'http.response.start' and after_result and not changed:
                changed = True
                await after_result(s)
            await send(message)
        await app(scope, receive, hooked_send)

    async def native_once():
        while True:
            work = await s.queue.next_channel_work(s.run, s.token, s.generation)
            if work is not None:
                break
            await s.queue.wait_channel_work(.01)
        assert work.kind == 'read'
        s.admission.mark_work_sent(work)
        async def encode(_):
            return VECTOR
        wire = await s.worker(work.query.model_dump(mode='json'), encode=encode)
        result = QueryResult.model_validate_json(json.dumps(wire))
        assert result.status in ('ok', 'degraded') and result.observed_data_seq == 2
        if limit == 1:
            assert len(result.evidence) == 1
            assert result.evidence[0].dependencies.source_ids == (B,)
        if composite:
            # Native source SQL returns independent passages. Compose a bounded
            # cross-source item from those exact native dependencies to exercise
            # the shared final gate's complete dependency semantics.
            dependencies = tuple(dep for item in result.evidence for dep in item.dependencies.source_revisions)
            assert {dep.source_id for dep in dependencies} == {A, B}
            joined = result.evidence[0].model_copy(update={'id': 'composite-native-fixture',
                'text': 'private cross-source A and B', 'text_form': 'complete',
                'source_continuation': None, 'continuation': None, 'continuation_complete': False,
                'dependencies': ItemDependencies(
                    source_ids=(A, B), source_revisions=dependencies, variant_refs=(), grant_refs=())})
            result = result.model_copy(update={'evidence': (*result.evidence, joined)})
        result = QueryResult.model_validate_json(result.model_dump_json())
        assert await s.admission.finish_read(s.run, s.token, s.generation, result)
        resource = work.resource
        await s.admission.resource_released(s.run, s.token, s.generation,
            runtime_incarnation=resource.runtime_incarnation, resource_kind=resource.resource_kind,
            resource_id=resource.resource_id, compute_claim=resource.compute_claim,
            dispatch_generation=resource.dispatch_generation)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=boundary), base_url='http://localhost') as client:
            worker = asyncio.create_task(native_once())
            try:
                response = await client.post('/mcp', json={'jsonrpc': '2.0', 'id': 'actual-source-client-id',
                    'method': 'tools/call', 'params': {'name': 'source_read', 'arguments': {}}},
                    headers={'accept': 'application/json, text/event-stream'})
                await asyncio.wait_for(worker, 3)
            finally:
                if not worker.done():
                    worker.cancel()
                    await asyncio.gather(worker, return_exceptions=True)
    assert response.status_code == 200
    envelope = response.json()
    assert envelope['id'] == 'actual-source-client-id'
    assert not s.gate._tickets and not s.gate._permits
    return envelope['result']


@pytest.mark.parametrize('scenario', ['cross-source', 'broad-top1-without-A', 'explicit-B'])
@pytest.mark.parametrize('phase', ['staged', 'accepted', 'durable'])
async def test_pending_withdrawal_between_native_result_and_http_issuance(vertical, scenario, phase):
    s = vertical
    async def change(s):
        command = await pending_withdrawal(s)
        if phase in ('accepted', 'durable'):
            await s.spool.seal(s.auth.p.admin, command.operation_key, writer_generation='writer-1',
                               predecessor=await s.spool.journal.head())
        if phase == 'durable':
            receipt = await s.spool.apply(command.operation_key, s.admin, writer_generation='writer-1')
            assert receipt.status == 'applied'
            await s.spool.publish_outcome(command.operation_key, writer_generation='writer-1')
    result = await request(s, source_ids=[B] if scenario == 'explicit-B' else None,
        limit=1 if scenario == 'broad-top1-without-A' else 3,
        composite=scenario == 'cross-source', after_result=change)
    assert not result['isError']
    payload = result['structuredContent']
    assert payload['observed_data_seq'] == 2 and payload['mode'] == 'hybrid'
    assert [parse_passage_fetch_id(item['id']).passage_id for item in payload['evidence']] == ['10000000-0000-0000-0000-000000000002']
    assert 'withdrawn-source-a' not in json.dumps(result)
    assert 'private withdrawn bibliography A' not in json.dumps(result)
    assert 'private cross-source A and B' not in json.dumps(result)
    if scenario == 'explicit-B':
        assert not payload['prior_write_pending']
        assert payload['reason'] != 'prior_write_pending'
    else:
        assert payload['status'] == 'degraded'
        assert payload['prior_write_pending'] is (phase != 'durable')
        assert payload['reason'] == ('projection_lag' if phase == 'durable' else 'prior_write_pending')
    await asyncio.to_thread(s.telemetry._queue.join)
    terminal = next(record for record in s.records if record.get('terminal'))
    assert terminal['status'] == payload['status']
    assert terminal['attempt_id'] == payload['read_trace']['attempt_id']


async def test_oauth_revocation_after_native_result_withholds_both_mcp_representations(vertical):
    s = vertical
    async def revoke(s):
        loaded = await s.auth.provider.load_access_token(s.auth.tokens.access_token)
        await s.auth.provider.revoke_token(loaded)
    result = await request(s, after_result=revoke)
    assert result['isError'] is True and 'structuredContent' not in result
    assert json.loads(result['content'][0]['text'])['reason'] == 'context_gap'
    assert 'safe-source-b' not in json.dumps(result) and 'withdrawn-source-a' not in json.dumps(result)
    await asyncio.to_thread(s.telemetry._queue.join)
    terminal = next(record for record in s.records if record.get('terminal'))
    assert terminal['status'] in ('denied', 'unavailable')


async def test_native_acl_pin_is_independent_of_search_path_and_timezone(vertical):
    s = vertical
    async with s.admin.transaction():
        await s.admin.execute("ALTER ROLE rkb_reader VALID UNTIL '2030-01-01 00:00:00+00'")
        await s.admin.execute("SET LOCAL search_path=public,rkb_v4,pg_catalog")
        await s.admin.execute("SET LOCAL timezone='UTC'")
        expected = await inspect_acl(s.admin)
        await s.admin.execute("SET LOCAL search_path=pg_catalog")
        await s.admin.execute("SET LOCAL timezone='Europe/Kaliningrad'")
        assert await inspect_acl(s.admin) == expected
