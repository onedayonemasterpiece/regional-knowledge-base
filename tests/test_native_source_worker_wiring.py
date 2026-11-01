"""Native14 notebook/worker wiring, without PG, weights, or provider effects.

Commands and accepted seals use the real contracts and RecoveryJournal memory
fixture. Only apply_accepted's PG boundary is replaced; these are not native SQL
acceptance or live model/readiness tests. Packaging runs in a fresh interpreter
so ambient imported modules cannot conceal a missing notebook dependency.
"""
from __future__ import annotations

import asyncio
import base64
import copy
import json
from pathlib import Path
import subprocess
import site
import sys
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_current_registry import current_registry as fixed_registry
from regional_knowledge.kaggle_apply import ApplyReceipt
from regional_knowledge.kaggle_bootstrap import ResidentEncoder
from regional_knowledge.kaggle_native_source_bindings import resolved_native_stage_command
from regional_knowledge.kaggle_source_commands import (
    ControllerAcceptedProof, WorkerSourceExecutor, decode_receipt, seal_wire,
)
from regional_knowledge.kaggle_source_materializer import (
    NativeSourceMaterializer, SourceMaterializationRejected, materializer_fingerprint,
)
from regional_knowledge.kaggle_target_launch import source_bundle
from regional_knowledge.kaggle_target_runtime import TargetRuntime
from regional_knowledge.recovery_journal import RecoveryJournal
from regional_knowledge.source_activation import (
    ACTIVATION_CODEC, ACTIVATION_REVISIONS, ACTIVATE_ROUTE, MATERIALIZE_CODEC,
    MATERIALIZE_ROUTE, resolved_activation_command, resolved_materialize_command,
)
from regional_knowledge.source_stage import SEAL_CODEC, SEAL_ROUTE, STAGE_ROUTE, canonical, digest
from test_recovery_journal import ConditionalMemoryStore
from test_source_stage import SOURCE, INGESTION, REQUEST, batch

PERSON = '00000000-0000-0000-0000-000000000099'


def test_fresh_notebook_bundle_imports_actual_native_compiler_and_bindings(tmp_path):
    repository = Path(__file__).resolve().parents[1]
    relative, encoded, entries = source_bundle(repository)
    root = tmp_path / relative
    for name, body in encoded.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body.encode('utf-8'))
    assert len(entries) == len(encoded)
    for entry in entries:
        raw = (tmp_path / entry['name']).read_bytes()
        assert entry['role'] == 'code'
        assert len(raw) == entry['size'] and digest(raw) == entry['sha256']
    expected = materializer_fingerprint()
    # -I excludes PYTHONPATH/cwd; the explicit materialized tree is the only
    # source of regional_knowledge, while reviewed installed dependencies remain.
    program = r'''
import importlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1]).resolve()
sys.path.extend(json.loads(sys.argv[2]))
sys.path.insert(0, str(root))
from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler, materializer_fingerprint
from regional_knowledge.kaggle_native_source_bindings import NATIVE_ROUTES, validate_native_source_command
from regional_knowledge.kaggle_source_commands import WorkerSourceExecutor
from regional_knowledge.kaggle_target_runtime import TargetRuntime
assert callable(NativeSourceCompiler.compile) and callable(validate_native_source_command)
assert {'source.stage.v1', 'source.stage.seal.v1', 'source.materialize.v1', 'source.activate.v1'} <= NATIVE_ROUTES
assert callable(WorkerSourceExecutor.__call__) and callable(TargetRuntime._native_source_materializer)
loaded = {}
for name, module in tuple(sys.modules.items()):
    if name == 'regional_knowledge' or name.startswith('regional_knowledge.'):
        path = pathlib.Path(module.__file__).resolve()
        assert path.is_relative_to(root), (name, str(path))
        loaded[name] = str(path.relative_to(root))
assert 'regional_knowledge.source_graph_native' in loaded
assert 'regional_knowledge.source_activation' in loaded
print(json.dumps({'fingerprint':materializer_fingerprint(), 'loaded':loaded}, sort_keys=True))
'''
    completed = subprocess.run([sys.executable, '-I', '-c', program, str(root),
        json.dumps([site.getusersitepackages(), *site.getsitepackages()])],
        cwd=tmp_path, capture_output=True, text=True, timeout=30, check=False)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    observed = json.loads(completed.stdout)
    assert observed['fingerprint'] == expected
    assert all(name in encoded for name in observed['loaded'].values())


def retained_encoder():
    """An exact ResidentEncoder object with synthetic in-memory handles only.

    Deliberately bypasses artifact loading; this tests reuse, not verification.
    The production count/call/locking implementations themselves remain real.
    """
    counted, forwarded = [], []

    def tokenize(text, **kwargs):
        counted.append((text, kwargs))
        return [1, 2, 3]

    vector = [0.0] * 1024

    def encode(text):
        forwarded.append(text)
        return vector

    encoder = object.__new__(ResidentEncoder)
    object.__setattr__(encoder, 'encode', encode)
    object.__setattr__(encoder, 'tokenizer', SimpleNamespace(encode=tokenize))
    object.__setattr__(encoder, '_counter', encoder.tokenizer)
    object.__setattr__(encoder, '_tokenizer_lock', threading.Lock())
    object.__setattr__(encoder, 'forward_lock', asyncio.Lock())
    return encoder, counted, forwarded, vector


@pytest.mark.asyncio
async def test_runtime_native14_retains_exact_encoder_and_observes_current_closed():
    runtime = object.__new__(TargetRuntime)
    runtime.database = SimpleNamespace(schema_version=14)
    runtime.target={'registry_sha256':'a'*64}
    runtime.closed = False
    encoder, counted, forwarded, vector = retained_encoder()
    materializer = runtime._native_source_materializer(encoder)
    assert type(materializer) is NativeSourceMaterializer
    assert materializer.count.__self__ is encoder
    assert materializer.current() is True
    assert await materializer.count('selected model transcription') == 3
    assert counted == [('selected model transcription', dict(add_special_tokens=True, truncation=False, padding=False))]
    assert await materializer.encode('same native input') is vector
    assert forwarded == ['same native input']
    materializer._current()
    runtime.closed = True
    assert materializer.current() is False
    with pytest.raises(PermissionError, match='owner_changed'):
        materializer._current()


@pytest.mark.parametrize('schema', [11,12,13])
def test_runtime_does_not_install_native14_materializer_for_another_profile(schema):
    runtime = object.__new__(TargetRuntime)
    runtime.database = SimpleNamespace(schema_version=schema)
    runtime.target={'registry_sha256':'a'*64}
    runtime.closed = False
    # An arbitrary object would fail if the resident factory were reached.
    assert runtime._native_source_materializer(object()) is None


def test_runtime_native14_refuses_an_unverified_encoder_type():
    runtime = object.__new__(TargetRuntime)
    runtime.database = SimpleNamespace(schema_version=14)
    runtime.target={'registry_sha256':'a'*64}
    runtime.closed = False
    with pytest.raises(TypeError, match='same verified resident'):
        runtime._native_source_materializer(SimpleNamespace(count=lambda text:3))


def exact_command(route):
    args = dict(sequence=1, operation_key='native-wiring:' + route, request_id=REQUEST,
        person_id=PERSON, logical_time='2026-10-21T19:00:00Z', source_id=SOURCE)
    chain = dict(ingestion_id=INGESTION, batch_count=1, last_batch_sha256='c'*64)
    if route == SEAL_ROUTE:
        return resolved_native_stage_command(route=route, **args, payload=dict(codec=SEAL_CODEC, **chain))
    if route == STAGE_ROUTE:
        return resolved_native_stage_command(route=route, **args, payload=batch(count=1))
    payload = dict(codec=ACTIVATION_CODEC, **chain, materializer_sha256=materializer_fingerprint())
    if route == MATERIALIZE_ROUTE:
        return resolved_materialize_command(**args, payload=dict(payload, codec=MATERIALIZE_CODEC, ordinal=0))
    return resolved_activation_command(**args, payload=payload)


async def worker_fixture(route, monkeypatch, *, materializer_error=None, status='applied'):
    command = exact_command(route)
    database = SimpleNamespace(database_generation='synthetic-native14-db', writer_generation='writer-1',
        schema_revision=ACTIVATION_REVISIONS.schema, function_revision=ACTIVATION_REVISIONS.function,
        executor_revision=ACTIVATION_REVISIONS.executor)
    registry = fixed_registry()
    journal = RecoveryJournal(ConditionalMemoryStore(), stream='native-wiring-only')
    seal = await journal.seal([command], predecessor=None, writer_generation='writer-1')
    command_hash = await journal.verify_accepted(seal, command)
    handler = registry.resolve(command)
    events, calls = [], []
    connection = object()

    @asynccontextmanager
    async def acquire():
        events.append('acquire')
        yield connection

    async def prehook(observed, *, acquire: object, database: object):
        events.append('prehook')
        assert observed.encode() == command.encode()
        assert acquire is worker.acquire and database is worker.database
        if materializer_error is not None:
            raise materializer_error

    original_verify = ControllerAcceptedProof.verify_accepted

    async def verify(proof, captured_seal, captured_command):
        events.append('proof')
        return await original_verify(proof, captured_seal, captured_command)

    monkeypatch.setattr(ControllerAcceptedProof, 'verify_accepted', verify)

    async def apply_boundary(observed_connection, **kwargs):
        events.append('apply')
        calls.append(kwargs)
        assert observed_connection is connection
        assert kwargs['command'].encode() == command.encode() and kwargs['seal'] == seal
        assert kwargs['registry'] is registry
        assert kwargs['database_generation'] == database.database_generation
        assert kwargs['writer_generation'] == database.writer_generation
        assert kwargs['max_outcome_bytes'] == 8192
        assert await kwargs['journal'].verify_accepted(kwargs['seal'], kwargs['command']) == command_hash
        result = canonical({'accepted': status == 'applied'})
        outcome = canonical(dict(codec='rkb.applied-outcome.v1', operation_key=command.operation_key,
            command_sha256=command_hash, cursor=command.sequence, status=status, ids=command.ids,
            request_id=command.request_id, person_id=command.person_id, route=command.route,
            logical_time=command.logical_time, before={}, after={}, handler_sha256=handler.fingerprint,
            result_b64=base64.b64encode(result).decode(), result_sha256=digest(result)))
        return ApplyReceipt(database.database_generation, command.operation_key, command.sequence,
            command_hash, status, outcome, digest(outcome), False)

    monkeypatch.setattr('regional_knowledge.kaggle_source_commands.apply_accepted', apply_boundary)
    worker = WorkerSourceExecutor(acquire=acquire, registry=registry, database=database,
        run_id='synthetic-run', launch_nonce='synthetic-nonce', runtime_incarnation='synthetic-incarnation',
        materializer=prehook)
    # Same real captured accepted frame shape as test_source_import's PG fixture.
    frame = dict(type='DISPATCH', protocol_version=2, connection_generation=1, id='accepted-job',
        claim='durable-claim', kind='source_command', run_id='synthetic-run', launch_nonce='synthetic-nonce',
        runtime_incarnation='synthetic-incarnation', database_generation=database.database_generation,
        writer_generation=database.writer_generation, command_sha256=command_hash,
        handler_sha256=handler.fingerprint, command_b64=base64.b64encode(command.encode()).decode(),
        accepted=seal_wire(seal), compute_claim='actual-resource-claim', dispatch_generation=1)
    return SimpleNamespace(worker=worker, frame=frame, command=command, seal=seal, journal=journal,
        events=events, calls=calls, command_hash=command_hash)


@pytest.mark.asyncio
@pytest.mark.parametrize('route', [SEAL_ROUTE, MATERIALIZE_ROUTE])
async def test_native14_prehook_follows_exact_captured_proof_and_precedes_ordinary_apply(route, monkeypatch):
    value = await worker_fixture(route, monkeypatch)
    receipt, outcome = decode_receipt(await value.worker(value.frame))
    assert value.events == ['proof', 'prehook', 'acquire', 'apply', 'proof']
    assert receipt.status == outcome['status'] == 'applied'
    assert receipt.command_sha256 == value.command_hash and len(value.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('route', [STAGE_ROUTE, ACTIVATE_ROUTE])
async def test_stage_and_activate_are_ordinary_apply_without_materializer_prehook(route, monkeypatch):
    value = await worker_fixture(route, monkeypatch)
    decode_receipt(await value.worker(value.frame))
    assert value.events == ['acquire', 'apply', 'proof']
    assert len(value.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('route', [SEAL_ROUTE, MATERIALIZE_ROUTE])
@pytest.mark.parametrize('changed', [
    'run_id', 'launch_nonce', 'runtime_incarnation', 'database_generation', 'writer_generation',
    'handler_sha256', 'command_sha256', 'command_b64', 'accepted', 'dispatch_generation',
])
async def test_invalid_captured_binding_cannot_reach_private_prehook_or_pg(route, changed, monkeypatch):
    value = await worker_fixture(route, monkeypatch)
    frame = copy.deepcopy(value.frame)
    if changed == 'command_b64':
        document = json.loads(value.command.encode())
        document['request_id'] = 'different-captured-request'
        frame[changed] = base64.b64encode(canonical(document)).decode()
    elif changed == 'accepted':
        frame[changed]['command_hashes'] = ['0'*64]
    elif changed == 'dispatch_generation':
        frame[changed] = frame['connection_generation'] + 1
    else:
        frame[changed] = '0'*64 if changed.endswith('sha256') else 'different-binding'
    with pytest.raises((ValueError, PermissionError)):
        await value.worker(frame)
    assert value.events == [] and value.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize('route', [SEAL_ROUTE, MATERIALIZE_ROUTE])
async def test_failed_exact_accepted_proof_is_not_skipped_before_prehook(route, monkeypatch):
    value = await worker_fixture(route, monkeypatch)

    async def denied(*args):
        raise PermissionError('synthetic captured accepted proof revoked')

    monkeypatch.setattr(ControllerAcceptedProof, 'verify_accepted', denied)
    with pytest.raises(PermissionError, match='captured accepted proof'):
        await value.worker(value.frame)
    assert value.events == [] and value.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize('route', [SEAL_ROUTE, MATERIALIZE_ROUTE])
@pytest.mark.parametrize('reason', ['native_source_stage_requires_review',
    'complete_native_input_exceeds_512_tokens:chunk_key=Page-0.body:tokens=513'])
async def test_content_rejection_uses_ordinary_apply_and_preserves_rejected_receipt(route, monkeypatch, reason):
    value = await worker_fixture(route, monkeypatch,
        materializer_error=SourceMaterializationRejected(reason),
        status='rejected_precondition')
    receipt, outcome = decode_receipt(await value.worker(value.frame))
    assert value.events == ['proof', 'prehook', 'acquire', 'apply', 'proof']
    assert len(value.calls) == 1 and receipt.status == outcome['status'] == 'rejected_precondition'
    assert value.calls[0]['reject_reason'] == reason
    assert json.loads(base64.b64decode(outcome['result_b64'])) == {'accepted': False}


@pytest.mark.asyncio
async def test_review_rejection_survives_real_apply_validation_and_unknown_reason_is_denied(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from psycopg.pq import TransactionStatus
    from psycopg.rows import tuple_row
    from regional_knowledge.kaggle_apply import apply_accepted

    reason = 'native_source_stage_requires_review'
    value = await worker_fixture(MATERIALIZE_ROUTE, monkeypatch,
        materializer_error=SourceMaterializationRejected(reason))
    database = value.worker.database
    handler = value.worker.registry.resolve(value.command)
    inserts = []

    class Connection:
        closed = False
        row_factory = staticmethod(tuple_row)
        info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)

        @asynccontextmanager
        async def transaction(self):
            yield

        async def execute(self, query, params=None):
            row = None
            if 'SELECT database_generation' in query:
                row = (database.database_generation, database.writer_generation, 0)
            elif 'FROM pg_proc' in query:
                row = ('boolean' if params[0] == handler.check.signature else 'bytea',
                       's', False, 'sql', False, 'f')
            elif 'INSERT INTO rkb_v4.applied_ops' in query:
                inserts.append(params)
            else:
                assert query.startswith(('SELECT set_config', 'SELECT operation_key,sequence',
                                         'UPDATE rkb_v4.recovery_state'))
            return SimpleNamespace(fetchone=AsyncMock(return_value=row),
                                   fetchall=AsyncMock(return_value=[]))

    @asynccontextmanager
    async def acquire():
        yield Connection()

    value.worker.acquire = acquire
    monkeypatch.setattr('regional_knowledge.kaggle_source_commands.apply_accepted', apply_accepted)
    receipt, outcome = decode_receipt(await value.worker(value.frame))
    assert receipt.status == 'rejected_precondition'
    assert json.loads(base64.b64decode(outcome['result_b64'])) == {'reason': reason}
    assert len(inserts) == 1 and inserts[0][8] == receipt.outcome
    for unknown in ('native_source_unknown_review', reason + ': private text'):
        with pytest.raises(ValueError, match='fixed internal source rejection reason'):
            await apply_accepted(object(), journal=value.journal, seal=value.seal,
                command=value.command, database_generation=database.database_generation,
                writer_generation=database.writer_generation, registry=value.worker.registry,
                reject_reason=unknown)


@pytest.mark.asyncio
@pytest.mark.parametrize('route', [SEAL_ROUTE, MATERIALIZE_ROUTE])
@pytest.mark.parametrize('error_type', [PermissionError])
async def test_unknown_network_and_revoked_owner_are_not_content_rejections(route, error_type, monkeypatch):
    error = error_type('synthetic unavailable or revoked, not invalid model input')
    value = await worker_fixture(route, monkeypatch, materializer_error=error)
    with pytest.raises(error_type) as caught:
        await value.worker(value.frame)
    assert caught.value is error
    assert value.events == ['proof', 'prehook'] and value.calls == []
