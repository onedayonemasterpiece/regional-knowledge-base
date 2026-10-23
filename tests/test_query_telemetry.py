"""Correlation/privacy/loss behavior with actual adapters and synthetic leases."""
import asyncio
from hashlib import sha256
import json
import sqlite3
import threading

import pytest

from regional_knowledge.query_telemetry import ReadRuntime, bind_read_runtime, telemetry_counts
from regional_knowledge.read_telemetry import ReadTelemetry
from regional_knowledge.kaggle_worker_query import KaggleWorkerQuery, WorkerQueryInputError
from test_kaggle_worker_query import Lease, frame, pinned, VECTOR, forbidden_acquire, forbidden_encode


def collector():
    records = []
    return ReadTelemetry(sink=lambda raw: records.append(json.loads(raw))), records


async def flush(sink):
    await asyncio.wait_for(asyncio.to_thread(sink._queue.join), 1)


@pytest.mark.asyncio
async def test_completed_native_search_returns_ok_and_existing_stages_in_same_reply():
    sink, records = collector()
    runtime = ReadRuntime('run-a', 'incarnation-a')
    request = frame()
    handler = KaggleWorkerQuery(acquire=Lease().acquire, tokenizer=pinned(),
        telemetry=asyncio.Queue(maxsize=8))
    async def encode(text):
        return VECTOR
    with bind_read_runtime(runtime):
        response = await handler(request, encode=encode)
    assert response['status'] == 'ok' and response['reason'] is None
    events = response.pop('read_events')
    sink.native_events(events, request_id=request['request_id'],
        run_id=runtime.run_id, runtime_incarnation=runtime.runtime_incarnation,
        connection_generation=4, turn_generation=9)
    await flush(sink)
    assert sink.failed == 0 and len(records) == 2
    event = next(record for record in records if record['terminal'])
    assert event['request_ref'] == sha256(request['request_id'].encode()).hexdigest()
    assert event['status'] == 'ok' and event['executed_branches'] == ['lexical', 'dense']
    assert {'encode_ms', 'lexical_ms', 'dense_ms', 'hydrate_ms'} <= event['stages_ms'].keys()
    assert request['query'] not in json.dumps(events)
    from regional_knowledge.kaggle_protocol import decode_envelope
    assert decode_envelope(json.dumps(response).encode()).status == 'ok'


@pytest.mark.asyncio
async def test_concurrent_identical_queries_join_public_attempts_to_exact_native_runtime():
    sink, public = collector()
    native = asyncio.Queue(maxsize=16)
    runtime = ReadRuntime('run-a', 'incarnation-a')
    attempts = [sink.begin_attempt(trace_id='a' * 32) for _ in range(2)]
    assert len({a.request_id for a in attempts}) == len({a.attempt_id for a in attempts}) == 2

    async def execute(attempt, turn):
        assert sink.bind_dispatch(attempt, runtime=runtime, connection_generation=4, turn_generation=turn)
        handler = KaggleWorkerQuery(acquire=Lease().acquire, tokenizer=pinned(), telemetry=native)
        private = 'same private query for concurrent calls'
        request = frame(request_id=attempt.request_id, turn_generation=turn,
                        query=private, compute_claim='private-compute-secret')
        async def encode(text):
            assert text == private
            await asyncio.sleep(0)
            return VECTOR
        with bind_read_runtime(runtime):
            response = await handler(request, encode=encode)
        # The same bounded existing events are carried with their own result.
        native_events.extend(response.pop('read_events'))
        assert sink.finish_attempt(attempt, status=response['status'], reason=response['reason'],
            executed_branches=('dense', 'lexical'), stages_ms={'search_ms': 1, 'total_ms': 2})
        assert not sink.finish_attempt(attempt, status='error', reason='internal_error')

    native_events = []
    await asyncio.gather(*(execute(a, n + 1) for n, a in enumerate(attempts)))
    await flush(sink)
    assert len(native_events) == 4
    for attempt, turn in zip(attempts, (1, 2)):
        assert attempt.request_ref == sha256(attempt.request_id.encode()).hexdigest()
        mapped = [e for e in public if e.get('attempt_id') == attempt.attempt_id]
        assert {e['trace_id'] for e in mapped} == {'a' * 32}
        assert len([e for e in mapped if e.get('terminal')]) == 1
        events = [e for e in native_events if e['request_ref'] == attempt.request_ref]
        assert len(events) == 2
        terminal = next(e for e in events if e['plane'] == 'adapter')
        assert terminal['terminal'] and terminal['status'] == 'ok'
        assert terminal['reason'] is None
        assert terminal['run_id'] == 'run-a' and terminal['runtime_incarnation'] == 'incarnation-a'
        assert (terminal['connection_generation'], terminal['turn_generation']) == (4, turn)
        assert terminal['executed_branches'] == ('lexical', 'dense')
        assert terminal['timing_semantics'] == 'overlapping_do_not_sum'
        assert all(e['diagnostic_state'] == 'observed' for e in events)
        stage = next(e for e in events if e['plane'] == 'executor_stage')
        assert not stage['terminal'] and 'status' not in stage
    wire = json.dumps(public + native_events)
    for private in ('same private query', 'private-compute-secret', 'verified-context-only', 'context_ref', 'auth_epoch'):
        assert private not in wire
    assert all(attempt.request_id not in wire for attempt in attempts)


@pytest.mark.asyncio
async def test_retry_reconnect_new_attempt_never_overwrites_original_scope():
    sink, records = collector()
    old = sink.begin_attempt(trace_id='b' * 32)
    new = sink.begin_attempt(trace_id=old.trace_id)
    assert sink.bind_dispatch(old, runtime=ReadRuntime('run-a', 'incarnation-a'), connection_generation=1, turn_generation=1)
    assert not sink.bind_dispatch(old, runtime=ReadRuntime('run-a', 'incarnation-a'), connection_generation=2, turn_generation=1)
    assert sink.finish_attempt(old, status='cancelled', reason='disconnected')
    assert sink.bind_dispatch(new, runtime=ReadRuntime('run-a', 'incarnation-a'), connection_generation=2, turn_generation=2)
    assert sink.finish_attempt(new, status='degraded', reason='bounded_retrieval')
    await flush(sink)
    terminals = [r for r in records if r.get('terminal')]
    assert len(terminals) == 2 and len({r['request_ref'] for r in terminals}) == 2
    prior = next(r for r in terminals if r['attempt_id'] == old.attempt_id)
    assert prior['connection_generation'] == 1 and prior['diagnostic_state'] == 'inconclusive'
    current = next(r for r in terminals if r['attempt_id'] == new.attempt_id)
    assert current['connection_generation'] == 2 and current['diagnostic_state'] == 'observed'
    assert old.correlation()['trace_id'] == new.correlation()['trace_id']


@pytest.mark.asyncio
@pytest.mark.parametrize('status,reason', [('denied', 'access_denied'), ('cancelled', 'caller_cancelled')])
async def test_controller_refusal_or_cancel_before_native_dispatch_has_one_terminal(status, reason):
    sink, records = collector()
    attempt = sink.begin_attempt()
    assert sink.finish_attempt(attempt, status=status, reason=reason)
    await flush(sink)
    terminal = next(r for r in records if r.get('terminal'))
    assert terminal['status'] == status and terminal['reason'] == reason
    assert terminal['dispatch_state'] == 'not_dispatched' and 'run_id' not in terminal
    assert terminal['executed_branches'] == () or terminal['executed_branches'] == []


@pytest.mark.asyncio
async def test_actual_adapter_terminal_overrides_executor_stage_after_serialization_failure(monkeypatch):
    import regional_knowledge.kaggle_worker_query as adapter
    wire = adapter._wire
    def failed_after_search(result):
        if result.status in ('ok', 'degraded'):
            raise ValueError('private serialization failure')
        return wire(result)
    monkeypatch.setattr(adapter, '_wire', failed_after_search)
    native = asyncio.Queue(maxsize=8)
    handler = KaggleWorkerQuery(acquire=Lease().acquire, tokenizer=pinned(), telemetry=native)
    async def encode(_):
        return VECTOR
    with bind_read_runtime(ReadRuntime('run-a', 'incarnation-a')):
        response = await handler(frame(), encode=encode)
    assert response['status'] == 'unavailable' and response['reason'] == 'internal_error'
    events = response.get('read_events', []) + [native.get_nowait() for _ in range(native.qsize())]
    assert events[0]['plane'] == 'executor_stage' and not events[0]['terminal']
    assert events[-1]['plane'] == 'adapter' and events[-1]['terminal']
    assert events[-1]['status'] == 'unavailable' and events[-1]['reason'] == 'internal_error'
    assert events[-1]['executed_branches'] == ('lexical', 'dense')
    assert 'private serialization' not in repr(events)


@pytest.mark.asyncio
async def test_native_early_parse_refusal_and_invalid_identity_never_echo_private_payload():
    native = asyncio.Queue(maxsize=8)
    handler = KaggleWorkerQuery(acquire=forbidden_acquire, tokenizer=pinned(), telemetry=native)
    private = 'private bearer grant query body'
    with bind_read_runtime(ReadRuntime('run-a', 'incarnation-a')):
        reply = await handler(frame(query=private, extra=private), encode=forbidden_encode)
        with pytest.raises(WorkerQueryInputError):
            await handler({'request_id': private, 'connection_generation': True, 'query': private}, encode=forbidden_encode)
    assert reply['status'] == 'invalid_query'
    events = reply.get('read_events', []) + [native.get_nowait() for _ in range(native.qsize())]
    assert len(events) == 2 and all(e['terminal'] and e['reason'] == 'invalid_input' for e in events)
    assert events[0]['request_ref'] == sha256(b'synthetic-read').hexdigest()
    assert events[1]['request_ref'] is None and events[1]['diagnostic_state'] == 'inconclusive'
    assert all(e['executed_branches'] == () for e in events)
    assert private not in json.dumps(events)


@pytest.mark.asyncio
async def test_cancelled_before_sql_has_adapter_terminal_without_claiming_compute_death():
    entered = asyncio.Event()
    async def count(_):
        entered.set()
        await asyncio.Event().wait()
    native = asyncio.Queue(maxsize=8)
    handler = KaggleWorkerQuery(acquire=forbidden_acquire, tokenizer=pinned(count), telemetry=native)
    async def run():
        with bind_read_runtime(ReadRuntime('run-a', 'incarnation-a')):
            return await handler(frame(), encode=forbidden_encode)
    task = asyncio.create_task(run())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    events = [native.get_nowait() for _ in range(native.qsize())]
    assert events[-1]['status'] == 'cancelled' and events[-1]['reason'] == 'consumer_cancelled'
    assert events[-1]['executed_branches'] == ()
    assert 'resource_released' not in json.dumps(events)


@pytest.mark.asyncio
async def test_native_loss_counters_and_unknown_runtime_are_explicitly_inconclusive():
    native = asyncio.Queue(maxsize=1)
    native.put_nowait({'occupied': True})
    handler = KaggleWorkerQuery(acquire=Lease().acquire, tokenizer=pinned(), telemetry=native)
    async def encode(_):
        return VECTOR
    assert (await handler(frame(), encode=encode))['mode'] == 'hybrid'
    assert telemetry_counts(native) == {'emitted': 0, 'dropped': 2, 'failed': 0}
    assert native.get_nowait() == {'occupied': True}
    # A later early refusal can be retained and exposes the previous losses.
    reply = await handler(frame(extra='not logged'), encode=encode)
    assert reply['status'] == 'invalid_query'
    event = native.get_nowait()
    assert event['diagnostic_state'] == 'inconclusive' and event['loss_before']['dropped'] == 2
    assert 'run_id' not in event
    assert telemetry_counts(native) == {'emitted': 1, 'dropped': 2, 'failed': 0}


@pytest.mark.asyncio
async def test_public_attempt_queue_loss_cannot_replace_outcomes():
    entered, finish = threading.Event(), threading.Event()
    records = []
    def sink(raw):
        records.append(json.loads(raw))
        entered.set()
        assert finish.wait(2)
    diagnostics = ReadTelemetry(capacity=1, sink=sink)
    try:
        first = diagnostics.begin_attempt()
        assert await asyncio.to_thread(entered.wait, 1)
        assert diagnostics.finish_attempt(first, status='denied', reason='access_denied')
        second = diagnostics.begin_attempt()
        assert diagnostics.finish_attempt(second, status='cancelled', reason='caller_cancelled')
        assert diagnostics.dropped == 2
    finally:
        finish.set()
    await flush(diagnostics)
    third = diagnostics.begin_attempt()
    assert diagnostics.finish_attempt(third, status='unavailable', reason='arbitrary private reason',
        stages_ms={'secret-query': 123, 'total_ms': float('nan')})
    await flush(diagnostics)
    observed = [r for r in records if r.get('attempt_id') == third.attempt_id]
    assert observed and all(r['diagnostic_state'] == 'inconclusive' for r in observed)
    assert 'arbitrary private reason' not in json.dumps(records)


@pytest.mark.asyncio
async def test_sink_failure_is_reported_by_next_successfully_consumed_event():
    records = []
    def sink(raw):
        if not records:
            records.append(None)
            raise OSError('private sink error')
        records.append(json.loads(raw))
    diagnostics = ReadTelemetry(sink=sink)
    attempt = diagnostics.begin_attempt()
    await flush(diagnostics)
    assert diagnostics.failed == 1
    assert diagnostics.finish_attempt(attempt, status='denied', reason='access_denied')
    await flush(diagnostics)
    assert records[1]['loss_before']['failed'] == 1
    assert records[1]['diagnostic_state'] == 'inconclusive'
    assert records[1]['status'] == 'denied'


@pytest.mark.asyncio
async def test_adapter_deadline_after_serialization_is_the_final_terminal(monkeypatch):
    import regional_knowledge.kaggle_worker_query as adapter
    loop = asyncio.get_running_loop()
    original_time, original_wire = loop.time, adapter._wire
    expired = False
    def serialize(result):
        nonlocal expired
        encoded = original_wire(result)
        expired = True
        return encoded
    monkeypatch.setattr(adapter, '_wire', serialize)
    monkeypatch.setattr(loop, 'time', lambda: original_time() + (3 if expired else 0))
    native = asyncio.Queue(maxsize=8)
    handler = KaggleWorkerQuery(acquire=Lease().acquire, tokenizer=pinned(), telemetry=native)
    async def encode(_):
        return VECTOR
    with bind_read_runtime(ReadRuntime('run-a', 'incarnation-a')):
        result = await handler(frame(), encode=encode)
    assert result['status'] == 'unavailable' and result['reason'] == 'deadline_exceeded'
    events = result.get('read_events', []) + [native.get_nowait() for _ in range(native.qsize())]
    assert len(events) == 2 and not events[0]['terminal']
    assert events[1]['terminal'] and events[1]['reason'] == result['reason']


@pytest.mark.asyncio
async def test_read_diagnostics_do_not_touch_held_sqlite_writer(tmp_path, monkeypatch):
    held = sqlite3.connect(tmp_path / 'held-control.sqlite')
    held.execute('CREATE TABLE fixture(id INTEGER)')
    held.execute('BEGIN IMMEDIATE')
    held.execute('INSERT INTO fixture VALUES (1)')
    sink, records = collector()
    def forbidden(*args, **kwargs):
        raise AssertionError('telemetry attempted a database connection')
    monkeypatch.setattr(sqlite3, 'connect', forbidden)
    try:
        async def operation():
            attempt = sink.begin_attempt()
            sink.bind_dispatch(attempt, runtime=ReadRuntime('run-a', 'incarnation-a'), connection_generation=1, turn_generation=1)
            sink.finish_attempt(attempt, status='unavailable', reason='not_ready')
        await asyncio.wait_for(operation(), .1)
        await flush(sink)
        assert any(record.get('terminal') for record in records)
    finally:
        held.rollback()
        held.close()


@pytest.mark.asyncio
async def test_native_result_drain_is_exact_generation_bounded_and_payload_free():
    from regional_knowledge.query_telemetry import NativeReadTelemetry,drain_native_read_events,clean_native_read_events
    native=asyncio.Queue(maxsize=64);runtime=ReadRuntime('run-a','inc-a')
    request=dict(request_id='same-native-request',connection_generation=2,turn_generation=3)
    with bind_read_runtime(runtime):
        with NativeReadTelemetry(native,dict(request,connection_generation=1)) as event:
            event.finish(status='cancelled',reason='consumer_cancelled')
        with NativeReadTelemetry(native,request) as event:
            event.stages={'encode_ms':12.,'lexical_ms':3.,'dense_ms':4.}
            event.finish(status='degraded',reason='bounded_retrieval')
    values=drain_native_read_events(native,request)
    assert native.empty() and len(values)==1 and values[0]['connection_generation']==2
    assert values[0]['diagnostic_state']=='inconclusive' and values[0]['loss_before']['dropped']==1
    identity=dict(request_id=request['request_id'],run_id='run-a',runtime_incarnation='inc-a',
        connection_generation=2,turn_generation=3)
    assert clean_native_read_events(values,**identity)[0]['stages_ms']['encode_ms']==12.
    with pytest.raises(ValueError):clean_native_read_events([dict(values[0],private_query='SECRET')],**identity)
    with pytest.raises(ValueError):clean_native_read_events(values,**dict(identity,runtime_incarnation='other'))
    with pytest.raises(ValueError):clean_native_read_events(values*5,**identity)
