"""Actual worker-main summaries; no model, provider or native database."""
import asyncio
import json
from pathlib import Path

import pytest

from test_bge_shared_worker import Peer
from test_bge_wss_worker import CONFIG, module


def configured_worker(monkeypatch, tmp_path):
    worker = module('bge_kaggle_worker')
    summary = tmp_path / 'bge-run-summary.json'
    monkeypatch.setattr(worker, 'RUN_CONFIG', dict(CONFIG, transport='wss'))
    monkeypatch.setattr(worker, 'Path', lambda path:
                        summary if str(path) == '/kaggle/working/bge-run-summary.json' else Path(path))
    return worker, summary


@pytest.mark.parametrize('kind', ['connection', 'stopped', 'cancelled', 'long_class'])
def test_main_reports_raised_or_cancelled_and_reraises_same_exception(monkeypatch, tmp_path, kind):
    worker, path = configured_worker(monkeypatch, tmp_path)
    transport = module('bge_wss_transport')
    classes = dict(connection=ConnectionError, stopped=transport.BgeChannelStopped,
                   cancelled=asyncio.CancelledError,
                   long_class=type('X' * 100, (RuntimeError,), {}))
    private = 'PRIVATE https://private.invalid/?token=SECRET private payload'
    error = classes[kind](private)

    def failing_transport(*args, **kwargs):
        raise error

    monkeypatch.setattr(worker, 'run_wss_worker', failing_transport, raising=False)
    with pytest.raises(type(error)) as caught:
        worker.main()
    assert caught.value is error
    raw = path.read_text()
    summary = json.loads(raw)
    assert summary['transport_outcome'] == ('cancelled' if kind == 'cancelled' else 'raised')
    assert summary['error_type'] == ('BaseException' if kind == 'long_class' else type(error).__name__)
    assert len(summary['error_type']) <= 80
    assert summary['cooperative_worker_exit'] is True  # Legacy finally-reached flag.
    assert summary['provider_termination_verified'] is False
    assert summary['run_id'] == CONFIG['run_id'] and summary['transport'] == 'wss'
    assert summary['worker_wall_seconds'] >= 0
    assert not any(value in raw for value in (private, 'PRIVATE', 'SECRET', CONFIG['token'], CONFIG['channel']))


def test_main_reports_returned_after_actual_exact_idle_stop_and_runtime_close(monkeypatch, tmp_path):
    worker, path = configured_worker(monkeypatch, tmp_path)
    transport = module('bge_wss_transport')
    closed = []

    def run_transport(config, load_encoder, diagnostics, **kwargs):
        async def scenario():
            peer = Peer()
            async def close():
                closed.append(True)
            control = transport.BgeWssTransport(config, lambda: lambda _: [1.] + [0.] * 1023,
                lambda: {}, connector=peer.connect, close_runtime=close,
                heartbeat_seconds=.01, maximum_seconds=3)
            running = asyncio.create_task(control.run())
            try:
                await peer.wait('ATTACH')
                await peer.put(dict(type='STOP', run_id=config['run_id'],
                    launch_nonce=config['launch_nonce'], runtime_incarnation=control.incarnation))
                await asyncio.wait_for(running, 2)
                assert control.stop.is_set() and peer.sessions == 1
            finally:
                if not running.done():
                    running.cancel()
                    await asyncio.gather(running, return_exceptions=True)
        asyncio.run(scenario())

    monkeypatch.setattr(worker, 'run_wss_worker', run_transport, raising=False)
    assert worker.main() is None
    summary = json.loads(path.read_text())
    assert closed == [True]
    assert summary['transport_outcome'] == 'returned'
    assert 'error_type' not in summary
    assert summary['cooperative_worker_exit'] is True
    assert summary['provider_termination_verified'] is False
