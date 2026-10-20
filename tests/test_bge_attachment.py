"""SQLite authority at HTTP/WSS ownership, reconnect and lost-ACK boundaries."""
import concurrent.futures
import sqlite3
import threading

import pytest

from regional_knowledge.bge_contract import SPACE
from regional_knowledge.bge_queue import (
    AttachmentBusy, AttachmentConflict, BgeQueue, RevokedRun, StaleAttachment,
)


def prepared(tmp_path, *, provider=True):
    now = [1000.0]
    queue = BgeQueue(tmp_path / 'queue.sqlite', clock=lambda: now[0])
    job_id = queue.enqueue('actor', 'document', ['synthetic passage'], kind='document')
    with queue.connect(write=False) as db:
        run = db.execute('select id from runs').fetchone()[0]
    token = (queue.path.parent / 'bge-worker-credentials' / run).read_text()
    nonce = queue.prepare_channel(run)
    assert queue.prepare_channel(run) == nonce
    if provider:
        assert queue.launch_claim(run) == token
        assert queue.prepare_channel(run) == nonce
        queue.launch_record(run, 'fixture-owner/stable-slug', provider_version=7)
    return queue, run, token, nonce, job_id, now


def complete(queue, run, token, job, generation):
    return queue.complete(run, token, job['id'], job['claim'], SPACE,
                          [[1] + [0] * 1023], {}, connection_generation=generation)


def test_two_connections_reconnect_and_stale_disconnect_are_fenced(tmp_path):
    queue, run, token, nonce, _, now = prepared(tmp_path)
    other = BgeQueue(queue.path, clock=lambda: now[0])
    first = queue.attach_channel(run, token, nonce, 'runtime-a')
    assert first == {'connection_generation': 1, 'provider_version': '7'}
    queue.heartbeat(run, token, ready=True, connection_generation=1)
    assert queue.channel_status(run, token, 1)['ready']
    job = queue.claim(run, token, connection_generation=1)
    second = other.attach_channel(run, token, nonce, 'runtime-a')
    assert second['connection_generation'] == 2
    for action in (
        lambda: queue.channel_status(run, token, 1),
        lambda: queue.claim(run, token, connection_generation=1),
        lambda: complete(queue, run, token, job, 1),
        lambda: queue.heartbeat(run, token, connection_generation=1),
        lambda: queue.claim(run, token),
        lambda: complete(queue, run, token, job, None),
    ):
        with pytest.raises(StaleAttachment):
            action()
    assert not queue.detach_channel(run, token, 1)
    assert other.channel_status(run, token, 2)['active']
    assert other.detach_channel(run, token, 2)
    with pytest.raises(StaleAttachment):
        other.claim(run, token, connection_generation=2)
    with pytest.raises(StaleAttachment):
        queue.claim(run, token)
    assert queue.attach_channel(run, token, nonce, 'runtime-a')['connection_generation'] == 3
    assert complete(queue, run, token, job, 3) == 'accepted'


def test_competing_incarnation_persists_conflict_and_blocks_both(tmp_path, caplog):
    queue, run, token, nonce, _, now = prepared(tmp_path)
    queue.attach_channel(run, token, nonce, 'runtime-a')
    queue.heartbeat(run, token, ready=True)
    job = queue.claim(run, token, connection_generation=1)
    with pytest.raises(AttachmentConflict):
        queue.attach_channel(run, token, nonce, 'runtime-b')
    reopened = BgeQueue(queue.path, clock=lambda: now[0])
    for action in (
        lambda: reopened.attach_channel(run, token, nonce, 'runtime-a'),
        lambda: reopened.attach_channel(run, token, nonce, 'runtime-b'),
        lambda: reopened.claim(run, token, connection_generation=1),
        lambda: reopened.claim(run, token),
        lambda: reopened.channel_status(run, token, 1),
        lambda: reopened.resume_channel_claim(run, token, 1),
        lambda: complete(reopened, run, token, job, 1),
        lambda: complete(reopened, run, token, job, None),
    ):
        with pytest.raises(AttachmentConflict):
            action()
    with reopened.connect(write=False) as db:
        row = db.execute('select * from runs where id=?', (run,)).fetchone()
        assert row['runtime_incarnation'] == 'runtime-a'
        assert row['attachment_conflict'] == 1 and row['channel_active'] == 0
        assert row['connection_generation'] == 1
    assert 'bge_attachment_conflict' in caplog.text
    assert nonce not in caplog.text and token not in caplog.text
    assert 'runtime-a' not in caplog.text and 'runtime-b' not in caplog.text


def test_wrong_nonce_or_token_cannot_poison_runtime_binding(tmp_path):
    queue, run, token, nonce, _, _ = prepared(tmp_path)
    for invalid_nonce in ('wrong', '\N{SNOWMAN}', None):
        with pytest.raises(StaleAttachment):
            queue.attach_channel(run, token, invalid_nonce, 'attacker')
    with pytest.raises(RevokedRun):
        queue.attach_channel(run, 'wrong-token', nonce, 'attacker')
    with pytest.raises(ValueError):
        queue.attach_channel(run, token, nonce, '')
    assert queue.attach_channel(run, token, nonce, 'runtime-a')['connection_generation'] == 1
    with pytest.raises(RevokedRun):
        queue.channel_status(run, 'wrong-token', 1)
    with pytest.raises(RevokedRun):
        queue.detach_channel(run, 'wrong-token', 1)
    # A failed nonce check from another incarnation cannot set the conflict.
    with pytest.raises(StaleAttachment):
        queue.attach_channel(run, token, 'wrong', 'runtime-b')
    assert queue.channel_status(run, token, 1)['active']


def test_concurrent_incarnations_commit_one_binding_and_fence_all_dispatch(tmp_path):
    queue, run, token, nonce, _, now = prepared(tmp_path)
    other = BgeQueue(queue.path, clock=lambda: now[0])
    barrier = threading.Barrier(2)

    def attach(instance, incarnation):
        barrier.wait()
        try:
            return instance.attach_channel(run, token, nonce, incarnation)
        except AttachmentConflict:
            return 'conflict'

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(attach, queue, 'runtime-a')
        second = pool.submit(attach, other, 'runtime-b')
        results = [first.result(timeout=2), second.result(timeout=2)]
    assert results.count('conflict') == 1
    assert sum(isinstance(result, dict) for result in results) == 1
    with pytest.raises(AttachmentConflict):
        other.channel_status(run, token, 1)
    with queue.connect(write=False) as db:
        row = db.execute('select * from runs where id=?', (run,)).fetchone()
        assert row['attachment_conflict'] == 1 and row['channel_active'] == 0
        assert row['connection_generation'] == 1


def test_early_hello_waits_for_native_provider_receipt(tmp_path):
    queue, run, token, nonce, _, _ = prepared(tmp_path, provider=False)
    assert queue.attach_channel(run, token, nonce, 'runtime-a')['provider_version'] is None
    queue.heartbeat(run, token, ready=True, connection_generation=1)
    assert queue.channel_status(run, token, 1)['ready'] is False
    assert queue.claim(run, token, connection_generation=1) is None
    queue.launch_record(run, 'fixture-owner/stable-slug', provider_version=7)
    assert queue.channel_status(run, token, 1)['ready'] is True
    assert queue.claim(run, token, connection_generation=1) is not None
    with pytest.raises(ValueError):
        queue.prepare_channel(run)


def test_http_claim_blocks_first_attachment_until_completion(tmp_path):
    queue, run, token, nonce, _, _ = prepared(tmp_path)
    queue.heartbeat(run, token, ready=True)
    job = queue.claim(run, token)
    with pytest.raises(AttachmentBusy):
        queue.attach_channel(run, token, nonce, 'runtime-a')
    with queue.connect(write=False) as db:
        row = db.execute('select * from runs where id=?', (run,)).fetchone()
        assert row['runtime_incarnation'] is None
        assert row['connection_generation'] == 0 and row['attachment_conflict'] == 0
    assert complete(queue, run, token, job, None) == 'accepted'
    queue.attach_channel(run, token, nonce, 'runtime-a')
    with pytest.raises(StaleAttachment):
        complete(queue, run, token, job, None)


@pytest.mark.parametrize('_attempt', range(6))
def test_http_claim_and_first_attachment_serialize_across_connections(tmp_path, _attempt):
    queue, run, token, nonce, _, now = prepared(tmp_path)
    queue.heartbeat(run, token, ready=True)
    other = BgeQueue(queue.path, clock=lambda: now[0])
    barrier = threading.Barrier(2)

    def claim_http():
        barrier.wait()
        try:
            return queue.claim(run, token)
        except StaleAttachment:
            return 'fenced'

    def attach():
        barrier.wait()
        try:
            return other.attach_channel(run, token, nonce, 'runtime-a')
        except AttachmentBusy:
            return 'busy'

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        claimed, attached = pool.submit(claim_http), pool.submit(attach)
        claimed, attached = claimed.result(timeout=2), attached.result(timeout=2)
    if attached == 'busy':
        assert isinstance(claimed, dict)
        assert complete(queue, run, token, claimed, None) == 'accepted'
    else:
        assert attached['connection_generation'] == 1
        assert claimed == 'fenced'
        assert queue.claim(run, token, connection_generation=1) is not None


def test_lost_completion_ack_reconciles_duplicate_after_reconnect(tmp_path):
    queue, run, token, nonce, job_id, now = prepared(tmp_path)
    queue.attach_channel(run, token, nonce, 'runtime-a')
    queue.heartbeat(run, token, ready=True)
    job = queue.claim(run, token, connection_generation=1)
    assert complete(queue, run, token, job, 1) == 'accepted'
    reopened = BgeQueue(queue.path, clock=lambda: now[0])
    reopened.attach_channel(run, token, nonce, 'runtime-a')
    assert complete(reopened, run, token, job, 2) == 'duplicate'
    with pytest.raises(ValueError, match='completion payload mismatch'):
        reopened.complete(run, token, job['id'], job['claim'], SPACE,
                          [[0, 1] + [0] * 1022], {}, connection_generation=2)
    # Diagnostic timing observations are not part of the accepted vector payload.
    assert reopened.complete(run, token, job['id'], job['claim'], SPACE,
                             [[1] + [0] * 1023], {'encoder_seconds': 9},
                             connection_generation=2) == 'duplicate'
    assert reopened.result('actor', job_id)['state'] == 'done'
    with reopened.connect(write=False) as db:
        assert db.execute('select attempt from jobs where id=?', (job_id,)).fetchone()[0] == 1


def test_channel_observation_and_compatible_constructor_do_not_wait_for_writer(tmp_path):
    queue, run, token, nonce, _, now = prepared(tmp_path)
    queue.attach_channel(run, token, nonce, 'runtime-a')
    queue.heartbeat(run, token, ready=True)
    with queue.connect() as writer:
        writer.execute('update runs set heartbeat=heartbeat where id=?', (run,))
        reopened = BgeQueue(queue.path, clock=lambda: now[0])
        assert reopened.channel_status(run, token, 1)['ready']
        now[0] += 121
        with pytest.raises(RevokedRun):
            reopened.channel_status(run, token, 1)
    with queue.connect(write=False) as db:
        row = db.execute('select * from runs where id=?', (run,)).fetchone()
        assert row['status'] == 'ready' and row['heartbeat'] == 1000.0
        assert row['connection_generation'] == 1


def test_additive_migration_preserves_legacy_identity_credentials_and_cache(tmp_path):
    queue, run, token, nonce, job_id, now = prepared(tmp_path)
    queue.heartbeat(run, token, ready=True)
    job = queue.claim(run, token)
    assert complete(queue, run, token, job, None) == 'accepted'
    with queue.connect(write=False) as db:
        original = dict(db.execute('select * from jobs where id=?', (job_id,)).fetchone())
        token_hash = db.execute('select token_hash from runs where id=?', (run,)).fetchone()[0]
    # Simulate the exact baseline schema, retaining rows/credentials/provider ID.
    with sqlite3.connect(queue.path) as db:
        for column in ('launch_nonce', 'runtime_incarnation', 'connection_generation',
                       'channel_active', 'attachment_conflict'):
            db.execute('alter table runs drop column ' + column)
    migrated = BgeQueue(queue.path, clock=lambda: now[0])
    with migrated.connect(write=False) as db:
        row = db.execute('select * from runs where id=?', (run,)).fetchone()
        assert row['token_hash'] == token_hash and row['provider_version'] == 7
        assert row['connection_generation'] == 0 and row['launch_nonce'] is None
        assert dict(db.execute('select * from jobs where id=?', (job_id,)).fetchone()) == original
        assert db.execute('select current_run from control').fetchone()[0] == run
    assert (queue.path.parent / 'bge-worker-credentials' / run).read_text() == token
    assert migrated.result('actor', job_id)['state'] == 'done'
    assert complete(migrated, run, token, job, None) == 'duplicate'


def test_never_attached_run_keeps_legacy_http_after_preparation(tmp_path):
    queue, run, token, _, _, _ = prepared(tmp_path)
    queue.heartbeat(run, token, ready=True)
    job = queue.claim(run, token)
    assert complete(queue, run, token, job, None) == 'accepted'
    assert not queue.detach_channel(run, token, 1)


def test_resume_reads_same_durable_claim_after_dispatch_loss_without_writes(tmp_path):
    queue, run, token, nonce, job_id, now = prepared(tmp_path)
    queue.attach_channel(run, token, nonce, 'runtime-a')
    queue.heartbeat(run, token, ready=True)
    job = queue.claim(run, token, connection_generation=1)
    with queue.connect(write=False) as db:
        before_job = dict(db.execute('select * from jobs where id=?', (job_id,)).fetchone())
        before_run = dict(db.execute('select * from runs where id=?', (run,)).fetchone())
    # No worker received DISPATCH. A fresh connection recovers its exact claim.
    queue.detach_channel(run, token, 1)
    assert queue.attach_channel(run, token, nonce, 'runtime-a')['connection_generation'] == 2
    now[0] += 10
    with queue.connect() as writer:
        writer.execute('update jobs set updated=updated where id=?', (job_id,))
        assert queue.resume_channel_claim(run, token, 2) == job
        assert queue.resume_channel_claim(run, token, 2) == job
    with queue.connect(write=False) as db:
        assert dict(db.execute('select * from jobs where id=?', (job_id,)).fetchone()) == before_job
        after_run = dict(db.execute('select * from runs where id=?', (run,)).fetchone())
        for key in ('heartbeat', 'useful', 'ready_at'):
            assert after_run[key] == before_run[key]
    with pytest.raises(StaleAttachment):
        queue.resume_channel_claim(run, token, 1)
    with pytest.raises(StaleAttachment):
        queue.resume_channel_claim(run, token, None)
    with pytest.raises(RevokedRun):
        queue.resume_channel_claim(run, 'wrong-token', 2)
    assert queue.claim(run, token, connection_generation=2) is None
    assert complete(queue, run, token, job, 2) == 'accepted'
    assert queue.resume_channel_claim(run, token, 2) is None


def test_expired_claim_is_not_resumed_or_rewritten(tmp_path):
    queue, run, token, nonce, job_id, now = prepared(tmp_path)
    queue.attach_channel(run, token, nonce, 'runtime-a')
    queue.heartbeat(run, token, ready=True)
    queue.claim(run, token, connection_generation=1)
    now[0] += 100
    queue.heartbeat(run, token, connection_generation=1)
    now[0] += 81
    assert queue.resume_channel_claim(run, token, 1) is None
    with queue.connect(write=False) as db:
        row = db.execute('select * from jobs where id=?', (job_id,)).fetchone()
        assert row['state'] == 'claimed' and row['attempt'] == 1
        assert row['lease_until'] == 1180.0
    now[0] += 40
    with pytest.raises(RevokedRun):
        queue.resume_channel_claim(run, token, 1)
