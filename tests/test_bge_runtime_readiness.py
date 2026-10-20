"""Run metadata only; actual loaded artifacts/provider evidence remain separate."""
from contextlib import contextmanager
from dataclasses import replace

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from regional_knowledge.bge_channel import channel_route
from regional_knowledge.bge_contract import REVISION, SPACE
from regional_knowledge.bge_queue import BgeQueue, StaleAttachment
from regional_knowledge.kaggle_readiness import (
    RUNTIME_HASHES, RunIdentity, SearchRuntimeExpectation, validate_search_runtime,
)
from test_read_authority import auth
from test_participants import people


def expected():
    return SearchRuntimeExpectation({key: 'a' * 64 for key in RUNTIME_HASHES},
        'synthetic-db', 's1', 'f1', 'e1', 5)


def observation(**updates):
    value = dict(hashes=dict(expected().hashes), database_generation='synthetic-db',
        schema_revision='s1', function_revision='f1', executor_revision='e1',
        applied_seq=5, projection_seq=5, visibility_epoch=0,
        text_search_ready=True, dense_search_ready=True,
        archive_read_ready=False, proof_ready=None)
    return value | updates


IDENTITY = RunIdentity('fixture-owner/stable-slug', '7', None, 'run', 'nonce', 'incarnation')


def test_runtime_expectations_copy_hashes_and_keep_independent_artifact_readiness():
    values = {key: 'a' * 64 for key in RUNTIME_HASHES}
    binding = replace(expected(), hashes=values)
    values['model'] = 'b' * 64
    assert binding.hashes['model'] == 'a' * 64
    assert validate_search_runtime(binding, observation(), IDENTITY).ok
    with pytest.raises(TypeError):
        binding.hashes['model'] = 'b' * 64


@pytest.mark.parametrize('key', sorted(RUNTIME_HASHES))
def test_each_loaded_hash_must_match_controller_expectation(key):
    value = observation(hashes=dict(expected().hashes) | {key: 'b' * 64})
    assert 'loaded_hashes_mismatch' in validate_search_runtime(expected(), value, IDENTITY).issues


@pytest.mark.parametrize('update,issue', [
    ({'projection_seq': None}, 'runtime_not_ready'),
    ({'projection_seq': 4}, 'runtime_not_ready'),
    ({'applied_seq': 4, 'projection_seq': 4}, 'runtime_not_ready'),
    ({'visibility_epoch': None}, 'runtime_not_ready'),
    ({'dense_search_ready': False}, 'runtime_not_ready'),
    ({'text_search_ready': False}, 'runtime_not_ready'),
    ({'database_generation': 'other'}, 'database_generation_mismatch'),
    ({'function_revision': 'f2'}, 'semantic_revision_mismatch'),
])
def test_real_search_readiness_not_model_only(update, issue):
    assert issue in validate_search_runtime(expected(), observation(**update), IDENTITY).issues


@pytest.mark.parametrize('update', [
    {'applied_seq': True}, {'projection_seq': 5.0}, {'visibility_epoch': -1},
    {'dense_search_ready': 1}, {'archive_read_ready': 'unknown'},
    {'hashes': {'model': 'a' * 64}}, {'extra': 'private metadata'},
])
def test_readiness_wire_is_strict_and_bounded(update):
    with pytest.raises(ValueError):
        validate_search_runtime(expected(), observation(**update), IDENTITY)


@pytest.fixture
def run(tmp_path):
    queue = BgeQueue(tmp_path / 'queue.sqlite')
    queue.enqueue('fixture', 'document', ['fixture text'], kind='document')
    with queue.connect(write=False) as db:
        ident = db.execute('SELECT id FROM runs').fetchone()[0]
    nonce = queue.prepare_channel(ident)
    queue.expect_search_runtime(ident, expected())
    token = queue.launch_claim(ident)
    queue.launch_record(ident, 'fixture-owner/stable-slug', provider_version=7)
    return queue, ident, token, nonce


@pytest.fixture
def shared_channel_run(run,auth):
    queue,*_=run
    admission=queue.configure_shared_reads(auth.resolver)
    yield run
    admission.close()


def test_current_readiness_is_required_again_after_reconnect(run, monkeypatch):
    queue, ident, token, nonce = run
    generation = queue.attach_channel(ident, token, nonce, 'incarnation')['connection_generation']
    assert not queue.channel_status(ident, token, generation)['runtime_verified']
    with pytest.raises(StaleAttachment):
        queue.heartbeat(ident, token, ready=True, connection_generation=generation)
    assert queue.accept_search_runtime(ident, token, generation, observation()).ok
    queue.heartbeat(ident, token, ready=True, connection_generation=generation)
    assert queue.channel_status(ident, token, generation)['ready']

    original = queue.connect
    with original() as writer:
        writer.execute('UPDATE runs SET useful=useful WHERE id=?', (ident,))
        @contextmanager
        def readonly(*, write=True):
            assert not write, 'duplicate/invalid readiness waited for writer'
            with original(write=False) as db:
                yield db
        with monkeypatch.context() as patch:
            patch.setattr(queue, 'connect', readonly)
            assert queue.accept_search_runtime(ident, token, generation, observation()).ok
            assert not queue.accept_search_runtime(ident, token, generation,
                observation(database_generation='wrong')).ok

    new = queue.attach_channel(ident, token, nonce, 'incarnation')['connection_generation']
    assert new > generation
    assert not queue.channel_status(ident, token, new)['ready']
    with pytest.raises(StaleAttachment):
        queue.accept_search_runtime(ident, token, generation, observation())
    with pytest.raises(StaleAttachment):
        queue.claim(ident, token, connection_generation=new)
    assert queue.accept_search_runtime(ident, token, new, observation()).ok
    assert queue.channel_status(ident, token, new)['ready']
    queue.expect_search_runtime(ident, expected())  # Exact setup retry is inert.
    with pytest.raises(ValueError, match='immutable'):
        queue.expect_search_runtime(ident, replace(expected(), database_generation='other'))


def test_model_only_ready_does_not_promote_target_channel(shared_channel_run):
    queue, ident, token, nonce = shared_channel_run
    with TestClient(Starlette(routes=[channel_route(queue)])) as app:
        with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer ' + token}) as socket:
            socket.send_json(dict(type='ATTACH', protocol_version=2, run_id=ident,
                launch_nonce=nonce, runtime_incarnation='incarnation'))
            generation = socket.receive_json()['connection_generation']
            socket.send_json(dict(type='RESOURCE_STATE',protocol_version=2,
                connection_generation=generation,state='idle'))

            def ready(payload=None):
                fields = {} if payload is None else {'runtime_readiness': payload}
                socket.send_json(dict(type='READY', protocol_version=2, connection_generation=generation,
                    model_revision=REVISION, space=SPACE, **fields))
                return socket.receive_json()

            assert ready()['status'] == 'runtime_not_ready'
            assert not queue.channel_status(ident, token, generation)['ready']
            assert ready(observation(database_generation='wrong'))['status'] == 'runtime_not_ready'
            assert ready(observation())['status'] == 'ready'
            assert queue.channel_status(ident, token, generation)['runtime_verified']
            # No original canonical import authority was restored for the
            # fixture document. READY cannot authorize that durable work.
            assert queue.shared_reads.status()['document_contexts']==0


@pytest.mark.parametrize('changed', [None, observation(database_generation='other')])
def test_promoted_target_rejects_changed_or_missing_runtime_proof(shared_channel_run, changed):
    queue, ident, token, nonce = shared_channel_run
    with TestClient(Starlette(routes=[channel_route(queue)])) as app:
        with app.websocket_connect('/bge-worker/channel', headers={'Authorization': 'Bearer ' + token}) as socket:
            socket.send_json(dict(type='ATTACH', protocol_version=2, run_id=ident,
                launch_nonce=nonce, runtime_incarnation='incarnation'))
            generation = socket.receive_json()['connection_generation']
            socket.send_json(dict(type='RESOURCE_STATE',protocol_version=2,
                connection_generation=generation,state='idle'))
            frame = dict(type='READY', protocol_version=2, connection_generation=generation,
                model_revision=REVISION, space=SPACE)
            socket.send_json(frame | {'runtime_readiness': observation()})
            assert socket.receive_json()['status'] == 'ready'
            socket.send_json(frame if changed is None else frame | {'runtime_readiness': changed})
            with pytest.raises(WebSocketDisconnect) as closed:
                socket.receive_json()
            assert closed.value.code == 4409


def test_exact_target_ready_stays_closed_without_canonical_shared_admission(run):
    queue,ident,token,nonce=run
    with TestClient(Starlette(routes=[channel_route(queue)])) as app:
        with app.websocket_connect('/bge-worker/channel',headers={'Authorization':'Bearer '+token}) as socket:
            socket.send_json(dict(type='ATTACH',protocol_version=2,run_id=ident,launch_nonce=nonce,
                runtime_incarnation='incarnation'))
            generation=socket.receive_json()['connection_generation']
            socket.send_json(dict(type='READY',protocol_version=2,connection_generation=generation,
                model_revision=REVISION,space=SPACE,runtime_readiness=observation()))
            response=socket.receive_json()
            assert response['status']=='runtime_not_ready' and response['reasons']==['shared_admission_required']
            assert not queue.channel_status(ident,token,generation)['ready']
