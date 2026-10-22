"""Connected source boundary: real signed ASGI ingress to real delegated read.

External Slack/provider, native Dataset publishing and Kaggle I/O remain fake
or absent. These checks do NOT assert a live Slack delivery or remote Saved.
"""
import asyncio
from types import SimpleNamespace
import time

import httpx
import pytest
from starlette.applications import Starlette
from starlette.routing import Route

from regional_knowledge.expert_slack_composition import compose_expert_evidence_receiver
from regional_knowledge.expert_slack_ingress import ExpertSlackIngress, PATH as INGRESS_PATH
from regional_knowledge.expert_evidence_receiver import PATH as EVIDENCE_PATH, CODEC
from regional_knowledge.read_issuance import ReadIssuanceMiddleware
from regional_knowledge.server import build_server
from test_control_spool import env
from test_expert_request_control import _prepare, _event
from test_kaggle_public_backend import target, settle, release
from test_read_authority import auth
from test_participants import people
from test_kaggle_service_startup import service


_SECRET = 'test-signing-secret'
_NOW = 1792000001


def _owned_ingress(s):
    control, executor, link = _prepare(SimpleNamespace(
        p=s.target.auth.p, spool=s.config.spool))
    ingress = ExpertSlackIngress(control, signing_secret=_SECRET, clock=lambda: _NOW)
    return control, executor, link, ingress


def test_exact_owner_factory_mounts_both_existing_routes_without_creating_service(service):
    s = service
    control, _executor, _link, ingress = _owned_ingress(s)
    receiver = compose_expert_evidence_receiver(
        target_startup=s.startup, expert_slack_ingress=ingress)
    assert receiver.control is control
    assert receiver.backend is s.startup.backend
    assert receiver.authority is s.startup.authority
    assert control.spool is s.config.spool

    absent = build_server(target_startup=s.startup).streamable_http_app(
        stateless_http=True, json_response=True)
    installed = build_server(
        target_startup=s.startup,
        expert_slack_ingress=ingress,
        expert_evidence_receiver=receiver,
    ).streamable_http_app(stateless_http=True, json_response=True)
    assert INGRESS_PATH not in {r.path for r in absent.routes}
    assert EVIDENCE_PATH not in {r.path for r in absent.routes}
    assert {INGRESS_PATH, EVIDENCE_PATH} <= {r.path for r in installed.routes}


def test_foreign_participants_and_spool_cannot_be_bound_to_target_read(service, env):
    other_control, _executor, _link = _prepare(env)
    foreign = ExpertSlackIngress(
        other_control, signing_secret=_SECRET, clock=lambda: _NOW)
    with pytest.raises(ValueError, match='authorities must be identical'):
        compose_expert_evidence_receiver(
            target_startup=service.startup, expert_slack_ingress=foreign)
    with pytest.raises(TypeError, match='exact target startup'):
        compose_expert_evidence_receiver(
            target_startup=object(), expert_slack_ingress=foreign)
    with pytest.raises(TypeError, match='verified Slack ingress'):
        compose_expert_evidence_receiver(
            target_startup=service.startup, expert_slack_ingress=object())


@pytest.mark.asyncio
async def test_signed_ingress_remote_journal_then_same_person_delegated_read(service):
    s = service
    control, executor, _link, ingress = _owned_ingress(s)
    receiver = compose_expert_evidence_receiver(
        target_startup=s.startup, expert_slack_ingress=ingress)
    app = ReadIssuanceMiddleware(Starlette(routes=[
        Route(INGRESS_PATH, ingress.__call__, methods=['POST']),
        Route(EVIDENCE_PATH, receiver.__call__, methods=['POST']),
    ]))
    raw, signed_headers = _event()
    # Establish the canonical native history/read gate using the existing
    # startup lifecycle; a cold target must NOT dispatch a forged 'ready' read.
    s.adapter.available = True  # synthetic existing native history adapter
    async with s.startup.lifespan(None):
        assert s.startup.reads_current()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, client=('127.0.0.1', 9001)),
            base_url='http://127.0.0.1'
        ) as http:
            # A valid signature is not a remote durable receipt or work permit.
            first = await http.post(
                INGRESS_PATH, content=raw, headers=signed_headers)
            assert first.status_code == 503
            assert first.json() == {'error': 'remote_intent_pending'}
            assert control.claim(executor) == {'state': 'empty'}
            with s.config.participants.corpus.connect() as db:
                ids = db.execute(
                    'SELECT id FROM rkb_expert_requests').fetchall()
                assert len(ids) == 1
                request_id = ids[0]['id']
            command = control.exact_remote_intent(request_id, 1)
            # Synthetic external journal I/O only, not target Dataset Saved.
            seal = await s.config.spool.journal.seal(
                [command], predecessor=None, writer_generation='writer-1')
            assert (await control.verify_remote_intent(
                request_id, command, seal))['remote_accepted']
            again = await http.post(
                INGRESS_PATH, content=raw, headers=signed_headers)
            assert again.status_code == 200 and again.json() == {'ok': True}

            lease = control.claim(executor, request_id=request_id)
            assert lease['state'] == 'leased'
            payload = dict(
                codec=CODEC, request_id=request_id,
                generation=lease['generation'],
                execution_token=lease['execution_token'],
                min_data_seq=5, deadline_monotonic=time.monotonic()+2)
            headers = {
                'Authorization': 'Bearer '+executor.token,
                'X-RKB-Expert-Service': executor.service_id}
            task = asyncio.create_task(
                http.post(EVIDENCE_PATH, json=payload, headers=headers))
            # Observe the actual shared queue; surface premature HTTP failure
            # instead of hiding it behind an unrelated 200 ms polling fixture.
            issued = None
            for _ in range(1500):
                if task.done():
                    early = await task
                    reason = early.json().get('reason') if early.headers.get('content-type', '').startswith('application/json') else 'non_json'
                    pytest.fail(f'delegated read ended before dispatch: status={early.status_code} reason={reason}')
                issued = await s.target.queue.next_channel_work(
                    s.target.run, s.target.token, s.target.generation)
                if issued is not None:
                    assert issued.kind == 'read'
                    s.target.admission.mark_work_sent(issued)
                    break
                await asyncio.sleep(.001)
            assert issued is not None, 'read never reached existing shared admission'
            assert issued.query.actor.person_id == s.target.auth.p.other.subject
            assert issued.query.actor.person_id != s.target.auth.p.owner.subject
            assert issued.query.request_id == request_id
            assert issued.query.turn_generation == lease['generation']
            await settle(s.target, issued)
            response = await task
            assert response.status_code == 200
            answer = response.json()
            assert answer['codec'] == CODEC
            assert answer['request_id'] == request_id
            assert not answer['context_gap']
            assert len(answer['evidence']) == 1
            assert answer['evidence'][0]['text'] == 'Evidence'
            assert not s.target.auth.resolver._contexts
            await release(s.target, issued)
