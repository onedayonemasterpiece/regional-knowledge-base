"""Persistent signed wake-up/reclaim tests; no real callback or model calls."""
import base64
import asyncio
from datetime import timedelta
import hashlib
import hmac
import json
from types import SimpleNamespace
from uuid import uuid4

from cryptography.fernet import Fernet
import pytest

from regional_knowledge.expert_events import EVENT, ExpertEvents
from regional_knowledge.expert_requests import ExpertRequestError, ExpertRequests, ExpertResult, ServiceCredentials
from regional_knowledge.expert_webhooks import WebhookError, WebhookSender, callback_parts, signed_headers
from regional_knowledge.request_authority import wake_event_id
from test_expert_requests import queue, observation


class Sender:
    def __init__(self):
        self.verified=[]; self.sent=[]; self.status=200; self.retry_after=None; self.fail_verify=False

    async def verify(self,url,subscription,secret):
        if self.fail_verify:raise WebhookError('synthetic verification failed')
        self.verified.append((url,subscription))

    async def send(self,url,subscription,event_id,payload,secret,old_secret=None):
        self.sent.append((url,subscription,event_id,payload,secret,old_secret))
        return self.status,b'',self.retry_after


@pytest.fixture
def events(queue):
    sender=Sender();key=Fernet.generate_key()
    return SimpleNamespace(q=queue,sender=sender,key=key,
        events=ExpertEvents(queue.requests,key,sender=sender),
        secret='whsec_'+base64.b64encode(b'synthetic-webhook-key-material-123').decode(),
        url='https://callback.example/expert-events')


@pytest.mark.asyncio
async def test_verified_subscription_restarts_thin_wake_and_generation_fence(events):
    e=events;q=e.q
    subscribed=await e.events.subscribe(q.executor,e.url,e.secret)
    accepted=q.requests.accept(observation())
    restarted=ExpertEvents(q.requests,e.key,sender=e.sender)
    assert await restarted.deliver_one()
    payload=e.sender.sent[0][3]
    assert payload['name']==EVENT and payload['data']=={'request_id':accepted['request_id'],'expected_generation':0}
    assert 'Synthetic research' not in json.dumps(payload)
    assert 'person_id' not in json.dumps(payload) and 'origin' not in json.dumps(payload)
    lease=q.requests.claim(q.executor,request_id=accepted['request_id'],event_id=payload['eventId'])
    context=q.requests.context(q.executor,lease['request_id'],lease['generation'],lease['execution_token'])
    assert q.requests.get(context,lease['request_id'])['wake_event_id']==payload['eventId']
    q.clock[0]+=timedelta(seconds=121)
    assert await restarted.deliver_one()
    next_payload=e.sender.sent[1][3]
    assert next_payload['eventId']!=payload['eventId'] and next_payload['data']['expected_generation']==1
    with pytest.raises(ExpertRequestError,match='event_identity_conflict'):
        q.requests.claim(q.executor,request_id=lease['request_id'],event_id=payload['eventId'])
    new=q.requests.claim(q.executor,request_id=lease['request_id'],event_id=next_payload['eventId'])
    assert new['generation']==2
    with pytest.raises(ExpertRequestError):
        q.requests.context(q.executor,lease['request_id'],lease['generation'],lease['execution_token'])
    q.requests.record_result(q.executor,new['request_id'],new['generation'],new['execution_token'],ExpertResult(text='Synthetic completed result'))
    q.clock[0]+=timedelta(seconds=301)
    assert not await restarted.deliver_one()
    with q.corpus.connect() as db:
        raw=db.execute('SELECT secret_enc FROM rkb_expert_subscriptions WHERE id=?',(subscribed['id'],)).fetchone()[0]
    assert e.secret not in raw


@pytest.mark.asyncio
async def test_retry_unknown_uses_same_event_and_honors_retry_after(events):
    e=events;q=e.q
    await e.events.subscribe(q.executor,e.url,e.secret)
    q.requests.accept(observation())
    e.sender.status=429;e.sender.retry_after='601'
    assert await e.events.deliver_one()
    first=e.sender.sent[-1]
    q.clock[0]+=timedelta(seconds=600)
    assert not await e.events.deliver_one()
    q.clock[0]+=timedelta(seconds=2)
    e.sender.status=200
    assert await e.events.deliver_one()
    assert e.sender.sent[-1][2:4]==first[2:4]
    assert not await e.events.deliver_one()


@pytest.mark.asyncio
async def test_subscription_revoke_and_current_identity_prevent_late_wake(events):
    e=events;q=e.q
    await e.events.subscribe(q.executor,e.url,e.secret)
    q.requests.accept(observation())
    e.events.unsubscribe(q.executor,e.url)
    assert not await e.events.deliver_one()
    await e.events.subscribe(q.executor,e.url,e.secret,max_age_ms=0)
    # Link is no longer valid even though the event was already accepted.
    with q.corpus.connect() as db:
        db.execute("UPDATE rkb_external_identities SET state='revoked',revision=revision+1 WHERE provider_subject='U1'")
    assert await e.events.deliver_one() and not e.sender.sent
    assert not await e.events.deliver_one()


@pytest.mark.asyncio
async def test_rotation_verification_and_service_epoch_invalidate_cached_subscription(events):
    e=events;q=e.q
    await e.events.subscribe(q.executor,e.url,e.secret)
    await e.events.subscribe(q.executor,e.url,e.secret)
    assert len(e.sender.verified)==1
    await e.events.subscribe(q.executor,e.url,e.secret,max_age_ms=0)
    assert len(e.sender.verified)==2
    new='whsec_'+base64.b64encode(b'different-synthetic-key-material-42').decode()
    await e.events.subscribe(q.executor,e.url,new)
    q.requests.accept(observation())
    assert await e.events.deliver_one()
    assert e.sender.sent[0][-2:]==(new,e.secret)
    q.requests.provision_service(q.admin,q.executor.service_id,'executor',q.executor.token,expected_epoch=1,enabled=False)
    with pytest.raises(ExpertRequestError,match='service_denied'):
        await e.events.subscribe(q.executor,e.url,new)


@pytest.mark.asyncio
async def test_app_team_isolation_and_failed_verification_do_not_create_subscriptions(events):
    e=events;q=e.q
    e.sender.fail_verify=True
    with pytest.raises(WebhookError):await e.events.subscribe(q.executor,e.url,e.secret)
    with q.corpus.connect() as db:
        assert db.execute('SELECT COUNT(*) FROM rkb_expert_subscriptions').fetchone()[0]==0
    e.sender.fail_verify=False
    await e.events.subscribe(q.executor,e.url,e.secret)
    other=ExpertRequests(q.people,app_id='A222',team_id='T222',bot_user_id='UBOT2')
    with pytest.raises(ExpertRequestError,match='service_denied'):
        await ExpertEvents(other,e.key,sender=e.sender).subscribe(q.executor,e.url,e.secret)
    q.requests.accept(observation())
    assert not await ExpertEvents(other,e.key,sender=e.sender).deliver_one()
    assert await e.events.deliver_one()


def test_standard_webhooks_signature_exact_bytes_and_loopback_allowlist():
    secret='whsec_'+base64.b64encode(b'synthetic-test-signature-key-12345').decode()
    body=b'{"raw": "\\u0410"}'
    headers=signed_headers('subscription','event',body,secret,timestamp=123)
    expected=hmac.new(base64.b64decode(secret[6:]),b'event.123.'+body,hashlib.sha256).digest()
    assert headers['webhook-signature']=='v1,'+base64.b64encode(expected).decode()
    assert callback_parts('http://127.0.0.1:7777/events',['http://127.0.0.1:7777/events'])[1]
    for url in ('http://127.0.0.1:7777/other','http://localhost:7777/events','https://user:secret@callback.example/','https://callback.example/#fragment','https://callback.example:8443/'):
        with pytest.raises(WebhookError):callback_parts(url,['http://127.0.0.1:7777/events'])


@pytest.mark.asyncio
async def test_verification_requires_exact_challenge_reply():
    class Echo(WebhookSender):
        bad=False
        async def send(self,url,subscription,event,payload,secret,old_secret=None):
            return 200,json.dumps({'challenge':'wrong' if self.bad else payload['challenge']}).encode(),None
    sender=Echo()
    await sender.verify('https://callback.example/','subscription','unused-in-mocked-send')
    sender.bad=True
    with pytest.raises(WebhookError,match='callback_verification_failed'):
        await sender.verify('https://callback.example/','subscription','unused-in-mocked-send')


@pytest.mark.asyncio
async def test_busy_sqlite_wake_does_not_block_shared_http_event_loop(events):
    e=events;q=e.q
    await e.events.subscribe(q.executor,e.url,e.secret)
    q.requests.accept(observation())
    # A different writer holds the same real corpus. The loop must remain
    # available to ACK new Slack input while the wake-up worker waits for it.
    with q.corpus.connect() as db:
        db.execute('BEGIN IMMEDIATE')
        released=[]
        async def release():
            await asyncio.sleep(.05)
            db.commit();released.append(True)
        release_task=asyncio.create_task(release())
        try:
            assert await asyncio.wait_for(e.events.deliver_one(),2)
            assert released and len(e.sender.sent)==1
        finally:
            await release_task
