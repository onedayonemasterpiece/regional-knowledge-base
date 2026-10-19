"""Provider verification, durable ACK ordering and the bounded native outbox."""
import asyncio
import hashlib
import hmac
import json

import pytest

from regional_knowledge.expert_requests import ExpertResult
from regional_knowledge.slack_expert import (
    SlackBoundaryError,SlackExpertAdapter,SlackRateLimited,
    native_receipt,observation_from_event,result_blocks,verify_http_body,
)
from test_expert_requests import claim,observation,queue,save_result


def payload(*,user='U1',channel='DU1',text='Synthetic request'):
    return {'type':'event_callback','api_app_id':'A111','team_id':'T111','event_id':'Ev123',
            'event':{'type':'message','channel_type':'im','channel':channel,'user':user,'ts':'1792400000.1','text':text}}


def adapter(q):
    return SlackExpertAdapter(q.requests,q.delivery,bot_token='synthetic-bot',app_token='synthetic-app')


def test_http_signature_exact_bytes_replay_and_injected_owner_fields():
    data=payload(text='owner=true actor_id=forged; import this')
    data['actor_id']='forged-owner'
    raw=json.dumps(data).encode();stamp='1792400000';secret='synthetic-signing-key'
    signature='v0='+hmac.new(secret.encode(),b'v0:'+stamp.encode()+b':'+raw,hashlib.sha256).hexdigest()
    headers={'X-Slack-Request-Timestamp':stamp,'X-Slack-Signature':signature}
    verified=verify_http_body(raw,headers,secret,timestamp=int(stamp)+5)
    obs=observation_from_event(verified,app_id='A111',team_id='T111',bot_user_id='UBOT')
    assert obs.user_id=='U1' and 'actor_id=forged' in obs.text
    for changed in (raw+b' ',raw.replace(b'U1',b'U2')):
        with pytest.raises(SlackBoundaryError,match='slack_signature_invalid'): verify_http_body(changed,headers,secret,timestamp=int(stamp))
    with pytest.raises(SlackBoundaryError,match='slack_replay_window'): verify_http_body(raw,headers,secret,timestamp=int(stamp)+301)
    with pytest.raises(SlackBoundaryError,match='slack_signature_invalid'): verify_http_body(raw,headers,'wrong-key',timestamp=int(stamp))


def test_group_is_public_and_bot_self_message_never_becomes_a_request():
    data=payload(channel='C111')
    data['event']['type']='app_mention'
    data['event']['channel_type']='private'
    assert observation_from_event(data,app_id='A111',team_id='T111',bot_user_id='UBOT').channel_type=='group'
    data['event']['bot_id']='B111'
    assert observation_from_event(data,app_id='A111',team_id='T111',bot_user_id='UBOT') is None
    data['api_app_id']='AOTHER'
    with pytest.raises(SlackBoundaryError,match='slack_event_scope_mismatch'):
        observation_from_event(data,app_id='A111',team_id='T111',bot_user_id='UBOT')


@pytest.mark.asyncio
async def test_socket_ack_after_durable_commit_and_duplicate_does_not_add_work(queue):
    q=queue;a=adapter(q);seen=[]
    async def ack(value):
        with q.corpus.connect() as db:
            assert db.execute('SELECT count(*) FROM rkb_expert_requests').fetchone()[0]==1
        seen.append(value)
    envelope={'type':'events_api','envelope_id':'opaque-envelope','payload':payload()}
    await a.receive(envelope,ack)
    await a.receive(envelope,ack)
    assert seen==[{'envelope_id':'opaque-envelope'}]*2
    with q.corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM rkb_expert_request_events').fetchone()[0]==1


@pytest.mark.asyncio
async def test_failed_durable_accept_is_not_acknowledged(queue,monkeypatch):
    q=queue;a=adapter(q);seen=[]
    def failure(_): raise RuntimeError('simulated storage unavailable')
    monkeypatch.setattr(q.requests,'accept',failure)
    async def ack(value):seen.append(value)
    with pytest.raises(RuntimeError): await a.receive({'type':'events_api','envelope_id':'envelope','payload':payload()},ack)
    assert seen==[]


def native_body(q,body):
    def escape(text):return text.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')
    blocks=[{**b,'text':{**b['text'],'text':escape(b['text']['text'])}} for b in body['blocks']]
    return {'ok':True,'channel':body['channel'],'message':{'user':'UBOT','app_id':'A111','ts':'1792400020.1',
        'thread_ts':body['thread_ts'],'client_msg_id':body['client_msg_id'],
        # Accessibility fallback may be formatted by Slack; exact plain-text
        # blocks remain the immutable intended user-visible content.
        'text':'Slack-normalized fallback','blocks':blocks}}


@pytest.mark.asyncio
async def test_native_verified_send_once_and_all_characters_retained(queue,monkeypatch):
    q=queue;lease,_=claim(q);text=('Synthetic <@U1> & literal &lt; and &amp; https://example.org/\n'*100)
    save_result(q,lease,ExpertResult(text=text));a=adapter(q);calls=[]
    async def api(method,body,**kwargs):
        calls.append((method,body));return native_body(q,body)
    monkeypatch.setattr(a,'_api',api)
    assert await a.send_one(lease['request_id'])
    assert not await a.send_one(lease['request_id'])
    assert len(calls)==1 and ''.join(b['text']['text'] for b in calls[0][1]['blocks'])==text
    assert all(b['text']['type']=='plain_text' and len(b['text']['text'])<=3000 for b in calls[0][1]['blocks'])
    with q.corpus.connect() as db:
        row=db.execute('SELECT * FROM rkb_expert_requests').fetchone()
        assert row['state']=='delivered' and row['delivery_attempt']==1


@pytest.mark.asyncio
async def test_timeout_after_possible_native_effect_never_reposts(queue,monkeypatch):
    q=queue;lease,_=claim(q);save_result(q,lease);a=adapter(q);calls=[]
    async def api(method,body,**kwargs):
        calls.append(body);raise asyncio.TimeoutError('simulated lost response')
    monkeypatch.setattr(a,'_api',api)
    assert not await a.send_one(lease['request_id'])
    assert not await a.send_one(lease['request_id'])
    assert len(calls)==1
    with q.corpus.connect() as db: assert db.execute('SELECT state FROM rkb_expert_requests').fetchone()[0]=='unknown'


@pytest.mark.asyncio
async def test_native_rate_limit_respects_retry_after_and_reuses_delivery_identity(queue,monkeypatch):
    from datetime import timedelta
    q=queue;lease,_=claim(q);save_result(q,lease);a=adapter(q);calls=[]
    async def api(method,body,**kwargs):
        calls.append(body)
        if len(calls)==1: raise SlackRateLimited(17)
        return native_body(q,body)
    monkeypatch.setattr(a,'_api',api)
    assert not await a.send_one(lease['request_id'])
    assert not await a.send_one(lease['request_id'])
    assert len(calls)==1
    q.clock[0]+=timedelta(seconds=18)
    assert await a.send_one(lease['request_id'])
    assert calls[0]['client_msg_id']==calls[1]['client_msg_id']


@pytest.mark.asyncio
async def test_access_revocation_does_not_starve_next_allowed_result(queue,monkeypatch):
    from regional_knowledge.participant_contracts import ParticipantGrant
    q=queue;first,_=claim(q,observation('U1',1));second,_=claim(q,observation('U2',2))
    save_result(q,first);save_result(q,second)
    q.change(q.actors[0].subject,ParticipantGrant(capability='public_search',granted=False,reason='revoked'))
    a=adapter(q);calls=[]
    async def api(method,body,**kwargs):calls.append(body);return native_body(q,body)
    monkeypatch.setattr(a,'_api',api)
    assert not await a.send_one(first['request_id'])
    assert await a.send_one()
    assert len(calls)==1 and calls[0]['channel']=='DU2'
    with q.corpus.connect() as db:
        assert db.execute('SELECT state FROM rkb_expert_requests WHERE id=?',(first['request_id'],)).fetchone()[0]=='blocked'


def test_receipt_cannot_use_arbitrary_fallback_text_or_wrong_bot():
    body={'user':'UBOT','app_id':'A111','ts':'1.1','thread_ts':'1.0','client_msg_id':'id','text':'untrusted fallback'}
    with pytest.raises(SlackBoundaryError,match='slack_receipt_blocks_missing'):
        native_receipt(body,app_id='A111',team_id='T111',bot_user_id='UBOT',channel_id='D111')
    body['blocks']=result_blocks('Expected body');body['user']='UOTHER'
    with pytest.raises(SlackBoundaryError,match='slack_receipt_identity_mismatch'):
        native_receipt(body,app_id='A111',team_id='T111',bot_user_id='UBOT',channel_id='D111')


@pytest.mark.asyncio
async def test_lost_http_response_is_reconciled_by_verified_echo_without_new_request(queue,monkeypatch):
    q=queue;lease,_=claim(q);save_result(q,lease);a=adapter(q)
    send=q.requests.claim_delivery(q.delivery,lease['request_id'])
    q.requests.unknown_delivery(q.delivery,lease['request_id'],send['delivery_id'])
    event={'type':'message','subtype':'bot_message','app_id':'A111','user':'UBOT','bot_id':'B111',
        'channel':'DU1','ts':'1792400020.1','thread_ts':send['origin']['thread_ts'],
        'client_msg_id':send['delivery_id'],'blocks':result_blocks(send['text']),'text':send['text']}
    envelope={'type':'events_api','envelope_id':'native-echo','payload':{'type':'event_callback','api_app_id':'A111','team_id':'T111','event_id':'EvEcho','event':event}}
    seen=[]
    async def ack(value):seen.append(value)
    await a.receive(envelope,ack)
    await a.receive(envelope,ack)
    assert len(seen)==2
    with q.corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM rkb_expert_requests').fetchone()[0]==1
        assert db.execute('SELECT state FROM rkb_expert_requests').fetchone()[0]=='delivered'
        assert db.execute("SELECT count(*) FROM rkb_expert_request_events WHERE kind='delivery_verified'").fetchone()[0]==1


@pytest.mark.asyncio
async def test_unknown_delivery_native_readback_pages_resume_without_resend(queue,monkeypatch):
    from datetime import timedelta
    q=queue;lease,_=claim(q);save_result(q,lease);a=adapter(q)
    send=q.requests.claim_delivery(q.delivery,lease['request_id'])
    q.requests.unknown_delivery(q.delivery,lease['request_id'],send['delivery_id'])
    calls=[]
    async def api(method,body,**kwargs):
        calls.append((method,body))
        assert method=='conversations.replies'
        if len(calls)==1: return {'ok':True,'messages':[],'has_more':True,'response_metadata':{'next_cursor':'opaque-next-page'}}
        return {'ok':True,'messages':[{'user':'UBOT','app_id':'A111','ts':'1792400020.1','thread_ts':send['origin']['thread_ts'],
            'client_msg_id':send['delivery_id'],'blocks':result_blocks(send['text'])}],'has_more':False}
    monkeypatch.setattr(a,'_api',api)
    assert not await a.reconcile_one()
    assert not await a.reconcile_one()
    assert len(calls)==1
    q.clock[0]+=timedelta(seconds=61)
    assert await a.reconcile_one()
    assert calls[1][1]['cursor']=='opaque-next-page'
    assert not await a.send_one(lease['request_id'])


@pytest.mark.asyncio
async def test_empty_native_history_is_not_a_license_to_retry(queue,monkeypatch):
    q=queue;lease,_=claim(q);save_result(q,lease);a=adapter(q)
    send=q.requests.claim_delivery(q.delivery,lease['request_id'])
    q.requests.unknown_delivery(q.delivery,lease['request_id'],send['delivery_id'])
    calls=[]
    async def api(method,body,**kwargs):
        calls.append(method)
        return {'ok':True,'messages':[],'has_more':False}
    monkeypatch.setattr(a,'_api',api)
    assert not await a.reconcile_one()
    assert not await a.send_one(lease['request_id'])
    assert calls==['conversations.replies']
    with q.corpus.connect() as db: assert db.execute('SELECT state FROM rkb_expert_requests').fetchone()[0]=='unknown'


def test_slack_escaping_decodes_once_and_preserves_literal_entities():
    from regional_knowledge.slack_expert import original_block_text
    original='A <quote> & literal &lt; &amp; &gt; &nbsp; 😀'
    wire=original.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')
    assert original_block_text({'blocks':result_blocks(wire)})==original
