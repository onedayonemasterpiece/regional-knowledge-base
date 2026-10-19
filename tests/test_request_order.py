"""Order, fair selection and execution-time context in the corpus authority."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4
import pytest

from test_expert_requests import queue,observation,claim,save_result,expect
from regional_knowledge.expert_requests import ExpertRequests,ExpertResult,SlackDeliveryReceipt
from regional_knowledge.request_authority import wake_event_id


def deliver(q,lease):
    send=q.requests.claim_delivery(q.delivery,lease['request_id'])
    origin=send['origin']
    receipt=SlackDeliveryReceipt(app_id='A111',team_id='T111',bot_user_id='UBOT',
        channel_id=origin['channel_id'],thread_ts=origin['thread_ts'],message_ts='1792400050.1',
        client_message_id=send['delivery_id'],text=send['text'])
    return q.requests.receipt(q.delivery,lease['request_id'],receipt)


def test_dialog_waits_for_delivery_not_result_and_unknown_is_a_hold(queue):
    q=queue
    first,ctx=claim(q)
    second=q.requests.accept(observation(number=2))
    assert q.requests.claim(q.executor,request_id=second['request_id'])['state']=='empty'
    save_result(q,first)
    assert q.requests.claim(q.executor)['state']=='empty'
    sending=q.requests.claim_delivery(q.delivery,first['request_id'])
    q.requests.unknown_delivery(q.delivery,first['request_id'],sending['delivery_id'])
    restarted=ExpertRequests(q.people,app_id='A111',team_id='T111',bot_user_id='UBOT',allowed_channels=['C111'])
    assert restarted.claim(q.executor)['state']=='empty'
    # Other dialogs continue while the first delivery remains UNKNOWN.
    other,_=claim(q,observation('U2',3))
    assert other['state']=='leased'
    receipt=SlackDeliveryReceipt(app_id='A111',team_id='T111',bot_user_id='UBOT',channel_id='DU1',
        thread_ts='1792400000.111',message_ts='1792400050.1',client_message_id=sending['delivery_id'],text=sending['text'])
    restarted.receipt(q.delivery,first['request_id'],receipt)
    resumed=restarted.claim(q.executor,request_id=second['request_id'])
    assert resumed['state']=='leased'
    history=restarted.history(q.executor,resumed['request_id'],resumed['generation'],resumed['execution_token'])
    assert history['context_seq']==1 and history['items'][0]['result']['text']=='Synthetic answer'
    expect('request_fence_denied',q.people.require,ctx,'public_search')


@pytest.mark.parametrize('same_dialog',[False,True])
def test_person_fairness_explicit_id_and_wake_cannot_jump_turn(queue,same_dialog):
    q=queue
    many=[q.requests.accept(replace(observation(number=i),thread_ts='100.1' if same_dialog else str(100+i)+'.1')) for i in range(1,11)]
    lone=q.requests.accept(observation('U2',50))
    # Initially person 1 has the first turn, then person 2 gets a start even
    # while person 1 has ten independent dialogs queued.
    assert q.requests.claim(q.executor,request_id=lone['request_id'])=={'state':'deferred'}
    first=q.requests.claim(q.executor,request_id=many[0]['request_id'])
    assert first['state']=='leased'
    assert q.requests.claim(q.executor,request_id=many[1]['request_id'],event_id=wake_event_id(many[1]['request_id']))=={'state':'deferred'}
    assert q.requests.claim(q.executor,request_id=lone['request_id'])['state']=='leased'
    # Work conserving: an active question is not an arbitrary per-person cap.
    assert q.requests.claim(q.executor,request_id=many[1]['request_id'])['state']==('empty' if same_dialog else 'leased')


def test_concurrent_claims_respect_one_head_and_atomic_sequence(queue):
    q=queue
    with ThreadPoolExecutor(max_workers=5) as pool:
        accepted=list(pool.map(lambda i:q.requests.accept(observation(number=i)),range(1,11)))
    with ThreadPoolExecutor(max_workers=5) as pool:
        leases=list(pool.map(lambda _:q.requests.claim(q.executor),range(5)))
    assert sum(x['state']=='leased' for x in leases)==1
    with q.corpus.connect() as db:
        rows=db.execute('SELECT dialog_id,dialog_seq FROM rkb_expert_requests ORDER BY dialog_seq').fetchall()
        assert len({r['dialog_id'] for r in rows})==1
        assert [r['dialog_seq'] for r in rows]==list(range(1,11))
        assert len(accepted)==10


def test_context_is_current_acl_and_dependencies_follow_derived_answers(queue):
    q=queue;doc=str(uuid4())
    q.corpus.put('rkb_documents',[{'id':doc,'owner_user_id':q.owner.subject,'content_visibility':'private','rights_status':'unknown'}])
    q.corpus.put('rkb_document_grants',[{'document_id':doc,'grantee_user_id':q.actors[0].subject}])
    first,_=claim(q)
    save_result(q,first,ExpertResult(text='Synthetic private derivative',document_ids=[doc]));deliver(q,first)
    second,_=claim(q,observation(number=2))
    args=(q.executor,second['request_id'],second['generation'],second['execution_token'])
    assert q.requests.history(*args)['items'][0]['state']=='available'
    with q.corpus.connect() as db:
        deps=db.execute('SELECT resource_id FROM rkb_expert_request_resources WHERE request_id=?',(second['request_id'],)).fetchall()
        assert [r[0] for r in deps]==[doc]
        db.execute("DELETE FROM corpus_rows WHERE table_name='rkb_document_grants'")
    hidden=q.requests.history(*args)['items'][0]
    assert hidden['state']=='withheld' and 'input_text' not in hidden and 'result' not in hidden
    expect('resource_access_revoked',save_result,q,second)


def test_restart_migrates_legacy_order_without_rewriting_original_evidence(queue):
    q=queue
    for i in range(1,4):q.requests.accept(observation(number=i))
    with q.corpus.connect() as db:
        originals=[tuple(r) for r in db.execute('SELECT id,origin,event_hash,input_text FROM rkb_expert_requests ORDER BY rowid')]
        db.execute('UPDATE rkb_expert_requests SET dialog_id=NULL,dialog_seq=NULL')
        db.execute('DELETE FROM rkb_request_dialogs')
    restarted=ExpertRequests(q.people,app_id='A111',team_id='T111',bot_user_id='UBOT',allowed_channels=['C111'])
    with q.corpus.connect() as db:
        assert [tuple(r) for r in db.execute('SELECT id,origin,event_hash,input_text FROM rkb_expert_requests ORDER BY rowid')]==originals
        assert [r[0] for r in db.execute('SELECT dialog_seq FROM rkb_expert_requests ORDER BY rowid')]==[1,2,3]
    lease=restarted.claim(q.executor)
    assert lease['request_id']==originals[0][0]


def test_legacy_out_of_order_send_cannot_pass_last_effect_check(queue):
    q=queue
    first,_=claim(q);save_result(q,first)
    second=q.requests.accept(observation(number=2))
    # A pre-migration snapshot could already contain a later in-flight turn.
    # Recovered state never authorizes sending it ahead of the dialog head.
    with q.corpus.connect() as db:
        db.execute("UPDATE rkb_expert_requests SET state='sending',delivery_service_id=?,delivery_service_epoch=1,delivery_id='legacy-send' WHERE id=?",(q.delivery.service_id,second['request_id']))
    expect('request_fence_denied',q.requests.check_delivery,q.delivery,second['request_id'],'legacy-send')
