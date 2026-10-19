"""Request-bound isolation across actual domain operations and delayed sends."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.disagreement_contracts import CaseCreate,CaseGrant,ContributionAdd
from regional_knowledge.disagreement_journal import DisagreementJournal
from regional_knowledge.expert_requests import (
    ExpertRequestError,ExpertRequests,ExpertResult,ServiceCredentials,
    SlackDeliveryReceipt,SlackObservation,
)
from regional_knowledge.participant_contracts import AccountBegin,AccountConfirm,AccountUnlink,ParticipantGrant,ParticipantUpsert
from regional_knowledge.participants import ParticipantError,Participants,RequestAuthority
from regional_knowledge.sqlite_corpus import SQLiteCorpus


def expect(code,fn,*args,**kwargs):
    with pytest.raises((ExpertRequestError,ParticipantError)) as e: fn(*args,**kwargs)
    assert e.value.code==code


@pytest.fixture
def queue(tmp_path):
    corpus=SQLiteCorpus(tmp_path/'corpus.sqlite3')
    owner=Principal(subject=str(uuid4()),issuer='test',client_id='test',access_token='synthetic')
    corpus.put('rkb_users',[{'id':owner.subject,'status':'active'}])
    clock=[datetime(2026,10,19,11,tzinfo=timezone.utc)]
    people=Participants(corpus,owner.subject,clock=lambda:clock[0]); admin=RequestAuthority(owner,'owner')
    def change(person,command):
        revision=people.get(admin,person)['revision']
        return people.change(admin,person,revision,command,'change-'+str(uuid4()))
    actors=[]
    for index in (1,2):
        actor=owner.model_copy(update={'subject':str(uuid4())})
        people.change(admin,actor.subject,0,ParticipantUpsert(name='Synthetic '+str(index),reason='test'),'person-'+str(index))
        link=change(actor.subject,AccountBegin(provider='slack',tenant='T111',provider_subject='U'+str(index),reason='test'))
        change(actor.subject,AccountConfirm(link_id=link['link_id'],expected_link_revision=1,method='owner_confirmed_slack',reason='test'))
        for capability in ('public_search','case_read','case_contribute'):
            change(actor.subject,ParticipantGrant(capability=capability,granted=True,reason='test'))
        actors.append(actor)
    requests=ExpertRequests(people,app_id='A111',team_id='T111',bot_user_id='UBOT',allowed_channels=['C111'])
    executor=ServiceCredentials('test_executor','executor-synthetic-secret-'+'e'*32)
    delivery=ServiceCredentials('test_delivery','delivery-synthetic-secret-'+'d'*32)
    requests.provision_service(admin,executor.service_id,'executor',executor.token)
    requests.provision_service(admin,delivery.service_id,'delivery',delivery.token)
    return SimpleNamespace(corpus=corpus,owner=owner,people=people,admin=admin,clock=clock,change=change,actors=actors,requests=requests,executor=executor,delivery=delivery)


def observation(user='U1',number=1,group=False):
    return SlackObservation(app_id='A111',team_id='T111',user_id=user,channel_id='C111' if group else 'D'+user,
        channel_type='group' if group else 'im',thread_ts='1792400000.111',message_ts='1792400000.'+str(number),
        event_id='Ev'+str(number),text='Synthetic research request '+str(number))


def claim(q,obs=None):
    accepted=q.requests.accept(obs or observation())
    leased=q.requests.claim(q.executor,request_id=accepted['request_id'])
    context=q.requests.context(q.executor,leased['request_id'],leased['generation'],leased['execution_token'])
    return leased,context


def save_result(q,lease,result=None):
    return q.requests.record_result(q.executor,lease['request_id'],lease['generation'],lease['execution_token'],result or ExpertResult(text='Synthetic answer'))


def test_durable_accept_dedup_two_collectors_and_bad_origin(queue):
    q=queue; obs=observation()
    first=q.requests.accept(obs)
    assert q.requests.accept(obs)=={**first,'duplicate':True}
    assert q.requests.accept(replace(obs,event_id='EvAnotherCollector'))=={**first,'duplicate':True}
    expect('event_identity_conflict',q.requests.accept,replace(obs,text='Changed same event'))
    for changed in ({'team_id':'TOTHER'},{'app_id':'AOTHER'},{'user_id':'UBOT'},{'channel_type':'im','channel_id':'C111'}):
        expect('route_denied',q.requests.accept,replace(obs,**changed))
    expect('route_denied',q.requests.accept,replace(obs,channel_id='COTHER',channel_type='group'))
    with q.corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM rkb_expert_requests').fetchone()[0]==1
        assert db.execute('SELECT count(*) FROM rkb_expert_request_events').fetchone()[0]==1


def test_two_workers_claim_once_and_late_generation_cannot_write(queue):
    q=queue; accepted=q.requests.accept(observation())
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:q.requests.claim(q.executor,request_id=accepted['request_id'],lease_seconds=1),range(2)))
    assert sum(r['state']=='leased' for r in results)==1
    first=next(r for r in results if r['state']=='leased')
    old=q.requests.context(q.executor,first['request_id'],first['generation'],first['execution_token'])
    q.clock[0]+=timedelta(seconds=2)
    newer=q.requests.claim(q.executor,request_id=first['request_id'])
    assert newer['generation']==first['generation']+1
    expect('request_fence_denied',q.people.require,old,'case_contribute')
    expect('request_fence_denied',save_result,q,first)


def test_same_executor_cannot_mix_actor_request_generation_or_status(queue):
    q=queue
    first,a=claim(q,observation('U1',1)); second,b=claim(q,observation('U2',2))
    expect('request_fence_denied',q.requests.context,q.executor,second['request_id'],second['generation'],first['execution_token'])
    expect('identity_unverified',q.people.require,replace(a,principal=b.principal),'public_search')
    expect('request_unavailable',q.requests.get,a,second['request_id'])
    expect('request_fence_denied',q.people.require,replace(a,request_id=second['request_id']),'public_search')
    assert q.requests.get(b,second['request_id'])['input_text']=='Synthetic research request 2'


def test_domain_writes_carry_executor_and_request_scope_and_late_writes_fail(queue):
    q=queue
    journal=DisagreementJournal(q.corpus,participants=q.people)
    case=journal.create(q.admin,CaseCreate(question='Synthetic question',context='Context',classification='candidate',rationale='Reason',positions=[{'id':'a','text':'A','speaker':'Author','modality':'asserted'}]),'create-domain-case')['case_id']
    journal.grant(q.admin,case,1,CaseGrant(grantee=q.actors[0].subject,capability='contributor',reason='test'),'grant-case-access')
    first,a=claim(q,observation('U1',1)); second,b=claim(q,replace(observation('U1',2),thread_ts='200.2'))
    command=ContributionAdd(kind='argument',original_text='Synthetic argument')
    x=journal.contribute(a,case,command,'same-client-key')
    assert journal.contribute(a,case,command,'same-client-key')==x
    y=journal.contribute(b,case,command,'same-client-key')
    assert x['contribution_id']!=y['contribution_id']
    items=journal.get(q.admin,case,section='contributions')['items']
    assert {i['recorded_by'] for i in items}=={q.executor.service_id}
    assert {i['payload']['attribution']['request_id'] for i in items}=={first['request_id'],second['request_id']}
    save_result(q,first)
    expect('request_fence_denied',journal.contribute,a,case,command,'late-after-result')


def test_person_link_grant_and_service_revocation_are_live_fences(queue):
    q=queue; lease,ctx=claim(q)
    q.change(q.actors[0].subject,ParticipantGrant(capability='case_contribute',granted=False,reason='revoked'))
    expect('capability_denied',q.people.require,ctx,'case_contribute')
    q.requests.provision_service(q.admin,q.executor.service_id,'executor',q.executor.token,expected_epoch=1,enabled=False)
    expect('request_fence_denied',q.people.require,ctx,'public_search')
    expect('service_denied',save_result,q,lease)


def test_revoked_mapping_blocks_pending_claim_and_old_event_replay(queue):
    q=queue; obs=observation(); accepted=q.requests.accept(obs)
    context=q.people.resolve_external(provider='slack',tenant='T111',provider_subject='U1',principal_factory=lambda _:q.actors[0])
    q.change(q.actors[0].subject,AccountUnlink(link_id=context.link_id,expected_link_revision=context.link_revision,reason='wrong mapping',incorrect_binding=True))
    expect('identity_unverified',q.requests.accept,obs)
    assert q.requests.claim(q.executor,request_id=accepted['request_id'])['state']=='blocked'


def test_group_public_scope_cannot_read_personal_cases_or_private_documents(queue):
    q=queue; lease,ctx=claim(q,observation(group=True))
    assert ctx.public_only
    expect('public_projection_required',q.people.require,ctx,'case_read')
    expect('public_projection_required',q.people.get,ctx,q.actors[0].subject)
    doc=str(uuid4())
    q.corpus.put('rkb_documents',[{'id':doc,'owner_user_id':q.actors[0].subject,'content_visibility':'private','rights_status':'unknown'}])
    expect('public_projection_required',save_result,q,lease,ExpertResult(text='No private derivative in group',document_ids=[doc]))
    save_result(q,lease,ExpertResult(text='Please request the private proof in a verified personal DM.'))


def test_lost_result_response_exact_replay_and_no_send_replay_after_unknown(queue):
    q=queue; lease,ctx=claim(q)
    result=save_result(q,lease)
    assert save_result(q,lease)==result
    expect('result_conflict',save_result,q,lease,ExpertResult(text='Different answer'))
    send=q.requests.claim_delivery(q.delivery,lease['request_id'])
    assert send['send_allowed']
    assert q.requests.claim_delivery(q.delivery,lease['request_id'])['state']=='reconciliation_required'
    assert q.requests.check_delivery(q.delivery,lease['request_id'],send['delivery_id'])['text']=='Synthetic answer'
    q.requests.unknown_delivery(q.delivery,lease['request_id'],send['delivery_id'])
    restarted=ExpertRequests(q.people,app_id='A111',team_id='T111',bot_user_id='UBOT',allowed_channels=['C111'])
    assert not restarted.claim_delivery(q.delivery,lease['request_id'])['send_allowed']
    expect('request_fence_denied',restarted.check_delivery,q.delivery,lease['request_id'],send['delivery_id'])


def test_exact_native_receipt_reconciles_unknown_without_duplicate(queue):
    q=queue; lease,_=claim(q);save_result(q,lease)
    send=q.requests.claim_delivery(q.delivery,lease['request_id'])
    q.requests.unknown_delivery(q.delivery,lease['request_id'],send['delivery_id'])
    receipt=SlackDeliveryReceipt(app_id='A111',team_id='T111',bot_user_id='UBOT',channel_id='DU1',thread_ts='1792400000.111',message_ts='1792400050.1',client_message_id=send['delivery_id'],text=send['text'])
    for change in ({'bot_user_id':'UOTHER'},{'channel_id':'DU2'},{'thread_ts':'1792400000.222'},{'client_message_id':str(uuid4())},{'text':'Different content'}):
        expect('provider_receipt_mismatch',q.requests.receipt,q.delivery,lease['request_id'],replace(receipt,**change))
    saved=q.requests.receipt(q.delivery,lease['request_id'],receipt)
    assert q.requests.receipt(q.delivery,lease['request_id'],receipt)==saved
    assert q.requests.claim_delivery(q.delivery,lease['request_id'])=={'request_id':lease['request_id'],'state':'delivered','send_allowed':False}


def test_acl_revoked_after_result_or_delivery_claim_prevents_effect(queue):
    q=queue; lease,_=claim(q)
    doc=str(uuid4())
    q.corpus.put('rkb_documents',[{'id':doc,'owner_user_id':q.owner.subject,'content_visibility':'private','rights_status':'unknown'}])
    q.corpus.put('rkb_document_grants',[{'document_id':doc,'grantee_user_id':q.actors[0].subject}])
    save_result(q,lease,ExpertResult(text='Synthetic private answer',document_ids=[doc]))
    send=q.requests.claim_delivery(q.delivery,lease['request_id'])
    with q.corpus.connect() as db:
        db.execute("DELETE FROM corpus_rows WHERE table_name='rkb_document_grants'")
    expect('resource_access_revoked',q.requests.check_delivery,q.delivery,lease['request_id'],send['delivery_id'])
    expect('resource_access_revoked',q.requests.claim_delivery,q.delivery,lease['request_id'])
    expect('resource_access_revoked',q.requests.get,RequestAuthority(q.actors[0],'personal'),lease['request_id'])


def test_server_tracks_case_reads_so_omitted_result_references_do_not_launder_private_text(queue):
    from regional_knowledge.disagreement_contracts import EvidenceAdd
    q=queue
    journal=DisagreementJournal(q.corpus,participants=q.people)
    case=journal.create(q.admin,CaseCreate(question='Synthetic question',context='Context',classification='candidate',rationale='Compare sources',positions=[{'id':'a','text':'A','speaker':'Author','modality':'asserted'}]),'taint-test-case')['case_id']
    doc,page,region=[str(uuid4()) for _ in range(3)]
    q.corpus.put('rkb_documents',[{'id':doc,'owner_user_id':q.owner.subject,'content_visibility':'private','source_sha256':'a'*64,'active_revision':1,'rights_status':'unknown'}])
    q.corpus.put('rkb_pages',[{'id':page,'document_id':doc,'revision':1,'physical_page_index':1}])
    q.corpus.put('rkb_regions',[{'id':region,'page_id':page,'source_text':'Synthetic private sentence.'}])
    journal.add_evidence(q.admin,case,EvidenceAdd(document_id=doc,source_revision=1,source_sha256='a'*64,page_id=page,region_id=region,text_start=0,quote='Synthetic private sentence.',context='Synthetic private sentence.',position_ids=['a'],relation='supports',speaker='Author',attribution='direct_statement'),'private-source-evidence')
    q.corpus.put('rkb_document_grants',[{'document_id':doc,'grantee_user_id':q.actors[0].subject}])
    journal.grant(q.admin,case,1,CaseGrant(grantee=q.actors[0].subject,capability='viewer',reason='explicit case grant'),'private-case-access')
    lease,ctx=claim(q)
    assert journal.get(ctx,case,section='evidence')['items'][0]['payload']['quote']=='Synthetic private sentence.'
    # The executor deliberately omits its source references; server-known
    # input dependencies remain mandatory for later readback and delivery.
    save_result(q,lease,ExpertResult(text='A derivative of the private sentence.'))
    with q.corpus.connect() as db:
        assert {(r['kind'],r['resource_id']) for r in db.execute('SELECT * FROM rkb_expert_request_resources')}=={('case',case),('document',doc)}
        db.execute("DELETE FROM corpus_rows WHERE table_name='rkb_document_grants'")
    expect('resource_access_revoked',q.requests.claim_delivery,q.delivery,lease['request_id'])
    expect('resource_access_revoked',q.requests.get,RequestAuthority(q.actors[0],'personal'),lease['request_id'])


def test_service_purpose_and_cross_request_receipts_cannot_expand_scope(queue):
    q=queue; first,_=claim(q,observation('U1',1)); second,_=claim(q,observation('U2',2))
    save_result(q,first);save_result(q,second)
    expect('service_denied',q.requests.claim_delivery,q.executor,first['request_id'])
    expect('service_denied',q.requests.claim,q.delivery)
    send=q.requests.claim_delivery(q.delivery,first['request_id'])
    receipt=SlackDeliveryReceipt(app_id='A111',team_id='T111',bot_user_id='UBOT',channel_id='DU1',thread_ts='1792400000.111',message_ts='1792400050.1',client_message_id=send['delivery_id'],text=send['text'])
    expect('request_fence_denied',q.requests.receipt,q.delivery,second['request_id'],receipt)


def test_service_grant_cannot_cross_another_app_in_the_same_corpus(queue):
    q=queue
    other=ExpertRequests(q.people,app_id='AOTHER',team_id='T111',bot_user_id='UOTHER',allowed_channels=['C111'])
    credentials=ServiceCredentials('other_app_executor','other-app-executor-'+'s'*32)
    other.provision_service(q.admin,credentials.service_id,'executor',credentials.token)
    accepted=q.requests.accept(observation())
    expect('service_denied',q.requests.claim,credentials)
    assert other.claim(credentials)=={'state':'empty'}
    expect('service_denied',other.claim,q.executor)
    assert q.requests.claim(q.executor,request_id=accepted['request_id'])['state']=='leased'


def test_service_provision_lost_response_replay_keeps_epoch_and_audit(queue):
    q=queue
    saved=q.requests.provision_service(q.admin,q.executor.service_id,'executor',q.executor.token,expected_epoch=0)
    assert saved['epoch']==1
    q.requests.provision_service(q.admin,q.executor.service_id,'executor',q.executor.token,expected_epoch=1,enabled=False)
    expect('revision_conflict',q.requests.provision_service,q.admin,q.executor.service_id,'executor',q.executor.token,expected_epoch=0)
    with q.corpus.connect() as db:
        rows=db.execute('SELECT * FROM rkb_expert_service_events WHERE service_id=? ORDER BY epoch',(q.executor.service_id,)).fetchall()
        assert [r['epoch'] for r in rows]==[1,2]
        assert all(q.executor.token not in r['payload'] for r in rows)


def test_case_access_revocation_invalidates_server_tracked_result(queue):
    q=queue
    journal=DisagreementJournal(q.corpus,participants=q.people)
    case=journal.create(q.admin,CaseCreate(question='Synthetic case',context='Context',classification='candidate',rationale='Reason',positions=[{'id':'a','text':'A','speaker':'Author','modality':'asserted'}]),'revocable-case')['case_id']
    journal.grant(q.admin,case,1,CaseGrant(grantee=q.actors[0].subject,capability='viewer',reason='test'),'revocable-grant')
    lease,ctx=claim(q)
    journal.get(ctx,case)
    save_result(q,lease,ExpertResult(text='Case-derived answer without declared case references.'))
    journal.grant(q.admin,case,2,CaseGrant(grantee=q.actors[0].subject,capability=None,reason='revoked'),'revoke-case-grant')
    expect('resource_access_revoked',q.requests.claim_delivery,q.delivery,lease['request_id'])
