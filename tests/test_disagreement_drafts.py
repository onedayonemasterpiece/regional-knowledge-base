"""Private analysis, explicit publication and same journal/request authority."""
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from regional_knowledge.disagreement_contracts import PersonalDraft,DraftPublication,CaseCreate,CaseGrant
from regional_knowledge.disagreement_drafts import PersonalDrafts
from regional_knowledge.disagreement_journal import JournalError
from regional_knowledge.participant_contracts import ParticipantGrant
from regional_knowledge.request_authority import track_resources
from test_disagreement_journal import journal,grant,evidence,expect
from test_disagreement_tools import surface,unpack
from test_expert_requests import queue,claim,observation,save_result
from test_expert_executor import sources,surface as executor_surface,client_for,rpc,tool_value


def draft(text='PRIVATE_ANALYSIS before. Publish selected position. PRIVATE_ANALYSIS after.',based_on_seq=1,**kwargs):
    return PersonalDraft(text=text,based_on_seq=based_on_seq,**kwargs)


def test_private_versions_do_not_enter_case_or_let_owner_read_another_author(journal):
    j=journal;grant(j,j.other);drafts=PersonalDrafts(j.store);ident=str(uuid4())
    before=j.store.get(j.owner,j.case)
    saved=drafts.save(j.other,j.case,ident,0,draft(),'private-draft-save')
    assert drafts.save(j.other,j.case,ident,0,draft(),'private-draft-save')==saved
    assert j.store.get(j.owner,j.case)==before
    for reader in (j.owner,j.outsider):
        expect('not_found_or_not_accessible',drafts.get,reader,j.case,ident)
        expect('not_found_or_not_accessible',drafts.save,reader,j.case,ident,1,draft(),'overwrite-someones-draft')
    assert 'PRIVATE_ANALYSIS' not in str(j.store.get(j.owner,j.case,section='history'))
    assert j.store.get(j.owner,j.case,section='contributions')['items']==[]
    newer=draft('A revised private analysis.')
    drafts.save(j.other,j.case,ident,1,newer,'new-private-version')
    assert drafts.get(j.other,j.case,ident)['payload']['text']==newer.text
    assert drafts.get(j.other,j.case,ident,version=1)['payload']['text']==draft().text
    expect('revision_conflict',drafts.save,j.other,j.case,ident,1,newer,'stale-draft-version')
    expect('idempotency_conflict',drafts.save,j.other,j.case,ident,0,newer,'private-draft-save')


def test_publish_only_exact_selected_fragment_and_atomic_same_key_replay(journal,monkeypatch):
    j=journal;grant(j,j.other);drafts=PersonalDrafts(j.store);ident=str(uuid4());payload=draft()
    drafts.save(j.other,j.case,ident,0,payload,'private-draft-save')
    start=payload.text.index('Publish');end=payload.text.index(' PRIVATE_ANALYSIS after')
    command=DraftPublication(text_start=start,text_end=end)
    # Failure after the shared insert rolls back the whole publication, including
    # its case event and private publication link. Original draft survives.
    original=j.store._append_contribution
    def interrupted(*args):original(*args);raise RuntimeError('synthetic crash before commit')
    monkeypatch.setattr(j.store,'_append_contribution',interrupted)
    with pytest.raises(RuntimeError):drafts.publish(j.other,j.case,ident,1,command,'selected-publication')
    assert j.store.get(j.owner,j.case,section='contributions')['items']==[]
    assert drafts.get(j.other,j.case,ident)['publication_ids']==[]
    monkeypatch.setattr(j.store,'_append_contribution',original)
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes=list(pool.map(lambda _:drafts.publish(j.other,j.case,ident,1,command,'selected-publication'),range(2)))
    assert outcomes[0]==outcomes[1]
    rows=j.store.get(j.owner,j.case,section='contributions')['items']
    assert len(rows)==1 and rows[0]['payload']['original_text']=='Publish selected position.'
    assert not rows[0]['payload']['human_reading_confirmed']
    assert rows[0]['actor_id']==j.other.subject and rows[0]['recorded_by']==j.other.subject
    assert 'PRIVATE_ANALYSIS' not in str(rows)
    assert drafts.get(j.other,j.case,ident)['payload']==payload.model_dump(mode='json')
    drafts.save(j.other,j.case,ident,1,draft('New private thought'),'next-private-version')
    assert drafts.publish(j.other,j.case,ident,1,command,'selected-publication')==outcomes[0]
    expect('revision_conflict',drafts.publish,j.other,j.case,ident,1,command,'new-stale-publication')


def test_current_source_access_fences_old_private_versions_and_receipts(journal):
    j=journal;cmd=evidence(j);j.store.add_evidence(j.owner,j.case,cmd,'case-source-evidence')
    grant(j,j.other)
    j.corpus.put('rkb_document_grants',[{'document_id':str(cmd.document_id),'grantee_user_id':j.other.subject}])
    drafts=PersonalDrafts(j.store);ident=str(uuid4());payload=draft(based_on_seq=j.store.get(j.other,j.case)['event_seq'],evidence_ids=[cmd.region_id])
    # Region IDs are not evidence IDs; the server does not turn them into proof.
    expect('invalid_evidence',drafts.save,j.other,j.case,ident,0,payload,'wrong-draft-evidence')
    payload=draft(based_on_seq=j.store.get(j.other,j.case)['event_seq'])
    drafts.save(j.other,j.case,ident,0,payload,'case-private-analysis')
    with j.corpus.connect() as db:db.execute("DELETE FROM corpus_rows WHERE table_name='rkb_document_grants'")
    expect('not_found_or_not_accessible',drafts.get,j.other,j.case,ident)
    expect('not_found_or_not_accessible',drafts.save,j.other,j.case,ident,0,payload,'case-private-analysis')
    expect('not_found_or_not_accessible',drafts.publish,j.other,j.case,ident,1,DraftPublication(text_start=0,text_end=5),'revoked-publication')


@pytest.mark.parametrize('command',[
    DraftPublication(text_start=0,text_end=1000),
    DraftPublication(text_start=1,text_end=2),
    DraftPublication(text_start=0,text_end=5,evidence_ids=[uuid4()]),
    DraftPublication(text_start=0,text_end=5,position_ids=['missing-position']),
])
def test_bad_selected_span_or_references_cannot_publish(journal,command):
    j=journal;drafts=PersonalDrafts(j.store);ident=str(uuid4())
    drafts.save(j.owner,j.case,ident,0,draft('a bcd'),'span-private-analysis')
    with pytest.raises(JournalError):drafts.publish(j.owner,j.case,ident,1,command,'invalid-selected-publication')
    assert j.store.get(j.owner,j.case,section='contributions')['items']==[]


@pytest.mark.asyncio
async def test_existing_mcp_tool_saves_reads_and_explicitly_publishes_draft(surface):
    s=surface
    async def call(name,args):return unpack(await s.mcp._tool_manager.call_tool(name,args,None))
    created=await call('disagreement_edit',{'action':'create','operation_key':'draft-test-case',
        'command':{'question':'Synthetic case','context':'Source context','classification':'candidate','rationale':'Investigate',
            'positions':[{'id':'a','text':'Variant','speaker':'Author','modality':'asserted'}]}})
    case=created['case_id'];ident=str(uuid4())
    args={'action':'draft_save','case_id':case,'draft_id':ident,'expected_version':0,
        'operation_key':'mcp-private-draft','command':draft().model_dump(mode='json')}
    saved=await call('disagreement_conclusion',args)
    assert saved['visibility']=='personal' and await call('disagreement_conclusion',args)==saved
    read=await call('disagreement_conclusion',{'action':'draft_get','case_id':case,'draft_id':ident})
    assert 'PRIVATE_ANALYSIS' in read['payload']['text']
    out=await call('disagreement_conclusion',{'action':'draft_publish','case_id':case,'draft_id':ident,
        'expected_version':1,'operation_key':'mcp-selected-publication','command':{'text_start':25,'text_end':50}})
    assert 'contribution_id' in out
    assert (await call('disagreement_get',{'case_id':case,'section':'contributions'}))['items'][0]['payload']['original_text']==draft().text[25:50]


@pytest.mark.asyncio
async def test_executor_request_generation_and_group_scope_apply_to_private_drafts(sources):
    from regional_knowledge.expert_executor import ExpertExecutor
    s=sources;q=s.q;actor=q.actors[0]
    q.change(actor.subject,ParticipantGrant(capability='personal_conclusion',granted=True,reason='test'))
    case=q.requests.journal.create(q.admin,CaseCreate(question='Synthetic case',context='Context',classification='candidate',
        rationale='test',positions=[{'id':'a','text':'First','speaker':'Author','modality':'asserted'}]),'executor-draft-case')['case_id']
    q.requests.journal.grant(q.admin,case,1,CaseGrant(grantee=actor.subject,capability='contributor',reason='test'),'grant-draft-case')
    lease,ctx=claim(q);executor=ExpertExecutor(q.requests,s.backend);ident=str(uuid4())
    base=(q.executor,lease['request_id'],lease['generation'],lease['execution_token'],case,ident)
    await executor.draft(*base,'draft_save',command=draft(),expected_version=0,operation_key='bound-private-draft')
    save_result(q,lease)
    with pytest.raises(Exception) as error:await executor.draft(*base,'draft_get')
    assert error.value.code=='request_fence_denied'
    grouped,_=claim(q,observation(number=2,group=True))
    groupbase=(q.executor,grouped['request_id'],grouped['generation'],grouped['execution_token'],case,ident)
    with pytest.raises(Exception) as error:await executor.draft(*groupbase,'draft_get')
    assert error.value.code=='public_projection_required'
    await s.backend.aclose()


def test_context_dependencies_survive_private_edit_and_cannot_be_published_as_a_substring(queue):
    q=queue;actor=q.actors[0]
    q.change(actor.subject,ParticipantGrant(capability='personal_conclusion',granted=True,reason='test'))
    case=q.requests.journal.create(q.admin,CaseCreate(question='Synthetic case',context='Context',classification='candidate',
        rationale='test',positions=[{'id':'a','text':'First','speaker':'Author','modality':'asserted'}]),'dependency-draft-case')['case_id']
    q.requests.journal.grant(q.admin,case,1,CaseGrant(grantee=actor.subject,capability='contributor',reason='test'),'dependency-draft-grant')
    lease,ctx=claim(q);doc=str(uuid4())
    q.corpus.put('rkb_documents',[{'id':doc,'owner_user_id':actor.subject,'content_visibility':'private'}])
    with q.corpus.connect() as db:track_resources(db,ctx,document_ids=[doc])
    ident=str(uuid4());store=q.requests.drafts;payload=draft()
    store.save(ctx,case,ident,0,payload,'with-inherited-context')
    store.save(ctx,case,ident,1,payload,'still-inherited-context')
    with pytest.raises(JournalError) as error:store.publish(ctx,case,ident,2,DraftPublication(text_start=0,text_end=5),'unsafe-context-publication')
    assert error.value.code=='draft_publication_requires_case_evidence'
    assert q.requests.journal.get(q.admin,case,section='contributions')['items']==[]
    # Own-scope revocation also prevents reading an old draft even though its
    # case is still accessible. There is no fallback to the owner's source ACL.
    q.corpus.put('rkb_documents',[{'id':doc,'owner_user_id':q.owner.subject,'content_visibility':'private'}])
    with pytest.raises(JournalError):store.get(ctx,case,ident,version=1)


def test_own_draft_listing_paginates_without_other_authors_or_private_text(journal):
    j=journal;grant(j,j.other);store=PersonalDrafts(j.store)
    own=sorted(str(uuid4()) for _ in range(3))
    for ident in own:store.save(j.other,j.case,ident,0,draft(),'save-own-'+ident)
    store.save(j.owner,j.case,str(uuid4()),0,draft(),'case-owner-private')
    first=store.list(j.other,j.case,limit=2)
    assert [x['draft_id'] for x in first['items']]==own[:2]
    assert first['next_after_id']==own[1] and 'PRIVATE_ANALYSIS' not in str(first)
    last=store.list(j.other,j.case,after_id=first['next_after_id'],limit=2)
    assert [x['draft_id'] for x in last['items']]==own[2:] and last['next_after_id'] is None


@pytest.mark.asyncio
async def test_real_http_draft_workflow_and_independent_capability_fences(sources,executor_surface):
    s=sources;q=s.q;actor=q.actors[0]
    q.change(actor.subject,ParticipantGrant(capability='personal_conclusion',granted=True,reason='test'))
    case=q.requests.journal.create(q.admin,CaseCreate(question='HTTP draft case',context='Context',classification='candidate',
        rationale='test',positions=[{'id':'a','text':'First','speaker':'Author','modality':'asserted'}]),'http-draft-case')['case_id']
    q.requests.journal.grant(q.admin,case,1,CaseGrant(grantee=actor.subject,capability='contributor',reason='test'),'http-draft-case-grant')
    accepted=q.requests.accept(observation());ident=str(uuid4())
    async with client_for(executor_surface,q.executor) as client:
        leased=tool_value(await rpc(client,'tools/call',{'name':'expert_request_delivery','arguments':{'action':'claim','request_id':accepted['request_id']}}))
        base={k:leased[k] for k in ('request_id','generation','execution_token')}
        async def call(operation,**fields):
            return tool_value(await rpc(client,'tools/call',{'name':'expert_research',
                'arguments':{**base,'operation':operation,'case_id':case,**fields}}))
        saved=await call('draft_save',draft_id=ident,expected_version=0,operation_key='http-private-analysis',command=draft().model_dump(mode='json'))
        assert saved['visibility']=='personal'
        assert (await call('draft_list'))['items'][0]['draft_id']==ident
        assert (await call('draft_get',draft_id=ident))['payload']['text']==draft().text
        q.change(actor.subject,ParticipantGrant(capability='case_contribute',granted=False,reason='test'))
        args={'draft_id':ident,'expected_version':1,'operation_key':'http-private-publish','command':{'text_start':25,'text_end':50}}
        assert (await call('draft_publish',**args))['error']['code']=='capability_denied'
        assert (await call('draft_get',draft_id=ident))['payload']['text']==draft().text
        q.change(actor.subject,ParticipantGrant(capability='case_contribute',granted=True,reason='test'))
        published=await call('draft_publish',**args)
        assert 'contribution_id' in published and await call('draft_publish',**args)==published
        q.change(actor.subject,ParticipantGrant(capability='personal_conclusion',granted=False,reason='test'))
        assert (await call('draft_publish',**args))['error']['code']=='capability_denied'
    async with client_for(executor_surface,'synthetic-owner-token') as client:
        denied=tool_value(await rpc(client,'tools/call',{'name':'disagreement_conclusion','arguments':
            {'action':'draft_get','case_id':case,'draft_id':ident}}))
        assert denied['error']['code']=='not_found_or_not_accessible'
    await s.backend.aclose()
