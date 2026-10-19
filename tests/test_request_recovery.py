"""Owner recovery rechecks retained receipts, never creates provider work."""
from concurrent.futures import ThreadPoolExecutor
import json
from uuid import uuid4

import pytest

from regional_knowledge.participant_contracts import ParticipantGrant
from regional_knowledge.participants import RequestAuthority
from regional_knowledge.request_recovery import RequestRecovery,main
from test_expert_requests import queue,claim,observation,expect,save_result
from test_request_dependencies import dependent,observed,checkpoint,complete


@pytest.fixture
def broken(dependent):
    q=dependent;q.recovery=RequestRecovery(q.deps)
    q.lease,q.ctx=claim(q);q.job=observed(q,q.lease,q.ctx)
    q.saved=checkpoint(q,q.lease,q.job)
    with q.bge.connect() as db:
        q.original=dict(db.execute('SELECT * FROM jobs WHERE id=?',(q.job,)).fetchone())
        db.execute("UPDATE jobs SET identity='{}' WHERE id=?",(q.job,))
    q.deps.refresh()
    return q


def repair(q):
    with q.bge.connect() as db:db.execute('UPDATE jobs SET identity=? WHERE id=?',(q.original['identity'],q.job))


def recheck(q,**changes):
    args={'generation':q.lease['generation'],'checkpoint_hash':q.saved['checkpoint_hash'],
        'operation_key':'operator-recheck-0001','reason':'Retained dependency repaired and checked'}
    args.update(changes)
    return q.recovery.recheck(q.admin,q.lease['request_id'],**args)


def test_unresolved_recheck_is_audited_without_new_run_or_job(broken):
    q=broken
    with q.bge.connect() as db:
        original=[dict(r) for r in db.execute('SELECT * FROM runs')]
    out=recheck(q)
    assert out['state']=='needs_recovery' and not out['recovered']
    assert recheck(q)==out
    with q.bge.connect() as db:
        assert [dict(r) for r in db.execute('SELECT * FROM runs')]==original
        assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==1
    with q.corpus.connect() as db:
        row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(q.lease['request_id'],)).fetchone()
        assert row['state']=='needs_recovery' and row['input_text']=='Synthetic research request 1'
        assert db.execute('SELECT count(*) FROM rkb_request_checkpoints').fetchone()[0]==1
        assert db.execute('SELECT count(*) FROM rkb_request_recovery_operations').fetchone()[0]==1
    # A lost unresolved receipt remains that receipt after repair. Deliberate
    # re-observation uses a new operator key; it cannot change the old outcome.
    repair(q)
    assert recheck(q)==out
    expect('recovery_key_conflict',recheck,q,reason='Changed key payload')
    assert recheck(q,operation_key='operator-recheck-0002')['state']=='dependency_wait'


@pytest.mark.parametrize('ready',[False,True])
def test_repaired_dependency_resumes_same_plan_only_through_fair_claim(broken,ready):
    q=broken;repair(q)
    if ready:complete(q,q.job)
    out=recheck(q)
    assert out['state']==('pending' if ready else 'dependency_wait')
    expect('request_fence_denied',q.people.require,q.ctx,'public_search')
    if not ready:
        assert q.requests.claim(q.executor)['state']=='empty'
        complete(q,q.job)
    # Another person must receive their fair turn before an explicit ID hint.
    other=q.requests.accept(observation('U2',2))
    assert q.requests.claim(q.executor,request_id=q.lease['request_id'])['state']=='deferred'
    assert q.requests.claim(q.executor)['request_id']==other['request_id']
    resumed=q.requests.claim(q.executor)
    assert resumed['request_id']==q.lease['request_id'] and resumed['generation']==q.lease['generation']+1
    ctx=q.requests.context(q.executor,resumed['request_id'],resumed['generation'],resumed['execution_token'])
    assert q.requests.get(ctx,resumed['request_id'])['checkpoint']['continuation']=='Retained plan, not a human verification'
    save_result(q,resumed)
    assert recheck(q)==out  # Historical receipt, no reopening finished work.


def test_recovery_owner_only_and_current_person_capabilities_required(broken):
    q=broken;repair(q)
    args={'generation':q.lease['generation'],'checkpoint_hash':q.saved['checkpoint_hash'],
        'operation_key':'unauthorized-recovery-1','reason':'test'}
    for ctx in (q.ctx,RequestAuthority(q.actors[1],'personal'),RequestAuthority(q.owner,'personal')):
        expect('capability_denied',q.recovery.recheck,ctx,q.lease['request_id'],**args)
        expect('capability_denied',q.recovery.inspect,ctx)
    q.change(q.actors[0].subject,ParticipantGrant(capability='public_search',granted=False,reason='revoked'))
    expect('capability_denied',recheck,q)
    with q.corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM rkb_request_recovery_operations').fetchone()[0]==0
        assert db.execute('SELECT state FROM rkb_expert_requests WHERE id=?',(q.lease['request_id'],)).fetchone()[0]=='needs_recovery'


@pytest.mark.parametrize('tamper',['body','actor','kind','released'])
def test_original_job_id_is_not_enough_to_resume_a_dependency(broken,tamper):
    q=broken;repair(q)
    with q.bge.connect() as db:
        if tamper=='body':db.execute('UPDATE jobs SET texts=? WHERE id=?',(json.dumps(['Different query']),q.job))
        if tamper=='actor':db.execute("UPDATE jobs SET actor='other-person' WHERE id=?",(q.job,))
        if tamper=='kind':db.execute("UPDATE jobs SET kind='document' WHERE id=?",(q.job,))
        if tamper=='released':db.execute('UPDATE query_parents SET released=1')
    assert recheck(q)['state']=='needs_recovery'


def test_owner_and_request_fences_rechecked_after_bge_read(broken,monkeypatch):
    q=broken;repair(q);original=q.bge.consumer_state
    def read(*args):
        result=original(*args)
        q.change(q.actors[0].subject,ParticipantGrant(capability='public_search',granted=False,reason='race'))
        return result
    monkeypatch.setattr(q.bge,'consumer_state',read)
    expect('capability_denied',recheck,q)
    with q.corpus.connect() as db:
        assert db.execute('SELECT state FROM rkb_expert_requests WHERE id=?',(q.lease['request_id'],)).fetchone()[0]=='needs_recovery'
        assert db.execute('SELECT count(*) FROM rkb_request_recovery_operations').fetchone()[0]==0


def test_recovery_cannot_turn_owner_authority_into_expert_source_access(broken):
    q=broken;repair(q);doc=str(uuid4())
    q.corpus.put('rkb_documents',[{'id':doc,'owner_user_id':q.owner.subject,
        'content_visibility':'private','rights_status':'unknown'}])
    q.corpus.put('rkb_document_grants',[{'document_id':doc,'grantee_user_id':q.actors[0].subject}])
    # Retained record of an earlier authorized source read, before failure.
    with q.corpus.connect() as db:
        db.execute('INSERT INTO rkb_expert_request_resources VALUES(?,?,?,?)',
            (q.lease['request_id'],q.lease['generation'],'document',doc))
        db.execute("DELETE FROM corpus_rows WHERE table_name='rkb_document_grants'")
    expect('resource_access_revoked',recheck,q)
    with q.corpus.connect() as db:
        assert db.execute('SELECT state FROM rkb_expert_requests WHERE id=?',(q.lease['request_id'],)).fetchone()[0]=='needs_recovery'


def test_concurrent_operator_replay_records_once_and_rejects_stale_fences(broken):
    q=broken;repair(q)
    expect('recovery_conflict',recheck,q,generation=q.lease['generation']+1)
    expect('recovery_conflict',recheck,q,checkpoint_hash='0'*64)
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes=list(pool.map(lambda _:recheck(q),range(2)))
    assert outcomes[0]==outcomes[1]
    with q.corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM rkb_request_recovery_operations').fetchone()[0]==1
        assert db.execute("SELECT count(*) FROM rkb_expert_request_events WHERE kind='operator_dependency_recheck'").fetchone()[0]==1
    expect('recovery_conflict',recheck,q,operation_key='operator-recheck-0002')


def test_inspection_scoped_and_opaque_and_cli_exact_replay(broken,monkeypatch,capsys):
    from regional_knowledge import expert_admin
    q=broken
    page=q.recovery.inspect(q.admin)
    assert len(page['items'])==1 and page['items'][0]['id']==q.lease['request_id']
    assert page['items'][0]['checkpoint_hash']==q.saved['checkpoint_hash']
    assert 'input_text' not in str(page) and 'Retained plan' not in str(page)
    assert q.recovery.inspect(q.admin,after_id=q.lease['request_id'])['items']==[]
    # Wrong configured runtime cannot inspect or recover another app's rows.
    q.requests.app_id='AOTHER'
    assert q.recovery.inspect(q.admin)['items']==[]
    expect('request_unavailable',recheck,q)
    q.requests.app_id='A111'
    monkeypatch.setattr(expert_admin,'runtime',lambda:(q.requests,q.admin))
    monkeypatch.setenv('RKB_BGE_QUEUE_PATH',str(q.bge.path))
    main(['list']);assert json.loads(capsys.readouterr().out)['items'][0]['id']==q.lease['request_id']
    # Runtime CLI connects to the same retained queue; clock differences do not
    # cause expiry/run mutation in consumer_state.
    repair(q)
    args=['recheck','--request-id',q.lease['request_id'],'--generation',str(q.lease['generation']),
        '--checkpoint-hash',q.saved['checkpoint_hash'],'--operation-key','cli-recheck-0001','--reason','Synthetic repair']
    main(args);first=json.loads(capsys.readouterr().out)
    main(args);assert json.loads(capsys.readouterr().out)==first
    assert first['state']=='dependency_wait'
    q.requests.app_id='AOTHER'
    expect('request_unavailable',q.recovery.recheck,q.admin,q.lease['request_id'],
        generation=q.lease['generation'],checkpoint_hash=q.saved['checkpoint_hash'],
        operation_key='cli-recheck-0001',reason='Synthetic repair')


def test_deleted_dependency_recovery_requires_the_original_retained_job(broken):
    q=broken
    with q.bge.connect() as db:db.execute('DELETE FROM jobs WHERE id=?',(q.job,))
    assert not recheck(q)['recovered']
    # Simulate restoring the exact original job from a retained backup, without
    # enqueue/_ensure or another model execution. Parent reference stayed intact.
    with q.bge.connect() as db:
        keys=list(q.original)
        db.execute('INSERT INTO jobs('+','.join(keys)+') VALUES('+','.join('?' for _ in keys)+')',tuple(q.original[k] for k in keys))
    assert recheck(q,operation_key='after-exact-restore')['state']=='dependency_wait'
    with q.bge.connect() as db:
        assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==1
        assert db.execute('SELECT attempt FROM jobs WHERE id=?',(q.job,)).fetchone()[0]==0


@pytest.mark.parametrize('changes',[
    {'generation':True},{'reason':' '*5},{'reason':'r'+' '*1000},
    {'checkpoint_hash':'not-a-hash'},{'operation_key':'short'},
])
def test_invalid_recovery_arguments_have_no_effect(broken,changes):
    expect('invalid_arguments',recheck,broken,**changes)


def test_recovery_never_resends_an_unknown_answer(queue):
    q=queue
    from regional_knowledge.bge_queue import BgeQueue
    from regional_knowledge.request_dependencies import RequestDependencies
    q.recovery=RequestRecovery(RequestDependencies(q.requests,BgeQueue(q.corpus.path.parent/'bge.sqlite')))
    q.lease,q.ctx=claim(q);save_result(q,q.lease)
    send=q.requests.claim_delivery(q.delivery,q.lease['request_id'])
    q.requests.unknown_delivery(q.delivery,q.lease['request_id'],send['delivery_id'])
    expect('recovery_conflict',q.recovery.recheck,q.admin,q.lease['request_id'],generation=q.lease['generation'],
        checkpoint_hash='0'*64,operation_key='never-resend-unknown',reason='test')
    assert q.recovery.inspect(q.admin)['items']==[]
