"""One ordinary development attempt with preserved historical uncertainty."""
import concurrent.futures
import threading

import pytest
from regional_knowledge.bge_queue import BgeQueue

REF='owner/stable-notebook'


@pytest.fixture
def env(tmp_path):
    now=[1000.]
    q=BgeQueue(tmp_path/'queue.sqlite',clock=lambda:now[0])
    old=q.request_startup()['run_id']
    q.launch_claim(old,provider_ref=REF)
    with q.connect() as db:
        db.execute("update runs set status='expired' where id=?",(old,))
        candidate=q._new_run(db,now[0])
        db.execute('update control set current_run=? where id=1',(candidate,))
    return q,now,old,candidate


def prepare(q,candidate,**kwargs):
    return q.prepare_debug_launch(candidate,provider_ref=REF,operation_key='dev-attempt-1',
        reason_reference='owner-directed-service-development',**kwargs)


def protected(q,old):
    with q.connect(write=False) as db:
        return (dict(db.execute('select * from runs where id=?',(old,)).fetchone()),
                [tuple(r) for r in db.execute('select * from jobs order by id')])


def test_fenced_history_preserved_and_one_exact_claim(env):
    q,now,old,candidate=env
    assert q.launch_claim(candidate,provider_ref=REF) is None
    before=protected(q,old)
    receipt=prepare(q,candidate)
    assert receipt['historical_unknown_count']==1 and receipt['consumed_at'] is None
    assert protected(q,old)==before
    assert q.launch_claim(candidate,provider_ref=REF)
    assert q.launch_claim(candidate,provider_ref=REF) is None
    assert protected(q,old)==before
    replay=prepare(q,candidate)
    assert replay['consumed_at']==now[0]
    assert replay['inventory_sha256']==receipt['inventory_sha256']
    # The original source/run is the only reconciliation target after lost Save ACK.
    with q.connect(write=False) as db:
        assert db.execute('select launch_state from runs where id=?',(candidate,)).fetchone()[0]=='dispatching'
    assert q.status()['state']=='starting'


@pytest.mark.parametrize('mutation',["provider_version=7","channel_active=1","heartbeat=1001", "status='ready'", "provider_ref='another/slug'"])
def test_old_identity_or_authority_drift_blocks_claim(env,mutation):
    q,now,old,candidate=env;prepare(q,candidate)
    with q.connect() as db:db.execute('update runs set '+mutation+' where id=?',(old,))
    assert q.launch_claim(candidate,provider_ref=REF) is None
    with q.connect(write=False) as db:
        assert db.execute('select consumed_at from bge_debug_launches').fetchone()[0] is None


def test_new_unlisted_reservation_blocks_claim(env):
    q,now,old,candidate=env;prepare(q,candidate)
    with q.connect() as db:
        extra=q._new_run(db,now[0])
        db.execute("update runs set status='expired',launch_state='dispatching',provider_ref=? where id=?",(REF,extra))
    assert q.launch_claim(candidate,provider_ref=REF) is None


def test_wrong_ref_and_expiry_do_not_consume(env):
    q,now,old,candidate=env;prepare(q,candidate,seconds=10)
    assert q.launch_claim(candidate,provider_ref='owner/other') is None
    now[0]+=10
    assert q.launch_claim(candidate,provider_ref=REF) is None


def test_parallel_claims_produce_exactly_one_provider_intent(env):
    q,now,old,candidate=env;prepare(q,candidate);barrier=threading.Barrier(2)
    def claim(_):
        local=BgeQueue(q.path,clock=lambda:now[0]);barrier.wait()
        return local.launch_claim(candidate,provider_ref=REF)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(claim,range(2)))
    assert sum(x is not None for x in results)==1


def test_intent_key_cannot_be_rebound_to_another_candidate(env):
    q,now,old,candidate=env;prepare(q,candidate)
    with q.connect() as db:another=q._new_run(db,now[0])
    with pytest.raises(ValueError,match='changed intent'):prepare(q,another)
    assert q.launch_claim(another,provider_ref=REF) is None


def test_claimed_jobs_block_scope_without_cleanup(env):
    q,now,old,candidate=env
    q.enqueue('fixture-actor','fixture-request',['synthetic text'])
    with q.connect() as db:db.execute("update jobs set state='claimed',run_id=?,claim='fixture-claim'",(old,))
    before=protected(q,old)
    with pytest.raises(ValueError,match='claims'):prepare(q,candidate)
    assert protected(q,old)==before


def test_writer_and_publication_guards_rechecked_at_claim(env):
    q,now,old,candidate=env;prepare(q,candidate)
    with q.connect() as db:
        db.execute('create table rkb_writer_control(active integer,publication_json text)')
        db.execute('insert into rkb_writer_control values(1,NULL)')
    assert q.launch_claim(candidate,provider_ref=REF) is None
    with q.connect() as db:db.execute("update rkb_writer_control set active=0,publication_json='unsettled'")
    assert q.launch_claim(candidate,provider_ref=REF) is None


def test_known_not_sent_failure_does_not_rearm_allowance(env):
    q,now,old,candidate=env;prepare(q,candidate)
    assert q.launch_claim(candidate,provider_ref=REF)
    q.launch_record(candidate,REF,failed=True)
    now[0]+=61
    replacement=q.request_startup()['run_id']
    assert replacement!=candidate
    assert q.launch_claim(replacement,provider_ref=REF) is None
    assert prepare(q,candidate)['consumed_at'] is not None


@pytest.mark.parametrize('change',["delete from runs where id=?", "update runs set launch_state='terminal' where id=?"])
def test_missing_or_falsely_terminal_enrolled_row_blocks_consumption(env,change):
    q,now,old,candidate=env;prepare(q,candidate)
    with q.connect() as db:db.execute(change,(old,))
    assert q.launch_claim(candidate,provider_ref=REF) is None
    with q.connect(write=False) as db:
        assert db.execute('select consumed_at from bge_debug_launches').fetchone()[0] is None
