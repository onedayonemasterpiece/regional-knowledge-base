"""Real indexed queue and result receipts; fictional page metadata, no provider."""
import json
from types import SimpleNamespace

import pytest

from regional_knowledge.kaggle_recovery_dispatch import RecoveryPhaseDispatch
from regional_knowledge.kaggle_pg_snapshot_native import encode, sha, SnapshotError
from test_bge_admission import state, auth, people
from test_kaggle_recovery_phases import database


@pytest.mark.asyncio
@pytest.mark.parametrize('changed',[None,'handle','run_id','launch_nonce','runtime_incarnation',
    'database_generation','writer_generation','request_sha256','malformed','failed','error','missing','binding','parent'])
async def test_completed_same_handle_avoids_native_status_and_keeps_existing_fences(state,monkeypatch,changed):
    s=state;dbspec=database();binding=SimpleNamespace(connection_generation=s.generation)
    bridge=RecoveryPhaseDispatch(s.q,database=dbspec,identity=(s.run,s.nonce,'incarnation-a'),
        binding_current=lambda b:b is binding)
    page={'physical_page_index':7};pending={'status':'pending','handle':'fictional-render'}
    def identity(phase,args,key):
        return dict(phase=phase,arguments=args,operation_key=key,request_sha256=sha(encode(dict(phase=phase,arguments=args))),
            database_generation=dbspec.database_generation,writer_generation=dbspec.writer_generation,
            run_id=s.run,launch_nonce=s.nonce,runtime_incarnation='incarnation-a')
    original=identity('original_pages',{'index':7},'page7')
    parent,_=bridge._enqueue(original)
    cached=identity('snapshot_status',{'handle':'fictional-render'},'status7')
    child,_=bridge._enqueue(cached)
    completed={'value':{'status':'complete','value':page}}
    if changed=='handle':cached['arguments']['handle']='other'
    elif changed in cached:cached[changed]='other'
    elif changed=='failed':completed={'value':{'status':'rejected','reason':'snapshot_io_rejected'}}
    elif changed=='error':completed={'value':{'status':'complete','value':{'error':'unavailable'}}}
    elif changed=='missing':completed={'value':{'status':'complete'}}
    with s.q.connect() as db:
        db.execute("UPDATE jobs SET state='done',result=? WHERE id=?",(json.dumps({'value':pending}),parent))
        db.execute("UPDATE jobs SET state='done',identity=?,result=? WHERE id=?",
            (encode(cached).decode(), 'not-json' if changed=='malformed' else json.dumps(completed),child))
    calls=[];enqueue=bridge._enqueue
    def observed(value):
        calls.append(value['phase']);return enqueue(value)
    bridge._enqueue=observed
    async def wait_result(actor,job,timeout):
        with s.q.connect(write=False) as db:r=db.execute('SELECT identity,result FROM jobs WHERE id=?',(job,)).fetchone()
        return {'result':json.loads(r['result']) if r['result'] else completed_default}
    completed_default={'value':{'status':'complete','value':page}}
    monkeypatch.setattr(s.q,'wait_result',wait_result)
    try:
        if changed=='binding':
            with pytest.raises(PermissionError):await bridge.request(object(),'original_pages',{'index':7},operation_key='page7')
        elif changed=='parent':
            with pytest.raises(SnapshotError,match='operation_changed'):
                await bridge.request(binding,'original_pages',{'index':8},operation_key='page7')
        else:
            assert await bridge.request(binding,'original_pages',{'index':7},operation_key='page7')==page
            assert calls.count('snapshot_status')==(0 if changed is None else 1)
        assert not bridge._bindings
    finally:await bridge.aclose()
