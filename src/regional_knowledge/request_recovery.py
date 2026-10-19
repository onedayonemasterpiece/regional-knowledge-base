"""Audited local owner recheck of a retained, identical BGE dependency.

No inference, queue enqueue, identity changes or delivery effects. A repaired
dependency re-enters the existing fair request lifecycle with its original plan.
"""
import argparse
import hashlib
import json
import logging
import os
import re
from uuid import UUID

from .expert_requests import ExpertRequestError
from .participants import ParticipantError
from .sqlite_corpus import canonical

log=logging.getLogger(__name__)

def fail(code):raise ExpertRequestError(code)


class RequestRecovery:
    def __init__(self,dependencies):
        self.dependencies=dependencies;self.requests=dependencies.requests
        self.corpus=self.requests.corpus
        with self.corpus.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_request_recovery_operations(
                actor_id TEXT NOT NULL,operation_key TEXT NOT NULL,request_id TEXT NOT NULL,
                request_hash TEXT NOT NULL,receipt TEXT NOT NULL,created_at REAL NOT NULL,
                PRIMARY KEY(actor_id,operation_key))''')

    def _row(self,db,request_id):
        row=db.execute('''SELECT * FROM rkb_expert_requests WHERE id=?
            AND json_extract(origin,'$.app_id')=? AND json_extract(origin,'$.team_id')=?''',
            (str(request_id),self.requests.app_id,self.requests.team_id)).fetchone()
        if not row:fail('request_unavailable')
        return row

    def inspect(self,owner,*,after_id='',limit=20):
        """Opaque IDs/fences only; never return input, plan or private evidence."""
        if not isinstance(after_id,str) or type(limit) is not int or not 1<=limit<=100:fail('invalid_arguments')
        with self.corpus.connect() as db:
            db.execute('BEGIN')
            self.requests.people._authority(db,owner,admin=True)
            rows=db.execute('''SELECT r.id,r.generation,r.state,c.job_id,c.checkpoint_hash,
                (SELECT kind FROM rkb_expert_request_events e WHERE e.request_id=r.id ORDER BY seq DESC LIMIT 1) reason_code
                FROM rkb_expert_requests r LEFT JOIN rkb_request_checkpoints c
                ON c.request_id=r.id AND c.generation=r.generation
                WHERE r.state='needs_recovery' AND r.id>?
                AND json_extract(r.origin,'$.app_id')=? AND json_extract(r.origin,'$.team_id')=?
                ORDER BY r.id LIMIT ?''',(after_id,self.requests.app_id,self.requests.team_id,limit+1)).fetchall()
            items=[dict(r) for r in rows[:limit]]
            return {'items':items,'next_after_id':items[-1]['id'] if len(rows)>limit else None}

    def _check(self,db,row,generation,checkpoint_hash):
        if row['generation']!=generation or row['state']!='needs_recovery' or row['result_json'] is not None:fail('recovery_conflict')
        checkpoint=self.dependencies.saved(db,row)
        if not checkpoint or checkpoint['checkpoint_hash']!=checkpoint_hash:fail('recovery_conflict')
        self.requests._identity(db,row)
        return checkpoint

    @staticmethod
    def _replay(db,actor,key,digest):
        old=db.execute('SELECT * FROM rkb_request_recovery_operations WHERE actor_id=? AND operation_key=?',(actor,key)).fetchone()
        if old:
            if old['request_hash']!=digest:fail('recovery_key_conflict')
            return json.loads(old['receipt'])

    def recheck(self,owner,request_id,*,generation,checkpoint_hash,operation_key,reason):
        try:request_id=str(UUID(str(request_id)))
        except (ValueError,TypeError,AttributeError):fail('invalid_arguments')
        if (type(generation) is not int or generation<1 or not isinstance(checkpoint_hash,str)
            or not re.fullmatch('[0-9a-f]{64}',checkpoint_hash) or not isinstance(operation_key,str)
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{7,127}',operation_key)
            or not isinstance(reason,str) or not 1<=len(reason)<=1000 or not reason.strip()):fail('invalid_arguments')
        digest=hashlib.sha256(canonical([request_id,generation,checkpoint_hash,reason]).encode()).hexdigest()
        with self.corpus.connect() as db:
            db.execute('BEGIN')
            actor=self.requests.people._authority(db,owner,admin=True)
            row=self._row(db,request_id)
            replay=self._replay(db,actor,operation_key,digest)
            if replay is not None:return replay
            checkpoint=self._check(db,row,generation,checkpoint_hash)
            person=row['person_id']
        # No corpus writer lock during BGE access; this method cannot launch,
        # expire or re-enqueue work even when the underlying run is unavailable.
        try:
            dependency_state=self.dependencies.queue.consumer_state(person,checkpoint['main_job_id'],
                self.dependencies.consumer(request_id),checkpoint['query_sha256'])
            state='pending' if dependency_state=='done' else 'dependency_wait'
            if dependency_state not in {'pending','claimed','done'}:state='needs_recovery'
        except PermissionError:state='needs_recovery'
        with self.corpus.connect() as db:
            db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            actor=self.requests.people._authority(db,owner,admin=True)
            row=self._row(db,request_id)
            replay=self._replay(db,actor,operation_key,digest)
            if replay is not None:return replay
            self._check(db,row,generation,checkpoint_hash)
            receipt={'request_id':request_id,'generation':generation,'checkpoint_hash':checkpoint_hash,
                'main_job_id':checkpoint['main_job_id'],'state':state,'operation_key':operation_key,
                'recovered':state!='needs_recovery'}
            if state!='needs_recovery':
                db.execute('''UPDATE rkb_expert_requests SET state=?,lease_until=NULL,updated_at=?
                    WHERE id=?''',(state,self.requests._now(),request_id))
            self.requests._event(db,request_id,'operator_dependency_recheck',
                {**receipt,'actor_id':actor,'reason':reason})
            db.execute('INSERT INTO rkb_request_recovery_operations VALUES(?,?,?,?,?,?)',
                (actor,operation_key,request_id,digest,canonical(receipt),self.requests._now()))
        emit=log.info if receipt['recovered'] else log.warning
        emit('request_operator_recheck',extra={'component':'expert_requests','request_id':request_id,
            'generation':generation,'job_id':checkpoint['main_job_id'],'state':state,
            'reason_code':'dependency_restored' if receipt['recovered'] else 'dependency_unresolved'})
        return receipt


def main(argv=None):
    parser=argparse.ArgumentParser(description='Inspect/recheck retained RKB dependencies locally; no provider sends or inference.')
    commands=parser.add_subparsers(dest='command',required=True)
    listing=commands.add_parser('list');listing.add_argument('--after-id',default='');listing.add_argument('--limit',type=int,default=20)
    recheck=commands.add_parser('recheck')
    recheck.add_argument('--request-id',required=True);recheck.add_argument('--generation',type=int,required=True)
    recheck.add_argument('--checkpoint-hash',required=True);recheck.add_argument('--operation-key',required=True)
    recheck.add_argument('--reason',required=True)
    args=parser.parse_args(argv)
    try:
        from .expert_admin import runtime
        from .bge_queue import BgeQueue
        from .request_dependencies import RequestDependencies
        from pathlib import Path
        path=Path(os.environ['RKB_BGE_QUEUE_PATH'])
        if not path.is_file():raise ValueError('existing BGE queue required')
        requests,owner=runtime()
        recovery=RequestRecovery(RequestDependencies(requests,BgeQueue(path)))
        if args.command=='list':result=recovery.inspect(owner,after_id=args.after_id,limit=args.limit)
        else:result=recovery.recheck(owner,args.request_id,generation=args.generation,
            checkpoint_hash=args.checkpoint_hash,operation_key=args.operation_key,reason=args.reason)
    except (ValueError,KeyError,OSError,ParticipantError,ExpertRequestError) as error:
        code=error.code if isinstance(error,(ParticipantError,ExpertRequestError)) else type(error).__name__
        raise SystemExit('Request recovery failed ('+code+'). No input or credential printed.') from None
    print(json.dumps(result))


if __name__=='__main__':main()
