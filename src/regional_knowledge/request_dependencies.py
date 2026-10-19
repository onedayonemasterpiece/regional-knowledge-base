"""Dependency checkpoints within ExpertRequests, not another scheduler.

BGE reads/ref updates happen outside corpus write transactions. Durable parent
references precede the checkpoint effect and are reconciled against that parent.
"""
import hashlib
import hmac
import json
import logging

from .bge_queue import QueryConsumer
from .expert_requests import ExpertRequestError,ExpertResult
from .participants import ParticipantError
from .request_authority import token_hash
from .sqlite_corpus import canonical

log=logging.getLogger(__name__)


def fail(code):raise ExpertRequestError(code)


class RequestDependencies:
    def __init__(self,requests,queue):
        self.requests=requests;self.queue=queue;self.corpus=requests.corpus
        self.namespace=hashlib.sha256(str(self.corpus.path.resolve()).encode()).hexdigest()
        self.missing_parent_reported=False
        with self.corpus.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS rkb_request_bge_dependencies(
                  request_id TEXT NOT NULL,generation INTEGER NOT NULL,job_id TEXT NOT NULL,
                  query_sha256 TEXT NOT NULL,query_text TEXT NOT NULL,
                  PRIMARY KEY(request_id,generation,job_id));
                CREATE TABLE IF NOT EXISTS rkb_request_checkpoints(
                  request_id TEXT NOT NULL,generation INTEGER NOT NULL,checkpoint_hash TEXT NOT NULL,
                  job_id TEXT NOT NULL,continuation TEXT NOT NULL,created_at REAL NOT NULL,
                  PRIMARY KEY(request_id,generation));
                CREATE TABLE IF NOT EXISTS rkb_request_dependency_cursors(
                  app_id TEXT NOT NULL,team_id TEXT NOT NULL,kind TEXT NOT NULL,last_id TEXT NOT NULL,
                  PRIMARY KEY(app_id,team_id,kind));
                ''')
        requests.dependencies=self

    def consumer(self,request_id):return QueryConsumer(self.namespace,str(request_id))

    def observe(self,context,job_id,query):
        if not context.request_id or not context.request_generation:fail('request_fence_denied')
        digest=hashlib.sha256(query.encode()).hexdigest()
        # A client-supplied arbitrary ID is not proof. The real source-search
        # result, queue actor, kind and immutable query identity must agree.
        self.queue.bind_consumer(context.principal.subject,str(job_id),self.consumer(context.request_id),digest)
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self.requests.people.require(context,'public_search',connection=db)
            row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(context.request_id,)).fetchone()
            self.requests._resources(db,row,ExpertResult(text='dependency authority check'))
            db.execute('INSERT OR IGNORE INTO rkb_request_bge_dependencies VALUES(?,?,?,?,?)',
                (context.request_id,context.request_generation,str(job_id),digest,query))

    def list(self,credentials,request_id,generation,execution_token,*,after_id='',limit=20):
        context=self.requests.context(credentials,request_id,generation,execution_token)
        rows=self.queue.consumer_queries(context.principal.subject,self.consumer(request_id),after_id=after_id,limit=limit)
        items=[]
        for row in rows[:limit]:
            # Also recovers a queue receipt if a tool response/deadline was lost
            # before its post-read ledger write. This is a new, current guarded
            # observation of the retained job, not invented old-generation proof.
            self.observe(context,row['job_id'],row['query'])
            items.append({'main_job_id':row['job_id'],'query':row['query'],'state':row['state'],
                'query_sha256':row['identity']['query_sha256']})
        return {'request_id':str(request_id),'items':items,'next_after_id':items[-1]['main_job_id'] if len(rows)>limit else None}

    def saved(self,db,row):
        checkpoint=db.execute('''SELECT * FROM rkb_request_checkpoints WHERE request_id=?
            ORDER BY generation DESC LIMIT 1''',(row['id'],)).fetchone()
        if not checkpoint:return None
        self.requests._resources(db,row,ExpertResult(text='checkpoint authority check'))
        dependency=db.execute('''SELECT query_text,query_sha256 FROM rkb_request_bge_dependencies
            WHERE request_id=? AND generation=? AND job_id=?''',(row['id'],checkpoint['generation'],checkpoint['job_id'])).fetchone()
        if not dependency:fail('checkpoint_unavailable')
        return {'generation':checkpoint['generation'],'checkpoint_hash':checkpoint['checkpoint_hash'],
            'main_job_id':checkpoint['job_id'],'continuation':checkpoint['continuation'],
            'query':dependency['query_text'],'query_sha256':dependency['query_sha256']}

    def checkpoint(self,credentials,request_id,generation,execution_token,job_id,continuation):
        if not isinstance(continuation,str) or not 1<=len(continuation)<=8000:fail('invalid_arguments')
        digest=hashlib.sha256(canonical([str(job_id),continuation]).encode()).hexdigest()
        with self.corpus.connect() as db:
            db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            service=self.requests._service(db,credentials,'executor')
            row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(str(request_id),)).fetchone()
            if (not row or row['generation']!=generation or row['executor_id']!=service['id'] or row['service_epoch']!=service['epoch']
                or not hmac.compare_digest(row['execution_token_hash'] or '',token_hash(execution_token))):fail('request_fence_denied')
            self.requests._identity(db,row)
            prior=db.execute('SELECT * FROM rkb_request_checkpoints WHERE request_id=? AND generation=?',(str(request_id),generation)).fetchone()
            if prior:
                if prior['checkpoint_hash']!=digest:fail('checkpoint_conflict')
                self.saved(db,row)
                return {'request_id':row['id'],'state':row['state'],'checkpoint_hash':digest}
            self.requests.context(credentials,request_id,generation,execution_token,connection=db)
            self.requests._resources(db,row,ExpertResult(text='checkpoint authority check'))
            dependency=db.execute('''SELECT 1 FROM rkb_request_bge_dependencies
                WHERE request_id=? AND generation=? AND job_id=?''',(str(request_id),generation,str(job_id))).fetchone()
            if not dependency:fail('dependency_unobserved')
            db.execute('INSERT INTO rkb_request_checkpoints VALUES(?,?,?,?,?,?)',
                (str(request_id),generation,digest,str(job_id),continuation,self.requests._now()))
            db.execute("UPDATE rkb_expert_requests SET state='dependency_wait',lease_until=NULL,updated_at=? WHERE id=?",(self.requests._now(),str(request_id)))
            self.requests._event(db,str(request_id),'dependency_checkpoint',{'generation':generation,'job_id':str(job_id),'checkpoint_hash':digest})
            return {'request_id':str(request_id),'state':'dependency_wait','checkpoint_hash':digest}

    def refresh(self):
        """Existing claim/wake lifecycle calls this bounded reconciliation."""
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            cursor=self._cursor(db,'waiting')
            sql='''SELECT r.*,c.job_id,d.query_sha256 FROM rkb_expert_requests r
                JOIN rkb_request_checkpoints c ON c.request_id=r.id AND c.generation=r.generation
                JOIN rkb_request_bge_dependencies d ON d.request_id=r.id AND d.generation=c.generation AND d.job_id=c.job_id
                WHERE r.state='dependency_wait' AND json_extract(r.origin,'$.app_id')=? AND json_extract(r.origin,'$.team_id')=?
                AND r.id>? ORDER BY r.id LIMIT 10'''
            rows=[dict(r) for r in db.execute(sql,(self.requests.app_id,self.requests.team_id,cursor))]
            if not rows and cursor:
                rows=[dict(r) for r in db.execute(sql,(self.requests.app_id,self.requests.team_id,''))]
            self._set_cursor(db,'waiting',rows[-1]['id'] if rows else '')
        for row in rows:
            try:
                result=self.queue.result(row['person_id'],row['job_id'],query_only=True)
                state='pending' if result['state']=='done' else 'dependency_wait'
                reason='dependency_ready'
                if result['identity'].get('query_sha256')!=row['query_sha256']:
                    state='needs_recovery';reason='dependency_identity_changed'
            except PermissionError:
                state='needs_recovery';reason='dependency_missing'
            transitioned=False
            with self.corpus.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                current=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(row['id'],)).fetchone()
                if current['state']!='dependency_wait' or current['generation']!=row['generation']:continue
                try:
                    self.requests._identity(db,current)
                    self.saved(db,current)
                except (ParticipantError,ExpertRequestError):state='blocked';reason='dependency_access_revoked'
                db.execute('UPDATE rkb_expert_requests SET state=?,updated_at=? WHERE id=?',(state,self.requests._now(),row['id']))
                if state!='dependency_wait':
                    self.requests._event(db,row['id'],reason,{'generation':row['generation'],'job_id':row['job_id']})
                    transitioned=True
            if transitioned:
                emit=log.warning if state in {'blocked','needs_recovery'} else log.info
                emit('request_dependency_transition',extra={'component':'expert_requests','request_id':row['id'],
                    'generation':row['generation'],'job_id':row['job_id'],'state':state,'reason_code':reason})
        self.reconcile_consumers()

    def reconcile_consumers(self):
        with self.corpus.connect() as db:cursor=self._cursor(db,'consumers')
        parents=self.queue.consumer_parents(self.namespace,after_id=cursor,limit=100)
        if not parents and cursor:parents=self.queue.consumer_parents(self.namespace,limit=100)
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if self._cursor(db,'consumers')==cursor:self._set_cursor(db,'consumers',parents[-1] if parents else '')
        terminal={'result_ready','sending','unknown','retry_wait','delivered','blocked'}
        for ident in parents:
            with self.corpus.connect() as db:
                row=db.execute('SELECT state FROM rkb_expert_requests WHERE id=?',(ident,)).fetchone()
            # Missing parent is an unexplained data loss, not proof of an orphan.
            if row and row['state'] in terminal:self.queue.release_consumer(self.namespace,ident)
            elif row is None and not self.missing_parent_reported:
                log.warning('request_dependency_parent_unavailable',extra={'component':'expert_requests','request_id':ident})
                self.missing_parent_reported=True

    def _cursor(self,db,kind):
        row=db.execute('SELECT last_id FROM rkb_request_dependency_cursors WHERE app_id=? AND team_id=? AND kind=?',
            (self.requests.app_id,self.requests.team_id,kind)).fetchone()
        return row[0] if row else ''

    def _set_cursor(self,db,kind,value):
        db.execute('''INSERT INTO rkb_request_dependency_cursors VALUES(?,?,?,?)
            ON CONFLICT(app_id,team_id,kind) DO UPDATE SET last_id=excluded.last_id''',
            (self.requests.app_id,self.requests.team_id,kind,value))
