"""Small durable run/job queue. SQLite transactions fence starts and late results.

Only authorized text supplied by the consuming service enters this queue. Worker
credentials are run-bound; the worker receives neither DB nor object credentials.
"""
import contextlib
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import time
import uuid
from .bge_contract import SPACE, validate_vector

IDLE_SECONDS = 1800
ROTATE_SECONDS = 10 * 3600 + 45 * 60
MAX_LIFETIME = 11 * 3600
HEARTBEAT_SECONDS = 120
JOB_SECONDS = 180

class BgeQueue:
    def __init__(self, path, *, clock=time.time):
        self.path = Path(path)
        self.clock = clock
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self.connect() as db:
            db.executescript('''
            pragma journal_mode=WAL;
            create table if not exists runs(
              id text primary key,status text not null,token_hash text not null,
              started real not null,heartbeat real not null,useful real not null,
              ready_at real,provider_ref text,launch_state text not null default 'pending');
            create table if not exists control(id integer primary key check(id=1),current_run text,successor text);
            insert or ignore into control(id) values(1);
            create table if not exists jobs(
              id text primary key,actor text not null,idempotency text not null,
              kind text not null,texts text not null,identity text not null,
              state text not null,created real not null,updated real not null,
              run_id text,claim text,lease_until real,result text,attempt integer not null default 0,
              unique(actor,idempotency));
            create index if not exists bge_pending on jobs(state,kind,created);
            ''')
            if 'diagnostics' not in {row['name'] for row in db.execute('pragma table_info(runs)')}:
                db.execute("alter table runs add column diagnostics text not null default '{}'")
        self.path.chmod(0o600)

    @contextlib.contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute('begin immediate')
            yield db
            if db.in_transaction:db.commit()
        except BaseException:
            if db.in_transaction:db.rollback()
            raise
        finally:db.close()

    def _new_run(self, db, now):
        run_id = str(uuid.uuid4());token = secrets.token_urlsafe(32)
        # Runtime secret, never a run report. Atomic creation before committing
        # run identity allows safe launch recovery without generating a new token.
        folder = self.path.parent / 'bge-worker-credentials'
        folder.mkdir(exist_ok=True, mode=0o700)
        fd = os.open(folder/run_id,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'w') as file:file.write(token)
        db.execute('insert into runs(id,status,token_hash,started,heartbeat,useful) values(?,?,?,?,?,?)',
                   (run_id,'starting',hashlib.sha256(token.encode()).hexdigest(),now,now,now))
        return run_id

    def _expire(self, db, now):
        for run in db.execute("select * from runs where status in ('starting','ready','draining','failed')").fetchall():
            if run['status']=='failed' and now-run['heartbeat']<60:continue
            deadline = run['started'] + MAX_LIFETIME
            idle = run['useful'] + IDLE_SECONDS
            # A cold start gets at most 15 minutes to become ready. Heartbeats
            # cannot perpetually preserve an idle run or a failed warm-up.
            lost = (now-run['heartbeat']>HEARTBEAT_SECONDS if run['status']!='starting' else now-run['started']>900)
            if now>=deadline or now>=idle or lost or run['status']=='failed':
                db.execute("update runs set status='expired' where id=?",(run['id'],))
                db.execute("update jobs set state='pending',run_id=null,claim=null,lease_until=null where run_id=? and state='claimed'",(run['id'],))
                db.execute('update control set current_run=null where current_run=?',(run['id'],))
                db.execute('update control set successor=null where successor=?',(run['id'],))
        db.execute("update jobs set state='pending',run_id=null,claim=null where state='claimed' and lease_until<?",(now,))

    def _ensure(self, db, now, *, useful=True):
        self._expire(db,now)
        control = db.execute('select * from control where id=1').fetchone()
        current = db.execute('select * from runs where id=?',(control['current_run'],)).fetchone()
        if current is None:
            # Reuse a warming successor if the previous serving worker was lost.
            successor=db.execute('select * from runs where id=?',(control['successor'],)).fetchone()
            run_id=successor['id'] if successor else self._new_run(db,now)
            db.execute('update control set current_run=?,successor=null where id=1',(run_id,))
        else:
            run_id=current['id']
            if current['status']=='ready' and now-current['started']>=ROTATE_SECONDS and not control['successor']:
                successor=self._new_run(db,now)
                db.execute('update control set successor=? where id=1',(successor,))
        if useful:db.execute('update runs set useful=? where id=?',(now,run_id))
        return run_id

    def enqueue(self, actor, idempotency, texts, *, kind='query', identity=None):
        if kind not in ('query','document') or len(texts)!=1 or not isinstance(texts[0],str) or not texts[0].strip() or len(texts[0])>16000:
            raise ValueError('BGE job contract')
        identity=identity or {};now=self.clock()
        with self.connect() as db:
            previous=db.execute('select * from jobs where actor=? and idempotency=?',(actor,idempotency)).fetchone()
            if previous:
                if json.loads(previous['texts'])!=texts or json.loads(previous['identity'])!=identity or previous['kind']!=kind:
                    raise ValueError('idempotency payload mismatch')
                if previous['state']!='done':self._ensure(db,now)
                return previous['id']
            if db.execute("select count(*) from jobs where state!='done'").fetchone()[0]>=256:
                raise RuntimeError('BGE queue full')
            self._ensure(db,now);job_id=str(uuid.uuid4())
            db.execute('insert into jobs(id,actor,idempotency,kind,texts,identity,state,created,updated) values(?,?,?,?,?,?,?,?,?)',
                       (job_id,actor,idempotency,kind,json.dumps(texts),json.dumps(identity),'pending',now,now))
            return job_id

    def _authorize(self, db, run_id, token, now):
        run=db.execute('select * from runs where id=?',(run_id,)).fetchone()
        digest=hashlib.sha256(token.encode()).hexdigest()
        if not run or not secrets.compare_digest(run['token_hash'],digest) or run['status'] not in ('starting','ready','draining') or now>=run['started']+MAX_LIFETIME:
            raise PermissionError('invalid or stale BGE worker')
        return run

    def heartbeat(self, run_id, token, *, ready=False, diagnostics=None):
        now=self.clock()
        with self.connect() as db:
            self._expire(db,now);run=self._authorize(db,run_id,token,now)
            db.execute('update runs set heartbeat=? where id=?',(now,run_id))
            if diagnostics is not None:
                if not isinstance(diagnostics,dict):raise ValueError('BGE diagnostics type')
                safe=json.loads(run['diagnostics'])
                for key in ('startup_seconds','rss_kib','hwm_kib','pss_kib','cpu_seconds','cpu_threads','pid'):
                    if key in diagnostics:
                        import math
                        value=float(diagnostics[key])
                        if not math.isfinite(value) or value<0:raise ValueError('BGE diagnostics value')
                        safe[key]=value
                db.execute('update runs set diagnostics=? where id=?',(json.dumps(safe),run_id))
            if ready and run['status']=='starting':
                control=db.execute('select * from control where id=1').fetchone()
                db.execute("update runs set status='ready',ready_at=? where id=?",(now,run_id))
                if control['successor']==run_id:
                    db.execute("update runs set status='draining' where id=?",(control['current_run'],))
                    db.execute('update control set current_run=?,successor=null where id=1',(run_id,))
            return self._status(db,now)

    def claim(self, run_id, token):
        now=self.clock()
        with self.connect() as db:
            self._expire(db,now);run=self._authorize(db,run_id,token,now)
            db.execute('update runs set heartbeat=? where id=?',(now,run_id))
            if run['status']!='ready' or db.execute("select 1 from jobs where run_id=? and state='claimed'",(run_id,)).fetchone():return None
            job=db.execute("select * from jobs where state='pending' order by case kind when 'query' then 0 else 1 end,created,id limit 1").fetchone()
            if job is None:return None
            claim=secrets.token_urlsafe(24)
            db.execute("update jobs set state='claimed',run_id=?,claim=?,lease_until=?,attempt=attempt+1,updated=? where id=?",(run_id,claim,now+JOB_SECONDS,now,job['id']))
            db.execute('update runs set useful=? where id=?',(now,run_id))
            return {'id':job['id'],'claim':claim,'kind':job['kind'],'texts':json.loads(job['texts']),'space':SPACE}

    def complete(self, run_id, token, job_id, claim, space, vectors, timings):
        if space!=SPACE or len(vectors)!=1:raise ValueError('BGE result space/batch mismatch')
        vectors=[validate_vector(vector) for vector in vectors];now=self.clock()
        safe_times={key:float(timings[key]) for key in ('encoder_seconds',) if key in timings}
        with self.connect() as db:
            self._expire(db,now);self._authorize(db,run_id,token,now)
            job=db.execute('select * from jobs where id=?',(job_id,)).fetchone()
            if not job or job['run_id']!=run_id or job['claim']!=claim:raise PermissionError('late/stale BGE result')
            if job['state']=='done':return 'duplicate'
            if job['state']!='claimed' or now>=job['lease_until']:raise PermissionError('expired BGE job')
            result={'space':SPACE,'vectors':vectors,'queue_seconds':max(0,job['updated']-job['created']),**safe_times}
            db.execute("update jobs set state='done',result=?,updated=? where id=?",(json.dumps(result),now,job_id))
            db.execute('update runs set useful=? where id=?',(now,run_id))
            return 'accepted'

    def result(self, actor, job_id):
        with self.connect() as db:
            self._expire(db,self.clock())
            row=db.execute('select * from jobs where id=? and actor=?',(job_id,actor)).fetchone()
            if not row:raise PermissionError('BGE job not found')
            return {'state':row['state'],'result':json.loads(row['result']) if row['result'] else None,'identity':json.loads(row['identity'])}

    def _status(self, db, now):
        control=db.execute('select * from control where id=1').fetchone()
        run=db.execute('select * from runs where id=?',(control['current_run'],)).fetchone()
        return {'state':run['status'] if run else 'stopped','space':SPACE,'queue_depth':db.execute("select count(*) from jobs where state!='done'").fetchone()[0],'successor_starting':bool(control['successor']),'lease_remaining_seconds':max(0,run['useful']+IDLE_SECONDS-now) if run else 0}

    def status(self):
        with self.connect() as db:
            self._expire(db,self.clock());return self._status(db,self.clock())

    def maintenance(self):
        now=self.clock()
        with self.connect() as db:
            self._expire(db,now)
            if db.execute("select 1 from jobs where state!='done'").fetchone():self._ensure(db,now,useful=False)
            control=db.execute('select * from control where id=1').fetchone()
            current=db.execute('select * from runs where id=?',(control['current_run'],)).fetchone()
            if current and current['status']=='ready' and now-current['started']>=ROTATE_SECONDS and not control['successor']:
                db.execute('update control set successor=? where id=1',(self._new_run(db,now),))
            # Old workers drain only their already claimed job; no more claims.
            db.execute("update runs set status='expired' where status='draining' and not exists(select 1 from jobs where jobs.run_id=runs.id and state='claimed')")
            return [dict(row) for row in db.execute('select id,status,launch_state,provider_ref from runs').fetchall()]

    def launch_claim(self, run_id):
        with self.connect() as db:
            row=db.execute('select * from runs where id=?',(run_id,)).fetchone()
            if not row or row['status']!='starting' or row['launch_state']!='pending':return None
            # Claim once. An ambiguous provider response is reconciled by its
            # stable notebook slug, never retried as an unknown second launch.
            db.execute("update runs set launch_state='dispatching' where id=?",(run_id,))
            token=(self.path.parent/'bge-worker-credentials'/run_id).read_text()
            return token

    def launch_record(self, run_id, provider_ref, *, failed=False):
        with self.connect() as db:
            db.execute('update runs set provider_ref=?,launch_state=?,status=case when ? then ? else status end where id=?',
                       (provider_ref,'failed' if failed else 'dispatched',failed,'failed',run_id))
            if failed:
                db.execute('update runs set heartbeat=? where id=?',(self.clock(),run_id))
                db.execute("update jobs set state='pending',run_id=null,claim=null,lease_until=null where run_id=? and state='claimed'",(run_id,))
