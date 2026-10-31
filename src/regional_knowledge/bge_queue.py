"""Small durable run/job queue. SQLite transactions fence starts and late results.

Only authorized text supplied by the consuming service enters this queue. Worker
credentials are run-bound; the worker receives neither DB nor object credentials.
"""
import contextlib
import asyncio
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import secrets
import sqlite3
import time
import threading
import uuid
from .bge_contract import SPACE, validate_vector

IDLE_SECONDS = 1800
ROTATE_SECONDS = 8 * 3600 + 15 * 60  # Drain/checkpoint before the nine-hour limit.
MAX_LIFETIME = 11 * 3600
HEARTBEAT_SECONDS = 120
JOB_SECONDS = 180
DONE_QUERY_RETENTION_SECONDS = 24 * 3600
DONE_AUDIT_RETENTION_SECONDS = 24 * 3600
MAX_STARTUP_FAILURES = 3  # Finite verified-not-sent/terminal retries per durable demand.

# A notification is only a hint; committed SQLite rows remain authoritative.
# Cross-process completions use the bounded reconciliation below.
_WAITERS = {}
_WAITERS_LOCK = threading.Lock()
RECONCILE_SECONDS = 0.25
LIVE_JOBS = "state!='done'"
_LOG = logging.getLogger(__name__)


class StaleClaim(PermissionError):
    """This assignment ended; the authenticated run may continue working."""


class RevokedRun(PermissionError):
    """The run has no current worker authority."""


class StaleAttachment(PermissionError):
    """The run is authenticated but this dispatch attachment has no authority."""


class AttachmentConflict(StaleAttachment):
    """Competing runtime incarnations permanently fenced this run's dispatch."""


class AttachmentBusy(StaleAttachment):
    """A legacy HTTP assignment must finish before the first WSS attachment."""


class BgeQueue:
    def __init__(self, path, *, clock=time.time):
        self.path = Path(path)
        self.clock = clock
        self._shared_reads = None
        self._source_commands = None
        self._story_editorial = None
        self._recovery_phases = None
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Serving readers must not rerun schema initialization behind every
        # writer. A missing/older schema still takes the serialized migration.
        if self.path.exists() and self._schema_current():
            self.path.chmod(0o600)
            return
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
            create index if not exists bge_retention on jobs(state,kind,updated);
            create index if not exists bge_assigned on jobs(run_id,state);
            create index if not exists bge_live on jobs(kind,state,run_id) where state!='done';
            ''')
            # executescript ends its caller's transaction. Serialize additive
            # migrations explicitly, including concurrent first-time openers.
            db.execute('begin immediate')
            control_columns={row['name'] for row in db.execute('pragma table_info(control)')}
            for name,definition in (('startup_ref','text'),('startup_state','text'),
                                    ('startup_failures','integer not null default 0')):
                if name not in control_columns:
                    db.execute(f'alter table control add column {name} {definition}')
            if 'diagnostics' not in {row['name'] for row in db.execute('pragma table_info(runs)')}:
                db.execute("alter table runs add column diagnostics text not null default '{}'")
            if 'provider_version' not in {row['name'] for row in db.execute('pragma table_info(runs)')}:
                db.execute('alter table runs add column provider_version integer')
            columns={row['name'] for row in db.execute('pragma table_info(runs)')}
            for name,definition in (
                ('launch_nonce','text'),
                ('runtime_incarnation','text'),
                ('connection_generation','integer not null default 0'),
                ('channel_active','integer not null default 0'),
                ('attachment_conflict','integer not null default 0'),
                ('bootstrap_finalized_at','real'),
            ):
                if name not in columns:
                    db.execute(f'alter table runs add column {name} {definition}')
            db.execute("""create table if not exists bge_provider_terminal_receipts(
                run_id text primary key,receipt_json text not null)""")
            db.execute("""create table if not exists bge_provider_reconciliation(
                id integer primary key check(id=1),cursor integer not null,
                next_cycle real not null)""")
            db.execute('insert or ignore into bge_provider_reconciliation values(1,0,0)')
        self.path.chmod(0o600)

    def _schema_current(self):
        try:
            with self.connect(write=False) as db:
                db.execute('select id,status,token_hash,started,heartbeat,useful,ready_at,provider_ref,launch_state,diagnostics,provider_version,launch_nonce,runtime_incarnation,connection_generation,channel_active,attachment_conflict,bootstrap_finalized_at from runs limit 0')
                if db.execute('select current_run,successor,startup_ref,startup_state,startup_failures from control where id=1').fetchone() is None:
                    return False
                db.execute('select run_id,receipt_json from bge_provider_terminal_receipts limit 0')
                if db.execute('select cursor,next_cycle from bge_provider_reconciliation where id=1').fetchone() is None:
                    return False
                db.execute('select id,actor,idempotency,kind,texts,identity,state,created,updated,run_id,claim,lease_until,result,attempt from jobs limit 0')
                # Existing databases need the same bounded maintenance/claim
                # lookups as new ones; the schema fast path must not skip them.
                indexes={row['name'] for row in db.execute('pragma index_list(jobs)')}
                if not {'bge_retention','bge_assigned','bge_live'}<=indexes:
                    return False
            return True
        except sqlite3.OperationalError:
            return False

    @contextlib.contextmanager
    def connect(self, *, write=True):
        db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            if not write:
                db.execute('pragma query_only=ON')
            db.execute('begin immediate' if write else 'begin')
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

    @staticmethod
    def _provider_outcome_unknown(run):
        # The local run lease is not proof that the remote notebook terminated.
        return (run['launch_state'] in ('dispatching','dispatched')
                and run['status'] in ('starting','ready','draining'))

    @staticmethod
    def _run_expired(run, now):
        prepared=('bootstrap_finalized_at' in run.keys() and run['bootstrap_finalized_at'] is not None)
        lost = (now-run['heartbeat']>HEARTBEAT_SECONDS
                if run['status']!='starting' or prepared else now-run['started']>900)
        return (run['status']=='expired' or now>=run['started']+MAX_LIFETIME
                or (run['status']!='draining' and now>=run['useful']+IDLE_SECONDS)
                or lost or run['status']=='failed')

    def _retire_recovery_jobs(self,db,now):
        # One retirement path, including historical rows; no native success is
        # fabricated, and original assignments/known outcomes remain evidence.
        db.execute("""UPDATE jobs SET state='done',updated=?,
            result=coalesce(result,'{"value":{"error":"run_expired"}}')
            WHERE kind='recovery_phase' AND state!='done'
            AND actor IN (SELECT 'maintenance:'||id FROM runs WHERE status IN ('expired','failed'))""",(now,))

    @staticmethod
    def _idle_drain_due(db,run,now):
        from .queue_writer import pending
        return (run['status']=='ready' and run['launch_state']=='dispatched'
            and run['provider_version'] is not None
            and run['channel_active'] and not run['attachment_conflict']
            and now>=run['useful']+IDLE_SECONDS
            and now<run['started']+MAX_LIFETIME
            and now-run['heartbeat']<=HEARTBEAT_SECONDS
            and not pending(db,run['id']))

    def _expire(self, db, now):
        from .queue_writer import pending
        for run in db.execute("select * from runs where status in ('starting','ready','draining','failed')").fetchall():
            if run['status']=='draining' and run['launch_state'] in ('dispatching','dispatched'):
                continue  # Keep this reservation/channel until exact provider terminal proof.
            if self._idle_drain_due(db,run,now):
                # Keep the exact live channel for checkpoint/STOP and existing
                # claims. Draining denies fresh serving/claims; native terminal
                # proof still owns retirement, never the useful-idle clock.
                db.execute("UPDATE runs SET status='draining' WHERE id=?",(run['id'],))
                _LOG.info('bge_idle_drain requested run_id=%s provider_version=%s',
                          run['id'],run['provider_version'])
                continue
            # A cold start gets at most 15 minutes to prove the completed native
            # bootstrap or become ready. Only trusted finalized pins permit the
            # normal heartbeat lease; idle and maximum lifetimes still apply.
            if self._run_expired(run, now):
                if pending(db,run['id']):
                    continue
                db.execute("update runs set status='expired' where id=?",(run['id'],))
                db.execute("update jobs set state='pending',run_id=null,claim=null,lease_until=null where run_id=? and state='claimed' and kind!='recovery_phase'",(run['id'],))
                db.execute('update control set current_run=null where current_run=?',(run['id'],))
                db.execute('update control set successor=null where successor=?',(run['id'],))
        self._retire_recovery_jobs(db,now)
        db.execute("update jobs set state='pending',run_id=null,claim=null where state='claimed' and lease_until<=?",(now,))
        self._finalize_provider_terminal_pointers(db)

    def _ensure(self, db, now, *, useful=True):
        self._expire(db,now)
        control = db.execute('select * from control where id=1').fetchone()
        if control['startup_failures']>=MAX_STARTUP_FAILURES:
            return control['current_run']
        current = db.execute('select * from runs where id=?',(control['current_run'],)).fetchone()
        if current is None:
            # Reuse a warming successor if the previous serving worker was lost.
            successor=db.execute('select * from runs where id=?',(control['successor'],)).fetchone()
            run_id=successor['id'] if successor else self._new_run(db,now)
            db.execute('update control set current_run=?,successor=null where id=1',(run_id,))
        else:
            run_id=current['id']
        if useful:db.execute('update runs set useful=? where id=?',(now,run_id))
        return run_id

    def record_request_activity(self):
        """User intent renews useful-idle time; liveness and max age stay fenced."""
        # Useful-idle precision is one minute; ordinary reads must not queue
        # behind a SQLite writer just to renew the same activity timestamp.
        now=self.clock()
        with self.connect(write=False) as db:
            current=db.execute("SELECT useful FROM runs WHERE id=(SELECT current_run FROM control WHERE id=1) "
                "AND status IN ('starting','ready','draining')").fetchone()
        if current is None:return False
        if now-current['useful']<60:return True
        started=time.monotonic()
        with self.connect() as db:
            acquired=time.monotonic()
            row=db.execute("UPDATE runs SET useful=? WHERE id=(SELECT current_run FROM control WHERE id=1) "
                "AND status IN ('starting','ready','draining')",(self.clock(),))
            recorded=row.rowcount==1
        logging.getLogger('regional_knowledge.reads').info('bge_request_activity state=recorded connect_begin_ms=%.3f transaction_ms=%.3f',
            (acquired-started)*1000,(time.monotonic()-acquired)*1000)
        return recorded

    def request_startup(self):
        """One persisted cold demand, independent of a particular run's TTL.

        An authorized public caller creates this demand; controller maintenance
        can resume it after restart without another user request.
        """
        now=self.clock()
        with self.connect() as db:
            row=db.execute('select startup_ref,startup_state,startup_failures from control where id=1').fetchone()
            if row['startup_failures']>=MAX_STARTUP_FAILURES or (row['startup_ref'] is not None and row['startup_state']!='pending'):
                ref='rkb:startup:v1:'+str(uuid.uuid4())
                db.execute("update control set startup_ref=?,startup_state='pending',startup_failures=0 where id=1",(ref,))
            elif row['startup_ref'] is None:
                ref='rkb:startup:v1:'+str(uuid.uuid4())
                db.execute("update control set startup_ref=?,startup_state='pending' where id=1",(ref,))
            else:ref=row['startup_ref']
            return {'status':'pending','run_id':self._ensure(db,now),'startup_ref':ref}

    def startup_status(self, startup_ref):
        """Read only. A locator is NOT an access grant: caller reauths at MCP."""
        if (type(startup_ref) is not str or not startup_ref.startswith('rkb:startup:v1:')
                or len(startup_ref)!=len('rkb:startup:v1:')+36):
            raise ValueError('invalid startup ref')
        try:
            if str(uuid.UUID(startup_ref.split(':')[-1]))!=startup_ref.split(':')[-1]:
                raise ValueError('noncanonical startup ref')
        except (ValueError,AttributeError):
            raise ValueError('invalid startup ref') from None
        with self.connect(write=False) as db:
            row=db.execute('select startup_ref,startup_state,startup_failures from control where id=1').fetchone()
            if row['startup_ref']!=startup_ref:
                return {'protocol':'rkb.startup.v1','startup_ref':startup_ref,
                        'status':'unavailable','reason':'startup_ref_stale'}
            if row['startup_state']=='ready':
                return {'protocol':'rkb.startup.v1','startup_ref':startup_ref,
                        'status':'ready','reason':None}
            if row['startup_failures']>=MAX_STARTUP_FAILURES:
                return {'protocol':'rkb.startup.v1','startup_ref':startup_ref,
                        'status':'unavailable','reason':'startup_retry_exhausted'}
            return {'protocol':'rkb.startup.v1','startup_ref':startup_ref,
                    'status':'pending','reason':'target_warming'}

    def startup_estimate(self, startup_ref):
        """Read-only, bounded historical hint; never grants launch/read authority.

        Before launch has a provider identity, use history only if the recent
        successful sample agrees on one notebook. Versions of that stable
        notebook share startup history; unrelated providers do not.
        """
        with self.connect(write=False) as db:
            current=db.execute("""select r.* from control c join runs r on r.id=c.current_run
                where c.id=1 and c.startup_ref=? and c.startup_state='pending'
                and c.startup_failures<?""",(startup_ref,MAX_STARTUP_FAILURES)).fetchone()
            if (current is None or current['status']!='starting'
                    or self._run_expired(current,self.clock())):
                return None,'unknown'
            # Bound both fetched rows and the candidate window, without a new
            # index/migration or scanning historical corpus/job data.
            recent=db.execute("""select started,ready_at,provider_ref,provider_version from
                (select * from runs order by rowid desc limit 32)
                where ready_at>started and provider_ref is not null
                and provider_version>0 order by ready_at desc limit 32""").fetchall()
            provider=current['provider_ref']
            if provider is None:
                providers={r['provider_ref'] for r in recent}
                if len(providers)!=1:return None,'unknown'
                provider=next(iter(providers))
            now=self.clock()
            durations=[r['ready_at']-r['started'] for r in recent
                if r['provider_ref']==provider and r['ready_at']<=now
                and 30<=r['ready_at']-r['started']<=1800][:8]
            if len(durations)<3 or now<current['started']:return None,'unknown'
            from statistics import median
            remaining=median(durations)-(now-current['started'])
            if remaining<=0:return None,'overdue'
            # Minute granularity communicates an estimate rather than a
            # countdown; overdue launches explicitly stop offering an ETA.
            return max(60,int((remaining+30)//60)*60),'estimated'

    def finish_startup(self, run_id):
        """Mark ready only after current full history/PG/BGE proof on caller."""
        with self.connect() as db:
            control=db.execute('select current_run,startup_state from control where id=1').fetchone()
            run=db.execute('select * from runs where id=?',(run_id,)).fetchone()
            if (control['current_run']!=run_id or not run or run['status']!='ready'
                    or self._run_expired(run,self.clock()) or not run['channel_active']):
                return False
            if control['startup_state']=='pending':
                db.execute("update control set startup_state='ready',startup_failures=0 where id=1")
            return True

    def enqueue(self, actor, idempotency, texts, *, kind='query', identity=None):
        if kind not in ('query','document') or len(texts)!=1 or not isinstance(texts[0],str) or not texts[0].strip() or len(texts[0])>(16000 if kind=='query' else 40000):
            raise ValueError('BGE job contract')
        identity=identity or {};now=self.clock()
        with self.connect() as db:
            if kind=='query' and self._shared_enabled(db):
                raise StaleAttachment('target reads must use ephemeral admission')
            previous=db.execute('select * from jobs where actor=? and idempotency=?',(actor,idempotency)).fetchone()
            if previous:
                if json.loads(previous['texts'])!=texts or json.loads(previous['identity'])!=identity or previous['kind']!=kind:
                    raise ValueError('idempotency payload mismatch')
                if previous['state']!='done':self._ensure(db,now)
                return previous['id']
            db.execute('update control set startup_ref=null,startup_state=null,startup_failures=0 where id=1 and startup_failures>=?',(MAX_STARTUP_FAILURES,))
            self._ensure(db,now);job_id=str(uuid.uuid4())
            db.execute('insert into jobs(id,actor,idempotency,kind,texts,identity,state,created,updated) values(?,?,?,?,?,?,?,?,?)',
                       (job_id,actor,idempotency,kind,json.dumps(texts),json.dumps(identity),'pending',now,now))
            return job_id

    def lookup(self,actor,idempotency):
        with self.connect(write=False) as db:
            row=db.execute('select * from jobs where actor=? and idempotency=?',(actor,idempotency)).fetchone()
            if not row:return None
            return {'id':row['id'],**self._job_result(db,row),'kind':row['kind']}

    def _enqueue_source_command(self,owner,actor,identity):
        if owner is not self._source_commands or owner is None or self._shared_reads is None:
            raise PermissionError('configured canonical source dispatch required')
        now=self.clock();encoded=json.dumps(identity,sort_keys=True,separators=(',',':'))
        with self.connect() as db:
            key='source-command:'+identity['operation_key']
            prior=db.execute('select * from jobs where actor=? and idempotency=?',(actor,key)).fetchone()
            if prior:
                if prior['kind']!='source_command' or prior['identity']!=encoded:
                    raise ValueError('source operation has different exact dispatch binding')
                return prior['id']
            db.execute('update control set startup_ref=null,startup_state=null,startup_failures=0 where id=1 and startup_failures>=?',(MAX_STARTUP_FAILURES,))
            self._ensure(db,now);job_id=str(uuid.uuid4())
            db.execute('insert into jobs(id,actor,idempotency,kind,texts,identity,state,created,updated) values(?,?,?,?,?,?,?,?,?)',
                (job_id,actor,key,'source_command','[]',encoded,'pending',now,now))
            return job_id

    def _source_claim(self,owner,run_id,token,generation,job_id=None):
        if owner is None or owner is not self._source_commands:
            raise PermissionError('configured source dispatch required')
        with self.connect(write=False) as db:
            run=self._authorize(db,run_id,token,self.clock());self._dispatch_authorize(run,generation)
            self._require_runtime(db,run_id,generation)
            if job_id is None:
                job=db.execute("select * from jobs where kind='source_command' and state='claimed' and run_id=? order by created,id limit 1",(run_id,)).fetchone()
            else:
                job=db.execute("select * from jobs where id=? and kind='source_command' and run_id=?",(job_id,run_id)).fetchone()
            if job is None or job['state'] not in ('claimed','done'):raise LookupError('exact source assignment absent')
            return dict(run),dict(job)

    def _complete_source_command(self,owner,run_id,token,generation,job_id,claim,command_hash,*,remote_outcome_durable=True):
        if owner is None or owner is not self._source_commands:raise PermissionError('canonical source dispatch required')
        with self.connect() as db:
            run=self._authorize(db,run_id,token,self.clock());self._dispatch_authorize(run,generation)
            self._require_runtime(db,run_id,generation)
            job=db.execute("select * from jobs where id=? and kind='source_command'",(job_id,)).fetchone()
            if not job or job['run_id']!=run_id or job['claim']!=claim:raise StaleClaim('source assignment changed')
            if json.loads(job['identity'])['command_sha256']!=command_hash:raise ValueError('source completion hash mismatch')
            if type(remote_outcome_durable) is not bool:raise ValueError('explicit outcome persistence state required')
            result=json.dumps(dict(command_sha256=command_hash,remote_outcome_durable=remote_outcome_durable),sort_keys=True)
            if job['state']=='done':
                if job['result']!=result:raise ValueError('source result changed')
                return 'duplicate'
            if job['state']!='claimed':raise StaleClaim('source assignment is no longer claimed')
            db.execute("update jobs set state='done',result=?,updated=? where id=?",(result,self.clock(),job_id))
        self._notify_result(job_id)
        return 'accepted' if remote_outcome_durable else 'checkpoint_pending'

    def document_pending(self):
        with self.connect(write=False) as db:
            return db.execute("select count(*) from jobs where kind='document' and state!='done'").fetchone()[0]

    def retire_done_documents(self, actor, job_ids):
        ids=[str(uuid.UUID(str(value))) for value in job_ids]
        if not ids:return 0
        if len(ids)>256:raise ValueError('too many BGE retirement ids')
        slots=','.join('?' for _ in ids)
        with self.connect() as db:
            cursor=db.execute(
                f"delete from jobs where actor=? and kind='document' and state='done' and id in ({slots})",
                (actor,*ids),
            )
            return cursor.rowcount

    def _authorize(self, db, run_id, token, now):
        run=db.execute('select * from runs where id=?',(run_id,)).fetchone()
        digest=hashlib.sha256(token.encode()).hexdigest()
        if not run or not secrets.compare_digest(run['token_hash'],digest) or run['status'] not in ('starting','ready','draining') or now>=run['started']+MAX_LIFETIME:
            raise RevokedRun('invalid or stale BGE worker')
        return run

    @staticmethod
    def _dispatch_authorize(run, connection_generation):
        if run['attachment_conflict']:
            raise AttachmentConflict('conflicting BGE runtime incarnations')
        if connection_generation is None:
            if run['connection_generation']:
                raise StaleAttachment('BGE run dispatch belongs to WSS')
        elif (type(connection_generation) is not int
              or connection_generation < 1
              or run['connection_generation'] != connection_generation
              or not run['channel_active']):
            raise StaleAttachment('stale BGE connection generation')

    def prepare_channel(self, run_id):
        """Persist launch intent before provider push; trusted controller only."""
        with self.connect() as db:
            run=db.execute('select * from runs where id=?',(run_id,)).fetchone()
            if (not run or run['status']!='starting'
                    or run['launch_state'] not in ('pending','dispatching')
                    or self._run_expired(run,self.clock())):
                raise ValueError('BGE run is not eligible for channel preparation')
            nonce=run['launch_nonce'] or secrets.token_urlsafe(32)
            db.execute('update runs set launch_nonce=? where id=?',(nonce,run_id))
            return nonce

    def attach_channel(self, run_id, token, launch_nonce, runtime_incarnation):
        """Bind an observed runtime identity and fence previous socket authority.

        The worker's incarnation is an authenticated claim, not native provider
        execution proof. A competing claim commits its fence before we raise.
        """
        now=self.clock();conflict=False
        with self.connect() as db:
            self._shared_owner(db)
            run=self._authorize(db,run_id,token,now)
            if self._run_expired(run,now):
                # A lost WSS heartbeat alone does not kill a live notebook.
                # Accept only the EXACT prior authenticated incarnation,
                # while useful/maximum lifetimes and conflict fences still
                # hold. Re-attachment must reprove the new read generation.
                same_unresolved = (self._provider_outcome_unknown(run)
                    and run['status']=='ready' and run['runtime_incarnation']==runtime_incarnation
                    and run['launch_nonce']==launch_nonce and not run['attachment_conflict']
                    and now<run['started']+MAX_LIFETIME
                    and now<run['useful']+IDLE_SECONDS)
                if not same_unresolved:
                    raise RevokedRun('invalid or stale BGE worker')
            if (not isinstance(launch_nonce,str) or not run['launch_nonce']
                    or not secrets.compare_digest(run['launch_nonce'].encode(),launch_nonce.encode())):
                raise StaleAttachment('invalid BGE launch nonce')
            if not isinstance(runtime_incarnation,str) or not 1<=len(runtime_incarnation)<=256:
                raise ValueError('invalid BGE runtime incarnation')
            if run['attachment_conflict']:
                raise AttachmentConflict('conflicting BGE runtime incarnations')
            if run['runtime_incarnation'] and run['runtime_incarnation']!=runtime_incarnation:
                db.execute('update runs set attachment_conflict=1,channel_active=0 where id=?',(run_id,))
                conflict=True
            else:
                if not run['connection_generation'] and db.execute(
                    "select 1 from jobs where run_id=? and state='claimed' and lease_until>?",
                    (run_id,now),
                ).fetchone():
                    raise AttachmentBusy('BGE HTTP assignment is still active')
                generation=run['connection_generation']+1
                # Authenticated exact-incarnation ATTACH is proof of the
                # socket's present liveness (NOT proof of search readiness).
                # Refresh its heartbeat atomically with the generation fence;
                # otherwise receiver/dispatcher status() would reject every
                # frame before the worker can submit new runtime proof.
                db.execute('update runs set runtime_incarnation=?,connection_generation=?,channel_active=1,heartbeat=? where id=?',
                           (runtime_incarnation,generation,now,run_id))
                result={'connection_generation':generation,
                        'provider_version':str(run['provider_version']) if run['provider_version'] is not None else None}
        if conflict:
            _LOG.warning('bge_attachment_conflict run_id=%s dispatch_fenced=true',run_id)
            raise AttachmentConflict('conflicting BGE runtime incarnations')
        _LOG.info('bge_attachment_active run_id=%s connection_generation=%s',run_id,generation)
        return result

    def channel_status(self, run_id, token, connection_generation):
        """Read readiness; only the exact idle edge commits its normal drain."""
        now=self.clock()
        with self.connect(write=False) as db:
            run=self._authorize(db,run_id,token,now)
            self._dispatch_authorize(run,connection_generation)
            if connection_generation is None:
                raise StaleAttachment('BGE channel generation required')
            idle_due=self._idle_drain_due(db,run,now)
            if self._run_expired(run,now) and not idle_due:
                raise RevokedRun('invalid or stale BGE worker')
            provider_version=str(run['provider_version']) if run['provider_version'] is not None else None
            runtime = self._runtime_row(db,run_id)
            observed={'state':run['status'],
                    'ready':run['status']=='ready' and provider_version is not None
                        and (runtime is None or runtime['accepted_generation']==connection_generation),
                    'provider_version':provider_version,
                    'connection_generation':run['connection_generation'],
                    'active':bool(run['channel_active']),
                    'runtime_required':runtime is not None,
                    'runtime_verified':runtime is not None and runtime['accepted_generation']==connection_generation}
        if idle_due:
            # WSS may observe the deadline before controller maintenance.
            # Healthy ready reads never enter this write transaction.
            self._drain_idle_channel(run_id,token,connection_generation)
            return self.channel_status(run_id,token,connection_generation)
        return observed

    def _drain_idle_channel(self,run_id,token,generation):
        with self.connect() as db:
            run=self._authorize(db,run_id,token,self.clock())
            self._dispatch_authorize(run,generation)
            self._expire(db,self.clock())

    @staticmethod
    def _runtime_row(db, run_id):
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bge_runtime_expectations'").fetchone():
            return None
        return db.execute('SELECT * FROM bge_runtime_expectations WHERE run_id=?',(run_id,)).fetchone()

    def expect_search_runtime(self, run_id, expected):
        """Trusted launch setup only: fixed hashes/versions, no corpus or grants.

        Call after final source bytes are frozen and before the first attachment.
        Changing an active manifest requires a separate ordered update barrier;
        this initial-adoption API deliberately cannot do that.
        """
        from .kaggle_readiness import SearchRuntimeExpectation
        if type(expected) is not SearchRuntimeExpectation:
            raise TypeError('typed controller runtime expectation required')
        data=json.dumps(expected.wire(),sort_keys=True,separators=(',',':'))
        with self.connect() as db:
            run=db.execute('SELECT * FROM runs WHERE id=?',(run_id,)).fetchone()
            if run is None:raise ValueError('known owned run required')
            prior=self._runtime_row(db,run_id)
            if prior is not None:
                if prior['expected_json']!=data:raise ValueError('initial runtime expectation is immutable')
                return
            if run['status']!='starting' or run['connection_generation'] or run['channel_active']:
                raise StaleAttachment('runtime binding must precede first attachment')
            db.execute('''CREATE TABLE IF NOT EXISTS bge_runtime_expectations(
                run_id TEXT PRIMARY KEY,expected_json TEXT NOT NULL,
                accepted_generation INTEGER,observed_json TEXT)''')
            db.execute('INSERT INTO bge_runtime_expectations(run_id,expected_json) VALUES(?,?)',(run_id,data))

    def expect_bootstrap_runtime(self,run_id,expected):
        from .kaggle_bootstrap_pins import expect_bootstrap
        return expect_bootstrap(self,run_id,expected)

    def record_bootstrap_observation(self,run_id,token,generation,value):
        from .kaggle_bootstrap_pins import record_observation
        return record_observation(self,run_id,token,generation,value)

    def finalize_bootstrap_runtime(self,run_id,expected,*,observation_sha256,review_evidence):
        from .kaggle_bootstrap_pins import finalize_bootstrap
        return finalize_bootstrap(self,run_id,expected,observation_sha256=observation_sha256,review_evidence=review_evidence)

    def bootstrap_pins_current(self,run_id,token,generation):
        from .kaggle_bootstrap_pins import finalized_pins
        return finalized_pins(self,run_id,token,generation)

    @staticmethod
    def _runtime_expected(row):
        from dataclasses import replace
        from .kaggle_readiness import SearchRuntimeExpectation
        expected=SearchRuntimeExpectation(**json.loads(row['expected_json']))
        prior=json.loads(row['observed_json']) if row['observed_json'] else None
        version=prior.get('schema_version',expected.schema_version) if prior else expected.schema_version
        hashes=dict(expected.hashes)
        if version!=expected.schema_version:hashes['schema']=hashlib.sha256(json.dumps(version).encode()).hexdigest()
        return replace(expected,hashes=hashes,schema_version=version)

    def _promote_schema_runtime(self,db,run,generation,observation,*,target_version):
        """Called only for the authenticated, claimed schema_migrate result."""
        from dataclasses import replace
        from .kaggle_readiness import RunIdentity,validate_search_runtime
        row=self._runtime_row(db,run['id'])
        if row is None:raise ValueError('current verified runtime required')
        expected=self._runtime_expected(row)
        if type(target_version) is not int or target_version<expected.schema_version:
            raise ValueError('schema runtime version cannot move backward')
        hashes=dict(expected.hashes);hashes['schema']=hashlib.sha256(json.dumps(target_version).encode()).hexdigest()
        expected=replace(expected,hashes=hashes,schema_version=target_version)
        identity=RunIdentity(run['provider_ref'],str(run['provider_version']),None,
            run['id'],run['launch_nonce'],run['runtime_incarnation'])
        if not validate_search_runtime(expected,observation,identity).ok:
            raise ValueError('migrated readiness differs from the retained runtime')
        db.execute('UPDATE bge_runtime_expectations SET observed_json=?,accepted_generation=? WHERE run_id=?',
            (json.dumps(observation,sort_keys=True,separators=(',',':')),generation,run['id']))

    def _runtime_check(self, db, run, generation, observation):
        from .kaggle_readiness import (SearchRuntimeExpectation, RunIdentity, Validation,
                                       validate_search_runtime)
        row=self._runtime_row(db,run['id'])
        if row is None:raise ValueError('unexpected target runtime observation')
        if json.loads(row['expected_json']).get('codec')=='rkb.bootstrap-expectation.v1':
            return Validation(('bootstrap_finalization_required',))
        if run['provider_version'] is None or not run['provider_ref']:
            return Validation(('provider_pending',))
        expected=self._runtime_expected(row)
        identity=RunIdentity(run['provider_ref'],str(run['provider_version']),None,
            run['id'],run['launch_nonce'],run['runtime_incarnation'])
        return validate_search_runtime(expected,observation,identity)

    def accept_search_runtime(self, run_id, token, connection_generation, observation):
        """Accept one bounded observed proof for this authenticated attachment.

        Invalid/mismatching or duplicate observations take only read snapshots.
        The successful setup write is repeated on reconnect, never per QUERY.
        """
        if type(connection_generation) is not int or connection_generation < 1:
            raise StaleAttachment('current authenticated channel required')
        with self.connect(write=False) as db:
            run=self._authorize(db,run_id,token,self.clock())
            self._dispatch_authorize(run,connection_generation)
            if self._run_expired(run,self.clock()):raise RevokedRun('stale BGE runtime')
            result=self._runtime_check(db,run,connection_generation,observation)
            if not result.ok:return result
            encoded=json.dumps(observation,sort_keys=True,separators=(',',':'),allow_nan=False)
            row=self._runtime_row(db,run_id)
            if row['accepted_generation']==connection_generation and row['observed_json']==encoded:
                return result
        with self.connect() as db:
            self._shared_owner(db)
            run=self._authorize(db,run_id,token,self.clock())
            self._dispatch_authorize(run,connection_generation)
            if self._run_expired(run,self.clock()):raise RevokedRun('stale BGE runtime')
            result=self._runtime_check(db,run,connection_generation,observation)
            if result.ok:
                db.execute('UPDATE bge_runtime_expectations SET accepted_generation=?,observed_json=? WHERE run_id=?',
                    (connection_generation,encoded,run_id))
        return result

    def _require_runtime(self, db, run_id, generation):
        row=self._runtime_row(db,run_id)
        if row is not None and (generation is None or row['accepted_generation']!=generation):
            raise StaleAttachment('target runtime readiness required')

    def detach_channel(self, run_id, token, connection_generation):
        """Drop only this active socket; never grant HTTP ownership on loss."""
        with self.connect() as db:
            self._shared_owner(db)
            run=self._authorize(db,run_id,token,self.clock())
            if (type(connection_generation) is not int or connection_generation<1
                    or run['connection_generation']!=connection_generation
                    or not run['channel_active']):
                return False
            db.execute('update runs set channel_active=0 where id=?',(run_id,))
            return True

    def resume_channel_claim(self, run_id, token, connection_generation):
        """Read this run's existing durable assignment without renewing it.

        A dropped socket can lose DISPATCH after claim commit. Reattachment
        recovers that same assignment; it must not allocate another attempt.
        """
        now=self.clock()
        with self.connect(write=False) as db:
            if self._shared_enabled(db):
                raise StaleAttachment('target assignments require shared admission')
            run=self._authorize(db,run_id,token,now)
            self._dispatch_authorize(run,connection_generation)
            self._require_runtime(db,run_id,connection_generation)
            if connection_generation is None:
                raise StaleAttachment('BGE channel generation required')
            if self._run_expired(run,now):
                raise RevokedRun('invalid or stale BGE worker')
            job=db.execute(
                "select * from jobs where run_id=? and state='claimed' and lease_until>? limit 1",
                (run_id,now),
            ).fetchone()
            return self._claim_payload(job) if job else None

    @staticmethod
    def _claim_payload(job):
        return {'id':job['id'],'claim':job['claim'],'kind':job['kind'],
                'texts':json.loads(job['texts']),'space':SPACE}

    def heartbeat(self, run_id, token, *, ready=False, diagnostics=None, connection_generation=None):
        now=self.clock()
        with self.connect() as db:
            self._shared_owner(db)
            if self._shared_enabled(db) and connection_generation is None:
                raise StaleAttachment('target heartbeat requires the current channel')
            self._expire(db,now);run=self._authorize(db,run_id,token,now)
            if connection_generation is not None:
                self._dispatch_authorize(run,connection_generation)
            if ready:
                self._require_runtime(db,run_id,connection_generation)
            db.execute('update runs set heartbeat=? where id=?',(now,run_id))
            # A live native worker can be waiting for its durable outcome ACK
            # after compute has finished. Do not expire that claim and dispatch
            # different work into the worker's still occupied result slot.
            db.execute('''update jobs set lease_until=max(coalesce(lease_until,0),?)
                where run_id=? and state='claimed' and kind in ('document','source_command','recovery_phase')''',
                (min(now+JOB_SECONDS,run['started']+MAX_LIFETIME),run_id))
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
                    from .queue_writer import pending
                    if pending(db,control['current_run']):
                        raise StaleAttachment('writer publication must reconcile before handover')
                    db.execute("update runs set status='draining' where id=?",(control['current_run'],))
                    db.execute('update control set current_run=?,successor=null where id=1',(run_id,))
                # A legacy document-only cycle has no MCP startup_ref, so
                # finish_startup() is never invoked. Its verified READY is
                # the only authority that clears prior known-not-sent startup
                # failures. Plain heartbeat / public request cannot reset it.
                db.execute('''UPDATE control SET startup_failures=0
                    WHERE current_run=? AND startup_ref IS NULL AND startup_state IS NULL''',(run_id,))
            return self._status(db,now)

    def claim(self, run_id, token, *, connection_generation=None):
        now=self.clock()
        with self.connect() as db:
            if self._shared_enabled(db):
                raise StaleAttachment('target assignments require shared admission')
            self._expire(db,now);run=self._authorize(db,run_id,token,now)
            self._dispatch_authorize(run,connection_generation)
            self._require_runtime(db,run_id,connection_generation)
            if connection_generation is not None and run['provider_version'] is None:
                return None
            db.execute('update runs set heartbeat=? where id=?',(now,run_id))
            if run['status']!='ready' or db.execute("select 1 from jobs where run_id=? and state='claimed'",(run_id,)).fetchone():return None
            job=db.execute("select * from jobs where state='pending' and kind in ('query','document') order by case kind when 'query' then 0 else 1 end,created,id limit 1").fetchone()
            if job is None:return None
            claim=secrets.token_urlsafe(24)
            db.execute("update jobs set state='claimed',run_id=?,claim=?,lease_until=?,attempt=attempt+1,updated=? where id=?",(run_id,claim,now+JOB_SECONDS,now,job['id']))
            db.execute('update runs set useful=? where id=?',(now,run_id))
            return self._claim_payload({**dict(job),'claim':claim})

    @staticmethod
    def _shared_enabled(db):
        if not db.execute("select 1 from sqlite_master where type='table' and name='bge_shared_admission'").fetchone():
            return False
        return bool(db.execute('select enabled from bge_shared_admission where id=1').fetchone()[0])

    def _shared_owner(self, db):
        if self._shared_enabled(db) and (self._shared_reads is None or self._shared_reads._fd is None):
            raise StaleAttachment('shared admission requires the owning controller process')

    def configure_shared_reads(self, read_authority, **options):
        """Explicit startup opt-in; leaves a persistent fence on legacy claimers."""
        from .bge_admission import SharedReadAdmission
        if self._shared_reads is not None:
            raise RuntimeError('shared admission already configured')
        helper=SharedReadAdmission(self,read_authority,**options)
        self._shared_reads=helper
        return helper

    @property
    def shared_reads(self):
        if self._shared_reads is None:
            raise StaleAttachment('shared admission is not configured')
        return self._shared_reads

    async def next_channel_work(self, run_id, token, connection_generation):
        return await self.shared_reads.next_channel_work(run_id,token,connection_generation)

    async def wait_channel_work(self, seconds=RECONCILE_SECONDS):
        await self.shared_reads.wait_channel_work(seconds)

    def _admission_snapshot(self, run_id, token, generation, *, document_actors=(),command_actors=(),editorial_documents=()):
        """Bounded indexed heads and current attachment, without a writer lock."""
        if len(editorial_documents)>64 or len(set(document_actors)|set(command_actors))>256:raise ValueError('bounded trusted durable actors required')
        with self.connect(write=False) as db:
            self._shared_owner(db)
            run=self._authorize(db,run_id,token,self.clock())
            self._dispatch_authorize(run,generation)
            if generation is None:raise StaleAttachment('channel generation required')
            if self._run_expired(run,self.clock()):raise RevokedRun('stale BGE worker')
            owner=db.execute('select run_id,incarnation from bge_shared_admission where id=1').fetchone()
            # One index seek per registered canonical actor. A legacy actor's
            # arbitrarily long pending tail cannot hide another actor's head.
            # No done journal or untrusted actor enumeration is necessary.
            rows=[]
            for actors,kind in ((document_actors,'document'),(command_actors,'source_command')):
                if kind=='source_command' and self._source_commands is None:continue
                for actor in actors:
                    row=db.execute("select * from jobs where state='pending' and kind=? and actor=? order by created,id limit 1",(kind,actor)).fetchone()
                    if row is not None:rows.append(dict(row))
            if self._story_editorial is not None:
                seen={row['id'] for row in rows}
                for job_id in editorial_documents:
                    row=db.execute("select * from jobs where id=? and kind='document' and state='pending'",(job_id,)).fetchone()
                    if row is not None and row['id'] not in seen:rows.append(dict(row))
            if self._recovery_phases is not None:
                row=db.execute("select * from jobs where state='pending' and kind='recovery_phase' and actor=? order by created,id limit 1",
                    ('maintenance:'+run_id,)).fetchone()
                if row is not None:rows.append(dict(row))
            rows.sort(key=lambda row:(row['created'],row['id']))
            resumed=db.execute("select * from jobs where state='claimed' and kind in ('document','source_command','recovery_phase') and run_id=? and lease_until>? limit 1",(run_id,self.clock())).fetchone()
            runtime=self._runtime_row(db,run_id)
            projected={**dict(run),'runtime_required':runtime is not None,
                       'runtime_verified':runtime is not None and runtime['accepted_generation']==generation}
            return projected,dict(owner),rows,dict(resumed) if resumed else None

    def _admission_bind(self, run_id, token, generation, incarnation):
        """Startup/reconciliation write, never performed for ready read intake."""
        with self.connect() as db:
            self._shared_owner(db)
            run=self._authorize(db,run_id,token,self.clock())
            self._dispatch_authorize(run,generation)
            if run['runtime_incarnation']!=incarnation:raise StaleAttachment('resource incarnation mismatch')
            current=db.execute('select current_run from control where id=1').fetchone()[0]
            if current!=run_id:raise StaleAttachment('resource baseline requires the owned current run')
            db.execute('update bge_shared_admission set run_id=?,incarnation=? where id=1',(run_id,incarnation))

    def _claim_specific(self, run_id, token, generation, job_id):
        """Internal admission reservation only; does not choose or authorize an actor."""
        now=self.clock()
        with self.connect() as db:
            self._shared_owner(db)
            run=self._authorize(db,run_id,token,now)
            self._dispatch_authorize(run,generation)
            if self._run_expired(run,now):return None
            job=db.execute('select * from jobs where id=?',(job_id,)).fetchone()
            if not job or job['kind'] not in ('document','source_command','recovery_phase'):return None
            maintenance=job['kind']=='recovery_phase'
            if maintenance:
                if self._recovery_phases is None or not self._recovery_phases.eligible(job,run,generation):return None
            elif run['status']!='ready' or run['provider_version'] is None:return None
            if job['kind']=='source_command' and self._source_commands is None:return None
            if job['state']=='claimed' and job['run_id']==run_id and job['lease_until']>now:
                return self._claim_payload(job)
            if job['state']!='pending' or db.execute("select 1 from jobs where run_id=? and state='claimed' and lease_until>?",(run_id,now)).fetchone():return None
            claim=secrets.token_urlsafe(24)
            db.execute("update jobs set state='claimed',run_id=?,claim=?,lease_until=?,attempt=attempt+1,updated=? where id=?",(run_id,claim,min(now+(900 if maintenance else JOB_SECONDS),run['started']+MAX_LIFETIME),now,job_id))
            db.execute('update runs set useful=? where id=?',(now,run_id))
            return self._claim_payload({**dict(job),'claim':claim})

    def _document_claim_waiting(self, job_id, claim):
        with self.connect(write=False) as db:
            row=db.execute('select state,claim,lease_until from jobs where id=?',(job_id,)).fetchone()
            # A committed result still belongs to the worker until its channel
            # has queued RESULT_ACK. Keep the existing lease as the stale bound.
            return bool(row and row['state'] in ('claimed','done') and row['claim']==claim
                        and row['lease_until'] is not None and row['lease_until']>self.clock())

    def _admission_terminal(self, expected, capabilities, observed, *, max_age_seconds):
        from .kaggle_readiness import validate_terminal
        with self.connect() as db:
            self._shared_owner(db)
            owner=db.execute('select run_id,incarnation from bge_shared_admission where id=1').fetchone()
            run=db.execute('select * from runs where id=?',(owner['run_id'],)).fetchone()
            if (not run or (expected.run_id,expected.runtime_instance_id)!=(owner['run_id'],owner['incarnation'])
                    or expected.owner_slug!=run['provider_ref']
                    or expected.provider_version!=str(run['provider_version'])
                    or expected.launch_nonce!=run['launch_nonce']):
                raise StaleAttachment('terminal proof does not own this resource')
            result=validate_terminal(expected,capabilities,observed,last_heartbeat_at=run['heartbeat'],
                                     now=self.clock(),max_age_seconds=max_age_seconds)
            if not result.ok:return result
            # The capability+execution-bound terminal proof is the ONLY
            # authority to release the old notebook's provider reservation.
            # Persist it in launch_state in the SAME transaction before any
            # subsequent launch_claim scans historical runs. Mere heartbeat,
            # Kaggle version status, or local timeout never sets this state.
            db.execute("update runs set status='expired',channel_active=0,launch_state='terminal' where id=?",
                       (run['id'],))
            self._expire(db,self.clock())
            db.execute("update jobs set state='pending',run_id=null,claim=null,lease_until=null where run_id=? and state='claimed'",(run['id'],))
            db.execute('update control set current_run=null where current_run=?',(run['id'],))
            db.execute('update control set successor=null where successor=?',(run['id'],))
            db.execute('update bge_shared_admission set run_id=null,incarnation=null where id=1')
            return result

    def complete(self, run_id, token, job_id, claim, space, vectors, timings, *, connection_generation=None):
        if space!=SPACE or len(vectors)!=1:raise ValueError('BGE result space/batch mismatch')
        vectors=[validate_vector(vector) for vector in vectors];now=self.clock()
        safe_times={key:float(timings[key]) for key in ('encoder_seconds',) if key in timings}
        with self.connect() as db:
            self._expire(db,now);run=self._authorize(db,run_id,token,now)
            self._dispatch_authorize(run,connection_generation)
            job=db.execute('select * from jobs where id=?',(job_id,)).fetchone()
            if not job or job['run_id']!=run_id or job['claim']!=claim:raise StaleClaim('late/stale BGE result')
            if job['kind'] not in ('query','document'):raise PermissionError('source command requires native durable receipt')
            if job['state']=='done':
                previous=json.loads(job['result'])
                if previous['space']!=space or previous['vectors']!=vectors:
                    raise ValueError('BGE completion payload mismatch')
                return 'duplicate'
            if job['state']!='claimed' or now>=job['lease_until']:raise StaleClaim('expired BGE job')
            result={'space':SPACE,'vectors':vectors,'queue_seconds':max(0,job['updated']-job['created']),**safe_times}
            db.execute("update jobs set state='done',result=?,updated=? where id=?",(json.dumps(result),now,job_id))
            db.execute('update runs set useful=? where id=?',(now,run_id))
        self._notify_result(job_id)
        return 'accepted'

    def _job_result(self, db, row):
        state=row['state']
        if state=='claimed':
            run=db.execute('select * from runs where id=?',(row['run_id'],)).fetchone()
            now=self.clock()
            if not run or self._run_expired(run,now) or row['lease_until'] is None or now>=row['lease_until']:
                state='pending'
        return {'state':state,'result':json.loads(row['result']) if row['result'] else None,'identity':json.loads(row['identity'])}

    def result(self, actor, job_id):
        with self.connect(write=False) as db:
            row=db.execute('select * from jobs where id=? and actor=?',(job_id,actor)).fetchone()
            if not row:raise PermissionError('BGE job not found')
            return self._job_result(db,row)

    def _notify_result(self, job_id):
        key=(str(self.path.resolve()),job_id)
        with _WAITERS_LOCK:
            waiters=tuple(_WAITERS.get(key,()))
        for loop,event in waiters:
            try:loop.call_soon_threadsafe(event.set)
            except RuntimeError:pass  # closed caller loop; its result remains durable

    async def wait_result(self, actor, job_id, seconds):
        """Wake after local commit; reconcile other processes within a bounded wait."""
        if not math.isfinite(seconds):
            raise ValueError('finite result wait required')
        loop=asyncio.get_running_loop()
        deadline=loop.time()+max(0.0,seconds)
        event=asyncio.Event()
        key=(str(self.path.resolve()),job_id)
        waiter=(loop,event)
        with _WAITERS_LOCK:
            _WAITERS.setdefault(key,set()).add(waiter)
        try:
            while True:
                event.clear()
                row=await asyncio.to_thread(self.result,actor,job_id)
                if row['state']=='done':return row
                remaining=deadline-loop.time()
                if remaining<=0:return None
                try:
                    await asyncio.wait_for(event.wait(),min(RECONCILE_SECONDS,remaining))
                except TimeoutError:pass
        finally:
            with _WAITERS_LOCK:
                entries=_WAITERS.get(key)
                if entries is not None:
                    entries.discard(waiter)
                    if not entries:_WAITERS.pop(key,None)

    def _status(self, db, now):
        control=db.execute('select * from control where id=1').fetchone()
        run=db.execute('select * from runs where id=?',(control['current_run'],)).fetchone()
        if run and self._run_expired(run,now):run=None
        successor=db.execute('select * from runs where id=?',(control['successor'],)).fetchone()
        successor_starting=bool(successor and not self._run_expired(successor,now))
        return {'state':run['status'] if run else 'stopped',
            'reason':'startup_retry_exhausted' if control['startup_failures']>=MAX_STARTUP_FAILURES else None,
            'space':SPACE,'queue_depth':db.execute('select count(*) from jobs where '+LIVE_JOBS).fetchone()[0],
            'successor_starting':successor_starting,
            'lease_remaining_seconds':max(0,run['useful']+IDLE_SECONDS-now) if run else 0}

    def status(self):
        with self.connect(write=False) as db:
            return self._status(db,self.clock())

    def stop_eligible(self, run_id, token, connection_generation):
        """Authenticated STOP predicate; never proof of provider terminal.

        A former owner can stop after handover; the current owner stops at
        nine hours or after its exact idle-drain request. Pending/claimed work and unknown writer publication
        retain their original run even when its compute resource looks idle.
        """
        from .queue_writer import pending
        now = self.clock()
        with self.connect(write=False) as db:
            run = self._authorize(db, run_id, token, now)
            self._dispatch_authorize(run, connection_generation)
            idle_due=self._idle_drain_due(db,run,now)
            if self._run_expired(run, now) and not idle_due:
                return False
            current = db.execute('SELECT current_run FROM control WHERE id=1').fetchone()[0]
            if not idle_due and current == run_id and run['status']!='draining' and now - run['started'] < 9 * 3600:
                return False
            if pending(db, run_id):
                return False
            unclaimed=db.execute(
                "SELECT 1 FROM jobs WHERE run_id=? AND state IN ('pending','claimed') LIMIT 1",
                (run_id,)).fetchone() is None
        if idle_due:
            self._drain_idle_channel(run_id,token,connection_generation)
            return self.stop_eligible(run_id,token,connection_generation)
        return unclaimed

    def request_idle_drain(self, run_id, *, provider_ref, provider_version):
        """Operator requests planned closure of the exact idle current run.

        This revokes serving through normal draining maintenance; it does not
        certify provider termination, retire its reservation or settle work.
        """
        from .queue_writer import pending
        with self.connect() as db:
            run=db.execute('SELECT * FROM runs WHERE id=?',(run_id,)).fetchone()
            current=db.execute('SELECT current_run FROM control WHERE id=1').fetchone()[0]
            if (run is None or current!=run_id or type(provider_version) is not int
                    or (run['provider_ref'],run['provider_version'])!=(provider_ref,provider_version)
                    or run['status'] not in ('ready','draining','expired')):
                raise StaleAttachment('planned drain requires exact current provider identity')
            if pending(db,run_id) or db.execute(
                    "SELECT 1 FROM jobs WHERE run_id=? AND state='claimed' LIMIT 1",(run_id,)).fetchone():
                return False
            if run['status']=='ready':
                db.execute("UPDATE runs SET status='draining' WHERE id=?",(run_id,))
                _LOG.info('bge_planned_drain requested run_id=%s provider_version=%s',run_id,provider_version)
            return True

    def maintenance(self):
        now=self.clock()
        with self.connect() as db:
            self._expire(db,now)
            control=db.execute('select startup_state from control where id=1').fetchone()
            if (db.execute('select 1 from jobs where '+LIVE_JOBS).fetchone()
                    or control['startup_state']=='pending'):
                self._ensure(db,now,useful=False)
            control=db.execute('select * from control where id=1').fetchone()
            current=db.execute('select * from runs where id=?',(control['current_run'],)).fetchone()
            from .queue_writer import pending
            if (current and current['status']=='ready' and not self._run_expired(current,now)
                    and now-current['started']>=ROTATE_SECONDS and not control['successor']
                    and not pending(db,current['id'])):
                # Serial handoff: the same checkpoint/STOP path retires this
                # run before the controller launches a new owned current.
                db.execute("update runs set status='draining' where id=?",(current['id'],))
                db.execute("update control set startup_state='pending' where id=1")
                _LOG.info('bge_rotation_drain requested run_id=%s provider_version=%s',
                          current['id'],current['provider_version'])
            # An attached drain retains its checkpoint/STOP channel. A detached
            # dispatched run retains capacity until exact provider terminal proof.
            draining=db.execute("select id from runs where status='draining' and channel_active=0 and launch_state not in ('dispatching','dispatched') and not exists(select 1 from jobs where jobs.run_id=runs.id and state='claimed')").fetchall()
            for row in draining:
                if not pending(db,row['id']):db.execute("update runs set status='expired' where id=?",(row['id'],))
            self._expire(db,now)
            db.execute(
                "delete from jobs where kind='query' and state='done' and updated<?",
                (now-DONE_QUERY_RETENTION_SECONDS,),
            )
            # Benchmark/audit document jobs are transient inference cache, not
            # production publication recovery state. Bound them independently.
            db.execute(
                "delete from jobs where kind='document' and state='done' and updated<? "
                "and json_extract(identity,'$.audit') is not null",
                (now-DONE_AUDIT_RETENTION_SECONDS,),
            )
            return [dict(row) for row in db.execute('select id,status,launch_state,provider_ref,provider_version from runs').fetchall()]

    def provider_reconciliation_page(self, *, limit=2):
        """Explicit diagnostic history sweep; not an automatic launch prerequisite.

        Durable keyset cursor: one unknown row cannot starve old reservations.

        Cursor advances only after an attempted observation; crash repeats the
        same read safely. An exhausted cycle backs off for 30 seconds. This is
        reconciliation in the existing controller, not a dispatcher.
        """
        if type(limit) is not int or not 1<=limit<=8:raise ValueError('bounded history page required')
        with self.connect() as db:
            state=db.execute('select * from bge_provider_reconciliation where id=1').fetchone()
            if self.clock()<state['next_cycle']:return []
            rows=db.execute("""select rowid as _cursor,* from runs where rowid>?
                and launch_state in ('dispatching','dispatched') order by rowid limit ?""",
                (state['cursor'],limit)).fetchall()
            if not rows:
                db.execute('update bge_provider_reconciliation set cursor=0,next_cycle=? where id=1',
                           (self.clock()+30,))
            return [dict(r) for r in rows]

    def advance_provider_reconciliation(self, cursor):
        if type(cursor) is not int or cursor<1:raise ValueError('valid history cursor required')
        with self.connect() as db:
            db.execute('update bge_provider_reconciliation set cursor=max(cursor,?) where id=1',(cursor,))

    def retire_provider_reservation(self, proof):
        """Internal adapter result only; never an operator termination boolean.

        Retires compute ownership, not accepted domain effects. Cold terminal
        starts use normal expiry; claim identities and known results remain evidence.
        A never-attached version needs no invented runtime incarnation.
        """
        from .kaggle_provider_terminal import validate_proof, _NativeTerminalProof, _OBSERVED
        if type(proof) is not _NativeTerminalProof or proof._seal is not _OBSERVED:
            raise TypeError('native observed proof required')
        with self.connect() as db:
            run=db.execute('select * from runs where id=?',(proof.snapshot['id'],)).fetchone()
            if run is None:return False
            encoded=json.dumps(proof.receipt(),sort_keys=True,separators=(',',':'))
            previous=db.execute('select receipt_json from bge_provider_terminal_receipts where run_id=?',
                                (run['id'],)).fetchone()
            if previous is not None:
                matched=run['launch_state']=='terminal' and previous['receipt_json']==encoded
                if matched:self._finalize_provider_terminal_pointers(db)
                return matched
            if (not validate_proof(proof,run,self.clock()) or run['attachment_conflict']
                    or run['status'] not in ('starting','ready','draining','expired')):return False
            from .queue_writer import pending
            if run['status']!='expired' and pending(db,run['id']):return False
            # CAS identity/freshness was read under this same write transaction.
            # A later heartbeat/reconnect or changed version invalidates proof.
            # Exact native terminal evidence also retires a current run whose
            # local ready status outlived its process. Do not rewrite that status
            # or any writer/publication state; normal maintenance handles leases.
            db.execute('insert into bge_provider_terminal_receipts values(?,?)',(run['id'],encoded))
            db.execute("update runs set channel_active=0,launch_state='terminal' where id=?",
                       (run['id'],))
            table=db.execute("select 1 from sqlite_master where type='table' and name='bge_shared_admission'").fetchone()
            if table:
                db.execute("""update bge_shared_admission set run_id=null,incarnation=null
                    where id=1 and run_id=? and incarnation is ?""",
                    (run['id'],run['runtime_incarnation']))
            if run['status']=='starting':
                # The sealed terminal receipt proves this cold start cannot finish.
                # Reuse failure accounting and expiry; accepted effects stay fenced.
                db.execute("update runs set status='failed' where id=?",(run['id'],))
                db.execute('''UPDATE control SET startup_failures=startup_failures+1,
                    startup_state=CASE WHEN startup_ref IS NOT NULL THEN 'pending'
                        ELSE startup_state END WHERE current_run=?''',(run['id'],))
                self._expire(db,self.clock())
                _LOG.info('bge_cold_terminal run_id=%s provider_version=%s status=%s',
                          run['id'],run['provider_version'],proof.status)
            self._finalize_provider_terminal_pointers(db)
            return True

    def _finalize_provider_terminal_pointers(self, db):
        """Revisit at most current/successor after legitimate domain settlement.

        Compute closure cannot settle claims or publications. When the existing
        lease/recovery machinery clears those guards, durable demand may advance
        without resubmitting the provider read or replaying an accepted effect.
        """
        from .queue_writer import pending
        pointers=db.execute('select current_run,successor from control where id=1').fetchone()
        for run_id in set(pointers):
            if run_id is None:continue
            closed=db.execute("""select 1 from runs r where r.id=? and r.status='expired'""",
                (run_id,)).fetchone()
            if not closed or pending(db,run_id):continue
            claimed=db.execute("""select 1 from jobs where run_id=? and state='claimed'
                and kind in ('source_command','recovery_phase') limit 1""",(run_id,)).fetchone()
            if claimed:continue
            db.execute('update control set current_run=null where current_run=?',(run_id,))
            db.execute('update control set successor=null where successor=?',(run_id,))

    def needs_dispatched_terminal_probe(self, run_id):
        """Only an exact owner with a lost ready connection needs terminal lookup.

        This is a read-only controller hint. It does not assert provider death,
        revoke credentials or authorize another push; the controller must
        inspect the original exact-version native session independently.
        """
        with self.connect(write=False) as db:
            # The standalone controller constructs BgeQueue BEFORE it has any
            # shared-admission object. Only a read-only run identity is used
            # here; this is not an actor admission / ready-query check.
            run=db.execute('select * from runs where id=?',(run_id,)).fetchone()
            if (run is None or run['launch_state']!='dispatched'
                    or type(run['provider_version']) is not int):
                return False
            if run['status']=='starting':return True
            return (run['status']=='draining' and not run['channel_active']
                or run['status'] in ('ready','draining') and self._run_expired(run,self.clock()))

    @staticmethod
    def _provider_owner_ids(db):
        control=db.execute('select current_run,successor from control where id=1').fetchone()
        owners={value for value in control if value is not None}
        if db.execute("select 1 from sqlite_master where type='table' and name='bge_shared_admission'").fetchone():
            owners.update(r[0] for r in db.execute('select run_id from bge_shared_admission where run_id is not null'))
        if db.execute("select 1 from sqlite_master where type='table' and name='rkb_writer_control'").fetchone():
            owners.update(r[0] for r in db.execute('select run_id from rkb_writer_control where active!=0 or publication_json is not null'))
        if db.execute("select 1 from sqlite_master where type='table' and name='rkb_telegram_session_lease'").fetchone():
            owners.update(r[0] for r in db.execute('select run_id from rkb_telegram_session_lease'))
        return owners

    def provider_owned_run_ids(self):
        with self.connect(write=False) as db:return self._provider_owner_ids(db)

    def launch_claim(self, run_id, *, provider_ref=None):
        with self.connect() as db:
            row=db.execute('select * from runs where id=?',(run_id,)).fetchone()
            if not row or row['status']!='starting' or row['launch_state']!='pending':return None
            # The current request is claimed once. Historical run rows retain
            # their original outcomes but do not reserve new development work.
            control=db.execute('select current_run,successor from control where id=1').fetchone()
            if run_id not in tuple(control):return None
            # Claim once. An ambiguous provider response is reconciled by its
            # stable notebook slug, never retried as an unknown second launch.
            db.execute("update runs set launch_state='dispatching',provider_ref=coalesce(provider_ref,?),started=? where id=?",(provider_ref,self.clock(),run_id))
            token=(self.path.parent/'bge-worker-credentials'/run_id).read_text()
            return token

    def launch_record(self, run_id, provider_ref, *, failed=False, provider_version=None):
        if provider_version is not None and (type(provider_version) is not int or provider_version<1):
            raise ValueError('invalid Kaggle notebook version')
        with self.connect() as db:
            row=db.execute('select provider_ref,provider_version,status,launch_state from runs where id=?',(run_id,)).fetchone()
            if row and row['launch_state']=='terminal':
                raise StaleAttachment('execution-qualified terminal run cannot be relaunched')
            if row and ((row['provider_ref'] and row['provider_ref']!=provider_ref)
                    or (row['provider_version'] and provider_version and row['provider_version']!=provider_version)):
                raise ValueError('Kaggle run identity changed')
            db.execute('update runs set provider_ref=?,provider_version=coalesce(provider_version,?),launch_state=?,status=case when ? then ? else status end where id=?',
                       (provider_ref,provider_version,'failed' if failed else 'dispatched',failed,'failed',run_id))
            if failed:
                # Keep failed distinct from expired until normal maintenance.
                self._retire_recovery_jobs(db,self.clock())
                # Count a known failure once. Repeated failed receipts cannot
                # re-arm the launch budget or start infinite 60-second loops.
                if row and row['status']!='failed':
                    control=db.execute('select current_run,startup_ref,startup_state from control where id=1').fetchone()
                    outstanding=bool(db.execute('select 1 from jobs where '+LIVE_JOBS+' limit 1').fetchone())
                    # A normal useful-idle shutdown is NOT fresh demand. Do
                    # not resurrect an already satisfied startup merely
                    # because its old worker later terminates.
                    if (control['current_run']==run_id
                            and (control['startup_state']!='ready' or outstanding)):
                        if row['status']=='starting' or control['startup_state']=='pending':
                            # Actual incomplete cold lifecycle. The plain
                            # notebook/session terminal after an already
                            # successful READY must NOT consume another
                            # preparation-failure attempt.
                            db.execute('''UPDATE control SET startup_failures=startup_failures+1,
                                startup_state=CASE WHEN startup_ref IS NOT NULL THEN 'pending'
                                    ELSE startup_state END WHERE current_run=?''',(run_id,))
                        elif control['startup_state']=='ready' and outstanding and control['startup_ref'] is not None:
                            # Reopen only unsettled demand, with fresh
                            # permissions on next read; old incarnation is
                            # still fenced until separate execution proof.
                            db.execute("update control set startup_state='pending' where current_run=?",(run_id,))
                db.execute('update runs set heartbeat=? where id=?',(self.clock(),run_id))
                db.execute("update jobs set state='pending',run_id=null,claim=null,lease_until=null where run_id=? and state='claimed'",(run_id,))
