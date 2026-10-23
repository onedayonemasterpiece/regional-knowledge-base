"""Initial source writer ownership in the existing queue/control database.

One advisory lock serializes publications, retirement and activation across
processes. A durable publication intent prevents control handover after a crash
or uncertain SDK response. No credentials, corpus, queue or identity is created.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import fcntl
import hashlib
import json
import logging
import os
import secrets
import time

from .bge_queue import BgeQueue
from .recovery_journal import RecoveryJournal,StaleWriter,JournalUnknown

_LOG=logging.getLogger(__name__)


def _text(value,maximum=512):
    if type(value) is not str or not 1<=len(value.encode())<=maximum or any(ord(c)<32 for c in value):
        raise ValueError('bounded writer identity required')
    return value


def pending(db,run_id):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='rkb_writer_control' AND type='table'").fetchone():return False
    return db.execute('SELECT 1 FROM rkb_writer_control WHERE id=1 AND run_id=? AND publication_json IS NOT NULL',(run_id,)).fetchone() is not None


class QueueWriterAuthority:
    def __init__(self,queue,*,run_id,database_generation,writer_generation,stream,lock_seconds=10):
        if not isinstance(queue,BgeQueue):raise TypeError('existing owned queue required')
        for value in (run_id,database_generation,writer_generation,stream):_text(value)
        if type(lock_seconds) not in (int,float) or not 0<lock_seconds<=30:raise ValueError('finite writer lock budget required')
        self.queue,self.run_id,self.database_generation,self.writer_generation,self.stream=(
            queue,run_id,database_generation,writer_generation,stream)
        self.lock_seconds=lock_seconds;self.closed=False
        self._store_identity=None
        self.lock_path=queue.path.resolve().with_suffix(queue.path.suffix+'.writer.lock')
        with queue.connect() as db:
            db.executescript('''CREATE TABLE IF NOT EXISTS rkb_writer_control(
                id INTEGER PRIMARY KEY CHECK(id=1),run_id TEXT NOT NULL,database_generation TEXT NOT NULL,
                writer_generation TEXT NOT NULL,stream TEXT NOT NULL,revision INTEGER NOT NULL,
                active INTEGER NOT NULL CHECK(active IN (0,1)),publication_json TEXT,storage_sha256 TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS rkb_writer_generations(
                writer_generation TEXT PRIMARY KEY,database_generation TEXT NOT NULL,run_id TEXT NOT NULL,
                stream TEXT NOT NULL,revision INTEGER NOT NULL,retired INTEGER NOT NULL CHECK(retired IN (0,1)));
              CREATE TABLE IF NOT EXISTS rkb_writer_restore_control(
                id INTEGER PRIMARY KEY CHECK(id=1),plan_json TEXT NOT NULL,identity_json TEXT NOT NULL,
                head_json TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN ('restoring','verified','active')));
              CREATE TRIGGER IF NOT EXISTS rkb_writer_control_fence BEFORE UPDATE OF current_run ON control
                WHEN OLD.current_run IS NOT NEW.current_run AND EXISTS(
                    SELECT 1 FROM rkb_writer_control WHERE run_id=OLD.current_run AND publication_json IS NOT NULL)
                BEGIN SELECT RAISE(ABORT,'writer publication unresolved'); END;
              CREATE TRIGGER IF NOT EXISTS rkb_writer_run_fence BEFORE UPDATE OF status ON runs
                WHEN OLD.status IS NOT NEW.status AND NEW.status IN ('expired','failed','draining') AND EXISTS(
                    SELECT 1 FROM rkb_writer_control WHERE run_id=OLD.id AND publication_json IS NOT NULL)
                BEGIN SELECT RAISE(ABORT,'writer publication unresolved'); END;
              CREATE TRIGGER IF NOT EXISTS rkb_writer_control_retire AFTER UPDATE OF current_run ON control
                WHEN OLD.current_run IS NOT NEW.current_run
                BEGIN UPDATE rkb_writer_generations SET retired=1 WHERE writer_generation IN(
                    SELECT writer_generation FROM rkb_writer_control WHERE run_id=OLD.current_run);
                  UPDATE rkb_writer_control SET active=0,revision=revision+1 WHERE run_id=OLD.current_run; END;''')

    def _storage(self,store):
        from .dataset_recovery_store import DatasetRecoveryStore
        if type(store) is DatasetRecoveryStore:return store.storage_identity
        endpoint=_text(getattr(store.client.meta,'endpoint_url',None),2048)
        return hashlib.sha256(json.dumps([endpoint,store.bucket,store.prefix],separators=(',',':')).encode()).hexdigest()

    def bind_store(self,store):
        identity=self._storage(store)
        if self._store_identity is not None and self._store_identity!=identity:
            raise StaleWriter('writer cannot change existing storage authority')
        with self.queue.connect(write=False) as db:
            row=db.execute('SELECT storage_sha256 FROM rkb_writer_control WHERE id=1').fetchone()
            if row is not None and row[0]!=identity:
                from .dataset_recovery_store import DatasetRecoveryStore
                if type(store) is not DatasetRecoveryStore or not store.allows_storage_transition(row[0]):
                    raise StaleWriter('persisted storage authority differs')
        self._store_identity=identity

    @asynccontextmanager
    async def _lock(self):
        if self.closed:raise StaleWriter('writer owner closed')
        # A fresh open description per guard is essential: flock on a shared
        # fd would silently permit concurrent tasks in the same process.
        fd=os.open(self.lock_path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
        acquired=False;deadline=time.monotonic()+self.lock_seconds
        try:
            while not acquired:
                try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);acquired=True
                except BlockingIOError:
                    if time.monotonic()>=deadline:raise StaleWriter('writer guard busy; reconcile pending publication') from None
                    await asyncio.sleep(.02)
            if self.closed:raise StaleWriter('writer owner closed')
            yield
        finally:
            if acquired:fcntl.flock(fd,fcntl.LOCK_UN)
            os.close(fd)

    def _binding(self,db,*,require_current=True):
        self.queue._shared_owner(db)
        row=db.execute('SELECT * FROM rkb_writer_control WHERE id=1').fetchone()
        if (self.closed or row is None or not row['active'] or self._store_identity is None
                or row['storage_sha256']!=self._store_identity or tuple(row[k] for k in
                ('run_id','database_generation','writer_generation','stream'))!=(
                    self.run_id,self.database_generation,self.writer_generation,self.stream)):
            raise StaleWriter('current persisted writer binding required')
        if require_current:self._runtime(db)
        return row

    def _runtime(self,db):
        run=db.execute('SELECT * FROM runs WHERE id=?',(self.run_id,)).fetchone()
        control=db.execute('SELECT current_run FROM control WHERE id=1').fetchone()
        runtime=self.queue._runtime_row(db,self.run_id)
        if (run is None or control[0]!=self.run_id or run['status']!='ready' or not run['channel_active']
                or run['attachment_conflict'] or self.queue._run_expired(run,self.queue.clock())
                or runtime is None or runtime['accepted_generation']!=run['connection_generation']):
            raise StaleWriter('current verified target runtime required')
        expected=json.loads(runtime['expected_json'])
        if expected.get('database_generation')!=self.database_generation:
            raise StaleWriter('writer database differs from verified runtime')
        owner=db.execute('SELECT run_id,incarnation FROM bge_shared_admission WHERE id=1').fetchone()
        if owner is None or tuple(owner)!=(self.run_id,run['runtime_incarnation']):
            raise StaleWriter('current shared resource baseline required')
        return run

    def _restore_row(self,db):
        return db.execute('SELECT * FROM rkb_writer_restore_control WHERE id=1').fetchone()

    def _restore_runtime(self,db,binding=None,*,allow_active=False):
        row=self._restore_row(db)
        if row is None or row['state']=='active' and not allow_active:raise StaleWriter('recovery-only writer binding required')
        run=db.execute('SELECT * FROM runs WHERE id=?',(self.run_id,)).fetchone()
        runtime=self.queue._runtime_row(db,self.run_id)
        control=db.execute('SELECT current_run FROM control WHERE id=1').fetchone()
        identity=json.loads(row['identity_json']);plan=json.loads(row['plan_json'])
        if (run is None or control[0]!=self.run_id or run['status'] not in ('starting','ready')
                or not run['channel_active'] or run['attachment_conflict'] or self.queue._run_expired(run,self.queue.clock())
                or [run['id'],run['launch_nonce'],run['runtime_incarnation']]!=identity or runtime is None):
            raise StaleWriter('current pre-ready recovery attachment required')
        expected=json.loads(runtime['expected_json'])
        if (expected.get('database_generation')!=self.database_generation
                or expected.get('hashes',{}).get('launch_config')!=plan['launch_config_sha256']):
            raise StaleWriter('recovery launch expectation differs')
        if binding is not None and (binding.run_id,binding.runtime_incarnation,binding.connection_generation,
                binding.database_generation,binding.writer_generation)!=(self.run_id,identity[2],run['connection_generation'],
                    self.database_generation,self.writer_generation):
            raise StaleWriter('recovery attachment generation changed')
        owner=db.execute('SELECT run_id,incarnation FROM bge_shared_admission WHERE id=1').fetchone()
        if owner is None or tuple(owner)!=(self.run_id,identity[2]):raise StaleWriter('recovery shared baseline required')
        return row

    def recovery_current(self,binding):
        try:
            with self.queue.connect(write=False) as db:
                self._binding(db,require_current=False);self._restore_runtime(db,binding)
            return True
        except Exception:return False

    async def begin_restore(self,journal,binding,*,plan):
        """Retire an already fenced old local writer; grant only recovery I/O.

        plan is fixed typed startup metadata. It is no native database proof and
        cannot activate ordinary writes. Remote head identity is read here.
        """
        if (not isinstance(journal,RecoveryJournal) or journal.stream!=self.stream
                or journal.store.writer_guard is not self or self._storage(journal.store)!=self._store_identity):
            raise TypeError('same existing recovery journal/store required')
        from .kaggle_restore_startup import SourceRestoreConfig,PreReadyRecoveryBinding
        if type(plan) is not SourceRestoreConfig or type(binding) is not PreReadyRecoveryBinding:
            raise TypeError('typed exact restore plan and pre-ready attachment required')
        encoded=json.dumps(plan.wire(),sort_keys=True,separators=(',',':'))
        async with self._lock():
            with self.queue.connect(write=False) as db:
                prior=db.execute('SELECT * FROM rkb_writer_control WHERE id=1').fetchone()
                restored=self._restore_row(db)
                if restored is not None and prior is not None and prior['writer_generation']==self.writer_generation:
                    self._binding(db,require_current=False);self._restore_runtime(db,binding,allow_active=True)
                    if restored['plan_json']!=encoded:raise StaleWriter('retained recovery plan differs')
                    return
                from .dataset_recovery_store import DatasetRecoveryStore
                transition=(type(journal.store) is DatasetRecoveryStore and prior is not None
                    and journal.store.allows_storage_transition(prior['storage_sha256']))
                if (prior is None or prior['active'] or prior['publication_json'] is not None
                        or prior['run_id']==self.run_id or prior['database_generation']!=self.database_generation
                        or prior['stream']!=self.stream or prior['storage_sha256']!=self._store_identity and not transition
                        or db.execute('SELECT 1 FROM rkb_writer_generations WHERE writer_generation=?',
                            (self.writer_generation,)).fetchone() is not None):
                    raise StaleWriter('previous persisted writer must be retired and settled before recovery')
                run=db.execute('SELECT * FROM runs WHERE id=?',(self.run_id,)).fetchone()
                if run is None:raise StaleWriter('actual recovery run required')
                identity=[run['id'],run['launch_nonce'],run['runtime_incarnation']]
            head=await journal.head()
            head_wire=None if head is None else dict(reference=head.reference.wire(),token=head.token)
            with self.queue.connect() as db:
                observed=db.execute('SELECT * FROM rkb_writer_control WHERE id=1').fetchone()
                if dict(observed)!=dict(prior):raise StaleWriter('previous writer changed during recovery acquisition')
                db.execute('DELETE FROM rkb_writer_restore_control')
                db.execute('INSERT INTO rkb_writer_restore_control VALUES(1,?,?,?,?)',
                    (encoded,json.dumps(identity),json.dumps(head_wire,sort_keys=True),'restoring'))
                db.execute('UPDATE rkb_writer_control SET run_id=?,writer_generation=?,storage_sha256=?,revision=revision+1,active=1 WHERE id=1',
                    (self.run_id,self.writer_generation,self._store_identity))
                db.execute('INSERT INTO rkb_writer_generations VALUES(?,?,?,?,?,0)',
                    (self.writer_generation,self.database_generation,self.run_id,self.stream,observed['revision']+1))
                self._binding(db,require_current=False);self._restore_runtime(db,binding)
        _LOG.info('target_writer state=recovery_only run_id=%s writer_generation=%s',self.run_id,self.writer_generation)

    def _publication_binding(self,db,*,key=None):
        row=self._binding(db,require_current=False)
        restore=self._restore_row(db)
        if restore is None or restore['state']=='active':
            self._runtime(db);return row
        self._restore_runtime(db)
        if key is not None:
            prefix='recovery/v1/'+hashlib.sha256(self.stream.encode()).hexdigest()+'/'
            plan=json.loads(restore['plan_json'])
            intent=prefix+'snapshots/recovery-intents/'+hashlib.sha256(plan['operation_key'].encode()).hexdigest()
            outcome=key.startswith(prefix+'outcomes/') and len(key[len(prefix+'outcomes/'):])==64
            pointer=key==prefix+'snapshots/current' and restore['state']=='verified'
            if key!=intent and not outcome and not pointer:
                raise StaleWriter('recovery-only owner cannot publish accepted commands or unrelated keys')
        return row

    async def activate_recovered(self,journal,proof,binding):
        from .pg_snapshot_recovery import ControllerRecoveryProof,PhysicalSnapshotRecovery
        if (type(proof) is not ControllerRecoveryProof or type(proof._factory) is not PhysicalSnapshotRecovery
                or proof._factory.journal is not journal or proof not in proof._factory._proofs):
            raise TypeError('retained native/controller recovery proof required')
        await proof.verify_for(proof.database,*proof.identity)
        if await journal.head()!=proof.journal_head:raise StaleWriter('recovered journal prefix changed')
        async with self._lock():
            with self.queue.connect() as db:
                self._binding(db,require_current=False);restore=self._restore_runtime(db,binding,allow_active=True)
                if (proof.database.database_generation!=self.database_generation
                        or proof.database.writer_generation!=self.writer_generation
                        or list(proof.identity)!=json.loads(restore['identity_json'])
                        or proof.manifest.wire()!=json.loads(restore['plan_json'])['manifest']):
                    raise StaleWriter('recovered proof differs from persisted operation')
                current_head=None if proof.journal_head is None else dict(reference=proof.journal_head.reference.wire(),token=proof.journal_head.token)
                if current_head!=json.loads(restore['head_json']):raise StaleWriter('recovery original head changed')
                run=self._runtime(db)
                runtime=self.queue._runtime_row(db,self.run_id)
                observed=json.loads(runtime['observed_json'])
                if observed['applied_seq']!=proof.applied_sequence or observed['projection_seq']!=proof.applied_sequence:
                    raise StaleWriter('recovered READY cursor differs from native proof')
                if db.execute('SELECT publication_json FROM rkb_writer_control WHERE id=1').fetchone()[0] is not None:
                    raise JournalUnknown('recovery publication requires exact reconciliation')
                db.execute("UPDATE rkb_writer_restore_control SET state='verified' WHERE id=1")
        # A captured base is not a verified current snapshot. Only this retained
        # physical restore/readiness proof may promote the canonical pointer.
        # Only the pointer's CAS token is needed to replace it. A pointer to a
        # base superseded by a verified Dataset promotion (Dataset1 after
        # Dataset2) no longer matches the current Dataset transport, so do not
        # re-validate its archive here; never let an older proof replace it.
        current=await proof._factory.current_pointer()
        if current is not None and proof.applied_sequence<current[0].last_sequence:
            raise StaleWriter('recovered base is older than the current snapshot pointer')
        await proof._factory.promote(proof,binding,expected_token=current[1] if current else None)
        async with self._lock():
            with self.queue.connect() as db:
                self._binding(db,require_current=False);self._restore_runtime(db,binding)
                self._runtime(db)
                if db.execute('SELECT publication_json FROM rkb_writer_control WHERE id=1').fetchone()[0] is not None:
                    raise JournalUnknown('verified snapshot pointer publication requires reconciliation')
                db.execute("UPDATE rkb_writer_restore_control SET state='active' WHERE id=1")
        _LOG.info('target_writer state=recovered_active run_id=%s writer_generation=%s',self.run_id,self.writer_generation)

    def current(self,*,database_generation,writer_generation):
        if (database_generation,writer_generation)!=(self.database_generation,self.writer_generation):return False
        try:
            with self.queue.connect(write=False) as db:
                self._binding(db)
                restore=self._restore_row(db)
                if restore is not None and restore['state']!='active':return False
            return True
        except Exception:return False

    async def ensure_current(self,journal):
        """Initial genesis only, or exact restart of this already active owner.

        Accepted-history restore and same-database writer rollover require their
        existing ordered recovery barrier; this method never guesses one.
        """
        if not isinstance(journal,RecoveryJournal) or journal.stream!=self.stream:
            raise TypeError('exact existing journal stream required')
        from .recovery_s3 import S3ConditionalRecoveryStore
        from .dataset_recovery_store import DatasetRecoveryStore
        if (type(journal.store) not in (S3ConditionalRecoveryStore,DatasetRecoveryStore) or journal.store.writer_guard is not self
                or self._storage(journal.store)!=self._store_identity):
            raise TypeError('same existing native recovery storage authority required')
        async with self._lock():
            with self.queue.connect(write=False) as db:
                prior=db.execute('SELECT * FROM rkb_writer_control WHERE id=1').fetchone()
                if prior is not None:
                    self._binding(db)
                    return  # Exact restart; native/history verifier still gates reads.
                run=self._runtime(db)
                runtime=self.queue._runtime_row(db,self.run_id)
                observed=json.loads(runtime['observed_json'])
                if observed.get('applied_seq')!=0 or observed.get('projection_seq')!=0:
                    raise StaleWriter('initial writer native generation is not empty')
                table=db.execute("SELECT 1 FROM sqlite_master WHERE name='bge_empty_generation_intents'").fetchone()
                intent=db.execute('SELECT intent_json FROM bge_empty_generation_intents WHERE run_id=?',(self.run_id,)).fetchone() if table else None
                if intent is None:raise StaleWriter('initial writer requires actual empty-generation intent')
                value=json.loads(intent[0]);database=value.get('database',{})
                if (value.get('journal_stream')!=self.stream or value.get('launch_nonce')!=run['launch_nonce']
                        or value.get('journal_head') is not None or database.get('mode')!='NEW_EMPTY_GENERATION'
                        or database.get('database_generation')!=self.database_generation
                        or database.get('writer_generation')!=self.writer_generation):
                    raise StaleWriter('initial writer intent differs')
            if await journal.head() is not None:raise StaleWriter('initial writer cannot adopt accepted history')
            with self.queue.connect() as db:
                self._runtime(db)
                if db.execute('SELECT 1 FROM rkb_writer_control').fetchone():raise StaleWriter('writer binding appeared')
                if db.execute("SELECT 1 FROM sqlite_master WHERE name='rkb_control_spool_meta'").fetchone():
                    if db.execute('SELECT operation_count FROM rkb_control_spool_meta WHERE id=1').fetchone()[0]:
                        raise StaleWriter('initial writer intake is not empty')
                values=(self.run_id,self.database_generation,self.writer_generation,self.stream)
                db.execute('INSERT INTO rkb_writer_control VALUES(1,?,?,?,?,1,1,NULL,?)',(*values,self._store_identity))
                db.execute('INSERT INTO rkb_writer_generations VALUES(?,?,?,?,1,0)',
                    (self.writer_generation,self.database_generation,self.run_id,self.stream))
            _LOG.info('target_writer state=active run_id=%s writer_generation=%s',self.run_id,self.writer_generation)

    @asynccontextmanager
    async def __call__(self,generation):
        """Serialize local owner admission; callers must recheck after awaits.

        Immutable snapshot ticket creation may leave harmless orphan objects.
        Canonical publications must use publication_guard: its durable exact
        intent additionally excludes queue pointer transitions across I/O.
        """
        if generation!=self.writer_generation:raise StaleWriter('stale writer generation')
        async with self._lock():
            with self.queue.connect(write=False) as db:
                row=self._publication_binding(db)
                if row['publication_json'] is not None:raise JournalUnknown('writer publication requires exact reconciliation')
            yield

    @asynccontextmanager
    async def publication_guard(self,generation,*,key,data_sha256,data_length,expected_token,store):
        if generation!=self.writer_generation:raise StaleWriter('stale writer generation')
        if self._storage(store)!=self._store_identity:raise StaleWriter('publication storage authority changed')
        prefix='recovery/v1/'+hashlib.sha256(self.stream.encode()).hexdigest()+'/'
        if (not key.startswith(prefix) or len(key)>2048 or len(data_sha256)!=64
                or type(data_length) is not int or not 1<=data_length<=4*1024*1024):
            raise ValueError('bounded exact journal publication required')
        async with self._lock():
            intent=dict(key=key,data_sha256=data_sha256,data_length=data_length,expected_token=expected_token,
                nonce=secrets.token_hex(24),state='IN_FLIGHT')
            encoded=json.dumps(intent,sort_keys=True,separators=(',',':'))
            with self.queue.connect() as db:
                row=self._publication_binding(db,key=key)
                if row['publication_json'] is not None:raise JournalUnknown('writer publication requires exact reconciliation')
                db.execute('UPDATE rkb_writer_control SET publication_json=? WHERE id=1',(encoded,))
            _LOG.info('target_writer_publication state=in_flight run_id=%s writer_generation=%s',self.run_id,self.writer_generation)
            known=[False]
            class Receipt:
                def resolved(self,result):
                    if type(result) is not bool:raise TypeError('native conditional result required')
                    known[0]=True
            try:yield Receipt()
            finally:
                with self.queue.connect() as db:
                    row=self._binding(db,require_current=False)
                    if row['publication_json']!=encoded:raise StaleWriter('publication intent changed')
                    if known[0]:db.execute('UPDATE rkb_writer_control SET publication_json=NULL WHERE id=1')
                    else:
                        intent['state']='UNKNOWN'
                        db.execute('UPDATE rkb_writer_control SET publication_json=? WHERE id=1',
                            (json.dumps(intent,sort_keys=True,separators=(',',':')),))
                _LOG.info('target_writer_publication state=%s run_id=%s writer_generation=%s',
                    'resolved' if known[0] else 'unknown',self.run_id,self.writer_generation)

    async def reconcile(self,store,*,exact_data=None):
        """Clear UNKNOWN only after native exact-byte readback, never by a bool."""
        from .recovery_s3 import S3ConditionalRecoveryStore
        from .dataset_recovery_store import DatasetRecoveryStore
        if type(store) not in (S3ConditionalRecoveryStore,DatasetRecoveryStore) or store.writer_guard is not self:
            raise TypeError('same configured native recovery store required')
        if self._storage(store)!=self._store_identity:raise StaleWriter('reconciliation storage authority changed')
        async with self._lock():
            with self.queue.connect(write=False) as db:
                row=self._binding(db,require_current=False);raw=row['publication_json']
            if raw is None:return True
            intent=json.loads(raw)
            value=await store.read(intent['key'],max_bytes=intent['data_length'])
            if (value is None or hashlib.sha256(value.data).hexdigest()!=intent['data_sha256']) and exact_data is not None:
                if type(store) is DatasetRecoveryStore:
                    raise StaleWriter('Dataset pending state cannot reconcile an old cloud effect')
                if (type(exact_data) is not bytes or len(exact_data)!=intent['data_length']
                        or hashlib.sha256(exact_data).hexdigest()!=intent['data_sha256']):
                    raise ValueError('only original exact unknown publication bytes may retry')
                from .recovery_s3 import _blocking
                condition=({'IfNoneMatch':'*'} if intent['expected_token'] is None
                    else {'IfMatch':intent['expected_token']})
                await _blocking(lambda:store._put(intent['key'],exact_data,condition))
                value=await store.read(intent['key'],max_bytes=intent['data_length'])
            if (value is None or len(value.data)!=intent['data_length']
                    or hashlib.sha256(value.data).hexdigest()!=intent['data_sha256']):
                raise JournalUnknown('native exact publication remains unknown')
            with self.queue.connect() as db:
                row=self._binding(db,require_current=False)
                if row['publication_json']!=raw:raise StaleWriter('publication changed during native reconciliation')
                db.execute('UPDATE rkb_writer_control SET publication_json=NULL WHERE id=1')
            _LOG.info('target_writer_publication state=reconciled run_id=%s writer_generation=%s',self.run_id,self.writer_generation)
            return True

    async def retire(self,generation):
        if generation!=self.writer_generation:raise StaleWriter('stale writer retirement')
        async with self._lock():
            with self.queue.connect() as db:
                row=self._binding(db,require_current=False)
                if row['publication_json'] is not None:raise JournalUnknown('reconcile publication before writer retirement')
                db.execute('UPDATE rkb_writer_generations SET retired=1 WHERE writer_generation=?',(generation,))
                db.execute('UPDATE rkb_writer_control SET active=0,revision=revision+1 WHERE id=1')
            _LOG.info('target_writer state=retired run_id=%s writer_generation=%s',self.run_id,self.writer_generation)

    def close(self):self.closed=True
