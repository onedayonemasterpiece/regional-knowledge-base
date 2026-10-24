"""Canonical source-control intake in the existing BgeQueue SQLite spool.

Internal controller API only. No dispatcher, new identity authority, read-path
write, provider adapter, or product closed/Saved acknowledgement is owned here.
"""
from __future__ import annotations

import asyncio
import base64
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import logging
import math
import os
import sqlite3
import time
from uuid import UUID

from .kaggle_apply import (
    ApplyReceipt, DependencySpec, HandlerRegistry, _snapshot, apply_accepted,
)
from .participants import ParticipantError, Participants, RequestAuthority
from .recovery_journal import (
    AcceptedSeal, ExactCommand, ObjectRef, RecoveryJournal, RemoteOutcomeReceipt,
    VerifiedHead, JournalUnknown,
)

SOURCE_WITHDRAW = 'source.withdraw.v1'
WITHDRAW_PAYLOAD = b'{"withdrawn":true}'
WITHDRAW_DEPENDENCY = DependencySpec(SOURCE_WITHDRAW,'public_source',
    frozenset({'source_id'}),'shared',True)
_UNSET=object()
_LOG=logging.getLogger(__name__)

# Dataset persistence and native read visibility are separate observations.
# Only these additive packet operations become read-safe after exact apply;
# withdrawal and all other mutations retain their existing durable barrier.
READ_ADDITIVE_ROUTES=frozenset({'source.stage.v1','source.batch.v1','source.finish.v1'})
# These commands own inactive candidate data, rather than public source heads.
INACTIVE_SOURCE_ROUTES=frozenset({'source.stage.v1','source.stage.seal.v1','source.materialize.v1'})

def _read_safe_sql(prefix=''):
    return (f"({prefix}seal_state='ACCEPTED' AND {prefix}apply_state IN ('APPLIED','REJECTED_PRECONDITION') "
        f"AND json_extract(CAST({prefix}command_bytes AS TEXT),'$.route') IN "
        "('source.stage.v1','source.batch.v1','source.finish.v1'))")

def _read_pending_sql(prefix=''):
    return f"({prefix}outcome_state<>'DURABLE' AND NOT {_read_safe_sql(prefix)})"

def _read_unknown_sql(prefix=''):
    return (f"({prefix}seal_state='SENT_UNKNOWN' OR {prefix}apply_state='SENT_UNKNOWN' OR "
        f"({prefix}outcome_state='SENT_UNKNOWN' AND NOT {_read_safe_sql(prefix)}))")


class SpoolError(RuntimeError):pass
class SpoolConflict(SpoolError):pass
class SpoolFull(SpoolError):pass
class SpoolUnknown(SpoolError):pass
class SpoolDenied(PermissionError):pass


def _json(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def _hash(data):return hashlib.sha256(data).hexdigest()


def _uuid(value):
    if not isinstance(value,str) or str(UUID(value))!=value:
        raise ValueError('canonical source/person UUID required')
    return value


def _bounded(value,limit=256):
    if not isinstance(value,str) or not 1<=len(value)<=limit or len(value.encode())>limit:
        raise ValueError('bounded explicit controller identity required')


def _command(data):
    try:
        value=json.loads(data)
        command=ExactCommand(**{k:value[k] for k in ('sequence','operation_key','request_id','person_id',
            'route','logical_time','schema_revision','function_revision','executor_revision','ids',
            'expected_revisions','kind')},payload=base64.b64decode(value['payload_b64'],validate=True))
        if command.encode()!=data:raise ValueError('changed command')
        return command
    except (ValueError,TypeError,KeyError):
        raise SpoolConflict('stored exact command integrity differs') from None


def _seal_wire(seal):
    return {'manifest':seal.manifest.wire(),'command_hashes':list(seal.command_hashes)}


def _seal(data):
    value=json.loads(data)
    return AcceptedSeal(ObjectRef(**value['manifest']),tuple(value['command_hashes']))


def _head_wire(head):
    return None if head is None else {'reference':head.reference.wire(),'token':head.token}


def _head(data):
    value=json.loads(data)
    return None if value is None else VerifiedHead(ObjectRef(**value['reference']),value['token'])


async def _database(call,*args):
    """Drain a committed local write before cancellation releases its owner."""
    task=asyncio.create_task(asyncio.to_thread(call,*args));cancelled=False
    while not task.done():
        try:await asyncio.shield(task)
        except asyncio.CancelledError:cancelled=True
        except Exception:break
    if cancelled:
        if not task.cancelled():task.exception()
        raise asyncio.CancelledError
    return task.result()


@dataclass(frozen=True)
class ControlOperation:
    command_bytes: bytes
    command_sha256: str
    database_generation: str
    handler_sha256: str
    seal_state: str
    apply_state: str
    outcome_state: str
    accepted: AcceptedSeal | None
    apply_receipt: ApplyReceipt | None
    remote_outcome: RemoteOutcomeReceipt | None

    @property
    def command(self):return _command(self.command_bytes)


@dataclass(frozen=True)
class AcceptedRestriction:
    operation_key: str
    sequence: int
    command_sha256: str
    apply_state: str
    outcome_state: str


@dataclass(frozen=True)
class SourceBarrier:
    operation_key: str
    source_id: str
    sequence: int
    command_sha256: str
    database_generation: str
    intent_remote_accepted: bool
    apply_state: str
    outcome_state: str
    accepted_restriction: AcceptedRestriction | None = None
    pending_sequence: int = 0
    unknown_sequence: int = 0
    restriction_active: bool = True


@dataclass(frozen=True)
class BarrierSnapshot:
    barriers: tuple[SourceBarrier,...]
    sequence: int
    accepted_sequence: int
    # Source-only intake cannot attest legacy writers or an un-restored remote
    # history. Empty local results must never be relabelled complete authority.
    history_complete: bool = False
    scope_complete: bool = False


@dataclass(frozen=True)
class SourceChange:
    source_id: str
    sequence: int
    command_sha256: str
    restriction: bool
    pending: bool
    unknown: bool


@dataclass(frozen=True, eq=False)
class SourceReadFacts:
    """One local snapshot, usable only while its owner's revision is current."""
    database_generation: str | None
    baseline_sequence: int
    covered_sequence: int
    scope_sequence: int
    pending: bool
    unknown: bool
    history_complete: bool
    changes: tuple[SourceChange, ...]
    _owner: object
    _anchor: object
    _version: object


@dataclass(frozen=True)
class SourceCursor:
    """Current Dataset prefix plus locally retained native apply receipts."""
    database_generation: str
    binding: object
    head: object
    accepted_sequence: int
    applied_sequence: int
    pending: tuple
    source_heads: tuple
    _current: object

    def current(self):
        return self._current(self.binding)


class ControlSpool:
    def __init__(self,queue,participants: Participants,journal: RecoveryJournal,registry: HandlerRegistry,
                 *,max_bytes=512*1024*1024,raw_retention_seconds=4*3600,clock=time.time):
        if not isinstance(participants,Participants) or not isinstance(journal,RecoveryJournal) or not isinstance(registry,HandlerRegistry):
            raise TypeError('existing canonical participants/journal/registry required')
        if type(max_bytes) is not int or max_bytes<1:
            raise ValueError('positive bounded spool budget required')
        if not isinstance(raw_retention_seconds,(int,float)) or isinstance(raw_retention_seconds,bool) or not math.isfinite(raw_retention_seconds) or raw_retention_seconds<=0:
            raise ValueError('finite raw retention boundary required')
        self.queue,self.participants,self.journal,self.registry=queue,participants,journal,registry
        self.max_bytes,self.raw_retention_seconds,self.clock=max_bytes,float(raw_retention_seconds),clock
        self.max_command_bytes=min(1048576,journal.max_segment_bytes)
        self.max_outcome_bytes=min(1048576,journal.max_outcome_bytes)
        self._lock=asyncio.Lock();self._loop=None;self._writing=0;self._intake_guard=None
        self._schema_intake_guard=None
        self._facts_owner=object();self._source_version=object();self._source_anchor=None
        self._source_head=None;self._source_covered_sequence=0
        self._fd=os.open(queue.path.resolve().with_suffix(queue.path.suffix+'.control-spool.lock'),os.O_RDWR|os.O_CREAT,0o600)
        try:
            fcntl.flock(self._fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            with self._db(write=True) as db:
                db.execute('''CREATE TABLE IF NOT EXISTS rkb_control_spool_meta(
                    id INTEGER PRIMARY KEY CHECK(id=1),stream TEXT NOT NULL,used_bytes INTEGER NOT NULL,
                    sequence INTEGER NOT NULL,accepted_sequence INTEGER NOT NULL,
                    operation_count INTEGER NOT NULL,pending_count INTEGER NOT NULL CHECK(pending_count>=0),
                    unknown_count INTEGER NOT NULL CHECK(unknown_count>=0))''')
                db.execute('INSERT OR IGNORE INTO rkb_control_spool_meta VALUES(1,?,0,0,0,0,0,0)',(journal.stream,))
                if db.execute('SELECT stream FROM rkb_control_spool_meta WHERE id=1').fetchone()[0]!=journal.stream:
                    raise SpoolConflict('control database belongs to another ordered stream')
                db.execute('''CREATE TABLE IF NOT EXISTS rkb_control_operations(
                    operation_key TEXT PRIMARY KEY,command_bytes BLOB NOT NULL CHECK(length(command_bytes) BETWEEN 1 AND 1048576),command_sha256 TEXT NOT NULL,
                    sequence INTEGER NOT NULL UNIQUE,source_id TEXT NOT NULL,person_id TEXT NOT NULL,
                    database_generation TEXT NOT NULL,dependency_bytes BLOB NOT NULL,handler_sha256 TEXT NOT NULL,
                    reserved_bytes INTEGER NOT NULL,created REAL NOT NULL,updated REAL NOT NULL,
                    seal_state TEXT NOT NULL CHECK(seal_state IN ('NOT_SENT','SENT_UNKNOWN','ACCEPTED')),
                    predecessor_bytes BLOB,seal_bytes BLOB,
                    apply_state TEXT NOT NULL CHECK(apply_state IN ('NOT_SENT','SENT_UNKNOWN','APPLIED','REJECTED_PRECONDITION')),
                    apply_bytes BLOB,outcome_bytes BLOB,outcome_sha256 TEXT,
                    outcome_state TEXT NOT NULL CHECK(outcome_state IN ('NOT_SENT','SENT_UNKNOWN','DURABLE')),
                    remote_receipt_bytes BLOB)''')
                # Derived counters for independent public-source reads. The
                # global stream meta remains the only sequence/intake authority.
                # Private/case/GEO pending operations do not imply a book write.
                db.execute('CREATE TABLE IF NOT EXISTS rkb_control_public_source_meta('
                    'id INTEGER PRIMARY KEY CHECK(id=1),sequence INTEGER NOT NULL,accepted_sequence INTEGER NOT NULL,'
                    'operation_count INTEGER NOT NULL,pending_count INTEGER NOT NULL CHECK(pending_count>=0),'
                    'unknown_count INTEGER NOT NULL CHECK(unknown_count>=0))')
                db.execute("""INSERT OR IGNORE INTO rkb_control_public_source_meta
                    SELECT 1,coalesce(max(sequence),0),coalesce(max(CASE WHEN seal_state='ACCEPTED' THEN sequence ELSE 0 END),0),
                    count(*),coalesce(sum(outcome_state<>'DURABLE'),0),
                    coalesce(sum(seal_state='SENT_UNKNOWN' OR apply_state='SENT_UNKNOWN' OR outcome_state='SENT_UNKNOWN'),0)
                    FROM rkb_control_operations WHERE json_extract(CAST(dependency_bytes AS TEXT),'$.scope_class')='public_source'""")
                db.execute("""CREATE TRIGGER IF NOT EXISTS rkb_control_public_source_insert AFTER INSERT ON rkb_control_operations
                    WHEN json_extract(CAST(NEW.dependency_bytes AS TEXT),'$.scope_class')='public_source'
                    BEGIN UPDATE rkb_control_public_source_meta SET sequence=max(sequence,NEW.sequence),
                    accepted_sequence=max(accepted_sequence,CASE WHEN NEW.seal_state='ACCEPTED' THEN NEW.sequence ELSE 0 END),
                    operation_count=operation_count+1,pending_count=pending_count+(NEW.outcome_state<>'DURABLE'),
                    unknown_count=unknown_count+(NEW.seal_state='SENT_UNKNOWN' OR NEW.apply_state='SENT_UNKNOWN' OR NEW.outcome_state='SENT_UNKNOWN')
                    WHERE id=1;END""")
                db.execute("""CREATE TRIGGER IF NOT EXISTS rkb_control_public_source_update AFTER UPDATE OF seal_state,apply_state,outcome_state ON rkb_control_operations
                    WHEN json_extract(CAST(NEW.dependency_bytes AS TEXT),'$.scope_class')='public_source'
                    BEGIN UPDATE rkb_control_public_source_meta SET
                    accepted_sequence=max(accepted_sequence,CASE WHEN NEW.seal_state='ACCEPTED' THEN NEW.sequence ELSE 0 END),
                    pending_count=pending_count+(NEW.outcome_state<>'DURABLE')-(OLD.outcome_state<>'DURABLE'),
                    unknown_count=unknown_count+(NEW.seal_state='SENT_UNKNOWN' OR NEW.apply_state='SENT_UNKNOWN' OR NEW.outcome_state='SENT_UNKNOWN')
                    -(OLD.seal_state='SENT_UNKNOWN' OR OLD.apply_state='SENT_UNKNOWN' OR OLD.outcome_state='SENT_UNKNOWN') WHERE id=1;END""")
                # Rebuild only the derived read counters. Canonical intake and
                # durability counters above keep their original semantics.
                for name in ('rkb_control_public_source_insert','rkb_control_public_source_update'):
                    db.execute('DROP TRIGGER IF EXISTS '+name)
                db.execute(f'''UPDATE rkb_control_public_source_meta SET
                    pending_count=(SELECT coalesce(sum({_read_pending_sql()}),0) FROM rkb_control_operations
                        WHERE json_extract(CAST(dependency_bytes AS TEXT),'$.scope_class')='public_source'),
                    unknown_count=(SELECT coalesce(sum({_read_unknown_sql()}),0) FROM rkb_control_operations
                        WHERE json_extract(CAST(dependency_bytes AS TEXT),'$.scope_class')='public_source') WHERE id=1''')
                db.execute(f'''CREATE TRIGGER rkb_control_public_source_insert AFTER INSERT ON rkb_control_operations
                    WHEN json_extract(CAST(NEW.dependency_bytes AS TEXT),'$.scope_class')='public_source'
                    BEGIN UPDATE rkb_control_public_source_meta SET sequence=max(sequence,NEW.sequence),
                    accepted_sequence=max(accepted_sequence,CASE WHEN NEW.seal_state='ACCEPTED' THEN NEW.sequence ELSE 0 END),
                    operation_count=operation_count+1,pending_count=pending_count+{_read_pending_sql('NEW.')},
                    unknown_count=unknown_count+{_read_unknown_sql('NEW.')} WHERE id=1;END''')
                db.execute(f'''CREATE TRIGGER rkb_control_public_source_update AFTER UPDATE OF seal_state,apply_state,outcome_state ON rkb_control_operations
                    WHEN json_extract(CAST(NEW.dependency_bytes AS TEXT),'$.scope_class')='public_source'
                    BEGIN UPDATE rkb_control_public_source_meta SET
                    accepted_sequence=max(accepted_sequence,CASE WHEN NEW.seal_state='ACCEPTED' THEN NEW.sequence ELSE 0 END),
                    pending_count=pending_count+{_read_pending_sql('NEW.')}-{_read_pending_sql('OLD.')},
                    unknown_count=unknown_count+{_read_unknown_sql('NEW.')}-{_read_unknown_sql('OLD.')} WHERE id=1;END''')
                db.execute('CREATE INDEX IF NOT EXISTS rkb_control_source ON rkb_control_operations(source_id,sequence DESC,operation_key)')
                db.execute('CREATE INDEX IF NOT EXISTS rkb_control_source_state ON rkb_control_operations(source_id,sequence,seal_state,apply_state,outcome_state)')
                db.execute('CREATE INDEX IF NOT EXISTS rkb_control_accepted ON rkb_control_operations(source_id,seal_state,sequence DESC,operation_key)')
                db.execute("CREATE INDEX IF NOT EXISTS rkb_control_pending ON rkb_control_operations(source_id,sequence DESC) WHERE outcome_state<>'DURABLE'")
                db.execute("CREATE INDEX IF NOT EXISTS rkb_control_unknown ON rkb_control_operations(source_id,sequence DESC) WHERE seal_state='SENT_UNKNOWN' OR apply_state='SENT_UNKNOWN' OR outcome_state='SENT_UNKNOWN'")
                db.execute('''CREATE TRIGGER IF NOT EXISTS rkb_control_intent_immutable BEFORE UPDATE OF
                    operation_key,command_bytes,command_sha256,sequence,source_id,person_id,database_generation,
                    dependency_bytes,handler_sha256,reserved_bytes,created ON rkb_control_operations
                    BEGIN SELECT RAISE(ABORT,'immutable canonical control intent'); END''')
                db.execute('''CREATE TABLE IF NOT EXISTS rkb_control_receipts(
                    operation_key TEXT PRIMARY KEY,command_sha256 TEXT NOT NULL,receipt_bytes BLOB NOT NULL)''')
                db.execute('''CREATE TABLE IF NOT EXISTS rkb_control_delivery_receipts(
                    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                    run_id TEXT NOT NULL,incarnation TEXT NOT NULL,claim TEXT NOT NULL,
                    command_sha256 TEXT NOT NULL,PRIMARY KEY(job_id,claim))''')
                db.execute("CREATE INDEX IF NOT EXISTS bge_source_done_runtime ON jobs("
                    "run_id,json_extract(identity,'$.operation_key'),json_extract(identity,'$.command_sha256')) "
                    "WHERE kind='source_command' AND state='done'")
                # Compact heads derived from verified accepted commands. No book
                # content, catalog or independent source permission is stored.
                db.execute('''CREATE TABLE IF NOT EXISTS rkb_control_source_heads(
                    source_id TEXT PRIMARY KEY,sequence INTEGER NOT NULL,command_sha256 TEXT NOT NULL,
                    restriction INTEGER NOT NULL,pending INTEGER NOT NULL,unknown INTEGER NOT NULL,
                    reserved_bytes INTEGER NOT NULL)''')
                db.execute('''CREATE TABLE IF NOT EXISTS rkb_control_source_head_meta(
                    id INTEGER PRIMARY KEY CHECK(id=1),sequence INTEGER NOT NULL,
                    pending_count INTEGER NOT NULL,unknown_count INTEGER NOT NULL)''')
                db.execute('INSERT OR IGNORE INTO rkb_control_source_head_meta VALUES(1,0,0,0)')
        except BaseException:
            os.close(self._fd);self._fd=None;raise

    def _active(self):
        if self._fd is None:raise SpoolError('control spool owner closed')
        loop=asyncio.get_running_loop()
        if self._loop is None:self._loop=loop
        elif self._loop is not loop:raise SpoolError('one controller event loop required')

    @contextmanager
    def _db(self,*,write=False):
        # Same existing queue database, with FULL configured BEFORE beginning a
        # mutation transaction. No provider/model operation runs under this lock.
        db=sqlite3.connect(self.queue.path,timeout=5,isolation_level=None)
        db.row_factory=sqlite3.Row
        try:
            db.execute('PRAGMA synchronous=FULL' if write else 'PRAGMA query_only=ON')
            db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
            yield db
            if db.in_transaction:db.commit()
        except BaseException:
            if db.in_transaction:db.rollback()
            raise
        finally:db.close()

    def configure_intake_guard(self,current):
        """Retain the configured startup gate for new staging/acceptance only."""
        if not callable(current) or self._intake_guard is not None:
            raise TypeError('one retained control intake guard required')
        self._intake_guard=current

    def _require_intake(self):
        try:allowed=(self._intake_guard is None or self._intake_guard()) and (
            self._schema_intake_guard is None or self._schema_intake_guard())
        except Exception:allowed=False
        if not allowed:raise SpoolDenied('configured control intake unavailable')

    async def _authorize(self,context):
        self._require_intake()
        if not isinstance(context,RequestAuthority) or context.route!='owner' or context.public_only:
            raise SpoolDenied('current owner personal route required')
        try:return await _database(self.participants.require,context,'import')
        except ParticipantError:raise SpoolDenied('current owner import authority required') from None

    async def authorize_command(self,context,command):
        """Fixed installed route policy; source import never inherits editor rights.

        Per-Story editor/manager scope is checked by the pinned native function
        in the ordered apply transaction. This current Participants check only
        admits a canonical person to the personal editorial intake path.
        """
        self._require_intake()
        if command.route.startswith('disagreement.'):
            from .disagreement_native import validate_case_command,required_capabilities
            if command.schema_revision=='rkb-v4-sql001-014-v1':
                from .kaggle_native_source_bindings import validate_native_source_command,required_native_source_capabilities
                doc=validate_native_source_command(command);capabilities=required_native_source_capabilities(command)
            elif command.schema_revision in ('rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                from .kaggle_domain_bindings import validate_current_command,required_current_capabilities
                doc=validate_current_command(command);capabilities=required_current_capabilities(command)
            else:doc=validate_case_command(command);capabilities=required_capabilities(command)
            if (not isinstance(context,RequestAuthority) or context.public_only
                    or context.route not in ('owner','personal','expert')):
                raise SpoolDenied('current personal case authority required')
            attr=doc['attribution']
            if (attr['route']!=context.route or attr['link_id']!=context.link_id
                    or attr['link_revision']!=context.link_revision
                    or attr['recorded_by']!=(context.executor_id or context.principal.subject)):
                raise SpoolDenied('case attribution differs from current authenticated context')
            try:
                for capability in sorted(capabilities):
                    actor=await _database(self.participants.require,context,capability)
            except ParticipantError:raise SpoolDenied('current case participant capabilities required') from None
            if actor!=command.person_id:raise SpoolDenied('command person does not authenticate its actor')
            return actor
        if command.route=='geo.manifest.install.v1':
            if command.schema_revision=='rkb-v4-sql001-014-v1':
                from .kaggle_native_source_bindings import validate_native_source_command
                validate_native_source_command(command)
            else:
                from .geo_manifest_commands import validate_geo_manifest_command
                validate_geo_manifest_command(command)
        if command.route not in ('story.edit.v1','story.access.v1'):
            actor=await self._authorize(context)
            if actor!=command.person_id:raise SpoolDenied('command person does not authenticate its actor')
            return actor
        if (not isinstance(context,RequestAuthority) or context.public_only
                or context.route not in ('owner','personal','expert','delegated')):
            raise SpoolDenied('current personal editorial authority required')
        try:actor=await _database(self.participants.require,context,'public_search')
        except ParticipantError:raise SpoolDenied('current participant authority required') from None
        if actor!=command.person_id:raise SpoolDenied('command person does not authenticate its actor')
        return actor

    async def _write(self,call,*args):
        self._active();self._writing+=1
        # Invalidate before starting a cancellable write, including UNKNOWN
        # commits. No read may reuse an asynchronously captured stale snapshot.
        self._source_version=object()
        try:return await _database(call,*args)
        finally:
            self._writing-=1
            self._source_version=object()

    def invalidate_source_history(self):
        """Trusted controller calls BEFORE DDL/grant/generation/route changes."""
        self._active();self._source_anchor=None;self._source_version=object()

    async def dataset_cursor(self,binding,*,control,current):
        """Use the restored cursor; do not re-certify historical operations.

        Control is already read from the verified Dataset or physical restore.
        Only suffix deliveries completed on this run advance its applied floor.
        A lost apply response remains pending until ordinary reconciliation.
        """
        self._active()
        async with self._lock:
            if not current(binding):raise SpoolConflict('current Dataset binding required')
            head=await self.journal.head()
            base=control['sequence']
            accepted=head.reference.last_sequence if head is not None else 0
            if accepted<base:raise SpoolConflict('Dataset cursor exceeds current journal')
            anchor=self._source_anchor
            applied,pending,changes=await _database(self._dataset_cursor_suffix,control,accepted,binding)
            cursor=SourceCursor(binding.database_generation,binding,head,accepted,applied,pending,changes,current)
            if cursor!=anchor:
                await self._write(self._replace_source_heads,changes)
                self._source_anchor=cursor;self._source_version=object()
            self._source_head=head.reference if head else None
            self._source_covered_sequence=accepted
            return self._source_anchor

    def _dataset_cursor_suffix(self,control,accepted,binding):
        base=control['sequence'];applied=base;pending=[]
        changes={row[0]:SourceChange(row[0],row[1],row[2],bool(row[3]),bool(row[4]),bool(row[5]))
            for row in control['heads']}
        with self._db() as db:
            # The existing writer restore record, not a new mode flag, proves
            # that a local APPLIED receipt may belong to a previous PG run.
            restored=False
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='rkb_writer_restore_control'").fetchone():
                owner=db.execute('SELECT state,identity_json FROM rkb_writer_restore_control WHERE id=1').fetchone()
                if owner is not None and owner['state']=='active':
                    identity=json.loads(owner['identity_json'])
                    restored=(identity[0],identity[2])==(binding.run_id,binding.runtime_incarnation)
            for row in db.execute('SELECT sequence,seal_state,apply_state,command_bytes,outcome_bytes,'
                    'source_id,command_sha256,dependency_bytes,'
                    "json_extract(CAST(command_bytes AS TEXT),'$.route') AS route,"
                    f'{_read_pending_sql()} AS read_pending,{_read_unknown_sql()} AS read_unknown '
                    'FROM rkb_control_operations WHERE sequence>? AND sequence<=? ORDER BY sequence',
                    (base,accepted)):
                known=row['seal_state']=='ACCEPTED' and row['apply_state'] in ('APPLIED','REJECTED_PRECONDITION')
                # After a physical restore, an old native receipt says nothing
                # about the successor PG. An exact completed job on this run
                # must confirm that the suffix is actually installed. Ordinary
                # (non-restore) receipts retain their established semantics.
                if known and restored:
                    known=db.execute("SELECT 1 FROM jobs WHERE kind='source_command' "
                        "AND state='done' AND run_id=? AND json_extract(identity,'$.operation_key')=? "
                        "AND json_extract(identity,'$.command_sha256')=? LIMIT 1",
                        (binding.run_id,json.loads(row['command_bytes'])['operation_key'],row['command_sha256'])).fetchone() is not None
                if row['sequence']==applied+1 and known:applied=row['sequence']
                if not known:pending.append((bytes(row['command_bytes']),row['outcome_bytes'],False))
                dependency=json.loads(row['dependency_bytes'])
                if dependency['scope_class']!='public_source' or row['route'] in INACTIVE_SOURCE_ROUTES:continue
                source=row['source_id'];old=changes.get(source)
                changes[source]=SourceChange(source,row['sequence'],row['command_sha256'],
                    bool(dependency['restriction'] or old and old.restriction),
                    bool(row['read_pending'] or old and old.pending),bool(row['read_unknown'] or old and old.unknown))
        return applied,tuple(pending),tuple(changes[k]for k in sorted(changes))

    def _advance_source_acceptance(self,command,predecessor,seal):
        if self._source_anchor is None or command.sequence<=self._source_covered_sequence:return
        if ((predecessor.reference if predecessor else None)!=self._source_head
                or command.sequence!=self._source_covered_sequence+1
                or seal.command_hashes!=(_hash(command.encode()),)
                or seal.manifest.last_sequence!=command.sequence):
            self.invalidate_source_history();return
        self._source_head=seal.manifest;self._source_covered_sequence=command.sequence
        self._source_version=object()

    def facts_current(self,facts):
        """Synchronous final issuance fence; perform no await after success."""
        self._active()
        return (type(facts) is SourceReadFacts and facts._owner is self._facts_owner
            and facts._anchor is self._source_anchor and facts._version is self._source_version
            and not self._writing and facts.history_complete and self._source_anchor is not None
            and self._source_anchor.current())

    def _validate(self,command,database_generation):
        _bounded(database_generation)
        if not isinstance(command,ExactCommand):raise TypeError('resolved exact command required')
        _bounded(command.operation_key,4096)
        frozen,data=_snapshot(command,self.max_command_bytes)
        handler=self.registry.resolve(frozen)
        if frozen.schema_revision=='rkb-v4-sql001-014-v1':
            from .kaggle_native_source_bindings import native_source_handler,validate_native_source_command
            expected=native_source_handler(frozen.route,check_sha256=handler.check.definition_sha256,
                apply_sha256=handler.apply.definition_sha256)
            if handler!=expected:raise SpoolDenied('exact reviewed native source handler required')
            validate_native_source_command(frozen)
            return frozen,data,handler
        if frozen.schema_revision in ('rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
            from .kaggle_domain_bindings import current_handler,validate_current_command
            from .kaggle_apply import Revisions
            expected=current_handler(frozen.route,check_sha256=handler.check.definition_sha256,
                apply_sha256=handler.apply.definition_sha256,
                revisions=Revisions(frozen.schema_revision,frozen.function_revision,frozen.executor_revision))
            if handler!=expected:raise SpoolDenied('only exact reviewed fresh11 handler supported')
            validate_current_command(frozen)
            return frozen,data,handler
        if frozen.route.startswith('disagreement.'):
            from .disagreement_native import case_handler,validate_case_command
            expected=case_handler(frozen.route,check_sha256=handler.check.definition_sha256,
                apply_sha256=handler.apply.definition_sha256)
            if handler!=expected:raise SpoolDenied('only exact installed case handler supported')
            validate_case_command(frozen)
            return frozen,data,handler
        if frozen.route=='geo.manifest.install.v1':
            from .geo_manifest_commands import geo_manifest_handler,validate_geo_manifest_command
            from .kaggle_apply import Revisions
            expected=geo_manifest_handler(revisions=Revisions(frozen.schema_revision,frozen.function_revision,frozen.executor_revision),check_sha256=handler.check.definition_sha256,
                apply_sha256=handler.apply.definition_sha256)
            if handler!=expected:raise SpoolDenied('only exact installed geographic handler supported')
            validate_geo_manifest_command(frozen)
            return frozen,data,handler
        if frozen.route in ('story.edit.v1','story.access.v1'):
            from .story_editorial import (STORY_EDIT,STORY_EDIT_DEPENDENCY,STORY_ACCESS_DEPENDENCY,
                EDIT_PRECONDITIONS,ACCESS_PRECONDITIONS,validate_story_edit_command,validate_story_access_command,
                editorial_handler)
            edit=frozen.route==STORY_EDIT
            expected=editorial_handler(route=frozen.route,check_sha256=handler.check.definition_sha256,
                apply_sha256=handler.apply.definition_sha256)
            if handler!=expected:raise SpoolDenied('only exact installed editorial handler supported')
            (validate_story_edit_command if edit else validate_story_access_command)(frozen)
            return frozen,data,handler
        if frozen.route=='source.import.v4':
            from .source_import_v4 import validate_import_v4_command,IMPORT_V4_DEPENDENCY
            from .source_import import IMPORT_PRECONDITIONS
            if handler.dependency!=IMPORT_V4_DEPENDENCY or handler.precondition_keys!=IMPORT_PRECONDITIONS:
                raise SpoolDenied('only exact installed fresh9 import supported')
            validate_import_v4_command(frozen)
            return frozen,data,handler
        if frozen.route in ('source.import.v1','source.import.v2','source.import.v3'):
            if frozen.route=='source.import.v1':
                from .source_import import SOURCE_IMPORT,IMPORT_DEPENDENCY,IMPORT_PRECONDITIONS,validate_import_command
            elif frozen.route=='source.import.v2':
                from .source_import import IMPORT_PRECONDITIONS
                from .source_import_v2 import (SOURCE_IMPORT_V2 as SOURCE_IMPORT,
                    IMPORT_V2_DEPENDENCY as IMPORT_DEPENDENCY,validate_import_v2_command as validate_import_command)
            else:
                from .source_import import IMPORT_PRECONDITIONS
                from .source_import_v3 import (SOURCE_IMPORT_V3 as SOURCE_IMPORT,
                    IMPORT_V3_DEPENDENCY as IMPORT_DEPENDENCY,validate_import_v3_command as validate_import_command)
            if (frozen.kind!='domain_command' or frozen.route!=SOURCE_IMPORT
                    or handler.dependency!=IMPORT_DEPENDENCY
                    or handler.precondition_keys!=IMPORT_PRECONDITIONS):
                raise SpoolDenied('only pinned canonical source import intent is supported')
            _uuid(frozen.person_id);_uuid(frozen.ids.get('source_id'))
            validate_import_command(frozen)
            return frozen,data,handler
        if (frozen.kind!='domain_command' or frozen.route!=SOURCE_WITHDRAW
                or handler.dependency!=WITHDRAW_DEPENDENCY
                or handler.precondition_keys!=frozenset({'eligibility_revision'})
                or set(frozen.ids)!={'source_id'} or set(frozen.expected_revisions)!={'eligibility_revision'}
                or frozen.payload!=WITHDRAW_PAYLOAD):
            raise SpoolDenied('only static source.withdraw.v1 control intent is supported')
        _uuid(frozen.person_id);_uuid(frozen.ids['source_id'])
        revision=frozen.expected_revisions['eligibility_revision']
        if (not revision.isascii() or not revision.isdecimal() or not 1<=len(revision)<=19
                or str(int(revision))!=revision or not 1<=int(revision)<2**63):
            raise ValueError('exact positive source eligibility revision required')
        return frozen,data,handler

    @staticmethod
    def _resource_key(command,dependency):
        if dependency is None:
            raise SpoolDenied('classified typed dependency required')
        if not dependency.resource_keys<=command.ids.keys():raise SpoolDenied('complete typed resource IDs required')
        if dependency.scope_class=='public_source':return _uuid(command.ids['source_id'])
        if dependency.scope_class=='case':return 'case:'+_uuid(command.ids['case_id'])
        if dependency.scope_class=='private_draft':
            return 'draft:'+_uuid(command.person_id)+':'+_uuid(command.ids['case_id'])+':'+_uuid(command.ids['object_id'])
        if dependency.scope_class=='geo_manifest':
            value=command.ids['manifest_id'];_bounded(value,2048)
            return 'geo:'+_uuid(command.person_id)+':'+value
        raise SpoolDenied('independent domain scope unavailable')

    def _stage(self,command,data,handler,database_generation):
        dependency=_json(None if handler.dependency is None else handler.dependency.wire())
        # Reserve post-acceptance outcome and bounded native receipt/head/seal
        # metadata now, so budget exhaustion cannot strand a newly accepted op.
        reserved=len(data)+len(dependency)+self.max_outcome_bytes+32768
        with self._db(write=True) as db:
            prior=db.execute('SELECT * FROM rkb_control_operations WHERE operation_key=?',(command.operation_key,)).fetchone()
            if prior is not None:
                if (bytes(prior['command_bytes'])!=data or prior['database_generation']!=database_generation
                        or bytes(prior['dependency_bytes'])!=dependency):
                    raise SpoolConflict('operation key has different exact command/scope/database bytes')
                return self._entry(prior)
            used=db.execute('SELECT used_bytes FROM rkb_control_spool_meta WHERE id=1').fetchone()[0]
            if used+reserved>self.max_bytes:raise SpoolFull('control spool budget exhausted before remote acceptance')
            if db.execute('SELECT 1 FROM rkb_control_operations WHERE sequence=?',(command.sequence,)).fetchone():
                raise SpoolConflict('another canonical local operation occupies this sequence')
            now=self.clock()
            db.execute('''INSERT INTO rkb_control_operations(operation_key,command_bytes,command_sha256,sequence,
                source_id,person_id,database_generation,dependency_bytes,handler_sha256,reserved_bytes,created,updated,
                seal_state,apply_state,outcome_state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'NOT_SENT','NOT_SENT','NOT_SENT')''',
                (command.operation_key,data,_hash(data),command.sequence,self._resource_key(command,handler.dependency),command.person_id,
                 database_generation,dependency,handler.fingerprint,reserved,now,now))
            db.execute('''UPDATE rkb_control_spool_meta SET used_bytes=used_bytes+?,sequence=max(sequence,?),
                operation_count=operation_count+1,pending_count=pending_count+1 WHERE id=1''',(reserved,command.sequence))
            return self._entry(db.execute('SELECT * FROM rkb_control_operations WHERE operation_key=?',(command.operation_key,)).fetchone())

    async def stage_source_withdraw(self,context,command,*,database_generation):
        self._active()
        # The same intake lock protects native empty-genesis proof and history
        # restore. No canonical intent can appear during checkpoint publication.
        async with self._lock:
            actor=await self._authorize(context)
            frozen,data,handler=self._validate(command,database_generation)
            if frozen.route!=SOURCE_WITHDRAW:raise SpoolDenied('withdrawal intake requires withdrawal route')
            if frozen.person_id!=actor:raise SpoolDenied('command person does not authenticate its actor')
            return await self._write(self._stage,frozen,data,handler,database_generation)

    async def stage_source_import(self,context,command,*,database_generation):
        """Exact already-resolved import bytes; no local inference or raw seed path."""
        self._active()
        async with self._lock:
            actor=await self._authorize(context)
            frozen,data,handler=self._validate(command,database_generation)
            if frozen.route not in ('source.import.v1','source.import.v2','source.import.v3','source.import.v4'):raise SpoolDenied('import intake requires import route')
            if frozen.person_id!=actor:raise SpoolDenied('command person does not authenticate its actor')
            return await self._write(self._stage,frozen,data,handler,database_generation)

    async def stage_story_editorial(self,context,command,*,database_generation):
        self._active()
        async with self._lock:
            frozen,data,handler=self._validate(command,database_generation)
            if frozen.route not in ('story.edit.v1','story.access.v1'):
                raise SpoolDenied('editorial intake requires an exact editorial route')
            await self.authorize_command(context,frozen)
            return await self._write(self._stage,frozen,data,handler,database_generation)

    async def stage_domain_command(self,context,command,*,database_generation):
        """Fixed installed domain validators; no caller-selected scope or authority."""
        self._active()
        async with self._lock:
            frozen,data,handler=self._validate(command,database_generation)
            if frozen.route not in ('story.edit.v1','story.access.v1','disagreement.edit.v1',
                    'disagreement.access.v1','disagreement.draft.v1','disagreement.publish.v1','geo.manifest.install.v1','geo.dependencies.upgrade.v2'):
                raise SpoolDenied('domain intake requires a fixed installed domain route')
            await self.authorize_command(context,frozen)
            return await self._write(self._stage,frozen,data,handler,database_generation)

    def _row(self,key):
        _bounded(key,4096)
        with self._db() as db:
            row=db.execute('SELECT * FROM rkb_control_operations WHERE operation_key=?',(key,)).fetchone()
            if row is None:raise SpoolError('control operation not found')
            return dict(row)

    def _entry(self,row):
        data=bytes(row['command_bytes'])
        if _hash(data)!=row['command_sha256']:raise SpoolConflict('stored command hash differs')
        command=_command(data)
        current=self.registry.handlers.get(command.route)
        historical=(current is None or row['handler_sha256']!=current.fingerprint)
        if historical and row['apply_bytes'] is not None and row['apply_state'] in {'APPLIED','REJECTED_PRECONDITION'}:
            from .kaggle_source_commands import read_historical_contract
            command,handler,dependency=read_historical_contract(data,self.registry)
        else:
            _,_,handler=self._validate(command,row['database_generation'])
            dependency=handler.dependency
            if row['handler_sha256']!=handler.fingerprint:
                raise SpoolConflict('stored original handler identity differs')
        if (command.operation_key!=row['operation_key'] or command.sequence!=row['sequence']
                or command.person_id!=row['person_id'] or self._resource_key(command,dependency)!=row['source_id']):
            raise SpoolConflict('stored operation identity differs')
        if bytes(row['dependency_bytes'])!=_json(None if dependency is None else dependency.wire()):
            raise SpoolConflict('stored original dependency identity differs')
        apply=None;remote=None;accepted=None
        if row['seal_bytes'] is not None:accepted=_seal(row['seal_bytes'])
        if row['apply_bytes'] is not None:
            value=json.loads(row['apply_bytes'])
            outcome=bytes(row['outcome_bytes'])
            if _hash(outcome)!=row['outcome_sha256']:raise SpoolConflict('stored apply outcome differs')
            apply=ApplyReceipt(**value,outcome=outcome,duplicate=True)
        if row['remote_receipt_bytes'] is not None:remote=RemoteOutcomeReceipt(**json.loads(row['remote_receipt_bytes']))
        if historical:
            from .kaggle_source_commands import verify_historical_receipt
            if apply is None or not verify_historical_receipt(command,row['handler_sha256'],apply):
                raise SpoolConflict('stored historical handler receipt differs')
        if ((row['seal_state']=='ACCEPTED')!=(accepted is not None)
                or accepted is not None and (accepted.command_hashes!=(row['command_sha256'],)
                    or accepted.manifest.last_sequence!=command.sequence)):
            raise SpoolConflict('stored acceptance phase/identity differs')
        if ((row['apply_state'] in {'APPLIED','REJECTED_PRECONDITION'})!=(apply is not None)
                or apply is not None and ((apply.operation_key,apply.sequence,apply.command_sha256,apply.database_generation)
                    !=(command.operation_key,command.sequence,row['command_sha256'],row['database_generation'])
                    or apply.status!=('applied' if row['apply_state']=='APPLIED' else 'rejected_precondition'))):
            raise SpoolConflict('stored apply phase/identity differs')
        if ((row['outcome_state']=='DURABLE')!=(remote is not None)
                or remote is not None and ((remote.operation_key,remote.command_sha256,remote.database_generation,remote.outcome_sha256)
                    !=(command.operation_key,row['command_sha256'],row['database_generation'],row['outcome_sha256'])
                    or remote.key!=self.journal.prefix+'/outcomes/'+_hash(command.operation_key.encode()))):
            raise SpoolConflict('stored remote outcome phase/identity differs')
        return ControlOperation(data,row['command_sha256'],row['database_generation'],row['handler_sha256'],
            row['seal_state'],row['apply_state'],row['outcome_state'],accepted,apply,remote)

    async def get(self,key):
        self._active()
        return self._entry(await _database(self._row,key))

    def _before_seal(self,key,predecessor):
        with self._db(write=True) as db:
            row=db.execute('SELECT * FROM rkb_control_operations WHERE operation_key=?',(key,)).fetchone()
            if row is None:raise SpoolError('control operation not found')
            prior=row['predecessor_bytes']
            if predecessor is _UNSET:
                if prior is None:raise ValueError('first seal requires an explicit verified predecessor')
                data=bytes(prior)
            else:
                if predecessor is not None and not isinstance(predecessor,VerifiedHead):raise TypeError('verified predecessor required')
                data=_json(_head_wire(predecessor))
                if len(data)>8192:raise SpoolFull('predecessor exceeds reserved metadata budget')
                if prior is not None and bytes(prior)!=data:raise SpoolConflict('seal predecessor is immutable across retry')
            if row['seal_state']!='ACCEPTED':
                db.execute("UPDATE rkb_control_operations SET seal_state='SENT_UNKNOWN',predecessor_bytes=?,updated=? WHERE operation_key=?",(data,self.clock(),key))
                if row['seal_state']=='NOT_SENT':db.execute('UPDATE rkb_control_spool_meta SET unknown_count=unknown_count+1 WHERE id=1')
            return self._entry(row),_head(data)

    def _accepted(self,key,seal):
        data=_json(_seal_wire(seal))
        if len(data)>8192:raise SpoolFull('seal exceeds reserved metadata budget')
        with self._db(write=True) as db:
            row=db.execute('SELECT command_sha256,sequence,seal_bytes,seal_state FROM rkb_control_operations WHERE operation_key=?',(key,)).fetchone()
            if seal.command_hashes!=(row['command_sha256'],) or seal.manifest.last_sequence!=row['sequence']:
                raise SpoolConflict('seal does not bind the exact singleton command')
            if row['seal_bytes'] is not None and bytes(row['seal_bytes'])!=data:raise SpoolConflict('accepted seal identity differs')
            db.execute("UPDATE rkb_control_operations SET seal_state='ACCEPTED',seal_bytes=?,updated=? WHERE operation_key=?",(data,self.clock(),key))
            db.execute('UPDATE rkb_control_spool_meta SET accepted_sequence=max(accepted_sequence,?) WHERE id=1',(row['sequence'],))
            if row['seal_state']=='SENT_UNKNOWN':db.execute('UPDATE rkb_control_spool_meta SET unknown_count=unknown_count-1 WHERE id=1')

    async def seal(self,context,key,*,writer_generation,predecessor=_UNSET):
        self._active();_bounded(writer_generation,4096)
        async with self._lock:
            existing=await self.get(key)
            if existing.accepted is not None and existing.apply_receipt is not None:return existing.accepted
            # Unapplied commands still require the current executable contract.
            _,_,handler=self._validate(existing.command,existing.database_generation)
            if handler.fingerprint!=existing.handler_sha256:raise SpoolConflict('staged static handler changed')
            if existing.accepted is None and existing.seal_state=='SENT_UNKNOWN':
                reconciled=await self.journal.locate_accepted(existing.command)
                if reconciled is not None:
                    await self._write(self._accepted,key,reconciled)
                    prior=await _database(self._row,key)
                    self._advance_source_acceptance(existing.command,_head(prior['predecessor_bytes']),reconciled)
                    return reconciled
            if existing.accepted is None:
                # Waiting for another remote operation must not carry stale
                # pre-lock permission into a new acceptance boundary.
                actor=await self.authorize_command(context,existing.command)
                if existing.command.person_id!=actor:raise SpoolDenied('control intent belongs to another actor')
            # Accepted commands retain original author/semantics. Outer current
            # receipt/output authorization is independent of internal replay.
            entry,head=await self._write(self._before_seal,key,predecessor)
            if entry.accepted is not None:return entry.accepted
            accepted=await self.journal.seal([entry.command],predecessor=head,writer_generation=writer_generation)
            await self._write(self._accepted,key,accepted)
            self._advance_source_acceptance(entry.command,head,accepted)
            _LOG.info('control_intent_remote_accepted operation_hash=%s sequence=%s',_hash(key.encode()),entry.command.sequence)
            return accepted

    def _apply_sent(self,key):
        with self._db(write=True) as db:
            row=db.execute('SELECT seal_state,apply_state FROM rkb_control_operations WHERE operation_key=?',(key,)).fetchone()
            if not row or row['seal_state']!='ACCEPTED':raise SpoolDenied('remote accepted intent required before apply')
            if row['apply_state'] in {'NOT_SENT','SENT_UNKNOWN'}:
                db.execute("UPDATE rkb_control_operations SET apply_state='SENT_UNKNOWN',updated=? WHERE operation_key=?",(self.clock(),key))
                if row['apply_state']=='NOT_SENT':db.execute('UPDATE rkb_control_spool_meta SET unknown_count=unknown_count+1 WHERE id=1')

    async def mark_apply_sent(self,key):
        self._active();await self._write(self._apply_sent,key)

    def _record_apply(self,key,receipt,delivery=None):
        if not isinstance(receipt,ApplyReceipt) or receipt.status not in {'applied','rejected_precondition'}:
            raise TypeError('exact native apply receipt required')
        if not isinstance(receipt.outcome,bytes) or len(receipt.outcome)>self.max_outcome_bytes or _hash(receipt.outcome)!=receipt.outcome_sha256:
            raise SpoolConflict('apply outcome integrity/budget differs')
        values={k:getattr(receipt,k) for k in ('database_generation','operation_key','sequence','command_sha256','status','outcome_sha256')}
        data=_json(values)
        with self._db(write=True) as db:
            row=db.execute('SELECT * FROM rkb_control_operations WHERE operation_key=?',(key,)).fetchone()
            if not row or row['seal_state']!='ACCEPTED' or (receipt.operation_key,receipt.sequence,receipt.command_sha256,receipt.database_generation)!=(key,row['sequence'],row['command_sha256'],row['database_generation']):
                raise SpoolConflict('apply receipt does not bind the original accepted operation/database')
            if row['apply_bytes'] is not None and (bytes(row['apply_bytes'])!=data or bytes(row['outcome_bytes'])!=receipt.outcome):
                raise SpoolConflict('exact committed outcome differs')
            if delivery is not None:
                job_id,run_id,incarnation,claim=delivery
                job=db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
                if (job is None or job['kind']!='source_command' or job['run_id']!=run_id
                        or job['claim']!=claim or job['state'] not in ('claimed','done')
                        or json.loads(job['identity'])['command_sha256']!=receipt.command_sha256):
                    raise SpoolConflict('native receipt differs from current delivery')
                prior=db.execute('SELECT run_id,incarnation,claim,command_sha256 '
                    'FROM rkb_control_delivery_receipts WHERE job_id=? AND claim=?',(job_id,claim)).fetchone()
                evidence=(run_id,incarnation,claim,receipt.command_sha256)
                if prior is not None and tuple(prior)!=evidence:
                    raise SpoolConflict('native delivery observation changed')
                db.execute('INSERT OR IGNORE INTO rkb_control_delivery_receipts VALUES(?,?,?,?,?)',
                    (job_id,*evidence))
            db.execute('''UPDATE rkb_control_operations SET apply_state=?,apply_bytes=?,outcome_bytes=?,outcome_sha256=?,updated=?
                WHERE operation_key=?''',('APPLIED' if receipt.status=='applied' else 'REJECTED_PRECONDITION',data,receipt.outcome,receipt.outcome_sha256,self.clock(),key))
            if row['apply_state']=='SENT_UNKNOWN':db.execute('UPDATE rkb_control_spool_meta SET unknown_count=unknown_count-1 WHERE id=1')

    async def record_apply(self,key,receipt,*,delivery=None):
        self._active();await self._write(self._record_apply,key,receipt,delivery)

    async def apply(self,key,connection,*,writer_generation):
        self._active()
        async with self._lock:
            entry=await self.get(key)
            if entry.apply_receipt is not None:return entry.apply_receipt
            await self.mark_apply_sent(key)
            receipt=await apply_accepted(connection,journal=self.journal,seal=entry.accepted,
                command=entry.command,registry=self.registry,
                database_generation=entry.database_generation,writer_generation=writer_generation,
                max_command_bytes=self.max_command_bytes,max_outcome_bytes=self.max_outcome_bytes)
            await self.record_apply(key,receipt)
            return receipt

    def _outcome_sent(self,key):
        with self._db(write=True) as db:
            row=db.execute('SELECT apply_state,outcome_state FROM rkb_control_operations WHERE operation_key=?',(key,)).fetchone()
            if not row or row['apply_state'] not in {'APPLIED','REJECTED_PRECONDITION'}:raise SpoolDenied('known committed/rejected PG outcome required')
            if row['outcome_state']!='DURABLE':
                db.execute("UPDATE rkb_control_operations SET outcome_state='SENT_UNKNOWN',updated=? WHERE operation_key=?",(self.clock(),key))
                if row['outcome_state']=='NOT_SENT':db.execute('UPDATE rkb_control_spool_meta SET unknown_count=unknown_count+1 WHERE id=1')

    def _durable(self,key,receipt):
        data=_json(receipt.__dict__)
        if len(data)>8192:raise SpoolFull('remote receipt exceeds reserved metadata budget')
        with self._db(write=True) as db:
            row=db.execute('SELECT * FROM rkb_control_operations WHERE operation_key=?',(key,)).fetchone()
            if (receipt.operation_key,receipt.command_sha256,receipt.outcome_sha256,receipt.database_generation)!=(key,row['command_sha256'],row['outcome_sha256'],row['database_generation']):
                raise SpoolConflict('remote outcome receipt differs')
            prior=db.execute('SELECT receipt_bytes FROM rkb_control_receipts WHERE operation_key=?',(key,)).fetchone()
            if prior is not None and bytes(prior[0])!=data:raise SpoolConflict('compact terminal receipt differs')
            db.execute('INSERT OR IGNORE INTO rkb_control_receipts VALUES(?,?,?)',(key,row['command_sha256'],data))
            db.execute("UPDATE rkb_control_operations SET outcome_state='DURABLE',remote_receipt_bytes=?,updated=? WHERE operation_key=?",(data,self.clock(),key))
            if row['outcome_state']!='DURABLE':
                db.execute('UPDATE rkb_control_spool_meta SET pending_count=pending_count-1,unknown_count=unknown_count-? WHERE id=1',(int(row['outcome_state']=='SENT_UNKNOWN'),))

    def _settle_recovered_jobs(self,key,covered=None):
        """Only an exact locally recorded apply + remote durable outcome settles jobs.

        Called after physical restore verification, while intake is excluded.
        Original actor, accepted command, writer identity and attempt stay intact.
        """
        with self._db(write=True) as db:
            row=db.execute('SELECT * FROM rkb_control_operations WHERE operation_key=?',(key,)).fetchone()
            if covered is None:
                if (row is None or row['seal_state']!='ACCEPTED' or row['apply_bytes'] is None
                        or row['outcome_state']!='DURABLE' or row['remote_receipt_bytes'] is None):
                    raise SpoolConflict('verified recovered durable outcome required')
            else:
                if (type(covered) is not SourceCursor or self._source_anchor is not covered
                        or not covered.current() or row is None
                        or row['database_generation']!=covered.database_generation
                        or not 0<row['sequence']<=covered.applied_sequence
                        or row['seal_state']!='ACCEPTED'
                        or row['apply_state'] not in ('APPLIED','REJECTED_PRECONDITION')
                        or row['outcome_state']!='NOT_SENT' or row['apply_bytes'] is None
                        or row['remote_receipt_bytes'] is not None):
                    raise SpoolConflict('verified recovered native receipt and current cursor required')
                entry=self._entry(row)
                if entry.accepted is None or entry.apply_receipt is None or entry.remote_outcome is not None:
                    raise SpoolConflict('recovered exact native apply receipt required')
            result=json.dumps(dict(command_sha256=row['command_sha256'],
                remote_outcome_durable=covered is None),sort_keys=True)
            settled=[]
            for job in db.execute("select * from jobs where kind='source_command' and json_extract(identity,'$.operation_key')=?",(key,)).fetchall():
                if covered is not None and (job['state']!='pending' or job['attempt']<1 or job['result'] is not None):
                    continue  # A fresh/in-flight/unknown claim is never settled by a restored receipt.
                identity=json.loads(job['identity'])
                if (job['actor']!=row['person_id'] or identity.get('command_sha256')!=row['command_sha256']
                        or identity.get('database_generation')!=row['database_generation']
                        or identity.get('sequence')!=row['sequence']
                        or covered is not None and job['idempotency']!='source-command:'+key):
                    raise SpoolConflict('recovered job differs from original accepted command')
                if job['state']=='done' and job['result']!=result:
                    pending_result=json.dumps(dict(command_sha256=row['command_sha256'],remote_outcome_durable=False),sort_keys=True)
                    if job['result']!=pending_result:
                        raise SpoolConflict('recovered job already has a different durable result')
                db.execute("update jobs set state='done',result=?,updated=? where id=?",(result,self.clock(),job['id']))
                settled.append(job['id'])
            return settled

    async def publish_outcome(self,key,*,writer_generation):
        self._active()
        async with self._lock:
            entry=await self.get(key)
            if entry.remote_outcome is not None:return entry.remote_outcome
            await self._write(self._outcome_sent,key)
            try:
                receipt=await self.journal.record_outcome(entry.accepted,command=entry.command,
                    database_generation=entry.database_generation,outcome=entry.apply_receipt.outcome,
                    writer_generation=writer_generation)
            except JournalUnknown as exc:
                if exc.args==('outcome_pending_dataset_checkpoint',):
                    from .dataset_recovery_store import DatasetRecoveryStore
                    if type(self.journal.store) is DatasetRecoveryStore:
                        def known_pending():
                            with self._db(write=True) as db:
                                changed=db.execute("UPDATE rkb_control_operations SET outcome_state='NOT_SENT',updated=? "
                                    "WHERE operation_key=? AND outcome_state='SENT_UNKNOWN'",(self.clock(),key)).rowcount
                                if changed:db.execute('UPDATE rkb_control_spool_meta SET unknown_count=unknown_count-1 WHERE id=1')
                        await self._write(known_pending)
                raise
            await self._write(self._durable,key,receipt)
            return receipt

    @staticmethod
    def _barrier(row,accepted_row=None,pending_sequence=None,unknown_sequence=None):
        restriction=bool(json.loads(row['dependency_bytes'])['restriction'])
        if accepted_row is None and row['seal_state']=='ACCEPTED' and restriction:accepted_row=row
        accepted=(AcceptedRestriction(accepted_row['operation_key'],accepted_row['sequence'],accepted_row['command_sha256'],
            accepted_row['apply_state'],accepted_row['outcome_state']) if accepted_row is not None else None)
        return SourceBarrier(row['operation_key'],row['source_id'],row['sequence'],row['command_sha256'],
            row['database_generation'],row['seal_state']=='ACCEPTED',row['apply_state'],row['outcome_state'],accepted,
            (row['sequence'] if row['outcome_state']!='DURABLE' else 0) if pending_sequence is None else pending_sequence,
            (row['sequence'] if 'SENT_UNKNOWN' in (row['seal_state'],row['apply_state'],row['outcome_state']) else 0) if unknown_sequence is None else unknown_sequence,
            restriction or accepted is not None)

    def _barriers_for(self,sources):
        with self._db() as db:
            facts=[]
            for source in sources:
                row=db.execute('''SELECT operation_key,source_id,sequence,command_sha256,database_generation,
                    seal_state,apply_state,outcome_state,dependency_bytes FROM rkb_control_operations
                    WHERE source_id=? ORDER BY sequence DESC,operation_key LIMIT 1''',(source,)).fetchone()
                if row is not None:
                    accepted=db.execute('''SELECT operation_key,sequence,command_sha256,apply_state,outcome_state
                        FROM rkb_control_operations WHERE source_id=? AND seal_state='ACCEPTED'
                        AND json_extract(CAST(dependency_bytes AS TEXT),'$.restriction')=1
                        ORDER BY sequence DESC,operation_key LIMIT 1''',(source,)).fetchone()
                    pending=db.execute("SELECT sequence FROM rkb_control_operations WHERE source_id=? AND outcome_state<>'DURABLE' ORDER BY sequence DESC LIMIT 1",(source,)).fetchone()
                    unknown=db.execute("SELECT sequence FROM rkb_control_operations WHERE source_id=? AND (seal_state='SENT_UNKNOWN' OR apply_state='SENT_UNKNOWN' OR outcome_state='SENT_UNKNOWN') ORDER BY sequence DESC LIMIT 1",(source,)).fetchone()
                    facts.append(self._barrier(row,accepted,pending[0] if pending else 0,unknown[0] if unknown else 0))
            meta=db.execute('SELECT sequence,accepted_sequence FROM rkb_control_spool_meta WHERE id=1').fetchone()
            return BarrierSnapshot(tuple(facts),meta['sequence'],meta['accepted_sequence'])

    async def barriers_for(self,source_ids):
        self._active()
        if not isinstance(source_ids,(tuple,list)) or len(source_ids)>160 or len(set(source_ids))!=len(source_ids):
            raise ValueError('bounded exhaustive top-K source union required')
        sources=tuple(_uuid(source) for source in source_ids)
        return await _database(self._barriers_for,sources)

    def _scope_state(self,sources):
        with self._db() as db:
            if sources is None:
                row=db.execute('SELECT sequence,accepted_sequence,operation_count,pending_count,unknown_count FROM rkb_control_spool_meta WHERE id=1').fetchone()
            elif not sources:
                row={'sequence':0,'accepted_sequence':0,'operation_count':0,'pending_count':0,'unknown_count':0}
            else:
                slots=','.join('?' for _ in sources)
                row=db.execute(f'''SELECT coalesce(max(sequence),0) AS sequence,
                    coalesce(max(CASE WHEN seal_state='ACCEPTED' THEN sequence ELSE 0 END),0) AS accepted_sequence,
                    count(*) AS operation_count,
                    coalesce(sum(outcome_state<>'DURABLE'),0) AS pending_count,
                    coalesce(sum(seal_state='SENT_UNKNOWN' OR apply_state='SENT_UNKNOWN' OR outcome_state='SENT_UNKNOWN'),0) AS unknown_count
                    FROM rkb_control_operations INDEXED BY rkb_control_source_state WHERE source_id IN ({slots})''',sources).fetchone()
            return {**dict(row),'pending':bool(row['pending_count']),'unknown':bool(row['unknown_count']),
                    'history_complete':False,'scope_complete':False,'scope_class':'public_source'}

    async def scope_state(self,source_ids=None):
        """Query scope, including no-hit/zero-item results; never infer absence
        from the top-K barrier list. Whole-scope counts use atomic metadata;
        finite scope aggregates use the covering source/state index.
        """
        self._active()
        if source_ids is not None:
            if not isinstance(source_ids,(tuple,list)) or len(source_ids)>32 or len(set(source_ids))!=len(source_ids):
                raise ValueError('bounded finite query source scope required')
            source_ids=tuple(_uuid(source) for source in source_ids)
        return await _database(self._scope_state,source_ids)

    def _replace_source_heads(self,changes):
        """Recovery only: replace compact derived heads after verified genesis replay."""
        with self._db(write=True) as db:
            old=db.execute('SELECT coalesce(sum(reserved_bytes),0) FROM rkb_control_source_heads').fetchone()[0]
            sizes=[256+len(c.source_id.encode()) for c in changes]
            used=db.execute('SELECT used_bytes FROM rkb_control_spool_meta WHERE id=1').fetchone()[0]
            if used-old+sum(sizes)>self.max_bytes:raise SpoolFull('verified source tail exceeds control budget; checkpoint required')
            db.execute('DELETE FROM rkb_control_source_heads')
            db.executemany('INSERT INTO rkb_control_source_heads VALUES(?,?,?,?,?,?,?)',
                [(c.source_id,c.sequence,c.command_sha256,int(c.restriction),int(c.pending),int(c.unknown),n)
                 for c,n in zip(changes,sizes)])
            db.execute('UPDATE rkb_control_spool_meta SET used_bytes=used_bytes-?+? WHERE id=1',(old,sum(sizes)))
            db.execute('''UPDATE rkb_control_source_head_meta SET
                sequence=(SELECT coalesce(max(sequence),0) FROM rkb_control_source_heads),
                pending_count=(SELECT coalesce(sum(pending),0) FROM rkb_control_source_heads),
                unknown_count=(SELECT coalesce(sum(unknown),0) FROM rkb_control_source_heads) WHERE id=1''')

    def _read_facts(self,sources,items,anchor,version,covered):
        with self._db() as db:
            def scope(table,columns):
                where='' if sources is None else (' WHERE source_id IN ('+','.join('?' for _ in sources)+')')
                if sources==():return (0,0,0)
                return db.execute('SELECT '+columns+' FROM '+table+where,() if sources is None else sources).fetchone()
            if sources is None:
                local=db.execute('SELECT sequence,pending_count,unknown_count FROM rkb_control_public_source_meta WHERE id=1').fetchone()
                history=db.execute('SELECT sequence,pending_count,unknown_count FROM rkb_control_source_head_meta WHERE id=1').fetchone()
            else:
                local=scope('rkb_control_operations',f"coalesce(max(sequence),0),coalesce(sum({_read_pending_sql()}),0),coalesce(sum({_read_unknown_sql()}),0)")
                history=scope('rkb_control_source_heads','coalesce(max(sequence),0),coalesce(sum(pending),0),coalesce(sum(unknown),0)')
            changes=[]
            for source in items:
                hist=db.execute('SELECT sequence,command_sha256,restriction,pending,unknown FROM rkb_control_source_heads WHERE source_id=?',(source,)).fetchone()
                local_head=db.execute(f"""SELECT sequence,command_sha256,
                    EXISTS(SELECT 1 FROM rkb_control_operations restrictions WHERE restrictions.source_id=?
                        AND json_extract(CAST(restrictions.dependency_bytes AS TEXT),'$.restriction')=1),{_read_pending_sql()},
                    {_read_unknown_sql()}
                    FROM rkb_control_operations WHERE source_id=? ORDER BY sequence DESC LIMIT 1""",(source,source)).fetchone()
                rows=[r for r in (hist,local_head) if r is not None]
                if rows:
                    latest=max(rows,key=lambda r:r[0])
                    changes.append(SourceChange(source,latest[0],latest[1],any(r[2] for r in rows),
                        any(r[3] for r in rows),any(r[4] for r in rows)))
            return SourceReadFacts(getattr(anchor,'database_generation',None),0,covered,
                max(local[0],history[0]),bool(local[1] or history[1]),bool(local[2] or history[2]),
                anchor is not None,tuple(changes),self._facts_owner,anchor,version)

    async def read_facts(self,query_sources,item_sources=()):
        """Atomic query-scope + exhaustive top-K facts, never a cloud/read write."""
        self._active()
        if query_sources is not None:
            if not isinstance(query_sources,(tuple,list)) or not 1<=len(query_sources)<=32 or len(set(query_sources))!=len(query_sources):
                raise ValueError('bounded nonempty query source scope required')
            query_sources=tuple(_uuid(s) for s in query_sources)
        if not isinstance(item_sources,(tuple,list)) or len(item_sources)>160 or len(set(item_sources))!=len(item_sources):
            raise ValueError('bounded exhaustive item source union required')
        item_sources=tuple(_uuid(s) for s in item_sources)
        return await _database(self._read_facts,query_sources,item_sources,self._source_anchor,
            self._source_version,self._source_covered_sequence)

    def _pending_page(self,after_key,limit):
        with self._db() as db:
            rows=db.execute('''SELECT operation_key,source_id,sequence,command_sha256,database_generation,
                seal_state,apply_state,outcome_state,dependency_bytes FROM rkb_control_operations WHERE operation_key>?
                AND (seal_state<>'ACCEPTED' OR apply_state NOT IN ('APPLIED','REJECTED_PRECONDITION') OR outcome_state<>'DURABLE')
                ORDER BY operation_key LIMIT ?''',(after_key,limit+1)).fetchall()
            return {'items':tuple(self._barrier(row) for row in rows[:limit]),'has_more':len(rows)>limit,
                    'next_after_key':rows[limit-1]['operation_key'] if len(rows)>limit else None,
                    'history_complete':False,'scope_complete':False}

    async def pending_page(self,*,after_key='',limit=100):
        self._active()
        if not isinstance(after_key,str) or len(after_key)>4096 or type(limit) is not int or not 1<=limit<=160:
            raise ValueError('bounded pending page required')
        return await _database(self._pending_page,after_key,limit)

    def close(self):
        if self._writing or self._lock.locked():raise SpoolError('drain active control operations before shutdown')
        if self._fd is not None:os.close(self._fd);self._fd=None
