"""Fixed opt-in GEO bootstrap using the existing controller's retained owners."""
import asyncio
from dataclasses import dataclass,replace
from datetime import datetime,timezone
import json
import hashlib
import logging
import os
from pathlib import Path
import stat

from .control_spool import SpoolError,SpoolConflict,_database
from .geo_inventory import GeoInventoryObservation,geo_watermark
from .geo_migration import GeoMigrationManifest,canonical
from .geo_manifest_commands import plan_geo_manifest,geo_dependency_limit_rejection,geo_dependency_repair_commands

_LOG=logging.getLogger(__name__)


@dataclass(frozen=True)
class SourceGeoInstallConfig:
    manifest_path:str
    manifest_sha256:str
    legacy_source_path:str

    def __post_init__(self):
        if (type(self.manifest_path) is not str or not Path(self.manifest_path).is_absolute()
                or type(self.legacy_source_path) is not str or not Path(self.legacy_source_path).is_absolute()
                or type(self.manifest_sha256) is not str or len(self.manifest_sha256)!=64
                or set(self.manifest_sha256)-set('0123456789abcdef')):
            raise ValueError('private GEO manifest path and exact canonical SHA required')

    def wire(self):return dict(manifest_path=self.manifest_path,manifest_sha256=self.manifest_sha256,legacy_source_path=self.legacy_source_path)

    @classmethod
    def from_wire(cls,value):
        if type(value) is not dict or set(value)!=set(cls.__dataclass_fields__):raise ValueError('exact GEO bootstrap configuration required')
        return cls(**value)


def read_manifest(config):
    path=Path(config.manifest_path)
    if (path.resolve(strict=True)!=path or path.parent.stat().st_mode&0o077
            or path.parent.stat().st_uid!=os.geteuid()):
        raise ValueError('private nonsymlink GEO manifest parent required')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    try:
        info=os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o077
                or not 1<=info.st_size<=4*1024*1024+1):raise ValueError('private bounded GEO manifest required')
        with os.fdopen(fd,'rb',closefd=False) as stream:raw=stream.read(4*1024*1024+2)
    finally:os.close(fd)
    value=GeoMigrationManifest.from_wire(json.loads(raw)).require_ready()
    if (value.sha256!=config.manifest_sha256 or raw not in (canonical(value.wire()),canonical(value.wire())+b'\n')):
        raise ValueError('GEO manifest canonical bytes or SHA changed')
    return value


class SourceGeoInstaller:
    def __init__(self,startup,config):
        if type(config) is not SourceGeoInstallConfig:raise TypeError('typed fixed GEO bootstrap configuration required')
        self.startup,self.config=startup,config
        self.spool=startup.config.spool;self.queue=startup.config.queue
        if (self.spool.queue is not self.queue or startup.geo_gate is None or not startup.geo_gate.supported()
                or startup.config.writer_owner is None or
                startup.config.source_snapshot is None and startup.config.source_restore is None):
            raise ValueError('same fixed GEO/recovery/writer owners required')
        self.manifest=read_manifest(config);self.plan=plan_geo_manifest(self.manifest)
        startup.geo_gate._places=frozenset(item.stable_id for item in self.manifest.items if item.payload.kind=='place')
        self.keys=tuple('geo-bootstrap11:'+self.plan.manifest_id+':'+str(n) for n in range(len(self.plan.payloads)))
        self.effective_keys=self.keys
        if startup.config.geo_manifest_operation_keys!=self.keys:
            raise ValueError('exact ordered configured GEO bootstrap keys required')
        self.expected_watermark=geo_watermark(self.manifest)
        self.inventory=GeoInventoryObservation(config.legacy_source_path)
        self.task=None;self.complete=False;self.closed=False;self.commands=None;self.command_identity=None
        self.plan_json=canonical(dict(config=config.wire(),operation_keys=self.keys,
            index_sha256=self.plan.index_sha256,source_watermark=self.expected_watermark)).decode()
        with self.queue.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_geo_bootstrap(
                id INTEGER PRIMARY KEY CHECK(id=1),plan_json TEXT NOT NULL,database_generation TEXT NOT NULL,
                person_id TEXT NOT NULL,first_sequence INTEGER NOT NULL,logical_time TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('prepared','complete')))''')
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_geo_legacy_retirement(
                id INTEGER PRIMARY KEY CHECK(id=1),receipt_json TEXT NOT NULL)''')
            db.execute('''CREATE TRIGGER IF NOT EXISTS rkb_geo_retirement_immutable
                BEFORE UPDATE ON rkb_geo_legacy_retirement
                BEGIN SELECT RAISE(ABORT,'immutable legacy GEO cutover receipt'); END''')
            db.execute('''CREATE TRIGGER IF NOT EXISTS rkb_geo_retirement_nodelete
                BEFORE DELETE ON rkb_geo_legacy_retirement
                BEGIN SELECT RAISE(ABORT,'retained legacy GEO cutover receipt'); END''')
            row=db.execute('SELECT receipt_json FROM rkb_geo_legacy_retirement WHERE id=1').fetchone()
            self.retirement=json.loads(row[0]) if row else None
        if self.retirement is not None:self._validate_retirement(self.retirement)

    def current(self):
        return bool(not self.closed and self.complete and self.startup.reads_current()
            and (self._retirement_current() if self.retirement is not None else self.inventory.current(self.expected_watermark))
            and self.startup.geo_gate.installation_current(self.startup.binding,self.effective_keys))

    async def _dependency_repair(self,binding,commands,context):
        if self.startup.config.history_adapter.database.schema_version<12:return False
        outcomes={}
        for command in commands:
            entry=await self.spool.get(command.operation_key)
            if (entry.command_bytes!=command.encode() or entry.apply_receipt is None
                    or entry.outcome_state!='DURABLE' and not self._locally_applied_checkpoint_pending(entry)):
                return False
            outcomes[command.operation_key]=entry.apply_receipt.status
        from .kaggle_native_source_bindings import new_native_command
        repair=tuple(new_native_command(command) for command in
            geo_dependency_repair_commands(commands,settled_outcomes=outcomes))
        for command in repair:
            try:entry=await self.spool.get(command.operation_key)
            except SpoolError as exc:
                if type(exc) is not SpoolError:raise
                await self.spool.stage_domain_command(context,command,database_generation=binding.database_generation)
                entry=await self.spool.get(command.operation_key)
            if entry.command_bytes!=command.encode():raise SpoolConflict('retained geographic repair intent changed')
            if entry.accepted is None:
                if entry.seal_state=='NOT_SENT':
                    await self.spool.seal(context,command.operation_key,writer_generation=self.startup.config.writer_generation,
                        predecessor=await self.spool.journal.head())
                else:
                    await self.spool.seal(context,command.operation_key,writer_generation=self.startup.config.writer_generation)
                entry=await self.spool.get(command.operation_key)
            if entry.apply_state=='REJECTED_PRECONDITION':raise SpoolConflict('geographic repair native precondition rejected')
            if entry.apply_state!='APPLIED' or (entry.outcome_state!='DURABLE'
                    and not self._locally_applied_checkpoint_pending(entry)):
                await self.startup.source_dispatch.enqueue(context,command.operation_key,
                    writer_generation=self.startup.config.writer_generation)
                return False
        rejected=[command for command in commands if outcomes[command.operation_key]=='rejected_precondition']
        replacements={old.operation_key:new.operation_key for old,new in zip(rejected,repair)}
        effective=tuple(replacements.get(key,key) for key in self.keys)
        await self.startup.geo_gate.establish(effective,binding=binding)
        self.effective_keys=effective
        with self.queue.connect() as db:
            db.execute("UPDATE rkb_geo_bootstrap SET state='complete' WHERE id=1 AND plan_json=?",(self.plan_json,))
        self.complete=True
        _LOG.info('geo_bootstrap state=repaired manifest_sha256=%s activation_sequence=%s',self.plan.manifest_id,repair[-1].sequence)
        return self.current()

    def _validate_retirement(self,receipt):
        expected=dict(codec='rkb.geo-legacy-retirement.v1',plan_sha256=hashlib.sha256(self.plan_json.encode()).hexdigest(),
            legacy_source_path=self.config.legacy_source_path,source_watermark=self.expected_watermark,
            manifest_sha256=self.manifest.sha256,operation_keys=list(self.keys),person_id=self.spool.participants.owner)
        if (type(receipt) is not dict or any(receipt.get(k)!=v for k,v in expected.items())
                or set(receipt)!=set(expected)|{'database_generation','writer_generation','run_id','logical_time',
                    'activation_sequence','activation_operation_key','activation_command_sha256','verified_applied_sequence','journal_head'}
                or type(receipt.get('activation_sequence')) is not int or receipt['activation_sequence']<1
                or type(receipt.get('verified_applied_sequence')) is not int
                or receipt['verified_applied_sequence']<receipt['activation_sequence']):
            raise SpoolConflict('retained legacy GEO cutover receipt differs')

    def _load_retirement(self):
        with self.queue.connect(write=False) as db:
            row=db.execute('SELECT receipt_json FROM rkb_geo_legacy_retirement WHERE id=1').fetchone()
            value=json.loads(row[0]) if row else None
        if value is not None:self._validate_retirement(value)
        self.retirement=value
        return value

    def _retirement_current(self):
        receipt=self.retirement;proof=self.startup.geo_gate._manifest
        try:
            self._validate_retirement(receipt)
            return bool(proof is not None and receipt['database_generation']==self.startup.binding.database_generation
                and (receipt['manifest_sha256'],receipt['activation_sequence'],receipt['activation_operation_key'],
                    receipt['activation_command_sha256'])==(proof.manifest_id,proof.activation_sequence,proof.operation,proof.command_sha256)
                and self.startup.geo_gate.installation_current(self.startup.binding,self.keys))
        except Exception:return False

    def _retire_legacy(self,context,binding,head):
        # The operator first quiesces the actual old writer processes. SQLite
        # exclusion proves the final logical point; no caller boolean is proof.
        import sqlite3
        path=Path(self.config.legacy_source_path)
        if path.resolve(strict=True)!=path or not path.is_file():raise SpoolConflict('explicit legacy source required')
        db=sqlite3.connect(path.as_uri()+'?mode=rw',uri=True,timeout=.25)
        try:
            db.execute('PRAGMA busy_timeout=250');db.execute('BEGIN IMMEDIATE')
            self.inventory.refresh(self.expected_watermark)
            if not self._source_ready(binding) or not self.startup.geo_gate.installation_current(binding,self.keys):
                raise SpoolConflict('current native GEO activation required')
            proof=self.startup.geo_gate._manifest
            if self.spool.participants.require(context,'import')!=self.spool.participants.owner:
                raise SpoolConflict('current canonical owner required')
            if not self.startup.config.writer_current(database_generation=binding.database_generation,
                    writer_generation=self.startup.config.writer_generation):raise SpoolConflict('current writer required')
            receipt=dict(codec='rkb.geo-legacy-retirement.v1',
                plan_sha256=hashlib.sha256(self.plan_json.encode()).hexdigest(),
                legacy_source_path=self.config.legacy_source_path,source_watermark=self.expected_watermark,
                manifest_sha256=self.manifest.sha256,operation_keys=list(self.keys),person_id=self.spool.participants.owner,
                database_generation=binding.database_generation,writer_generation=self.startup.config.writer_generation,
                run_id=binding.run_id,logical_time=datetime.now(timezone.utc).isoformat(),
                activation_sequence=proof.activation_sequence,activation_operation_key=proof.operation,
                activation_command_sha256=proof.command_sha256,
                verified_applied_sequence=self.startup.coverage.applied_sequence,
                journal_head=head.reference.wire())
            self._validate_retirement(receipt)
            with self.queue.connect() as queue_db:
                # Queue write exclusion also prevents a concurrent run-pointer
                # transition between the final writer check and receipt commit.
                if not self._source_ready(binding) or not self.startup.config.writer_current(
                        database_generation=binding.database_generation,writer_generation=self.startup.config.writer_generation):
                    raise SpoolConflict('current writer changed before legacy cutover commit')
                metadata=queue_db.execute('SELECT * FROM rkb_geo_bootstrap WHERE id=1').fetchone()
                if (metadata is None or metadata['state']!='complete' or metadata['plan_json']!=self.plan_json
                        or metadata['database_generation']!=binding.database_generation):raise SpoolConflict('completed exact GEO plan required')
                rows=queue_db.execute('SELECT operation_key,seal_state,apply_state,outcome_state FROM rkb_control_operations '
                    'WHERE operation_key IN ('+','.join('?' for _ in self.keys)+')',self.keys).fetchall()
                if len(rows)!=len(self.keys) or any(tuple(row)[1:]!=('ACCEPTED','APPLIED','DURABLE') for row in rows):
                    raise SpoolConflict('all exact GEO outcomes must be durable')
                queue_db.execute('INSERT INTO rkb_geo_legacy_retirement VALUES(1,?)',(canonical(receipt).decode(),))
            self.retirement=receipt
            self.inventory.close()
            return receipt
        finally:
            db.rollback();db.close()

    async def finalize_legacy(self,context):
        """LOCAL operator only, after quiescing actual legacy writer processes.

        Not exposed over MCP/HTTP. A receipt retires only the old-source watch;
        current native activation/history/owner/writer fences remain required.
        """
        binding=self.startup.binding
        async with self.spool._lock:
            await self.spool._authorize(context)
            await _database(self._load_retirement)
            if self.retirement is not None:
                if not self._retirement_current():raise SpoolConflict('retained native activation changed')
                return self.retirement
            if not self.current():raise SpoolConflict('completed current GEO migration required')
            async with self.startup.config.writer_owner(self.startup.config.writer_generation):
                head=await self.spool.journal.head()
                if head is None or head.reference.last_sequence<self.startup.geo_gate._manifest.activation_sequence:
                    raise SpoolConflict('exact accepted GEO prefix required')
                # Drain cancellation before releasing spool/writer/SQLite locks.
                receipt=await _database(self._retire_legacy,context,binding,head)
                self.retirement=receipt
                await asyncio.to_thread(self.inventory.close)
                return receipt

    def _context(self):
        policy=self.startup.config.oauth_provider.personal_policy
        if policy.participants is not self.startup.config.participants:raise SpoolConflict('GEO owner policy changed')
        return policy._context(policy.participants.owner,owner=True)

    def _metadata(self):
        with self.queue.connect(write=False) as db:
            row=db.execute('SELECT * FROM rkb_geo_bootstrap WHERE id=1').fetchone()
            return dict(row) if row else None

    def _prepare(self,binding,person):
        with self.queue.connect() as db:
            row=db.execute('SELECT * FROM rkb_geo_bootstrap WHERE id=1').fetchone()
            if row is None:
                db.execute("INSERT INTO rkb_geo_bootstrap VALUES(1,?,?,?,1,?,'prepared')",
                    (self.plan_json,binding.database_generation,person,datetime.now(timezone.utc).isoformat()))
                row=db.execute('SELECT * FROM rkb_geo_bootstrap WHERE id=1').fetchone()
            if (row['plan_json']!=self.plan_json or row['database_generation']!=binding.database_generation
                    or row['person_id']!=person):raise SpoolConflict('existing GEO bootstrap plan or owner changed')
            return dict(row)

    def _reserved(self,metadata):
        with self.queue.connect(write=False) as db:
            return [tuple(row) for row in db.execute('SELECT sequence,operation_key FROM rkb_control_operations '
                'WHERE sequence>=? AND sequence<? ORDER BY sequence',
                (metadata['first_sequence'],metadata['first_sequence']+len(self.keys)))]

    def _locally_applied_checkpoint_pending(self,entry):
        from .dataset_recovery_store import DatasetRecoveryStore
        return (type(self.spool.journal.store) is DatasetRecoveryStore
            and entry.accepted is not None and entry.apply_receipt is not None
            and entry.apply_state in ('APPLIED','REJECTED_PRECONDITION') and entry.outcome_state=='NOT_SENT')

    def _source_ready(self,binding):
        return self.startup.binding==binding and self.startup.reads_current()

    async def _reserve(self,binding,context,*,source_current):
        async with self.spool._lock:
            metadata=await asyncio.to_thread(self._metadata)
            if not self._source_ready(binding):raise SpoolConflict('GEO native startup coverage unavailable')
            if metadata is None:
                if not source_current:raise SpoolConflict('GEO source watermark changed')
                snapshot=self.startup.snapshot;coverage=self.startup.coverage
                if (snapshot is None or not snapshot.verified or coverage.accepted_sequence!=0
                        or coverage.applied_sequence!=0 or await self.spool.journal.head() is not None
                        or (await self.spool.scope_state())['operation_count']):
                    raise SpoolConflict('GEO requires verified empty cursor-zero snapshot before first intake')
            person=await asyncio.to_thread(self.spool.participants.require,context,'import')
            if metadata is None and not self.inventory.current(self.expected_watermark):raise SpoolConflict('GEO source observation expired')
            metadata=await asyncio.to_thread(self._prepare,binding,person)
            identity=(metadata['first_sequence'],metadata['person_id'],metadata['logical_time'])
            if self.command_identity!=identity:
                self.commands=await asyncio.to_thread(self.plan.commands,first_sequence=metadata['first_sequence'],
                    operation_key='geo-bootstrap11:'+self.plan.manifest_id,request_id='geo-bootstrap11:'+self.plan.manifest_id,
                    person_id=metadata['person_id'],logical_time=metadata['logical_time'])
                from .kaggle_native_source_bindings import new_native_command
                resolved=[]
                for original in self.commands:
                    try:retained=await self.spool.get(original.operation_key)
                    except SpoolError as exc:
                        if type(exc) is not SpoolError:raise
                        retained=None
                    if retained is None:resolved.append(new_native_command(original))
                    else:
                        # Compare the original intent without changing retained
                        # bytes. The three v1 audit fields are codec metadata.
                        view=replace(retained.command,schema_revision=original.schema_revision,
                            function_revision=original.function_revision,executor_revision=original.executor_revision)
                        if view.encode()!=original.encode():raise SpoolConflict('retained GEO intent differs')
                        resolved.append(retained.command)
                self.commands=tuple(resolved)
                self.command_identity=identity
            commands=self.commands
            for sequence,key in await asyncio.to_thread(self._reserved,metadata):
                if key!=self.keys[sequence-metadata['first_sequence']]:raise SpoolConflict('another command occupies GEO reserved sequence')
            for command in commands:
                try:entry=await self.spool.get(command.operation_key)
                except SpoolError as exc:
                    if type(exc) is not SpoolError:raise
                    entry=None
                if entry is not None:
                    if entry.command_bytes!=command.encode() or entry.database_generation!=binding.database_generation:
                        raise SpoolConflict('retained GEO command bytes changed')
                    continue
                if not source_current:return commands
                frozen,data,handler=await asyncio.to_thread(self.spool._validate,command,binding.database_generation)
                await self.spool.authorize_command(context,frozen)
                if not self._source_ready(binding) or not self.inventory.current(self.expected_watermark):
                    raise SpoolConflict('GEO reservation authority changed')
                await self.spool._write(self.spool._stage,frozen,data,handler,binding.database_generation)
            return commands

    async def advance(self,binding):
        """One bounded continuation. Every effect uses its original durable key."""
        if self.closed or not self._source_ready(binding):return False
        if self.current():return True
        await _database(self._load_retirement)
        if self.retirement is not None:
            if not self.startup.geo_gate.installation_current(binding,self.effective_keys):
                await self.startup.geo_gate.establish(self.effective_keys,binding=binding)
            self.complete=self._retirement_current()
            return self.current()
        async with asyncio.timeout(10):
            source_current=True
            try:await asyncio.to_thread(self.inventory.refresh,self.expected_watermark)
            except ValueError:
                self.complete=False;source_current=False
                _LOG.warning('geo_bootstrap state=source_changed reason=geo_source_watermark_changed manifest_sha256=%s',self.plan.manifest_id)
                if await asyncio.to_thread(self._metadata) is None:raise
            context=self._context()
            commands=await self._reserve(binding,context,source_current=source_current)
            dependency_repair_required=False
            for command in commands:
                try:entry=await self.spool.get(command.operation_key)
                except SpoolError as exc:
                    if type(exc) is not SpoolError:raise
                    return False
                if entry.command.encode()!=command.encode():raise SpoolConflict('retained GEO command bytes changed')
                if entry.apply_state=='REJECTED_PRECONDITION' and (entry.outcome_state=='DURABLE'
                        or self._locally_applied_checkpoint_pending(entry)):
                    if (geo_dependency_limit_rejection(command)
                            or dependency_repair_required and json.loads(command.payload)['action']=='activate'):
                        # Keep the exact rejection. Complete the original ordered
                        # plan before the versioned repair; never fabricate apply.
                        dependency_repair_required=True
                        continue
                    raise SpoolConflict('GEO native precondition rejected')
                if entry.accepted is None:
                    if not source_current or not self.inventory.current(self.expected_watermark):return False
                    if entry.seal_state=='NOT_SENT':
                        head=await self.spool.journal.head()
                        if (head.reference.last_sequence if head else 0)!=command.sequence-1:
                            raise SpoolConflict('GEO predecessor is not the exact accepted prefix')
                        await self.spool.seal(context,command.operation_key,
                            writer_generation=self.startup.config.writer_generation,predecessor=head)
                    else:
                        # UNKNOWN retains its original predecessor and identity.
                        await self.spool.seal(context,command.operation_key,
                            writer_generation=self.startup.config.writer_generation)
                    entry=await self.spool.get(command.operation_key)
                if entry.apply_state!='APPLIED' or (entry.outcome_state!='DURABLE'
                        and not self._locally_applied_checkpoint_pending(entry)):
                    await self.startup.source_dispatch.enqueue(context,command.operation_key,
                        writer_generation=self.startup.config.writer_generation)
                    _LOG.info('geo_bootstrap state=pending phase=%s command_sha256=%s',command.sequence,entry.command_sha256)
                    self.complete=False
                    return False
            if dependency_repair_required:
                self.complete=False
                return await self._dependency_repair(binding,commands,context)
            if not source_current:return False
            if not self.startup.geo_gate.installation_current(binding,self.keys):
                if not self.startup.geo_gate._history_current(binding,commands[-1].sequence):return False
                await self.startup.geo_gate.establish(self.keys,binding=binding)
            # A fresh relevant-source snapshot after remote outcomes and native
            # activation is required; initial file/ACK alone cannot open imports.
            await asyncio.to_thread(self.inventory.refresh,self.expected_watermark)
            if not self._source_ready(binding):return False
            with self.queue.connect() as db:
                db.execute("UPDATE rkb_geo_bootstrap SET state='complete' WHERE id=1 AND plan_json=?",(self.plan_json,))
            self.complete=True
            _LOG.info('geo_bootstrap state=durable manifest_sha256=%s phases=%s',self.plan.manifest_id,len(commands))
            return self.current()

    def kick(self,binding):
        if self.closed or self.task is not None and not self.task.done():return
        async def run():
            try:await self.advance(binding)
            except asyncio.CancelledError:raise
            except Exception as exc:
                self.complete=False
                _LOG.warning('geo_bootstrap state=pending reason=%s',type(exc).__name__)
        self.task=asyncio.create_task(run(),name='rkb-fixed-geo-bootstrap')

    async def aclose(self):
        self.closed=True;self.complete=False
        if self.task is not None:
            self.task.cancel();await asyncio.gather(self.task,return_exceptions=True)
        await asyncio.to_thread(self.inventory.close)
