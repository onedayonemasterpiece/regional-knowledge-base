"""Existing control journal staged locally between versioned Dataset checkpoints.

This is pending control state in the existing queue, never remote persistence.
Only exact bytes covered by an independently verified Dataset version qualify
for a remote outcome receipt. PG archives stay in Kaggle and are not read here.
"""
from __future__ import annotations

import base64
import hashlib
from io import BytesIO
import json
import secrets

from .kaggle_dataset_base import DatasetCheckpoint
from .recovery_journal import StoredObject, JournalConflict, JournalLimit

CONTROL_PAGE_SIZE = 16


class DatasetRecoveryStore:
    def __init__(self, queue, *, checkpoint, manifest_ref, publisher, writer_guard, stream):
        if type(checkpoint) is not DatasetCheckpoint:
            raise TypeError('typed versioned Dataset checkpoint required')
        self.queue,self.checkpoint,self.publisher,self.writer_guard=queue,checkpoint,publisher,writer_guard
        self.prefix='recovery/v1/'+hashlib.sha256(stream.encode()).hexdigest()+'/'
        self.storage_identity=hashlib.sha256(json.dumps(['kaggle-dataset',checkpoint.owner_slug,
            checkpoint.dataset_slug,self.prefix],separators=(',',':')).encode()).hexdigest()
        with queue.connect() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_dataset_recovery_objects(
                storage TEXT NOT NULL,key TEXT NOT NULL,data BLOB NOT NULL,token TEXT NOT NULL,
                covered_version INTEGER,PRIMARY KEY(storage,key))''')
            db.execute('''CREATE TABLE IF NOT EXISTS rkb_dataset_recovery_store(
                storage TEXT PRIMARY KEY,origin_storage TEXT NOT NULL,checkpoint_json TEXT NOT NULL,latest_json TEXT)''')
            columns={r[1] for r in db.execute('PRAGMA table_info(rkb_dataset_recovery_store)')}
            if 'latest_json' not in columns:db.execute('ALTER TABLE rkb_dataset_recovery_store ADD COLUMN latest_json TEXT')
            existing=db.execute('SELECT * FROM rkb_dataset_recovery_store WHERE storage=?',(self.storage_identity,)).fetchone()
            previous=db.execute('SELECT storage_sha256 FROM rkb_writer_control WHERE id=1').fetchone()
        self.origin_storage_identity=existing['origin_storage'] if existing is not None else previous['storage_sha256']
        if existing is not None:
            if json.loads(existing['checkpoint_json'])!=checkpoint.wire():
                if not existing['latest_json'] or json.loads(existing['latest_json'])!=checkpoint.wire():
                    raise JournalConflict('dataset_checkpoint_change_requires_verified_promotion')
                with queue.connect() as db:
                    db.execute('UPDATE rkb_dataset_recovery_store SET checkpoint_json=? WHERE storage=?',
                        (json.dumps(checkpoint.wire(),sort_keys=True,separators=(',',':')),self.storage_identity))
        else:
            journal_bytes=publisher.read_control(checkpoint.journal,version=checkpoint.dataset_version)
            control_bytes=publisher.read_control(checkpoint.control,version=checkpoint.dataset_version)
            manifest_bytes=publisher.read_control(checkpoint.manifest,version=checkpoint.dataset_version)
            self._import(checkpoint,manifest_ref,journal_bytes,control_bytes,manifest_bytes)
        writer_guard.bind_store(self)

    def _key(self,key):
        if (type(key) is not str or not key.startswith(self.prefix) or len(key)>2048
                or any(part in ('','.','..') for part in key.split('/'))):
            raise ValueError('Dataset journal object outside fixed stream')

    def _import(self,checkpoint,manifest_ref,journal_bytes,control_bytes,manifest_bytes):
        manifest=json.loads(manifest_bytes);journal=json.loads(journal_bytes)
        if (journal.get('codec')!='rkb.dataset-journal.v1' or type(journal.get('objects')) is not list
                or manifest_ref.sha256!=checkpoint.manifest.sha256
                or manifest_ref.length!=len(manifest_bytes) or manifest_ref.last_sequence!=checkpoint.base_sequence
                or manifest['source_control']['sha256']!=hashlib.sha256(control_bytes).hexdigest()):
            raise JournalConflict('dataset_control_metadata_binding_changed')
        objects={}
        for item in journal['objects']:
            self._key(item['key'])
            if item['key'] in objects or set(item)!={'key','data_b64','token'}:
                raise JournalConflict('dataset_control_object_duplicated')
            raw=base64.b64decode(item['data_b64'],validate=True)
            if len(raw)>1024*1024 or type(item['token']) is not str or not item['token']:
                raise JournalLimit('dataset_control_object_budget')
            objects[item['key']]=(raw,item['token'])
        objects[manifest_ref.key]=(manifest_bytes,'dataset:'+str(checkpoint.dataset_version)+':manifest')
        objects[manifest['source_control']['key']]=(control_bytes,'dataset:'+str(checkpoint.dataset_version)+':control')
        for key in objects:self._key(key)
        with self.queue.connect() as db:
            for key,(raw,token) in objects.items():
                db.execute('INSERT INTO rkb_dataset_recovery_objects VALUES(?,?,?,?,?)',
                    (self.storage_identity,key,raw,token,checkpoint.dataset_version))
            db.execute('INSERT INTO rkb_dataset_recovery_store(storage,origin_storage,checkpoint_json) VALUES(?,?,?)',
                (self.storage_identity,self.origin_storage_identity,json.dumps(checkpoint.wire(),sort_keys=True,separators=(',',':'))))

    @property
    def latest_checkpoint(self):
        with self.queue.connect(write=False) as db:
            row=db.execute('SELECT checkpoint_json,latest_json FROM rkb_dataset_recovery_store WHERE storage=?',
                (self.storage_identity,)).fetchone()
        return DatasetCheckpoint.from_wire(json.loads(row['latest_json'] or row['checkpoint_json']))

    def export_control(self):
        # One SQLite read snapshot, read in bounded pages and key order.
        raw=BytesIO()
        raw.write(b'{"codec":"rkb.dataset-journal.v1","objects":[')
        count=0
        with self.queue.connect(write=False) as db:
            rows=db.execute('SELECT key,data,token FROM rkb_dataset_recovery_objects WHERE storage=? '
                'AND key NOT LIKE ? ORDER BY key',
                (self.storage_identity,self.prefix+'snapshots/%'))
            while page:=rows.fetchmany(CONTROL_PAGE_SIZE):
                for row in page:
                    if len(row['data'])>1024*1024:raise JournalLimit('dataset_control_object_budget')
                    record=json.dumps(dict(key=row['key'],token=row['token'],
                        data_b64=base64.b64encode(row['data']).decode()),sort_keys=True,separators=(',',':')).encode()
                    if count:raw.write(b',')
                    raw.write(record);count+=1
        raw.write(b']}')
        return raw.getvalue()

    def pending_control(self):
        """Exact pending control revision for the existing grouped owner.

        Tokens change on every CAS, including a same-byte rewrite. Only small
        metadata is read here; source commands still use their native cursor.
        """
        digest=hashlib.sha256();count=0
        with self.queue.connect(write=False) as db:
            rows=db.execute('SELECT key,token FROM rkb_dataset_recovery_objects WHERE storage=? '
                'AND key>=? AND key<? AND covered_version IS NULL ORDER BY key',
                (self.storage_identity,self.prefix+'control/',self.prefix+'control0'))
            while page:=rows.fetchmany(CONTROL_PAGE_SIZE):
                for row in page:
                    count+=1
                    digest.update(json.dumps([row['key'],row['token']],separators=(',',':')).encode())
        return digest.hexdigest() if count else None

    def promote(self,checkpoint,native_readback,*,manifest_ref,current=None):
        expected=dict(dataset_ref=checkpoint.owner_slug+'/'+checkpoint.dataset_slug,dataset_version=checkpoint.dataset_version,
            base_sequence=checkpoint.base_sequence,archive_sha256=checkpoint.archive.sha256,archive_size=checkpoint.archive.size)
        if native_readback!=expected:raise JournalConflict('dataset_native_snapshot_readback_changed')
        old=self.latest_checkpoint
        if checkpoint==old:return  # Lost promotion ACK: never cover later local writes.
        if (checkpoint.dataset_version<=old.dataset_version or checkpoint.base_sequence<old.base_sequence
                or checkpoint.database_generation!=old.database_generation
                or (checkpoint.owner_slug,checkpoint.dataset_slug)!=(old.owner_slug,old.dataset_slug)):
            raise JournalConflict('dataset_checkpoint_promotion_predecessor')
        journal=self.publisher.read_control(checkpoint.journal,version=checkpoint.dataset_version)
        manifest=self.publisher.read_control(checkpoint.manifest,version=checkpoint.dataset_version)
        control=self.publisher.read_control(checkpoint.control,version=checkpoint.dataset_version)
        parsed=json.loads(journal);m=json.loads(manifest)
        c=json.loads(control)
        if (parsed.get('codec')!='rkb.dataset-journal.v1'
                or type(parsed.get('objects')) is not list
                or (manifest_ref.sha256,manifest_ref.length,manifest_ref.last_sequence)!=(
                    checkpoint.manifest.sha256,checkpoint.manifest.size,checkpoint.base_sequence)
                or (m.get('database_generation'),m.get('applied_sequence'))!=(
                    checkpoint.database_generation,checkpoint.base_sequence)
                or (m.get('blob',{}).get('sha256'),m.get('blob',{}).get('size'))!=(
                    checkpoint.archive.sha256,checkpoint.archive.size)
                or (c.get('database_generation'),c.get('sequence'))!=(
                    checkpoint.database_generation,checkpoint.base_sequence)
                or hashlib.sha256(control).hexdigest()!=m['source_control']['sha256']):
            raise JournalConflict('dataset_checkpoint_promoted_control_changed')
        with self.queue.connect() as db:
            # Fence the local pointer and exact coverage in one transaction.
            # A writer retirement during remote readback must not promote, and
            # a competing promotion must not be overwritten by a stale caller.
            if not db.in_transaction:db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT checkpoint_json,latest_json FROM rkb_dataset_recovery_store WHERE storage=?',
                (self.storage_identity,)).fetchone()
            if json.loads(row['latest_json'] or row['checkpoint_json'])!=old.wire():
                raise JournalConflict('dataset_checkpoint_promotion_predecessor')
            if current is not None and not current():
                raise JournalConflict('dataset_checkpoint_writer_changed')
            for item in parsed['objects']:
                self._key(item['key']);raw=base64.b64decode(item['data_b64'],validate=True)
                # New writes may have advanced after capture; only exact bytes
                # contained in this saved version gain a persistence receipt.
                db.execute('UPDATE rkb_dataset_recovery_objects SET covered_version=? '
                    'WHERE storage=? AND key=? AND data=? AND token=?',
                    (checkpoint.dataset_version,self.storage_identity,item['key'],raw,item['token']))
            for key,data,tag in ((manifest_ref.key,manifest,'manifest'),(m['source_control']['key'],control,'control')):
                self._key(key)
                db.execute('INSERT INTO rkb_dataset_recovery_objects VALUES(?,?,?,?,?) '
                    'ON CONFLICT(storage,key) DO NOTHING',(self.storage_identity,key,data,
                        f'dataset:{checkpoint.dataset_version}:{tag}',checkpoint.dataset_version))
            db.execute('UPDATE rkb_dataset_recovery_store SET latest_json=? WHERE storage=?',
                (json.dumps(checkpoint.wire(),sort_keys=True,separators=(',',':')),self.storage_identity))

    def allows_storage_transition(self,previous):
        if previous!=self.origin_storage_identity:return False
        # The old authority is changed only in the explicit recovery acquisition;
        # normal writes remain unavailable until native Dataset restore verifies.
        with self.queue.connect(write=False) as db:
            row=db.execute("SELECT package_json,version FROM rkb_dataset_seed WHERE state='published' AND provider_ref=?",
                (self.publisher.ref,)).fetchone()
        if row is None or row['version']!=self.checkpoint.dataset_version:return False
        package=json.loads(row['package_json'])
        return (package['manifest_sha256']==self.checkpoint.manifest.sha256
            and package['database_generation']==self.checkpoint.database_generation
            and package['base_sequence']==self.checkpoint.base_sequence)

    async def read(self,key,*,max_bytes):
        self._key(key)
        with self.queue.connect(write=False) as db:
            row=db.execute('SELECT data,token FROM rkb_dataset_recovery_objects WHERE storage=? AND key=?',
                (self.storage_identity,key)).fetchone()
        if row is None:return None
        if len(row['data'])>max_bytes:raise JournalLimit('dataset_control_read_budget')
        return StoredObject(bytes(row['data']),row['token'])

    def _put(self,key,data,expected_token,*,immutable=False):
        self._key(key)
        if type(data) is not bytes or len(data)>1024*1024:raise JournalLimit('dataset_pending_object_budget')
        with self.queue.connect() as db:
            row=db.execute('SELECT token FROM rkb_dataset_recovery_objects WHERE storage=? AND key=?',
                (self.storage_identity,key)).fetchone()
            if (row is not None if immutable else (row['token'] if row else None)!=expected_token):return False
            size=db.execute('SELECT coalesce(sum(length(data)),0) FROM rkb_dataset_recovery_objects WHERE storage=? AND covered_version IS NULL',
                (self.storage_identity,)).fetchone()[0]
            if size+len(data)>512*1024**2:raise JournalLimit('dataset_pending_control_spool_budget')
            # Opaque SQLite row version. Never described as a Kaggle receipt.
            token='pending:'+secrets.token_hex(24)
            db.execute('INSERT INTO rkb_dataset_recovery_objects VALUES(?,?,?,?,NULL) '
                'ON CONFLICT(storage,key) DO UPDATE SET data=excluded.data,token=excluded.token,covered_version=NULL',
                (self.storage_identity,key,data,token))
        return True

    async def put_if_absent(self,key,data):
        return self._put(key,data,None,immutable=True)

    async def compare_and_swap(self,key,data,*,expected_token,writer_generation):
        async with self.writer_guard(writer_generation):
            return self._put(key,data,expected_token)

    def pending_outcomes(self):
        with self.queue.connect(write=False) as db:
            return db.execute('SELECT 1 FROM rkb_dataset_recovery_objects WHERE storage=? '
                'AND covered_version IS NULL AND key LIKE ? LIMIT 1',
                (self.storage_identity,self.prefix+'outcomes/%')).fetchone() is not None

    def remote_durable(self,key,data):
        with self.queue.connect(write=False) as db:
            row=db.execute('SELECT data,covered_version FROM rkb_dataset_recovery_objects WHERE storage=? AND key=?',
                (self.storage_identity,key)).fetchone()
        return row is not None and row['covered_version'] is not None and bytes(row['data'])==data


class DatasetSnapshotTransport:
    def __init__(self,store):
        if type(store) is not DatasetRecoveryStore:raise TypeError('versioned Dataset recovery store required')
        self.store=store

    def matches(self,blob):
        pin=self.store.checkpoint.archive
        return (blob.key.rsplit('/',1)[-1],blob.size,blob.sha256)==(pin.name,pin.size,pin.sha256)

    async def verify(self,blob):
        if not self.matches(blob):raise JournalConflict('dataset_snapshot_blob_changed')
        # Native scoped download and PG prepare independently verify archive bytes.
