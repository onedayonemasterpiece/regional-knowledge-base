"""Fixed bounded native observations for Dev's genesis/journal verifier.

Only receipt hashes and control metadata leave PG. A short owned transaction
holds source/applied-prefix locks while Dev checks immutable remote history.
There is no supplied SQL, database endpoint, user actor or storage credential.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
import json
import logging
import secrets
import time
from pathlib import Path
from uuid import UUID

from psycopg import Error as PgError
from psycopg.rows import tuple_row
from psycopg.pq import TransactionStatus

from .kaggle_database_probe import native_probe_for_schema
from .kaggle_source_acl import inspect_acl,NativeHistoryError
from .kaggle_domain_schema import TABLES_13,TABLES_14

_LOG=logging.getLogger(__name__)


def canonical(v):return json.dumps(v,sort_keys=True,ensure_ascii=False,allow_nan=False,separators=(',',':')).encode()
def digest(v):return hashlib.sha256(v).hexdigest()


def observer_fingerprint():
    """Exact fixed observation/ACL/catalog implementation, not a supplied pin."""
    root=Path(__file__).parent
    return digest(canonical({name:digest((root/name).read_bytes()) for name in (
        'kaggle_source_history_native.py','source_packet_tails.py','kaggle_source_acl.py','kaggle_database_probe.py','kaggle_domain_schema.py')}))


@dataclass(eq=False)
class _Session:
    lease:object
    connection:object
    transaction:object
    cursor:object
    binding:tuple
    nonce:str
    mode:str
    sequence:int
    old:object
    deadline:float
    empty:bool
    seen:int=0
    last:int=0
    exhausted:bool=False
    timer:object=None


class NativeSourceHistoryObserver:
    def __init__(self,*,acquire,database,identity,registry,current,lease_seconds=10,max_commands=10000,probe=None):
        if (not callable(acquire) or not callable(current) or type(lease_seconds) not in (int,float)
                or not 0<lease_seconds<=10 or type(max_commands) is not int or not 1<=max_commands<=10000):
            raise ValueError('bounded fixed native history owner required')
        self.acquire,self.database,self.identity,self.registry,self.current=acquire,database,identity,registry,current
        self.lease_seconds,self.max_commands=lease_seconds,max_commands
        self.probe=probe or native_probe_for_schema()
        self.code_hash=observer_fingerprint()
        self.registry_hash=digest(canonical(sorted(h.fingerprint for h in registry.handlers.values())))
        self._sessions={};self._prefixes={};self._lock=asyncio.Lock();self.generation=None

    def attached(self,generation):self.generation=generation
    def detached(self,generation):
        if self.generation==generation:self.generation=None
    def valid(self,binding):return binding[2]==self.generation and self.current(binding)

    async def __call__(self,frame):
        fields={'type','protocol_version','connection_generation','id','claim','kind','run_id',
            'launch_nonce','runtime_incarnation','database_generation','writer_generation','phase',
            'operation_key','request_sha256','arguments','compute_claim','dispatch_generation'}
        if (type(frame) is not dict or set(frame)!=fields or frame['type']!='DISPATCH' or frame['protocol_version']!=2
                or frame['kind']!='recovery_phase' or frame['phase']!='source_history'
                or tuple(frame[k] for k in ('run_id','launch_nonce','runtime_incarnation'))!=self.identity
                or (frame['database_generation'],frame['writer_generation'])!=(self.database.database_generation,self.database.writer_generation)
                or type(frame['connection_generation']) is not int or frame['connection_generation']<1
                or frame['connection_generation']!=frame['dispatch_generation']
                or any(type(frame[k]) is not str or not 1<=len(frame[k])<=256 for k in ('id','claim','compute_claim','operation_key'))
                or len(canonical(frame))>4*1024*1024):
            raise NativeHistoryError('native_history_dispatch_binding')
        binding=(frame['run_id'],frame['runtime_incarnation'],frame['connection_generation'])
        if not self.valid(binding):raise NativeHistoryError('native_history_attachment_changed')
        args=frame['arguments']
        if type(args) is not dict or frame['request_sha256']!=digest(canonical({'phase':'source_history','arguments':args})):
            raise NativeHistoryError('native_history_request_hash')
        try:
            async with self._lock:
                return await self._observe(frame,args,binding)
        except NativeHistoryError as exc:
            return {'phase':'source_history','request_sha256':frame['request_sha256'],'value':{'error':str(exc)}}
        except Exception as exc:
            _LOG.warning('native_history_observation state=unavailable run_id=%s generation=%s error_type=%s sqlstate=%s',
                self.identity[0],self.generation,type(exc).__name__,exc.sqlstate if isinstance(exc,PgError) else None)
            return {'phase':'source_history','request_sha256':frame['request_sha256'],'value':{'error':'native_history_observation_unavailable'}}

    async def _observe(self,frame,args,binding):
        action=args.get('action')
        if action=='open':value=await self._open(binding,args)
        elif action=='source_tails':
            if set(args)!={'action','source_id','ingestion_id','nonce'}:
                raise NativeHistoryError('native_history_arguments')
            for key in ('source_id','ingestion_id'):
                if str(UUID(args[key]))!=args[key]:raise NativeHistoryError('native_history_arguments')
            if type(args['nonce']) is not str or len(args['nonce'])!=32:
                raise NativeHistoryError('native_history_arguments')
            from .source_packet_tails import pending_tail_refs
            async with self.acquire() as connection:
                async with connection.transaction():
                    await connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                    await connection.execute("SET LOCAL statement_timeout='2s'")
                    value=await pending_tail_refs(connection,args['source_id'],args['ingestion_id'])
            if not self.valid(binding):raise NativeHistoryError('native_history_attachment_changed')
        elif action in ('page','close','abort'):
            session=self._sessions.get(args.get('session'))
            if session is None or session.binding!=binding or session.deadline<=time.monotonic():
                raise NativeHistoryError('native_history_session_expired')
            if action=='page':value=await self._page(session,args)
            elif action=='close':value=await self._close(session,args)
            else:
                if set(args)!={'action','session'}:raise NativeHistoryError('native_history_arguments')
                await self._release(session,abort=True);value={'aborted':True}
        else:raise NativeHistoryError('native_history_action_unsupported')
        if len(canonical(value))>120*1024:raise NativeHistoryError('native_history_response_bound')
        return {'phase':'source_history','request_sha256':frame['request_sha256'],'value':value}

    async def _open(self,binding,args):
        expected={'action','mode','nonce','schema_version','registry_sha256','observer_sha256',
            'routes','prefix','pending_sequences'}
        if (set(args)!=expected or args['mode'] not in ('full','delta','renew')
                or type(args['nonce']) is not str or len(args['nonce'])!=48
                or set(args['nonce'])-set('0123456789abcdef') or args['observer_sha256']!=self.code_hash
                or args['registry_sha256']!=self.registry_hash
                or type(args['routes']) is not list or args['routes']!=sorted(set(args['routes']))
                or not {'source.fetch','source.search','source.withdraw'}<=set(args['routes'])
                or set(args['routes'])-{'source.fetch','source.search','source.withdraw','source.import',
                    'story.get','story.history','story.search','story.variants','story.edit','story.access',
                    'source.calendar','source.fact_read','source.observations','source.observation_compare','source.catalog',
                    'case.read','case.edit','case.access','case.contribute','case.conclusion','geo.identity_read','geo.members_read','source.entity_read','source.entity_list'}
                or type(args['pending_sequences']) is not list or len(args['pending_sequences'])>self.max_commands
                or len(set(args['pending_sequences']))!=len(args['pending_sequences'])
                or any(type(i) is not int or i<1 for i in args['pending_sequences'])):
            raise NativeHistoryError('native_history_open_closure')
        if self._sessions:raise NativeHistoryError('native_history_session_busy')
        old=None
        if args['mode']!='full':
            old=self._prefixes.get(args['prefix'])
            if old is None or old['closure']!=[args[k] for k in ('schema_version','registry_sha256','observer_sha256','routes')]:
                raise NativeHistoryError('native_history_retained_prefix_required')
            if any(i>old['sequence'] for i in args['pending_sequences']):raise NativeHistoryError('native_history_pending_prefix')
        elif args['prefix'] is not None or args['pending_sequences']:
            raise NativeHistoryError('native_history_full_arguments')
        catalog,fields=await self.probe(acquire=self.acquire)
        if (fields['schema_version']!=args['schema_version']
                or fields['database_generation']!=self.database.database_generation):
            raise NativeHistoryError('native_history_schema_version_changed')
        lease=self.acquire();connection=await lease.__aenter__();tx=None
        try:
            if connection.closed or connection.row_factory is not tuple_row or connection.info.transaction_status!=TransactionStatus.IDLE:
                raise NativeHistoryError('native_history_connection_unavailable')
            tx=connection.transaction();await tx.__aenter__()
            await connection.execute("SET LOCAL statement_timeout='2000ms'")
            await connection.execute("SET LOCAL lock_timeout='1000ms'")
            await connection.execute("SET LOCAL idle_in_transaction_session_timeout='12000ms'")
            await connection.execute('SET LOCAL search_path=pg_catalog')
            await connection.execute('LOCK TABLE rkb_v4.sources,rkb_v4.revisions,rkb_v4.active_revision,'
                'rkb_v4.passage_search,rkb_v4.applied_ops,rkb_v4.recovery_state,rkb_v4.writer_generations IN SHARE MODE')
            if self.database.schema_version>=7:
                await connection.execute('LOCK TABLE rkb_v4.source_regions,rkb_v4.story_assertion_versions,'
                    'rkb_v4.stories,rkb_v4.story_versions,rkb_v4.story_variant_versions,rkb_v4.story_search,'
                    'rkb_v4.source_story_pending,rkb_v4.story_history_receipts IN SHARE MODE')
            if self.database.schema_version>=8:
                await connection.execute('LOCK TABLE rkb_v4.source_event_dates,rkb_v4.source_observations IN SHARE MODE')
            if self.database.schema_version>=11:
                await connection.execute('LOCK TABLE rkb_v4.source_graph_documents,rkb_v4.source_entity_ids,'
                    'rkb_v4.source_entity_representations,rkb_v4.source_entity_references,rkb_v4.source_entity_mentions,'
                    'rkb_v4.source_entity_aliases,rkb_v4.source_entity_relations,rkb_v4.source_entity_finding_links,'
                    'rkb_v4.source_poi_hypotheses,rkb_v4.source_graph_receipts IN SHARE MODE')
            if self.database.schema_version>=9:
                await connection.execute('LOCK TABLE rkb_v4.source_manifests,rkb_v4.source_catalog,rkb_v4.source_passage_provenance,rkb_v4.editorial_story_heads,'
                    'rkb_v4.editorial_story_versions,rkb_v4.editorial_variant_versions,rkb_v4.editorial_variant_heads,'
                    'rkb_v4.editorial_story_receipts,rkb_v4.story_editor_grants,rkb_v4.editorial_story_search IN SHARE MODE')
            if self.database.schema_version>=14:
                # Fixed deployment inventory only; no request-selected table or
                # SQL. Keep accepted model inputs/origins stable through close.
                await connection.execute('LOCK TABLE '+','.join('rkb_v4.'+name
                    for name in sorted(TABLES_13|TABLES_14))+' IN SHARE MODE')
            cursor=await connection.execute('SELECT CASE WHEN octet_length(s.database_generation)<=256 THEN s.database_generation END,CASE WHEN octet_length(s.writer_generation)<=256 THEN s.writer_generation END,s.applied_seq '
                'FROM rkb_v4.recovery_state s JOIN rkb_v4.writer_generations w ON w.generation=s.writer_generation '
                'WHERE s.singleton AND NOT w.retired LIMIT 2')
            states=await cursor.fetchall()
            if (len(states)!=1 or states[0][:2]!=(self.database.database_generation,self.database.writer_generation)
                    or fields['applied_seq']!=states[0][2] or type(states[0][2]) is not int
                    or states[0][2]<0 or old is not None and states[0][2]<old['sequence']
                    or args['mode']=='renew' and states[0][2]!=old['sequence']):
                raise NativeHistoryError('native_history_writer_prefix_changed')
            sequence=states[0][2]
            cursor=await connection.execute('SELECT EXISTS(SELECT 1 FROM rkb_v4.sources) OR '
                'EXISTS(SELECT 1 FROM rkb_v4.revisions) OR EXISTS(SELECT 1 FROM rkb_v4.active_revision) OR '
                'EXISTS(SELECT 1 FROM rkb_v4.passage_search) OR EXISTS(SELECT 1 FROM rkb_v4.applied_ops)')
            empty=await cursor.fetchone()==(False,)
            receipts=connection.cursor(name='rkb_remote_history_observations')
            start=0 if old is None else old['sequence']
            await receipts.execute('SELECT sequence,CASE WHEN octet_length(operation_key)<=256 THEN operation_key END,command_sha256,handler_sha256,accepted_manifest_sha256,'
                'status,outcome_sha256,encode(sha256(outcome_bytes),\'hex\'),octet_length(outcome_bytes),CASE WHEN octet_length(database_generation)<=256 THEN database_generation END,'
                'encode(sha256(command_bytes),\'hex\') FROM rkb_v4.applied_ops '
                'WHERE (sequence>%s OR sequence=ANY(%s::bigint[])) AND %s ORDER BY sequence LIMIT %s',
                (start,args['pending_sequences'],args['mode']!='renew',self.max_commands+1))
            session=_Session(lease,connection,tx,receipts,binding,args['nonce'],args['mode'],sequence,old,
                time.monotonic()+self.lease_seconds,empty)
            session.closure=[args[k] for k in ('schema_version','registry_sha256','observer_sha256','routes')]
            token=secrets.token_urlsafe(24);self._sessions[token]=session
            async def expiry():
                await asyncio.sleep(self.lease_seconds)
                async with self._lock:
                    if session in self._sessions.values():await self._release(session,abort=True)
            session.timer=asyncio.create_task(expiry())
            return dict(session=token,nonce=session.nonce,applied_sequence=sequence,empty=empty,
                database_generation=self.database.database_generation,writer_generation=self.database.writer_generation,
                schema_version=args['schema_version'],registry_sha256=self.registry_hash,
                observer_sha256=self.code_hash)
        except BaseException:
            if tx is not None:
                try:await tx.__aexit__(NativeHistoryError,NativeHistoryError('aborted'),None)
                finally:await lease.__aexit__(None,None,None)
            else:await lease.__aexit__(None,None,None)
            raise

    async def _page(self,session,args):
        if set(args)!={'action','session','after'} or args['after']!=session.last:
            raise NativeHistoryError('native_history_page_order')
        rows=await session.cursor.fetchmany(16)
        if len(rows)<16:session.exhausted=True
        for row in rows:
            session.seen+=1
            if session.seen>self.max_commands or type(row[0]) is not int or row[0]<=session.last or row[0]>session.sequence:
                raise NativeHistoryError('native_history_receipt_bound')
            session.last=row[0]
        if any(len(canonical(row))>4096 for row in rows):raise NativeHistoryError('native_history_receipt_metadata_bound')
        return dict(rows=rows,after=session.last,done=session.exhausted)

    async def _close(self,session,args):
        if (set(args)!={'action','session','nonce','origin_ids'} or args['nonce']!=session.nonce or not session.exhausted
                or type(args['origin_ids']) is not list or len(args['origin_ids'])>self.max_commands
                or len(set(args['origin_ids']))!=len(args['origin_ids'])
                or any(type(v) is not str or str(UUID(v))!=v for v in args['origin_ids'])):
            raise NativeHistoryError('native_history_close_arguments')
        if session.mode=='full':
            cursor=await session.connection.execute('SELECT EXISTS(SELECT 1 FROM rkb_v4.sources '
                'WHERE NOT (source_id=ANY(%s::uuid[])))',(args['origin_ids'],))
            if await cursor.fetchone()!=(False,):raise NativeHistoryError('native_history_source_origin_uncovered')
        if self.database.schema_version>=7:
            statement=('SELECT rkb_v4.verify_source_story_origins_v4(%s::uuid[])'
                if self.database.schema_version>=14 else
                'SELECT rkb_v4.verify_source_story_origins_v3(%s::uuid[])'
                if self.database.schema_version>=9 else
                'SELECT rkb_v4.verify_source_story_origins_v2(%s::uuid[])'
                if self.database.schema_version>=8 else
                'SELECT rkb_v4.verify_source_story_origins_v1(%s::uuid[])')
            cursor=await session.connection.execute(statement,(args['origin_ids'],))
            if await cursor.fetchone()!=(True,):raise NativeHistoryError('native_history_story_origin_uncovered')
            if self.database.schema_version>=11:
                statement=('SELECT rkb_v4.verify_source_graph_origins_v2(%s::uuid[])'
                    if self.database.schema_version>=14 else
                    'SELECT rkb_v4.verify_source_graph_origins_v1(%s::uuid[])')
                cursor=await session.connection.execute(statement,(args['origin_ids'],))
                if await cursor.fetchone()!=(True,):raise NativeHistoryError('native_history_graph_origin_uncovered')
        if not self.valid(session.binding):
            raise NativeHistoryError('native_history_closure_changed')
        cursor=await session.connection.execute('SELECT %s::text',(session.nonce,))
        if await cursor.fetchone()!=(session.nonce,):raise NativeHistoryError('native_history_nonce_changed')
        await self._release(session,abort=False)
        catalog,fields=await self.probe(acquire=self.acquire)
        if (fields['schema_version']!=session.closure[0] or fields['applied_seq']!=session.sequence
                or fields['database_generation']!=self.database.database_generation or not self.valid(session.binding)):
            raise NativeHistoryError('native_history_completion_changed')
        # Keep a single exact verified baseline. A supplied token never creates
        # a prefix, nor skips full verification on a new native owner/process.
        self._prefixes.clear();token=secrets.token_urlsafe(24)
        self._prefixes[token]={'sequence':session.sequence,'closure':session.closure}
        return dict(prefix=token,nonce=session.nonce,applied_sequence=session.sequence)

    async def _release(self,session,*,abort):
        for token,value in tuple(self._sessions.items()):
            if value is session:del self._sessions[token]
        if session.timer is not None and session.timer is not asyncio.current_task():session.timer.cancel()
        try:
            await session.cursor.close()
            await session.transaction.__aexit__(NativeHistoryError if abort else None,
                NativeHistoryError('aborted') if abort else None,None)
        finally:await session.lease.__aexit__(None,None,None)

    async def aclose(self):
        async with self._lock:
            for session in tuple(self._sessions.values()):await self._release(session,abort=True)
            self._prefixes.clear()
