"""Genesis-anchored control history from exact remote bytes and native PG facts.

Only this verifier retains the coverage capability. Serialized checkpoints are
evidence to verify, never permission. No non-genesis snapshot shortcut exists.
"""
from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
import hashlib
import json
import logging
import secrets
import time
from types import MappingProxyType

from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .control_spool import SourceChange, SpoolConflict, _command, _database
from .kaggle_database_probe import NativeDatabaseProbe
from .source_read_gate import binding_key

_LOG=logging.getLogger(__name__)


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, separators=(',', ':')).encode()


def _hash(data):
    return hashlib.sha256(data).hexdigest()


ACL_SQL = """SELECT kind, name, payload FROM (
 SELECT 'role' kind, rolname name, jsonb_build_array(rolsuper,rolinherit,rolcreaterole,
    rolcreatedb,rolcanlogin,rolreplication,rolbypassrls,rolconnlimit,rolvaliduntil)::text payload
 FROM pg_roles
 UNION ALL SELECT 'membership',r.rolname||':'||m.rolname,
    jsonb_build_array(a.admin_option,a.inherit_option,a.set_option)::text
 FROM pg_auth_members a JOIN pg_roles r ON r.oid=a.roleid JOIN pg_roles m ON m.oid=a.member
 UNION ALL SELECT 'schema',nspname,jsonb_build_array(pg_get_userbyid(nspowner),nspacl)::text
 FROM pg_namespace WHERE nspname IN ('rkb_v4','public')
 UNION ALL SELECT 'table',n.nspname||'.'||c.relname,
    jsonb_build_array(pg_get_userbyid(c.relowner),c.relacl)::text
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='rkb_v4'
 UNION ALL SELECT 'column',c.relname||'.'||a.attname,a.attacl::text
 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
 JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='rkb_v4' AND a.attacl IS NOT NULL
 UNION ALL SELECT 'function',p.oid::regprocedure::text,
    jsonb_build_array(pg_get_userbyid(p.proowner),p.proacl)::text
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='rkb_v4'
 UNION ALL SELECT 'default',d.defaclrole||':'||d.defaclnamespace||':'||d.defaclobjtype,d.defaclacl::text
 FROM pg_default_acl d
 UNION ALL SELECT 'database',datname,jsonb_build_array(pg_get_userbyid(datdba),datacl)::text
 FROM pg_database WHERE datname=current_database()
 UNION ALL SELECT 'setting',setdatabase||':'||setrole,setconfig::text FROM pg_db_role_setting
 ) q ORDER BY kind COLLATE "C",name COLLATE "C" LIMIT 513"""


async def inspect_acl(connection):
    """Safe, bounded role/ACL bytes for deployment pinning; never pg_authid."""
    async with connection.cursor() as cursor:
        # Guard individual values on the server before transferring bytes.
        await cursor.execute('SELECT kind,name,CASE WHEN octet_length(payload)<=8192 THEN payload END '
            'FROM (' + ACL_SQL + ') bounded')
        rows = await cursor.fetchall()
    if len(rows) > 512 or any(not isinstance(r[2], str) for r in rows):
        raise SpoolConflict('native role ACL metadata bound exceeded')
    data = _json(rows)
    if len(data) > 256 * 1024:
        raise SpoolConflict('native role ACL byte bound exceeded')
    roles = {r[1]: json.loads(r[2]) for r in rows if r[0] == 'role'}
    if not {'rkb_bootstrap', 'rkb_writer', 'rkb_reader'} <= roles.keys():
        raise SpoolConflict('native role closure missing')
    for name in ('rkb_writer', 'rkb_reader'):
        attrs = roles[name]
        if any(attrs[i] is not False for i in (0, 2, 3, 5, 6)):
            raise SpoolConflict('native role privilege bypass')
    if any(r[0] == 'membership' and any(name in r[1].split(':')
           for name in ('rkb_writer', 'rkb_reader')) for r in rows):
        raise SpoolConflict('native role membership bypass')
    return data


@dataclass(frozen=True, eq=False)
class _Coverage:
    database_generation: str
    verifier: object
    binding: object
    key: tuple
    deadline: float
    nonce: str
    head: object
    accepted_sequence: int
    applied_sequence: int

    def current(self):
        v = self.verifier
        return (v._coverage is self and self.deadline > v.clock()
                and binding_key(self.binding) == self.key and v.binding_current(self.binding)
                and v._registry_hash() == v.registry_hash and v._routes_current())


class NativeSourceHistoryVerifier:
    def __init__(self, spool, *, acquire, binding_current, routes,
                 expected_catalog_sha256, expected_acl_sha256, writer_generation,
                 timeout_seconds=5, coverage_seconds=30, max_commands=2048,
                 max_history_bytes=4*1024*1024, clock=time.monotonic):
        if (not callable(acquire) or not callable(binding_current) or not isinstance(routes, dict)
                or not {'source.search', 'source.fetch', 'source.withdraw'} <= routes.keys()
                or set(routes) - {'source.search', 'source.fetch', 'source.withdraw', 'source.import'}
                or any(not callable(v) for v in routes.values())):
            raise ValueError('closed actual source route inventory required')
        if (type(timeout_seconds) not in (int,float) or not 0 < timeout_seconds <= 10
                or type(coverage_seconds) not in (int,float) or not 0 < coverage_seconds <= 120
                or type(max_commands) is not int or not 1 <= max_commands <= 10000
                or type(max_history_bytes) is not int or not 1 <= max_history_bytes <= 16*1024*1024):
            raise ValueError('bounded native verification required')
        for pin in (expected_catalog_sha256, expected_acl_sha256):
            if not isinstance(pin,str) or len(pin)!=64 or set(pin)-set('0123456789abcdef'):
                raise ValueError('pinned native catalog and ACL hashes required')
        self.spool, self.acquire, self.binding_current = spool, acquire, binding_current
        self.routes, self.route_objects = routes, MappingProxyType(dict(routes))
        self.catalog_hash, self.acl_hash = expected_catalog_sha256, expected_acl_sha256
        self.writer_generation = writer_generation
        self.timeout_seconds, self.coverage_seconds = timeout_seconds, coverage_seconds
        self.max_commands, self.max_history_bytes, self.clock = max_commands, max_history_bytes, clock
        self.registry_hash, self._coverage = self._registry_hash(), None
        # A verifier's loaded bytes, rather than a request-supplied hash.
        with open(__file__, 'rb') as source:
            self.verifier_hash = _hash(source.read())

    def _registry_hash(self):
        return _hash(_json(sorted(h.fingerprint for h in self.spool.registry.handlers.values())))

    def _routes_current(self):
        return self.routes.keys() == self.route_objects.keys() and all(
            self.routes[k] is v for k,v in self.route_objects.items())

    def _checkpoint(self, generation):
        return _json({'codec':'rkb.source-genesis.v1', 'stream':self.spool.journal.stream,
            'database_generation':generation, 'sequence':0, 'catalog_sha256':self.catalog_hash,
            'acl_sha256':self.acl_hash, 'registry_sha256':self.registry_hash,
            'routes':sorted(self.routes), 'verifier_sha256':self.verifier_hash})

    def _checkpoint_key(self, generation):
        return self.spool.journal.prefix + '/source-genesis/' + _hash(generation.encode())

    async def establish_genesis(self, binding):
        return await self._verify(binding, genesis=True)

    async def restore(self, binding):
        return await self._verify(binding, genesis=False)

    async def _head_unchanged(self,head):
        # The retained verified token names these exact previously checked
        # pointer bytes. Re-read only that bounded pointer, never old segments.
        j=self.spool.journal
        value=await j._read(j.head_key,4096)
        if head is None:return value is None
        if value is None or value.token!=head.token:return False
        pointer=json.loads(value.data)
        return (pointer.get('codec')=='rkb.head.v1' and pointer.get('stream')==j.stream
            and j._parse_ref(pointer.get('manifest'))==head.reference)

    async def renew(self,binding):
        """Renew an unchanged, already verified exact prefix without replay.

        This is controller maintenance, never an implicit step on ready reads.
        A changed head/native cursor requires explicit recovery verification.
        """
        s,j=self.spool,self.spool.journal
        key=binding_key(binding)
        async with s._lock:
            proof=self._coverage
            if (proof is None or s._source_anchor is not proof or proof.binding is not binding
                    or proof.key!=key or not self.binding_current(binding)
                    or not self._routes_current() or self._registry_hash()!=self.registry_hash
                    or s._source_head!=(proof.head.reference if proof.head else None)
                    or s._source_covered_sequence!=proof.accepted_sequence or s._writing):
                s.invalidate_source_history();self._coverage=None
                raise SpoolConflict('exact retained source prefix unavailable for renewal')
            # Invalidate before any cancellable native/provider read: an unknown
            # verification outcome cannot prolong the previous capability.
            s.invalidate_source_history();self._coverage=None
            renewal_version=s._source_version
            async with asyncio.timeout(self.timeout_seconds):
                if not await self._head_unchanged(proof.head):
                    raise SpoolConflict('source journal prefix changed during renewal')
                catalog,fields=await NativeDatabaseProbe(timeout_seconds=min(2,self.timeout_seconds))(acquire=self.acquire)
                if (_hash(catalog)!=self.catalog_hash or fields['database_generation']!=key[3]
                        or fields['applied_seq']!=proof.applied_sequence):
                    raise SpoolConflict('native source prefix changed during renewal')
                async with self.acquire() as connection:
                    if (connection.closed or connection.row_factory is not tuple_row
                            or connection.info.transaction_status!=TransactionStatus.IDLE):
                        raise SpoolConflict('native verifier connection unavailable')
                    async with connection.transaction():
                        async with connection.cursor() as cursor:
                            await cursor.execute('SET TRANSACTION READ ONLY')
                            await cursor.execute("SET LOCAL statement_timeout='2000ms'")
                            await cursor.execute('SET LOCAL search_path=pg_catalog')
                            if _hash(await inspect_acl(connection))!=self.acl_hash:
                                raise SpoolConflict('native role ACL closure differs during renewal')
                            await cursor.execute('SELECT s.database_generation,s.writer_generation,s.applied_seq '
                                'FROM rkb_v4.recovery_state s JOIN rkb_v4.writer_generations w '
                                'ON w.generation=s.writer_generation WHERE s.singleton AND NOT w.retired LIMIT 2')
                            if await cursor.fetchall()!=[(key[3],self.writer_generation,proof.applied_sequence)]:
                                raise SpoolConflict('native writer prefix changed during renewal')
                            nonce=secrets.token_hex(24)
                            await cursor.execute('SELECT %s::text',(nonce,))
                            if await cursor.fetchone()!=(nonce,):raise SpoolConflict('native renewal nonce differs')
                # invalidate_source_history itself replaced version once. Any
                # additional write while the native observation awaited is stale.
                if (not await self._head_unchanged(proof.head) or not self.binding_current(binding)
                        or s._writing or s._source_version is not renewal_version):
                    raise SpoolConflict('source prefix changed at renewal completion')
                coverage=_Coverage(key[3],self,binding,key,self.clock()+self.coverage_seconds,
                    nonce,proof.head,proof.accepted_sequence,proof.applied_sequence)
                self._coverage=coverage;s._source_anchor=coverage;s._source_version=object()
                _LOG.info('source_history_renewed generation_hash=%s accepted_seq=%s applied_seq=%s',
                    _hash(key[3].encode()),proof.accepted_sequence,proof.applied_sequence)
                return coverage

    async def _verify(self, binding, *, genesis):
        s, j = self.spool, self.spool.journal
        key = binding_key(binding)
        async with s._lock:
            s.invalidate_source_history()
            self._coverage = None
            async with asyncio.timeout(self.timeout_seconds):
                if not self.binding_current(binding) or not self._routes_current() or self._registry_hash()!=self.registry_hash:
                    raise SpoolConflict('source closure changed')
                catalog, fields = await NativeDatabaseProbe(timeout_seconds=min(2,self.timeout_seconds))(acquire=self.acquire)
                if _hash(catalog)!=self.catalog_hash or fields['database_generation']!=key[3]:
                    raise SpoolConflict('native catalog pin or generation differs')
                async with self.acquire() as connection:
                    if (connection.closed or connection.row_factory is not tuple_row
                            or connection.info.transaction_status != TransactionStatus.IDLE):
                        raise SpoolConflict('native verifier connection unavailable')
                    async with connection.transaction():
                        async with connection.cursor() as cursor:
                            await cursor.execute("SET LOCAL statement_timeout='2000ms'")
                            await cursor.execute("SET LOCAL lock_timeout='1000ms'")
                            await cursor.execute('SET LOCAL search_path=pg_catalog')
                            # Keep the PG source state stable until remote checkpoint
                            # publication/readback and local coverage installation.
                            await cursor.execute('LOCK TABLE rkb_v4.sources,rkb_v4.revisions,rkb_v4.active_revision,'
                                'rkb_v4.passage_search,rkb_v4.applied_ops,rkb_v4.recovery_state,'
                                'rkb_v4.writer_generations IN SHARE MODE')
                            if _hash(await inspect_acl(connection)) != self.acl_hash:
                                raise SpoolConflict('native role ACL closure differs')
                            await cursor.execute('SELECT s.database_generation,s.writer_generation,s.applied_seq '
                                'FROM rkb_v4.recovery_state s JOIN rkb_v4.writer_generations w '
                                'ON w.generation=s.writer_generation WHERE s.singleton AND NOT w.retired LIMIT 2')
                            states = await cursor.fetchall()
                            if len(states)!=1 or states[0][:2]!=(key[3],self.writer_generation):
                                raise SpoolConflict('native writer generation differs')
                            applied_seq=states[0][2]
                            head=await j.head()
                            checkpoint=self._checkpoint(key[3]); checkpoint_key=self._checkpoint_key(key[3])
                            if genesis:
                                if head is not None or applied_seq!=0:
                                    raise SpoolConflict('genesis requires empty exact remote and native history')
                                await cursor.execute('SELECT EXISTS(SELECT 1 FROM rkb_v4.sources) OR '
                                    'EXISTS(SELECT 1 FROM rkb_v4.revisions) OR EXISTS(SELECT 1 FROM rkb_v4.active_revision) OR '
                                    'EXISTS(SELECT 1 FROM rkb_v4.passage_search) OR EXISTS(SELECT 1 FROM rkb_v4.applied_ops)')
                                if await cursor.fetchone() != (False,):
                                    raise SpoolConflict('genesis contains source data or applied history')
                                local=await _database(s._scope_state,None)
                                if local['operation_count']:
                                    raise SpoolConflict('genesis contains unclassified local intake')
                                await j._fenced_exact(checkpoint_key,checkpoint,expected_token=None,
                                    writer_generation=self.writer_generation)
                            else:
                                remote=await j._read(checkpoint_key,8192)
                                if remote is None or remote.data!=checkpoint:
                                    raise SpoolConflict('exact immutable generation genesis missing')
                            commands=[]; ref=head.reference if head else None; used=0
                            for _ in range(j.max_reconcile_hops):
                                if ref is None:break
                                previous,raws=await j.read_segment(ref)
                                for raw in raws:
                                    used+=len(raw)
                                    if used>self.max_history_bytes or len(commands)>=self.max_commands:
                                        raise SpoolConflict('full source history budget exceeded')
                                    command=_command(raw); handler=s.registry.resolve(command); dep=handler.dependency
                                    if (dep is None or dep.scope_class!='public_source' or dep.scope_owner!='shared'
                                            or dep.resource_keys!=frozenset({'source_id'}) or handler.transition is not None
                                            or set(command.ids)!={'source_id'}
                                            or command.route not in ('source.withdraw.v1','source.import.v1')
                                            or command.route=='source.import.v1' and 'source.import' not in self.routes):
                                        raise SpoolConflict('unclassified accepted source dependency')
                                    commands.append((command,handler,ref.sha256))
                                ref=previous
                            if ref is not None:raise SpoolConflict('full source genesis traversal incomplete')
                            commands.sort(key=lambda item:item[0].sequence)
                            if [c.sequence for c,_,_ in commands]!=list(range(1,len(commands)+1)):
                                raise SpoolConflict('noncontiguous accepted source history')
                            if len({c.operation_key for c,_,_ in commands})!=len(commands):
                                raise SpoolConflict('duplicate accepted source operation')
                            if not 0<=applied_seq<=len(commands):raise SpoolConflict('applied prefix exceeds accepted history')
                            applied={}
                            successful_imports=set()
                            async with connection.cursor(name='rkb_source_applied_verifier') as receipts:
                                await receipts.execute('SELECT sequence,operation_key,command_sha256,handler_sha256,'
                                    'accepted_manifest_sha256,status,outcome_sha256,CASE WHEN octet_length(outcome_bytes)<=%s '
                                    'THEN outcome_bytes END,database_generation,CASE WHEN octet_length(command_bytes)<=%s '
                                    'THEN command_bytes END FROM rkb_v4.applied_ops ORDER BY sequence LIMIT %s',
                                    (min(self.max_history_bytes,j.max_outcome_bytes),j.max_segment_bytes,self.max_commands+1))
                                count=0
                                while True:
                                    row=await receipts.fetchone()
                                    if row is None:break
                                    if count>=applied_seq:raise SpoolConflict('native applied prefix exceeds cursor')
                                    command,handler,manifest_hash=commands[count]; count+=1
                                    seq,op,ch,hh,mh,status,oh,outcome,generation,command_bytes=row
                                    if (seq!=command.sequence or op!=command.operation_key or ch!=_hash(command.encode())
                                            or hh!=handler.fingerprint or mh!=manifest_hash or generation!=key[3]
                                            or status not in ('applied','rejected_precondition') or outcome is None
                                            or command_bytes is None or bytes(command_bytes)!=command.encode()
                                            or _hash(bytes(outcome))!=oh):
                                        raise SpoolConflict('native exact applied outcome differs')
                                    used+=len(outcome)
                                    if used>self.max_history_bytes:raise SpoolConflict('native outcome history budget exceeded')
                                    applied[seq]=(oh,bytes(outcome))
                                    if command.route=='source.import.v1' and status=='applied':
                                        successful_imports.add(seq)
                                if count!=applied_seq:raise SpoolConflict('native applied prefix incomplete')
                            changes={}
                            imported=[]
                            for command,handler,_ in commands:
                                outcome=applied.get(command.sequence); durable=False
                                if outcome is not None:
                                    remote=await j._read(j.prefix+'/outcomes/'+_hash(command.operation_key.encode()),j.max_outcome_bytes)
                                    if remote is not None:
                                        expected={'codec':'rkb.outcome.v1','stream':j.stream,
                                            'command_sha256':_hash(command.encode()),'sequence':command.sequence,
                                            'operation_key':command.operation_key,'database_generation':key[3],
                                            'outcome_b64':base64.b64encode(outcome[1]).decode(),
                                            'outcome_sha256':outcome[0],'outcome_length':len(outcome[1])}
                                        if remote.data!=_json(expected):raise SpoolConflict('remote outcome differs from native prefix')
                                        durable=True
                                if command.route=='source.import.v1' and outcome is not None:
                                    # Native success status is checked independently
                                    # from exact remote outcome durability below.
                                    if command.sequence in successful_imports:
                                        imported.append(command.ids['source_id'])
                                source=command.ids['source_id']; prior=changes.get(source)
                                changes[source]=SourceChange(source,command.sequence,_hash(command.encode()),
                                    bool(handler.dependency.restriction or prior and prior.restriction),
                                    bool(not durable or prior and prior.pending),False)
                            await cursor.execute('SELECT EXISTS(SELECT 1 FROM rkb_v4.sources '
                                'WHERE NOT (source_id=ANY(%s::uuid[])))',(imported,))
                            if await cursor.fetchone()!=(False,):
                                raise SpoolConflict('native source lacks canonical applied import origin')
                            if await j.head()!=head or not self.binding_current(binding):
                                raise SpoolConflict('source history changed during verification')
                            await s._write(s._replace_source_heads,tuple(changes.values()))
                            version=s._source_version
                            nonce=secrets.token_hex(24)
                            await cursor.execute('SELECT %s::text',(nonce,))
                            if await cursor.fetchone()!=(nonce,):raise SpoolConflict('native verifier nonce differs')
                # Publish the in-memory capability only after PG transaction and
                # lease exit succeed; lost COMMIT/readback never grants coverage.
                if (not self.binding_current(binding) or await j.head()!=head
                        or s._writing or s._source_version is not version):
                    raise SpoolConflict('source binding changed at native completion')
                coverage=_Coverage(key[3],self,binding,key,self.clock()+self.coverage_seconds,nonce,
                    head,len(commands),applied_seq)
                self._coverage=coverage
                s._source_anchor=coverage; s._source_head=head.reference if head else None
                s._source_covered_sequence=len(commands); s._source_version=object()
                _LOG.info('source_history_verified generation_hash=%s accepted_seq=%s applied_seq=%s genesis=%s',
                    _hash(key[3].encode()),len(commands),applied_seq,genesis)
                return coverage
