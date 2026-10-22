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

from .control_spool import SourceChange, SpoolConflict, _command, _database, _uuid
from .kaggle_database_probe import NativeDatabaseProbe
from .kaggle_schema_migrate import latest_schema_version
from .source_read_gate import binding_key

_LOG=logging.getLogger(__name__)


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, separators=(',', ':')).encode()


def _hash(data):
    return hashlib.sha256(data).hexdigest()


from .kaggle_source_acl import ACL_SQL,NativeHistoryError,inspect_acl as _native_inspect_acl

async def inspect_acl(connection):
    try:return await _native_inspect_acl(connection)
    except NativeHistoryError as exc:raise SpoolConflict(str(exc)) from None


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
    pending: tuple = ()
    case_state: object = None

    def current(self):
        v = self.verifier
        return (v._coverage is self and self.deadline > v.clock()
                and binding_key(self.binding) == self.key and v.binding_current(self.binding)
                and v._registry_hash() == v.registry_hash and v._routes_current())


def _case_projection(commands,applied,previous=None,*,after=0):
    from .case_history import project_cases
    try:
        selected=[value for value in commands if value[0].sequence>after and value[0].route.startswith('disagreement.')]
        return project_cases(selected,{value[0].sequence:json.loads(applied[value[0].sequence][1])['status']
            for value in selected if value[0].sequence in applied},previous)
    except (ValueError,TypeError,KeyError):
        raise SpoolConflict('verified Case policy history differs') from None


class NativeSourceHistoryVerifier:
    def __init__(self, spool, *, acquire, binding_current, routes,
                 writer_generation, expected_schema_version=None,
                 expected_catalog_sha256=None, expected_acl_sha256=None,
                 timeout_seconds=5, coverage_seconds=30, max_commands=2048,
                 max_history_bytes=4*1024*1024, clock=time.monotonic):
        if (acquire is None and type(self) is NativeSourceHistoryVerifier
                or acquire is not None and not callable(acquire) or not callable(binding_current) or not isinstance(routes, dict)
                or not {'source.search', 'source.fetch', 'source.withdraw'} <= routes.keys()
                or set(routes) - {'source.search', 'source.fetch', 'source.withdraw', 'source.import', 'story.search', 'story.get', 'story.history', 'story.variants',
                    'source.fact_read','source.calendar','source.observations','source.observation_compare','story.edit','story.access','source.catalog','case.read','case.edit','case.access','case.contribute','case.conclusion','geo.identity_read','geo.members_read','source.entity_read','source.entity_list'}
                or any(not callable(v) for v in routes.values())):
            raise ValueError('closed actual source route inventory required')
        if (type(timeout_seconds) not in (int,float) or not 0 < timeout_seconds <= 10
                or type(coverage_seconds) not in (int,float) or not 0 < coverage_seconds <= 120
                or type(max_commands) is not int or not 1 <= max_commands <= 10000
                or type(max_history_bytes) is not int or not 1 <= max_history_bytes <= 16*1024*1024):
            raise ValueError('bounded native verification required')
        self.spool, self.acquire, self.binding_current = spool, acquire, binding_current
        self.routes, self.route_objects = routes, MappingProxyType(dict(routes))
        self.schema_version=latest_schema_version() if expected_schema_version is None else expected_schema_version
        if type(self.schema_version) is not int or self.schema_version<1:
            raise ValueError('integer expected schema version required')
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
        return _json({'codec':'rkb.source-genesis.v3', 'stream':self.spool.journal.stream,
            'database_generation':generation, 'sequence':0,
            'schema_version':self.schema_version,
            'producer':{'verifier_sha256':self.verifier_hash}})

    def _verify_genesis(self,data,generation,*,expected_closure=None):
        """Known genesis facts are immutable; producer code is provenance.

        A compatible verifier reads v1/v2 metadata without comparing catalog
        or ACL fingerprints. Exact stream/generation/sequence remain bound.
        """
        try:
            value=json.loads(data)
            if _json(value)!=data:raise ValueError
            common={'codec','stream','database_generation','sequence'}
            if value.get('codec')=='rkb.source-genesis.v1':
                closure_keys={'catalog_sha256','acl_sha256','registry_sha256','routes'}
                if set(value)!=common|closure_keys|{'verifier_sha256'}:raise ValueError
                closure={k:value[k] for k in closure_keys};producer=value['verifier_sha256']
            elif value.get('codec')=='rkb.source-genesis.v2':
                if set(value)!=common|{'origin_closure','producer'}:raise ValueError
                closure=value['origin_closure']
                if not isinstance(value['producer'],dict) or set(value['producer'])!={'verifier_sha256'}:raise ValueError
                producer=value['producer']['verifier_sha256']
            elif value.get('codec')=='rkb.source-genesis.v3':
                if set(value)!=common|{'schema_version','producer'}:raise ValueError
                if type(value['schema_version']) is not int or not 1<=value['schema_version']<=self.schema_version:raise ValueError
                producer=value['producer']['verifier_sha256']
            else:raise ValueError
            if (value['stream']!=self.spool.journal.stream or value['database_generation']!=generation
                    or type(value['sequence']) is not int or value['sequence']!=0
                    or not isinstance(producer,str)
                    or len(producer)!=64 or set(producer)-set('0123456789abcdef')):raise ValueError
        except (ValueError,TypeError,KeyError):
            raise SpoolConflict('genesis facts differ') from None
        return value

    def _classify(self,raw,reference):
        """Verify every global command; derive book heads only for book writes.

        Fixed Case/GEO validators confer no public source dependency and never
        add private domain IDs to the source head cache. Unknown handlers fail.
        """
        command=_command(raw)
        try:command,_,handler=self.spool._validate(command,'history-classification')
        except (ValueError,TypeError):raise SpoolConflict('unclassified accepted source dependency') from None
        dep=handler.dependency
        if dep is None:raise SpoolConflict('unclassified accepted source dependency')
        if dep.scope_class=='public_source':
            expected_route=('source.import' if command.route.startswith('source.import.') else
                {'source.withdraw.v1':'source.withdraw','story.edit.v1':'story.edit','story.access.v1':'story.access'}.get(command.route))
            if expected_route not in self.routes or dep.scope_owner!='shared' or 'source_id' not in dep.resource_keys:
                raise SpoolConflict('unclassified accepted source dependency')
            _uuid(command.ids['source_id'])
        elif dep.scope_class not in ('case','private_draft','geo_manifest'):
            raise SpoolConflict('unclassified accepted source dependency')
        return command,handler,reference

    def _checkpoint_key(self, generation):
        return self.spool.journal.prefix + '/source-genesis/' + _hash(generation.encode())

    async def establish_genesis(self, binding):
        return await self._verify(binding, genesis=True)

    async def restore(self, binding):
        return await self._verify(binding, genesis=False)

    async def adopt_recovered(self,cap,binding):
        """Adopt a real physical-restore capability, never a serialized receipt."""
        from .pg_snapshot_recovery import ControllerRecoveryProof
        if type(cap) is not ControllerRecoveryProof:
            raise SpoolConflict('owner-retained physical recovery capability required')
        s=self.spool;key=binding_key(binding)
        async with s._lock:
            s.invalidate_source_history();self._coverage=None
            version=s._source_version
            await cap.verify_for(cap.database,*cap.identity)
            if len(cap.control)>self.max_history_bytes:raise SpoolConflict('restored source control byte budget exceeded')
            control=json.loads(cap.control)
            if (key[3]!=cap.database.database_generation or self.writer_generation!=cap.database.writer_generation
                    or not self.binding_current(binding)
                    or control.get('schema_version')!=self.schema_version
                    or control.get('sequence')!=cap.applied_sequence
                    or control.get('journal_head')!=(cap.journal_head.reference.wire() if cap.journal_head else None)
                    or await self.spool.journal.head()!=cap.journal_head):
                raise SpoolConflict('restored source closure or journal binding differs')
            changes=[];seen=set()
            for row in control['heads']:
                if (not isinstance(row,list) or len(row)!=6 or row[0] in seen
                        or type(row[1]) is not int or not 1<=row[1]<=cap.applied_sequence
                        or not isinstance(row[2],str) or len(row[2])!=64 or set(row[2])-set('0123456789abcdef')
                        or type(row[3]) is not int or row[3] not in (0,1) or row[4:]!=[0,0]):
                    raise SpoolConflict('restored source control checkpoint shape differs')
                source=_uuid(row[0]);seen.add(source)
                changes.append(SourceChange(source,row[1],row[2],bool(row[3]),False,False))
            catalog,fields=await NativeDatabaseProbe(timeout_seconds=min(2,self.timeout_seconds))(acquire=self.acquire)
            if (fields['database_generation']!=key[3] or fields['applied_seq']!=cap.applied_sequence
                    or fields['schema_version']!=self.schema_version or s._writing or s._source_version is not version):
                raise SpoolConflict('restored source native target differs')
            async with self.acquire() as connection:
                if (connection.info.host!=cap.database.socket or connection.info.port!=cap.database.port
                        or connection.info.dbname!=cap.database.dbname):
                    raise SpoolConflict('restored source connection is not the verified candidate')
            await s._write(s._replace_source_heads,tuple(changes))
            coverage=_Coverage(key[3],self,binding,key,self.clock()+self.coverage_seconds,secrets.token_hex(24),
                cap.journal_head,cap.applied_sequence,cap.applied_sequence,())
            self._coverage=coverage;s._source_anchor=coverage
            s._source_head=cap.journal_head.reference if cap.journal_head else None
            s._source_covered_sequence=cap.applied_sequence;s._source_version=object()
            return coverage

    async def extend(self,binding):
        """Verify only the contiguous suffix of an identical retained prefix."""
        s,j=self.spool,self.spool.journal; key=binding_key(binding)
        async with s._lock:
            proof=self._coverage
            if (proof is None or s._source_anchor is not proof or proof.binding is not binding
                    or proof.key!=key or not self.binding_current(binding)
                    or not self._routes_current() or self._registry_hash()!=self.registry_hash):
                s.invalidate_source_history();self._coverage=None
                raise SpoolConflict('verified source prefix unavailable for extension')
            s.invalidate_source_history();self._coverage=None
            version=s._source_version
            async with asyncio.timeout(self.timeout_seconds):
                head=await j.head(); ref=head.reference if head else None
                base=proof.head.reference if proof.head else None
                suffix=[];used=sum(len(p[0]) for p in proof.pending)
                for _ in range(j.max_reconcile_hops):
                    if ref==base:break
                    if ref is None:raise SpoolConflict('retained source prefix is not an ancestor')
                    previous,raws=await j.read_segment(ref)
                    for raw in raws:
                        used+=len(raw)
                        if used>self.max_history_bytes or len(suffix)+len(proof.pending)>=self.max_commands:
                            raise SpoolConflict('incremental source history budget exceeded')
                        c,h,_=self._classify(raw,ref)
                        suffix.append((c,h,ref))
                    ref=previous
                if ref!=base:raise SpoolConflict('incremental source ancestry budget exceeded')
                suffix.sort(key=lambda p:p[0].sequence)
                if [c.sequence for c,_,_ in suffix]!=list(range(proof.accepted_sequence+1,
                        proof.accepted_sequence+len(suffix)+1)):
                    raise SpoolConflict('incremental source sequence is not contiguous')
                known={}
                for raw,handler_hash,manifest in proof.pending:
                    c=_command(raw);h=s.registry.resolve(c)
                    if h.fingerprint!=handler_hash:raise SpoolConflict('retained pending handler changed')
                    known[c.sequence]=(c,h,manifest)
                known.update({c.sequence:(c,h,m) for c,h,m in suffix})
                if len({c.operation_key for c,_,_ in known.values()})!=len(known):
                    raise SpoolConflict('duplicate incremental source operation')
                catalog,fields=await NativeDatabaseProbe(timeout_seconds=min(2,self.timeout_seconds))(acquire=self.acquire)
                accepted=proof.accepted_sequence+len(suffix)
                if (fields['schema_version']!=self.schema_version or fields['database_generation']!=key[3]
                        or not proof.applied_sequence<=fields['applied_seq']<=accepted):
                    raise SpoolConflict('incremental native prefix differs')
                async with self.acquire() as connection:
                    if (connection.closed or connection.row_factory is not tuple_row
                            or connection.info.transaction_status!=TransactionStatus.IDLE):
                        raise SpoolConflict('native verifier connection unavailable')
                    async with connection.transaction():
                        async with connection.cursor() as cursor:
                            await cursor.execute("SET LOCAL statement_timeout='2000ms'")
                            await cursor.execute("SET LOCAL lock_timeout='1000ms'")
                            await cursor.execute('SET LOCAL search_path=pg_catalog')
                            await cursor.execute('LOCK TABLE rkb_v4.sources,rkb_v4.revisions,rkb_v4.active_revision,'
                                'rkb_v4.passage_search,rkb_v4.applied_ops,rkb_v4.recovery_state,'
                                'rkb_v4.writer_generations IN SHARE MODE')
                            if fields.get('schema_revision') in ('rkb-v4-sql001-007-v1','rkb-v4-sql001-008-v1','rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                await cursor.execute('LOCK TABLE rkb_v4.source_regions,rkb_v4.story_assertion_versions,'
                                    'rkb_v4.stories,rkb_v4.story_versions,rkb_v4.story_variant_versions,'
                                    'rkb_v4.story_search,rkb_v4.source_story_pending,rkb_v4.story_history_receipts IN SHARE MODE')
                            if fields.get('schema_revision') in ('rkb-v4-sql001-008-v1','rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                await cursor.execute('LOCK TABLE rkb_v4.source_event_dates,rkb_v4.source_observations IN SHARE MODE')
                            if fields.get('schema_revision') in ('rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                await cursor.execute('LOCK TABLE rkb_v4.source_graph_documents,rkb_v4.source_entity_ids,'
                                    'rkb_v4.source_entity_representations,rkb_v4.source_entity_references,rkb_v4.source_entity_mentions,'
                                    'rkb_v4.source_entity_aliases,rkb_v4.source_entity_relations,rkb_v4.source_entity_finding_links,'
                                    'rkb_v4.source_poi_hypotheses,rkb_v4.source_graph_receipts IN SHARE MODE')
                            if fields.get('schema_revision') in ('rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                await cursor.execute('LOCK TABLE rkb_v4.source_manifests,rkb_v4.source_catalog,rkb_v4.source_passage_provenance,rkb_v4.editorial_story_heads,'
                                    'rkb_v4.editorial_story_versions,rkb_v4.editorial_variant_versions,rkb_v4.editorial_variant_heads,'
                                    'rkb_v4.editorial_story_receipts,rkb_v4.story_editor_grants,rkb_v4.editorial_story_search IN SHARE MODE')
                            await cursor.execute('SELECT s.database_generation,s.writer_generation,s.applied_seq '
                                'FROM rkb_v4.recovery_state s JOIN rkb_v4.writer_generations w '
                                'ON w.generation=s.writer_generation WHERE s.singleton AND NOT w.retired LIMIT 2')
                            applied_seq=fields['applied_seq']
                            if await cursor.fetchall()!=[(key[3],self.writer_generation,applied_seq)]:
                                raise SpoolConflict('incremental native writer cursor changed')
                            earlier=[seq for seq in known if seq<=proof.applied_sequence]
                            expected=set(range(proof.applied_sequence+1,applied_seq+1))|set(earlier)
                            applied={}
                            async with connection.cursor(name='rkb_source_incremental_receipts') as receipts:
                                await receipts.execute('SELECT sequence,operation_key,command_sha256,handler_sha256,'
                                    'accepted_manifest_sha256,status,outcome_sha256,CASE WHEN octet_length(outcome_bytes)<=%s '
                                    'THEN outcome_bytes END,database_generation,CASE WHEN octet_length(command_bytes)<=%s '
                                    'THEN command_bytes END FROM rkb_v4.applied_ops WHERE sequence>%s OR sequence=ANY(%s::bigint[]) '
                                    'ORDER BY sequence LIMIT %s',(min(self.max_history_bytes,j.max_outcome_bytes),
                                    j.max_segment_bytes,proof.applied_sequence,earlier,self.max_commands+1))
                                while (row:=await receipts.fetchone()) is not None:
                                    seq,op,ch,hh,mh,status,oh,outcome,generation,raw=row
                                    if seq not in expected or seq in applied or seq not in known:
                                        raise SpoolConflict('incremental native receipt prefix is not exact')
                                    c,h,m=known[seq]
                                    if (op!=c.operation_key or ch!=_hash(c.encode()) or hh!=h.fingerprint
                                            or mh!=m.sha256 or generation!=key[3] or raw is None or bytes(raw)!=c.encode()
                                            or status not in ('applied','rejected_precondition') or outcome is None
                                            or _hash(bytes(outcome))!=oh):
                                        raise SpoolConflict('incremental native receipt bytes differ')
                                    used+=len(outcome)
                                    if used>self.max_history_bytes:raise SpoolConflict('incremental outcome budget exceeded')
                                    applied[seq]=(oh,bytes(outcome))
                            if applied.keys()!=expected:raise SpoolConflict('incremental native prefix has a gap')
                            pending=[];by_source={}
                            for c,h,m in known.values():
                                outcome=applied.get(c.sequence);durable=False
                                if outcome is not None:
                                    remote=await j.remote_outcome(c.operation_key)
                                    if remote is not None:
                                        expected_outcome={'codec':'rkb.outcome.v1','stream':j.stream,
                                            'command_sha256':_hash(c.encode()),'sequence':c.sequence,'operation_key':c.operation_key,
                                            'database_generation':key[3],'outcome_b64':base64.b64encode(outcome[1]).decode(),
                                            'outcome_sha256':outcome[0],'outcome_length':len(outcome[1])}
                                        if remote.data!=_json(expected_outcome):
                                            raise SpoolConflict('incremental remote outcome differs')
                                        durable=True
                                if not durable:pending.append((c.encode(),h.fingerprint,m))
                                if h.dependency is None or h.dependency.scope_class!='public_source':continue
                                source=c.ids['source_id'];previous=by_source.get(source)
                                by_source[source]=SourceChange(source,max(c.sequence,previous.sequence if previous else 0),
                                    _hash(c.encode()) if previous is None or c.sequence>=previous.sequence else previous.command_sha256,
                                    bool(h.dependency.restriction or previous and previous.restriction),
                                    bool(not durable or previous and previous.pending),False)
                            if fields.get('schema_revision') in ('rkb-v4-sql001-007-v1','rkb-v4-sql001-008-v1','rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                origins=sorted({c.ids['source_id'] for c,_,_ in known.values()
                                    if 'source_id' in c.ids and c.sequence in applied})
                                statement=('SELECT rkb_v4.verify_source_story_origins_v3(%s::uuid[])'
                                    if fields['schema_revision'] in ('rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1') else
                                    'SELECT rkb_v4.verify_source_story_origins_v2(%s::uuid[])'
                                    if fields['schema_revision']=='rkb-v4-sql001-008-v1' else
                                    'SELECT rkb_v4.verify_source_story_origins_v1(%s::uuid[])')
                                await cursor.execute(statement,(origins,))
                                if await cursor.fetchone()!=(True,):raise SpoolConflict('native story origin differs')
                                if fields.get('schema_revision') in ('rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                    await cursor.execute('SELECT rkb_v4.verify_source_graph_origins_v1(%s::uuid[])',(origins,))
                                    if await cursor.fetchone()!=(True,):raise SpoolConflict('native graph origin differs')
                            if await j.head()!=head or s._writing or s._source_version is not version:
                                raise SpoolConflict('source prefix changed during extension')
                            await s._write(self._merge_source_heads,tuple(by_source.values()))
                            version=s._source_version
                            nonce=secrets.token_hex(24)
                            await cursor.execute('SELECT %s::text',(nonce,))
                            if await cursor.fetchone()!=(nonce,):raise SpoolConflict('native extension nonce differs')
                if (await j.head()!=head or not self.binding_current(binding)
                        or s._writing or s._source_version is not version):
                    raise SpoolConflict('source prefix changed at extension completion')
                coverage=_Coverage(key[3],self,binding,key,self.clock()+self.coverage_seconds,nonce,
                    head,accepted,applied_seq,tuple(pending),
                    _case_projection(known.values(),applied,proof.case_state,after=proof.applied_sequence) if proof.case_state is not None else None)
                self._coverage=coverage;s._source_anchor=coverage;s._source_head=head.reference if head else None
                s._source_covered_sequence=accepted;s._source_version=object()
                _LOG.info('source_history_extended generation_hash=%s accepted_seq=%s applied_seq=%s suffix_count=%s',
                    _hash(key[3].encode()),accepted,applied_seq,len(suffix))
                return coverage

    def _merge_source_heads(self,changes):
        """Bounded affected-head update in the existing spool, not a second view."""
        s=self.spool
        with s._db(write=True) as db:
            used=db.execute('SELECT used_bytes FROM rkb_control_spool_meta WHERE id=1').fetchone()[0]
            pending_delta=unknown_delta=0;maximum=0
            for c in changes:
                old=db.execute('SELECT sequence,command_sha256,restriction,pending,unknown,reserved_bytes '
                    'FROM rkb_control_source_heads WHERE source_id=?',(c.source_id,)).fetchone()
                size=old[5] if old else 256+len(c.source_id.encode())
                if old is None:
                    used+=size
                    if used>s.max_bytes:raise SpoolConflict('incremental compact source heads exceed spool budget')
                seq,ch=(old[0],old[1]) if old and old[0]>c.sequence else (c.sequence,c.command_sha256)
                restriction=bool(c.restriction or old and old[2]);maximum=max(maximum,seq)
                pending_delta+=int(c.pending)-(old[3] if old else 0)
                unknown_delta+=int(c.unknown)-(old[4] if old else 0)
                db.execute('INSERT INTO rkb_control_source_heads VALUES(?,?,?,?,?,?,?) '
                    'ON CONFLICT(source_id) DO UPDATE SET sequence=excluded.sequence,command_sha256=excluded.command_sha256,'
                    'restriction=excluded.restriction,pending=excluded.pending,unknown=excluded.unknown',
                    (c.source_id,seq,ch,int(restriction),int(c.pending),int(c.unknown),size))
            db.execute('UPDATE rkb_control_spool_meta SET used_bytes=? WHERE id=1',(used,))
            db.execute('UPDATE rkb_control_source_head_meta SET sequence=max(sequence,?),'
                'pending_count=pending_count+?,unknown_count=unknown_count+? WHERE id=1',
                (maximum,pending_delta,unknown_delta))

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
                if (fields['schema_version']!=self.schema_version or fields['database_generation']!=key[3]
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
                            if fields.get('schema_revision') in ('rkb-v4-sql001-007-v1','rkb-v4-sql001-008-v1','rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                await cursor.execute('LOCK TABLE rkb_v4.source_regions,rkb_v4.story_assertion_versions,'
                                    'rkb_v4.stories,rkb_v4.story_versions,rkb_v4.story_variant_versions,'
                                    'rkb_v4.story_search,rkb_v4.source_story_pending,rkb_v4.story_history_receipts IN SHARE MODE')
                            if fields.get('schema_revision') in ('rkb-v4-sql001-008-v1','rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                await cursor.execute('LOCK TABLE rkb_v4.source_event_dates,rkb_v4.source_observations IN SHARE MODE')
                            if fields.get('schema_revision') in ('rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                await cursor.execute('LOCK TABLE rkb_v4.source_graph_documents,rkb_v4.source_entity_ids,'
                                    'rkb_v4.source_entity_representations,rkb_v4.source_entity_references,rkb_v4.source_entity_mentions,'
                                    'rkb_v4.source_entity_aliases,rkb_v4.source_entity_relations,rkb_v4.source_entity_finding_links,'
                                    'rkb_v4.source_poi_hypotheses,rkb_v4.source_graph_receipts IN SHARE MODE')
                            if fields.get('schema_revision') in ('rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                await cursor.execute('LOCK TABLE rkb_v4.source_manifests,rkb_v4.source_catalog,rkb_v4.source_passage_provenance,rkb_v4.editorial_story_heads,'
                                    'rkb_v4.editorial_story_versions,rkb_v4.editorial_variant_versions,rkb_v4.editorial_variant_heads,'
                                    'rkb_v4.editorial_story_receipts,rkb_v4.story_editor_grants,rkb_v4.editorial_story_search IN SHARE MODE')
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
                    nonce,proof.head,proof.accepted_sequence,proof.applied_sequence,proof.pending,proof.case_state)
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
                if fields['schema_version']!=self.schema_version or fields['database_generation']!=key[3]:
                    raise SpoolConflict('native schema version or generation differs')
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
                            if fields.get('schema_revision') in ('rkb-v4-sql001-007-v1','rkb-v4-sql001-008-v1','rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                await cursor.execute('LOCK TABLE rkb_v4.source_regions,rkb_v4.story_assertion_versions,'
                                    'rkb_v4.stories,rkb_v4.story_versions,rkb_v4.story_variant_versions,'
                                    'rkb_v4.story_search,rkb_v4.source_story_pending,rkb_v4.story_history_receipts IN SHARE MODE')
                            if fields.get('schema_revision') in ('rkb-v4-sql001-008-v1','rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                await cursor.execute('LOCK TABLE rkb_v4.source_event_dates,rkb_v4.source_observations IN SHARE MODE')
                            if fields.get('schema_revision') in ('rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                await cursor.execute('LOCK TABLE rkb_v4.source_graph_documents,rkb_v4.source_entity_ids,'
                                    'rkb_v4.source_entity_representations,rkb_v4.source_entity_references,rkb_v4.source_entity_mentions,'
                                    'rkb_v4.source_entity_aliases,rkb_v4.source_entity_relations,rkb_v4.source_entity_finding_links,'
                                    'rkb_v4.source_poi_hypotheses,rkb_v4.source_graph_receipts IN SHARE MODE')
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
                                existing=await j._read(checkpoint_key,8192)
                                if existing is None:
                                    await j._fenced_exact(checkpoint_key,checkpoint,expected_token=None,
                                        writer_generation=self.writer_generation)
                                else:
                                    self._verify_genesis(existing.data,key[3])
                            else:
                                remote=await j._read(checkpoint_key,8192)
                                if remote is None:
                                    raise SpoolConflict('exact immutable generation genesis missing')
                                self._verify_genesis(remote.data,key[3])
                            commands=[]; ref=head.reference if head else None; used=0
                            for _ in range(j.max_reconcile_hops):
                                if ref is None:break
                                previous,raws=await j.read_segment(ref)
                                for raw in raws:
                                    used+=len(raw)
                                    if used>self.max_history_bytes or len(commands)>=self.max_commands:
                                        raise SpoolConflict('full source history budget exceeded')
                                    command,handler,_=self._classify(raw,ref)
                                    commands.append((command,handler,ref))
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
                                    command,handler,manifest=commands[count]; count+=1
                                    seq,op,ch,hh,mh,status,oh,outcome,generation,command_bytes=row
                                    if (seq!=command.sequence or op!=command.operation_key or ch!=_hash(command.encode())
                                            or hh!=handler.fingerprint or mh!=manifest.sha256 or generation!=key[3]
                                            or status not in ('applied','rejected_precondition') or outcome is None
                                            or command_bytes is None or bytes(command_bytes)!=command.encode()
                                            or _hash(bytes(outcome))!=oh):
                                        raise SpoolConflict('native exact applied outcome differs')
                                    used+=len(outcome)
                                    if used>self.max_history_bytes:raise SpoolConflict('native outcome history budget exceeded')
                                    applied[seq]=(oh,bytes(outcome))
                                    if command.route in ('source.import.v1','source.import.v2','source.import.v3','source.import.v4') and status=='applied':
                                        successful_imports.add(seq)
                                if count!=applied_seq:raise SpoolConflict('native applied prefix incomplete')
                            changes={}
                            imported=[]
                            pending=[]
                            for command,handler,manifest in commands:
                                outcome=applied.get(command.sequence); durable=False
                                if outcome is not None:
                                    remote=await j.remote_outcome(command.operation_key)
                                    if remote is not None:
                                        expected={'codec':'rkb.outcome.v1','stream':j.stream,
                                            'command_sha256':_hash(command.encode()),'sequence':command.sequence,
                                            'operation_key':command.operation_key,'database_generation':key[3],
                                            'outcome_b64':base64.b64encode(outcome[1]).decode(),
                                            'outcome_sha256':outcome[0],'outcome_length':len(outcome[1])}
                                        if remote.data!=_json(expected):raise SpoolConflict('remote outcome differs from native prefix')
                                        durable=True
                                if not durable:pending.append((command.encode(),handler.fingerprint,manifest))
                                if command.route in ('source.import.v1','source.import.v2','source.import.v3','source.import.v4') and outcome is not None:
                                    # Native success status is checked independently
                                    # from exact remote outcome durability below.
                                    if command.sequence in successful_imports:
                                        imported.append(command.ids['source_id'])
                                if handler.dependency is None or handler.dependency.scope_class!='public_source':continue
                                source=command.ids['source_id']; prior=changes.get(source)
                                changes[source]=SourceChange(source,command.sequence,_hash(command.encode()),
                                    bool(handler.dependency.restriction or prior and prior.restriction),
                                    bool(not durable or prior and prior.pending),False)
                            await cursor.execute('SELECT EXISTS(SELECT 1 FROM rkb_v4.sources '
                                'WHERE NOT (source_id=ANY(%s::uuid[])))',(imported,))
                            if await cursor.fetchone()!=(False,):
                                raise SpoolConflict('native source lacks canonical applied import origin')
                            if fields.get('schema_revision') in ('rkb-v4-sql001-007-v1','rkb-v4-sql001-008-v1','rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                statement=('SELECT rkb_v4.verify_source_story_origins_v3(%s::uuid[])'
                                    if fields['schema_revision'] in ('rkb-v4-sql001-009-v1','rkb-v4-sql001-010-v1','rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1') else
                                    'SELECT rkb_v4.verify_source_story_origins_v2(%s::uuid[])'
                                    if fields['schema_revision']=='rkb-v4-sql001-008-v1' else
                                    'SELECT rkb_v4.verify_source_story_origins_v1(%s::uuid[])')
                                await cursor.execute(statement,(imported,))
                                if await cursor.fetchone()!=(True,):raise SpoolConflict('native story origin differs')
                                if fields.get('schema_revision') in ('rkb-v4-sql001-011-v1','rkb-v4-sql001-012-v1'):
                                    await cursor.execute('SELECT rkb_v4.verify_source_graph_origins_v1(%s::uuid[])',(imported,))
                                    if await cursor.fetchone()!=(True,):raise SpoolConflict('native graph origin differs')
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
                    head,len(commands),applied_seq,tuple(pending),_case_projection(commands,applied))
                self._coverage=coverage
                s._source_anchor=coverage; s._source_head=head.reference if head else None
                s._source_covered_sequence=len(commands); s._source_version=object()
                _LOG.info('source_history_verified generation_hash=%s accepted_seq=%s applied_seq=%s genesis=%s',
                    _hash(key[3].encode()),len(commands),applied_seq,genesis)
                return coverage
