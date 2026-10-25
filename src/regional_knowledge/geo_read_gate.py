"""Current private GEO output from native history and exact accepted manifest bytes.

Native PostgreSQL owns item bytes and membership; Dev checks activation and authority. No legacy corpus, geometry owner,
model output, identity store or independent queue/admission is created here.
"""
from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass
import time

from .control_spool import ControlSpool
from .geo_manifest_commands import validate_geo_manifest_command,geo_manifest_handler,GEO_REVISIONS,_name_projection
from .geo_migration import canonical,normalized
from .kaggle_geo_contracts import GeoReadRequest,GeoReadResult,decode_item
from .kaggle_protocol import TrustedActorContext
from .read_authority import ReadAuthority
from .source_read_gate import binding_key,SourceGateDenied
from .source_history_verifier import _Coverage



@dataclass(frozen=True,eq=False)
class GeoPermit:
    query:GeoReadRequest


@dataclass(frozen=True,eq=False)
class GeoTicket:pass


@dataclass(frozen=True,eq=False)
class GeoGatedResult:
    result:GeoReadResult
    ticket:GeoTicket


@dataclass(frozen=True)
class _Manifest:
    database:str
    manifest_id:str
    activation_sequence:int
    operation:str
    command_sha256:str
    operation_keys:tuple[str,...]


@dataclass
class _Read:
    authority:object
    query:GeoReadRequest
    binding:object
    key:tuple
    deadline:float
    manifest:_Manifest
    result:GeoReadResult|None=None


class GeoReadGate:
    def __init__(self,read_authority,spool,*,binding_current,operation_keys=(),ttl_seconds=30,max_retained=256,clock=time.monotonic):
        if not isinstance(read_authority,ReadAuthority) or not isinstance(spool,ControlSpool) or not callable(binding_current):
            raise TypeError('same canonical authority, control spool and runtime binding required')
        if type(ttl_seconds) not in (int,float) or not 0<ttl_seconds<=120 or type(max_retained) is not int or not 1<=max_retained<=1024:
            raise ValueError('bounded GEO output capacity required')
        self.authority,self.spool,self.binding_current=read_authority,spool,binding_current
        self.operation_keys=operation_keys
        self.ttl_seconds,self.max_retained,self.clock=ttl_seconds,max_retained,clock
        self._manifest=None;self._places=frozenset();self._permits={};self._tickets={}

    def _history_current(self,binding,sequence):
        anchor=self.spool._source_anchor
        return (isinstance(anchor,_Coverage) and anchor.current() and anchor.database_generation==binding.database_generation
            and anchor.key==binding_key(binding) and anchor.applied_sequence>=sequence
            and self.binding_current(binding))

    def supported(self):
        from .source_activation import ACTIVATION_REVISIONS
        from .kaggle_native_source_bindings import native_source_handler
        for handler in self.spool.registry.handlers.values():
            if handler.route!='geo.manifest.install.v1':continue
            if handler.revisions==GEO_REVISIONS:
                expected=geo_manifest_handler(revisions=GEO_REVISIONS,check_sha256=handler.check.definition_sha256,
                    apply_sha256=handler.apply.definition_sha256)
            elif handler.revisions==ACTIVATION_REVISIONS:
                expected=native_source_handler(handler.route,check_sha256=handler.check.definition_sha256,
                    apply_sha256=handler.apply.definition_sha256)
            else:continue
            if handler==expected:return True
        return False

    @staticmethod
    def _manifest_command(command):
        from .source_activation import ACTIVATION_REVISIONS
        if command.schema_revision==ACTIVATION_REVISIONS.schema:
            from .kaggle_native_source_bindings import validate_native_source_command
            return validate_native_source_command(command)
        return validate_geo_manifest_command(command)

    async def establish(self,operation_keys,*,binding):
        """Use the existing applied activation and current native history."""
        if type(operation_keys) is not tuple or not 2<=len(operation_keys)<=256 or len(set(operation_keys))!=len(operation_keys):
            raise ValueError('bounded exact manifest operation keys required')
        self._manifest=None
        entry=await self.spool.get(operation_keys[-1]);command=entry.command
        if (not self.supported() or command.route!='geo.manifest.install.v1'
                or self._manifest_command(command)['action']!='activate'
                or entry.apply_state!='APPLIED' or entry.outcome_state!='DURABLE'
                or entry.database_generation!=binding.database_generation):
            raise SourceGateDenied('GEO activation not durable')
        if not self._history_current(binding,command.sequence):
            raise SourceGateDenied('GEO native accepted manifest differs')
        self._manifest=_Manifest(binding.database_generation,command.ids['manifest_id'],
            command.sequence,command.operation_key,entry.command_sha256,operation_keys)
        return self._manifest.manifest_id

    def installation_current(self,binding,operation_keys):
        """Only an established durable manifest and its native prefix open import."""
        proof=self._manifest
        try:
            return bool(proof is not None and type(operation_keys) is tuple and
                proof.operation_keys==operation_keys and len(operation_keys)>=2 and
                proof.database==binding.database_generation and self.supported() and
                self._history_current(binding,proof.activation_sequence))
        except Exception:return False

    async def knows_entity(self,entity_id):
        # Route selection only, from the existing open-command key index.
        # Activation is required even if an installer has loaded its manifest.
        from .geo_migration import exact_uuid
        from .kaggle_public_backend import current_binding,TargetUnavailable
        exact_uuid(entity_id)
        try:binding=await asyncio.to_thread(current_binding,self.spool.queue)
        except TargetUnavailable:return False
        keys=self.operation_keys or (self._manifest.operation_keys if self._manifest else ())
        if not keys:return False
        if not self.installation_current(binding,keys):
            try:await self.establish(keys,binding=binding)
            except SourceGateDenied:return False
        if not self._places:
            proof=self._manifest
            opening=await self.spool.get(keys[0]);command=opening.command
            payload=json.loads(command.payload)
            if (command.route!='geo.manifest.install.v1' or payload.get('action')!='open'
                    or opening.database_generation!=binding.database_generation
                    or command.ids.get('manifest_id')!=proof.manifest_id):
                raise SourceGateDenied('GEO routing index differs')
            index=json.loads(base64.b64decode(payload['index_b64'],validate=True))
            places=frozenset(item['key'][6:] for item in index if item['key'].startswith('place:'))
            if self._manifest is not proof or not self.installation_current(binding,keys):
                raise SourceGateDenied('GEO routing activation changed')
            self._places=places
        return entity_id in self._places

    def _expire(self):
        for values in (self._permits,self._tickets):
            for token,read in tuple(values.items()):
                if read.deadline<=self.clock():del values[token]

    def _current(self,read):
        return (read.deadline>self.clock() and self._manifest is read.manifest and binding_key(read.binding)==read.key
            and self._history_current(read.binding,read.manifest.activation_sequence))

    async def dispatch(self,authority,query,binding):
        self._expire();await self.authority.require(authority,'public_search')
        if authority.public_only:raise SourceGateDenied('private GEO requires personal destination')
        if self.operation_keys and (self._manifest is None or self._manifest.database!=binding.database_generation):
            await self.establish(self.operation_keys,binding=binding)
        if (type(query) is not GeoReadRequest or self._manifest is None or not self.supported()
                or len(self._permits)+len(self._tickets)>=self.max_retained):raise SourceGateDenied('GEO proof unavailable')
        actor=TrustedActorContext(person_id=authority.person_id,audience=authority.destination.key,
            auth_epoch=authority.auth_revision,visibility_epoch=0,context_ref=authority.context_ref)
        query=query.model_copy(update=dict(actor=actor,min_data_seq=max(query.min_data_seq,binding.min_data_seq,self._manifest.activation_sequence)))
        read=_Read(authority,query,binding,binding_key(binding),self.clock()+min(self.ttl_seconds,query.deadline_ms/1000),self._manifest)
        if query.connection_generation!=read.key[2] or not self._current(read):raise SourceGateDenied('GEO runtime binding changed')
        permit=GeoPermit(query);self._permits[permit]=read;return permit

    async def _gate(self,read,result):
        await self.authority.require(read.authority,'public_search')
        if read.authority.public_only or not self._current(read):raise SourceGateDenied('GEO authority changed')
        q=read.query;proof=read.manifest
        if type(result) is not GeoReadResult or (result.request_id,result.connection_generation,result.turn_generation)!=(q.request_id,q.connection_generation,q.turn_generation):
            raise SourceGateDenied('GEO response identity differs')
        GeoReadResult.model_validate_json(canonical(result.model_dump(mode='json')))
        if result.status=='ok':
            if (result.observed_database_generation!=proof.database or result.observed_data_seq<q.min_data_seq
                    or result.observed_projection_seq<q.min_data_seq or len(result.records)>q.limit):raise SourceGateDenied('GEO native frontier differs')
            for record in result.records:
                if (record.manifest_id,record.activation_sequence,record.activation_operation_key,record.activation_command_sha256)!=(
                        proof.manifest_id,proof.activation_sequence,proof.operation,proof.command_sha256):raise SourceGateDenied('GEO activation receipt differs')
                item,digest=decode_item(record.wire_b64)
                if item.owner_person_id!=read.authority.person_id:
                    raise SourceGateDenied('GEO item bytes/private owner differ')
                if q.action=='identity':
                    if item.payload.kind!='place' or (q.entity_id is not None and item.stable_id!=q.entity_id) or (
                            q.poi_ref is not None and item.payload.canonical_poi_ref!=q.poi_ref):raise SourceGateDenied('GEO selector differs')
                    if q.name is not None:
                        if record.matched_wire_b64 is None:raise SourceGateDenied('GEO name proof missing')
                        matched,matched_hash=decode_item(record.matched_wire_b64)
                        if (matched.owner_person_id!=item.owner_person_id
                                or normalized(q.name) not in _name_projection(matched)
                                or (matched.key!=item.key and 'place:'+item.stable_id not in matched.dependencies)):
                            raise SourceGateDenied('GEO exact name proof differs')
                        if q.year is not None and matched.payload.kind=='source_geo_observation' and matched.payload.valid_year_from is not None and not matched.payload.valid_year_from<=q.year<=matched.payload.valid_year_to:
                            raise SourceGateDenied('GEO source time scope differs')
                elif (item.payload.kind!=q.category or not (('place:'+q.entity_id) in item.dependencies
                        or q.category=='provenance')):raise SourceGateDenied('GEO member scope differs')
        read.result=result;return result

    async def finish(self,permit,result):
        self._expire();read=self._permits.pop(permit,None)
        if read is None:raise SourceGateDenied('GEO permit expired or unknown')
        result=await self._gate(read,result);ticket=GeoTicket();self._tickets[ticket]=read
        return GeoGatedResult(result,ticket)

    async def refresh(self,gated):
        self._expire();read=self._tickets.get(gated.ticket) if type(gated) is GeoGatedResult else None
        if read is None or read.result is not gated.result:raise SourceGateDenied('GEO ticket expired or unknown')
        try:result=await self._gate(read,read.result)
        except BaseException:self.release(gated.ticket);raise
        self.release(gated.ticket);ticket=GeoTicket();self._tickets[ticket]=read;return GeoGatedResult(result,ticket)

    def issuance_current(self,ticket):
        read=self._tickets.get(ticket)
        try:return bool(read is not None and self._current(read))
        except Exception:return False

    def release(self,ticket):self._tickets.pop(ticket,None)
    def abandon(self,permit):self._permits.pop(permit,None)
    def invalidate(self):self._manifest=None;self._places=frozenset();self._permits.clear();self._tickets.clear()
