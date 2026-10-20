"""Current private GEO output from native history and exact accepted manifest bytes.

Only compact item hashes are retained on Dev. No legacy corpus, geometry owner,
model output, identity store or independent queue/admission is created here.
"""
from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
import hashlib
import json
import time
from types import MappingProxyType

from .control_spool import ControlSpool
from .geo_manifest_commands import validate_geo_manifest_command,plan_geo_manifest,geo_manifest_handler,GEO_REVISIONS,_name_projection
from .geo_migration import GeoMigrationManifest,canonical,normalized
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
    hashes:object
    provenance_members:object
    activation_sequence:int
    operation:str
    command_sha256:str


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
    def __init__(self,read_authority,spool,*,binding_current,ttl_seconds=30,max_retained=256,clock=time.monotonic):
        if not isinstance(read_authority,ReadAuthority) or not isinstance(spool,ControlSpool) or not callable(binding_current):
            raise TypeError('same canonical authority, control spool and runtime binding required')
        if type(ttl_seconds) not in (int,float) or not 0<ttl_seconds<=120 or type(max_retained) is not int or not 1<=max_retained<=1024:
            raise ValueError('bounded GEO output capacity required')
        self.authority,self.spool,self.binding_current=read_authority,spool,binding_current
        self.ttl_seconds,self.max_retained,self.clock=ttl_seconds,max_retained,clock
        self._manifest=None;self._permits={};self._tickets={}

    def _history_current(self,binding,sequence):
        anchor=self.spool._source_anchor
        return (isinstance(anchor,_Coverage) and anchor.current() and anchor.database_generation==binding.database_generation
            and anchor.key==binding_key(binding) and anchor.applied_sequence>=sequence
            and self.binding_current(binding))

    def supported(self):
        for handler in self.spool.registry.handlers.values():
            if handler.route=='geo.manifest.install.v1' and handler.revisions==GEO_REVISIONS:
                expected=geo_manifest_handler(revisions=GEO_REVISIONS,check_sha256=handler.check.definition_sha256,
                    apply_sha256=handler.apply.definition_sha256)
                if handler==expected:return True
        return False

    async def establish(self,operation_keys,*,binding):
        """Maintenance-only proof extraction from this actual spool/journal/native prefix.

        No caller manifest dictionary, receipt or boolean grants read authority.
        UNKNOWN/missing/outcome-only operations never activate a compact proof.
        """
        if type(operation_keys) is not tuple or not 2<=len(operation_keys)<=256 or len(set(operation_keys))!=len(operation_keys):
            raise ValueError('bounded exact manifest operation keys required')
        self._manifest=None
        async with asyncio.timeout(10):
            async with self.spool._lock:
                if not self.supported():raise SourceGateDenied('native GEO semantics unavailable')
                entries=[];used=0
                for key in operation_keys:
                    entry=await self.spool.get(key);c=entry.command
                    if (entry.seal_state!='ACCEPTED' or entry.apply_state!='APPLIED' or entry.outcome_state!='DURABLE'
                            or entry.accepted is None or entry.apply_receipt is None or entry.remote_outcome is None
                            or entry.database_generation!=binding.database_generation):raise SourceGateDenied('GEO outcome not durable')
                    payload=validate_geo_manifest_command(c)
                    handler=self.spool.registry.resolve(c)
                    if entry.handler_sha256!=handler.fingerprint:raise SourceGateDenied('GEO handler changed')
                    digest=await self.spool.journal.verify_accepted(entry.accepted,c)
                    if digest!=entry.command_sha256:raise SourceGateDenied('GEO accepted bytes differ')
                    receipt=entry.apply_receipt
                    if (receipt.status!='applied' or receipt.command_sha256!=digest or receipt.sequence!=c.sequence
                            or receipt.database_generation!=binding.database_generation
                            or hashlib.sha256(receipt.outcome).hexdigest()!=receipt.outcome_sha256):raise SourceGateDenied('GEO native receipt differs')
                    expected=canonical(dict(codec='rkb.outcome.v1',stream=self.spool.journal.stream,
                        command_sha256=digest,sequence=c.sequence,operation_key=c.operation_key,
                        database_generation=binding.database_generation,outcome_b64=base64.b64encode(receipt.outcome).decode(),
                        outcome_sha256=receipt.outcome_sha256,outcome_length=len(receipt.outcome)))
                    remote_key=self.spool.journal.prefix+'/outcomes/'+hashlib.sha256(c.operation_key.encode()).hexdigest()
                    if (entry.remote_outcome.key,entry.remote_outcome.command_sha256,entry.remote_outcome.outcome_sha256,entry.remote_outcome.database_generation)!=(
                            remote_key,digest,receipt.outcome_sha256,binding.database_generation):raise SourceGateDenied('GEO remote receipt binding differs')
                    remote=await self.spool.journal._read(remote_key,self.spool.journal.max_outcome_bytes)
                    if remote is None or remote.data!=expected or entry.remote_outcome.length!=len(expected):
                        raise SourceGateDenied('GEO remote outcome differs')
                    used+=len(c.encode())
                    if used>12*1024*1024:raise SourceGateDenied('GEO proof byte budget')
                    entries.append((entry,c,payload))
                if [c.sequence for _,c,_ in entries]!=list(range(entries[0][1].sequence,entries[0][1].sequence+len(entries))):
                    raise SourceGateDenied('GEO manifest phases not contiguous')
                first,last=entries[0],entries[-1]
                if first[2]['action']!='open' or last[2]['action']!='activate' or any(p['action']!='batch' for _,_,p in entries[1:-1]):
                    raise SourceGateDenied('GEO manifest phase ordering differs')
                c=last[1];header=first[2]
                bodies=[base64.b64decode(item['wire_b64']) for _,_,payload in entries[1:-1] for item in payload['items']]
                raw=base64.b64decode(header['header_prefix_b64'])+b','.join(bodies)+base64.b64decode(header['header_suffix_b64'])
                if len(raw)>4*1024*1024:raise SourceGateDenied('GEO manifest byte budget')
                manifest=GeoMigrationManifest.from_wire(json.loads(raw)).require_ready();plan=plan_geo_manifest(manifest)
                if (canonical(manifest.wire())!=raw or tuple(p.payload for _,p,_ in entries)!=plan.payloads
                        or any(p.ids!={'manifest_id':plan.manifest_id} or p.person_id!=c.person_id
                            or p.expected_revisions['manifest_index_sha256']!=plan.index_sha256 for _,p,_ in entries)
                        or not self._history_current(binding,c.sequence)):
                    raise SourceGateDenied('GEO native accepted manifest differs')
                proof=_Manifest(binding.database_generation,plan.manifest_id,
                    MappingProxyType({item.key:hashlib.sha256(canonical(item.wire())).hexdigest() for item in manifest.items}),
                    MappingProxyType({item.stable_id:frozenset(item.payload.provenance_keys) for item in manifest.items if item.payload.kind=='place'}),
                    c.sequence,c.operation_key,last[0].command_sha256)
                self._manifest=proof
                return plan.manifest_id

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
                if proof.hashes.get(item.key)!=digest or item.owner_person_id!=read.authority.person_id:
                    raise SourceGateDenied('GEO item bytes/private owner differ')
                if q.action=='identity':
                    if item.payload.kind!='place' or (q.entity_id is not None and item.stable_id!=q.entity_id) or (
                            q.poi_ref is not None and item.payload.canonical_poi_ref!=q.poi_ref):raise SourceGateDenied('GEO selector differs')
                    if q.name is not None:
                        if record.matched_wire_b64 is None:raise SourceGateDenied('GEO name proof missing')
                        matched,matched_hash=decode_item(record.matched_wire_b64)
                        if (proof.hashes.get(matched.key)!=matched_hash or matched.owner_person_id!=item.owner_person_id
                                or normalized(q.name) not in _name_projection(matched)
                                or (matched.key!=item.key and 'place:'+item.stable_id not in matched.dependencies)):
                            raise SourceGateDenied('GEO exact name proof differs')
                        if q.year is not None and matched.payload.kind=='source_geo_observation' and matched.payload.valid_year_from is not None and not matched.payload.valid_year_from<=q.year<=matched.payload.valid_year_to:
                            raise SourceGateDenied('GEO source time scope differs')
                elif (item.payload.kind!=q.category or not (('place:'+q.entity_id) in item.dependencies
                        or q.category=='provenance')):raise SourceGateDenied('GEO member scope differs')
                if q.action=='members' and q.category=='provenance':
                    if item.key not in proof.provenance_members.get(q.entity_id,()):raise SourceGateDenied('GEO exact provenance membership differs')
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
    def invalidate(self):self._manifest=None;self._permits.clear();self._tickets.clear()
