"""Exact bounded GEO manifest batches on the existing accepted-command stream.

No I/O, owner impersonation, metadata extraction or model choice occurs here.
The final native effect reassembles the exact canonical bytes and verifies the
whole manifest SHA before activating its immutable IDs.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass,replace
import hashlib
import json
import re

from .geo_migration import GeoManifestItem,GeoManifestHeader,GeoMigrationManifest,canonical,sha,normalized,exact_uuid
from .kaggle_apply import DependencySpec,HandlerSpec,MigrationTransition,PgFunction,Revisions
from .recovery_journal import ExactCommand

GEO_MANIFEST_INSTALL='geo.manifest.install.v1'
GEO_REVISIONS=Revisions('rkb-v4-sql001-011-v1','rkb-v4-source-story-cases-geo-v6','rkb-python-apply-v1')
GEO_V2_REVISIONS=Revisions('rkb-v4-sql001-012-v1','rkb-v4-source-story-cases-geo-v7','rkb-python-apply-v1')
GEO_DEPENDENCY_MIGRATION='geo-dependencies-v2'
GEO_PRECONDITIONS=frozenset({'manifest_state','manifest_index_sha256','installer_person_id'})
MAX_GEO_PAYLOAD=704*1024
MAX_GEO_BATCH=256*1024


def geo_manifest_dependency():
    return DependencySpec(GEO_MANIFEST_INSTALL,'geo_manifest',frozenset({'manifest_id'}),'actor',False)


def _digest(value):
    if type(value) is not str or re.fullmatch('[0-9a-f]{64}',value) is None:
        raise ValueError('exact geographic SHA256 required')
    return value


def _decode(value,limit):
    if type(value) is not str or len(value)>((limit+2)//3)*4:
        raise ValueError('geographic encoded byte bound')
    data=base64.b64decode(value,validate=True)
    if len(data)>limit or base64.b64encode(data).decode()!=value:
        raise ValueError('exact bounded geographic bytes required')
    return data


def _name_projection(item):
    payload=item.payload
    names=([payload.canonical_label,payload.exact_source_spelling,*payload.metadata_names] if payload.kind=='place'
           else [payload.value] if payload.kind=='alias'
           else list(payload.names) if payload.kind=='source_geo_observation' else [])
    result=sorted({normalized(name) for name in names})
    if any(not name or len(name)>4096 or len(name.encode())>16384 for name in result):
        raise ValueError('geographic lookup key byte bound')
    return result


def _header(manifest):
    value=manifest.wire();value['items']=[]
    raw=canonical(value);marker=b'"items":[]'
    at=raw.index(marker)+len(b'"items":[')
    return raw[:at],raw[at:]


def _validate_payload(raw,manifest_id,index_sha256):
    if type(raw) is not bytes or not 1<=len(raw)<=MAX_GEO_PAYLOAD:
        raise ValueError('bounded geographic command required')
    value=json.loads(raw)
    if canonical(value)!=raw or type(value) is not dict or value.get('codec')!='rkb.geo-manifest-command.v1':
        raise ValueError('exact canonical geographic command required')
    action=value.get('action')
    if action=='open':
        if set(value)!={'codec','action','header_prefix_b64','header_suffix_b64','index_b64'}:
            raise ValueError('exact geographic manifest header required')
        prefix=_decode(value['header_prefix_b64'],8192);suffix=_decode(value['header_suffix_b64'],8192)
        empty=json.loads(prefix+suffix)
        if empty.pop('items',None)!=[]:raise ValueError('empty geographic staging header required')
        header=GeoManifestHeader.model_validate_json(canonical(empty))
        if header.missing_dependencies or canonical(dict(header.model_dump(mode='json'),items=[]))!=prefix+suffix:
            raise ValueError('exact empty-items geographic header required')
        index_bytes=_decode(value['index_b64'],MAX_GEO_PAYLOAD)
        index=json.loads(index_bytes)
        if canonical(index)!=index_bytes or type(index) is not list or not 0<=len(index)<=4096 or sha(index)!=index_sha256:
            raise ValueError('exact geographic item index required')
        keys=[]
        for entry in index:
            if (type(entry) is not dict or set(entry)!={'key','sha256'} or type(entry['key']) is not str
                    or not 1<=len(entry['key'])<=512):raise ValueError('bounded geographic index entry required')
            _digest(entry['sha256']);keys.append(entry['key'])
        if keys!=sorted(keys) or len(set(keys))!=len(keys):
            raise ValueError('unique deterministic geographic index required')
    elif action=='batch':
        if set(value)!={'codec','action','items'} or len(raw)>MAX_GEO_BATCH:
            raise ValueError('exact bounded geographic item batch required')
        if type(value['items']) is not list or not 1<=len(value['items'])<=128:
            raise ValueError('bounded geographic items required')
        keys=[]
        for entry in value['items']:
            if type(entry) is not dict or set(entry)!={'key','sha256','wire_b64','lookup_names'}:
                raise ValueError('exact geographic item bytes required')
            body=_decode(entry['wire_b64'],64*1024)
            wire=json.loads(body);item=GeoManifestItem.from_wire(wire)
            if (canonical(item.wire())!=body or entry['key']!=item.key or
                    _digest(entry['sha256'])!=hashlib.sha256(body).hexdigest() or
                    entry['lookup_names']!=_name_projection(item) or
                    item.status=='blocked_missing_dependency'):
                raise ValueError('geographic item/index/lookup proof differs')
            keys.append(item.key)
        if keys!=sorted(keys) or len(set(keys))!=len(keys):raise ValueError('unique ordered geographic batch required')
    elif action=='activate':
        if set(value)!={'codec','action'}:raise ValueError('exact geographic activation required')
    else:raise ValueError('unsupported fixed geographic action')
    return value


def validate_geo_manifest_command(command):
    if (type(command) is not ExactCommand or command.kind!='domain_command' or command.route!=GEO_MANIFEST_INSTALL or
            (command.schema_revision,command.function_revision,command.executor_revision) not in (GEO_REVISIONS.wire(),GEO_V2_REVISIONS.wire()) or
            set(command.ids)!={'manifest_id'} or set(command.expected_revisions)!=GEO_PRECONDITIONS):
        raise ValueError('exact fresh11 geographic manifest command required')
    exact_uuid(command.person_id);_digest(command.ids['manifest_id'])
    if exact_uuid(command.expected_revisions['installer_person_id'])!=command.person_id:
        raise ValueError('exact accepted geographic installer identity required')
    index=_digest(command.expected_revisions['manifest_index_sha256'])
    value=_validate_payload(command.payload,command.ids['manifest_id'],index)
    if command.expected_revisions['manifest_state']!=('absent' if value['action']=='open' else 'staging'):
        raise ValueError('exact geographic manifest state precondition required')
    return value


@dataclass(frozen=True)
class GeoManifestInstallPlan:
    manifest_id:str
    index_sha256:str
    payloads:tuple[bytes,...]

    def commands(self,*,first_sequence,operation_key,request_id,person_id,logical_time):
        if type(first_sequence) is not int or first_sequence<1:raise ValueError('exact ordered starting sequence required')
        result=[]
        for offset,payload in enumerate(self.payloads):
            action=json.loads(payload)['action']
            command=ExactCommand(sequence=first_sequence+offset,operation_key=operation_key+':'+str(offset),
                request_id=request_id,person_id=person_id,route=GEO_MANIFEST_INSTALL,logical_time=logical_time,
                schema_revision=GEO_REVISIONS.schema,function_revision=GEO_REVISIONS.function,executor_revision=GEO_REVISIONS.executor,
                ids={'manifest_id':self.manifest_id},expected_revisions=dict(manifest_state='absent' if action=='open' else 'staging',
                    manifest_index_sha256=self.index_sha256,installer_person_id=person_id),payload=payload)
            validate_geo_manifest_command(command)
            if len(command.encode())>1024*1024:raise ValueError('existing canonical command byte bound exceeded')
            result.append(command)
        return tuple(result)


def plan_geo_manifest(manifest):
    if type(manifest) is not GeoMigrationManifest:raise TypeError('resolved geographic whitelist required')
    # Revalidate even a frozen object: model_copy is not a capability boundary.
    manifest=GeoMigrationManifest.from_wire(manifest.wire()).require_ready()
    prefix,suffix=_header(manifest)
    entries=[];index=[]
    for item in manifest.items:
        body=canonical(item.wire());digest=hashlib.sha256(body).hexdigest()
        index.append(dict(key=item.key,sha256=digest))
        entries.append(dict(key=item.key,sha256=digest,wire_b64=base64.b64encode(body).decode(),lookup_names=_name_projection(item)))
    start=canonical(dict(codec='rkb.geo-manifest-command.v1',action='open',
        header_prefix_b64=base64.b64encode(prefix).decode(),header_suffix_b64=base64.b64encode(suffix).decode(),
        index_b64=base64.b64encode(canonical(index)).decode()))
    payloads=[start];batch=[]
    for entry in entries:
        candidate=canonical(dict(codec='rkb.geo-manifest-command.v1',action='batch',items=[*batch,entry]))
        if len(candidate)>MAX_GEO_BATCH or len(batch)==128:
            if not batch:raise ValueError('single geographic item byte bound')
            payloads.append(canonical(dict(codec='rkb.geo-manifest-command.v1',action='batch',items=batch)))
            batch=[]
        batch.append(entry)
    if batch:payloads.append(canonical(dict(codec='rkb.geo-manifest-command.v1',action='batch',items=batch)))
    payloads.append(canonical(dict(codec='rkb.geo-manifest-command.v1',action='activate')))
    result=GeoManifestInstallPlan(manifest.sha256,sha(index),tuple(payloads))
    for payload in result.payloads:_validate_payload(payload,result.manifest_id,result.index_sha256)
    return result


def geo_manifest_handler(*,revisions,check_sha256,apply_sha256):
    if revisions not in (GEO_REVISIONS,GEO_V2_REVISIONS):raise ValueError('ordered geographic revision barrier required')
    version='v2' if revisions==GEO_V2_REVISIONS else 'v1'
    return HandlerSpec('domain_command',GEO_MANIFEST_INSTALL,revisions,
        PgFunction('check_geo_manifest_install_'+version,check_sha256),PgFunction('apply_geo_manifest_install_'+version,apply_sha256),
        GEO_PRECONDITIONS,dependency=geo_manifest_dependency())


def geo_dependency_transition_handler(*,artifact_sha256,check_sha256,apply_sha256):
    """A trusted installed schema12 artifact, activated after the exact old tail."""
    return HandlerSpec('schema_transition','geo.dependencies.upgrade.v2',GEO_REVISIONS,
        PgFunction('check_geo_dependencies_v2_transition',check_sha256),
        PgFunction('apply_geo_dependencies_v2_transition',apply_sha256),
        frozenset({'schema','function','executor','migration'}),
        MigrationTransition(GEO_DEPENDENCY_MIGRATION,artifact_sha256,None,GEO_V2_REVISIONS))


def geo_dependency_repair_commands(originals,*,rejected_sequence,artifact_sha256):
    """Resolve a repair from frozen inputs, after retiring only unsent reservations.

    This function grants no authority and changes no queue/journal state. The
    caller must prove the rejected outcome is durable and every later original
    reservation was never sent before installing the returned continuation.
    """
    _digest(artifact_sha256)
    originals=tuple(originals)
    if not originals or len(originals)>4096:
        raise ValueError('bounded original GEO plan required')
    for command in originals:
        validate_geo_manifest_command(command)
        if (command.schema_revision,command.function_revision,command.executor_revision)!=GEO_REVISIONS.wire():
            raise ValueError('original schema11 GEO plan required')
    if (any(b.sequence!=a.sequence+1 for a,b in zip(originals,originals[1:]))
            or any(command.ids!=originals[0].ids or command.person_id!=originals[0].person_id for command in originals)):
        raise ValueError('same actor and contiguous original manifest required')
    tail=tuple(command for command in originals if command.sequence>=rejected_sequence)
    if not tail or tail[0].sequence!=rejected_sequence or json.loads(tail[0].payload)['action']!='batch':
        raise ValueError('exact rejected GEO batch required')
    failed=tail[0];prefix='geo-repair12:'+failed.ids['manifest_id']+':'
    transition=replace(failed,sequence=rejected_sequence+1,operation_key=prefix+'migration',
        kind='schema_transition',route='geo.dependencies.upgrade.v2',ids={},
        expected_revisions={'schema':GEO_REVISIONS.schema,'function':GEO_REVISIONS.function,
            'executor':GEO_REVISIONS.executor,'migration':''},
        payload=canonical({'migration_id':GEO_DEPENDENCY_MIGRATION,
            'artifact_sha256':artifact_sha256,'predecessor':None}))
    retries=tuple(replace(command,sequence=transition.sequence+offset,
        operation_key=prefix+str(command.sequence),schema_revision=GEO_V2_REVISIONS.schema,
        function_revision=GEO_V2_REVISIONS.function,executor_revision=GEO_V2_REVISIONS.executor)
        for offset,command in enumerate(tail,start=1))
    return (transition,*retries)
