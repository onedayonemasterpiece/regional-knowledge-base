"""Resolved exact manifest commands, no journal/storage/native/model effects."""
import base64
import copy
from dataclasses import replace
import hashlib
import json

import pytest

from regional_knowledge.geo_migration import GeoMigrationManifest,InventoryCounts,canonical,sha
from regional_knowledge.geo_whitelist import build_geo_whitelist
from regional_knowledge.geo_manifest_commands import (GEO_REVISIONS,MAX_GEO_BATCH,
    plan_geo_manifest,validate_geo_manifest_command)
from test_geo_migration import snapshot,uid


def commands(plan):
    return plan.commands(first_sequence=17,operation_key='geo-migration-fixture',request_id='owner-request',
        person_id=uid(1),logical_time='2026-10-20T18:00:00Z')


def test_exact_manifest_bytes_reassemble_across_bounded_commands_without_self_hash_cycle():
    manifest=build_geo_whitelist(**snapshot())
    plan=plan_geo_manifest(manifest);resolved=commands(plan)
    assert [command.sequence for command in resolved]==list(range(17,17+len(resolved)))
    payloads=[validate_geo_manifest_command(command) for command in resolved]
    header=payloads[0];items=[entry for value in payloads[1:-1] for entry in value['items']]
    exact=base64.b64decode(header['header_prefix_b64'])+b','.join(base64.b64decode(item['wire_b64']) for item in items)+base64.b64decode(header['header_suffix_b64'])
    assert exact==canonical(manifest.wire())
    assert hashlib.sha256(exact).hexdigest()==plan.manifest_id==manifest.sha256
    assert sha(json.loads(base64.b64decode(header['index_b64'])))==plan.index_sha256
    assert payloads[-1]['action']=='activate'
    assert all(command.ids=={'manifest_id':manifest.sha256} and len(command.encode())<=1024*1024 for command in resolved)
    assert all((command.schema_revision,command.function_revision,command.executor_revision)==GEO_REVISIONS.wire() for command in resolved)
    assert plan==plan_geo_manifest(manifest)


@pytest.mark.parametrize('change',['raw_owner','lookup','bytes','phase','revision','manifest'])
def test_caller_changed_item_or_lookup_or_semantics_cannot_become_accepted_command(change):
    plan=plan_geo_manifest(build_geo_whitelist(**snapshot()));command=commands(plan)[1]
    value=json.loads(command.payload)
    if change=='lookup':value['items'][0]['lookup_names']=['arbitrary candidate']
    if change=='bytes':value['items'][0]['wire_b64']=base64.b64encode(b'{}').decode()
    if change=='raw_owner':
        entry=value['items'][0];wire=json.loads(base64.b64decode(entry['wire_b64']));wire['owner_person_id']='forged-owner'
        body=canonical(wire);entry.update(wire_b64=base64.b64encode(body).decode(),sha256=hashlib.sha256(body).hexdigest())
    command=replace(command,payload=canonical(value))
    if change=='phase':command=replace(command,expected_revisions=dict(command.expected_revisions,manifest_state='absent'))
    if change=='revision':command=replace(command,schema_revision='rkb-v4-sql001-008-v1')
    if change=='manifest':command=replace(command,ids={'manifest_id':'not-a-manifest-hash'})
    with pytest.raises(ValueError):validate_geo_manifest_command(command)


def test_missing_dependencies_or_empty_output_with_nonempty_inventory_never_produce_install_plan():
    data=snapshot();data['page_metadata']=[]
    with pytest.raises(ValueError,match='unresolved required dependencies'):
        plan_geo_manifest(build_geo_whitelist(**data))
    wire=build_geo_whitelist(**snapshot()).wire();wire['items']=[]
    with pytest.raises(ValueError,match='record counts mismatch'):GeoMigrationManifest.from_wire(wire)


def test_409_places_540_intents_are_batched_without_clipping_or_new_identities():
    data=snapshot();template=copy.deepcopy(data['entities'][0]);mention=copy.deepcopy(data['mentions'][0]);intent=copy.deepcopy(data['geo_intents'][0])
    nodes=[];mentions=[];intents=[]
    for index in range(409):
        node=copy.deepcopy(template);node['id']=uid(1000+index);nodes.append(node)
    for index in range(540):
        item=copy.deepcopy(mention);item['id']=uid(10000+index);item['entity_id']=nodes[index%409]['id'];mentions.append(item)
        observation=copy.deepcopy(intent);observation.update(request_id='geor_'+f'{index:032x}',mention_id=item['id'],entity_id=item['entity_id'])
        observation['mention_sha256']=sha(dict(doc=item['document_id'],revision=item['revision'],mention_id=item['id'],
            evidence=item['evidence'],name=item['exact_source_spelling']))
        intents.append(observation)
    data.update(entities=nodes,mentions=mentions,aliases=[],relations=[],geo_intents=intents,
        expected_counts=InventoryCounts(entities=409,geo_places=409,mentions=540,aliases=0,relations=0,geo_intents=540,accepted_geo_metadata=409))
    manifest=build_geo_whitelist(**data).require_ready();plan=plan_geo_manifest(manifest)
    resolved=commands(plan);batches=[json.loads(command.payload) for command in resolved[1:-1]]
    assert len(batches)>1 and all(len(command.payload)<=MAX_GEO_BATCH for command in resolved[1:-1])
    items=[json.loads(base64.b64decode(entry['wire_b64'])) for batch in batches for entry in batch['items']]
    assert len(items)==len(manifest.items)
    assert {item['stable_id'] for item in items if item['payload']['kind']=='place'}=={node['id'] for node in nodes}
    assert sum(item['payload']['kind']=='source_geo_observation' for item in items)==540


def test_accepted_installer_cannot_be_changed_between_manifest_phases():
    resolved=commands(plan_geo_manifest(build_geo_whitelist(**snapshot())))
    assert all(command.expected_revisions['installer_person_id']==uid(1) for command in resolved)
    for command in resolved:
        with pytest.raises(ValueError,match='accepted geographic installer'):
            validate_geo_manifest_command(replace(command,person_id=uid(99)))
