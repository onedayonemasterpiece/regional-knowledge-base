"""Bounded resolved source graph, independent of a Story or geometry authority.

Resolution is deterministic over the importing model's explicit GraphBundle
choices and actual reviewed passage/region bytes. Replay receives these exact
resolved bytes and does no extraction, name merge, encoding or owner RPC.
"""
from __future__ import annotations

import hashlib
import json
from uuid import UUID,uuid5

from .entity_graph import GraphBundle,digest,entity_id,normalize_alias,valid_relation_shape
from .geo_migration import canonical,exact_uuid,owner_ref
from .source_import import MAX_IMPORT_BYTES

GRAPH_CODEC='rkb.source-graph.v1'
MAX_GRAPH_BYTES=256*1024


def _unique(records,key):
    result={}
    for record in records:
        ident=record[key]
        if ident in result and result[ident]!=record:raise ValueError('conflicting explicit source graph identity')
        result[ident]=record
    return sorted(result.values(),key=lambda item:item[key])


def _resolve(*,source_id,person_id,bundles,poi_facts,regions,passages,findings):
    exact_uuid(source_id);exact_uuid(person_id)
    if type(bundles) is not list or len(bundles)>32:raise ValueError('bounded source graph bundles required')
    parsed=[GraphBundle.model_validate(raw) for raw in bundles]
    raw_bundles=[bundle.model_dump(mode='json') for bundle in parsed]
    if len({digest(raw) for raw in raw_bundles})!=len(raw_bundles):raise ValueError('duplicate source graph bundle')
    raw_bundles.sort(key=digest)
    def evidence(e):
        raw=e.model_dump(mode='json');region=regions.get(raw['region_id']);passage=passages.get(raw['chunk_id'])
        if (region is None or passage is None or region['page_id']!=raw['page_id']
                or raw['page_id'] not in passage['page_ids'] or raw['region_id'] not in passage['region_ids']
                or raw['exact_quote'] not in passage['body']):raise ValueError('source graph evidence outside exact staged source')
        text=region['text'];begin=text.find(raw['exact_quote'])
        if begin<0 or text.find(raw['exact_quote'],begin+1)>=0:raise ValueError('source graph quote requires unambiguous exact region span')
        locator=dict(raw,evidence_ref='knowledge://chunks/'+raw['chunk_id'])
        return dict(locator,start=begin,end=begin+len(raw['exact_quote']),source_region_sha256=hashlib.sha256(text.encode()).hexdigest()),locator
    entities=[];refs=[];relations=[];bindings={};kinds={}
    for bundle in parsed:
        for n in bundle.entities:
            ident=str(n.entity_id or entity_id(UUID(source_id),n.key))
            if n.key in bindings and bindings[n.key]!=ident:raise ValueError('ambiguous explicit source graph key')
            bindings[n.key]=ident;kinds[n.key]=n.kind
        for key,ident in bundle.entity_refs.items():
            if key in bindings and bindings[key]!=str(ident):raise ValueError('ambiguous explicit source graph key')
            bindings[key]=str(ident);refs.append(dict(key=key,entity_id=str(ident)))
    for bundle in parsed:
        for n in bundle.entities:
            ident=bindings[n.key];proof,locator=evidence(n.evidence)
            if n.exact_source_spelling not in n.evidence.exact_quote:raise ValueError('source graph spelling absent from exact quote')
            aliases=[]
            for alias in n.aliases:
                normal=normalize_alias(alias.value)
                if not normal:raise ValueError('nonempty source alias required')
                alias_proof,_=evidence(alias.evidence)
                aliases.append(dict(alias_id=str(uuid5(UUID(ident),f'alias:{source_id}:1:{normal}')),
                    value=alias.value,normalized_value=normal,language=alias.language,alias_type=alias.alias_type,
                    time_scope=alias.time_scope,evidence=alias_proof,
                    research_sources=[s.model_dump(mode='json') for s in alias.research_sources]))
            if n.canonical_poi_ref:owner_ref(n.canonical_poi_ref,'street_story')
            for ref in n.map_refs:owner_ref(ref,'cartography')
            metadata=dict(review_note=n.review_note,poi_locator=n.poi_locator.model_dump(mode='json') if n.poi_locator else None,
                place_kind=n.place_kind,place_context=n.place_context,research_sources=[s.model_dump(mode='json') for s in n.research_sources],
                canonical_poi_ref=n.canonical_poi_ref,map_refs=n.map_refs,owner_reference_verification='not_verified',
                historical_geometry='not_verified')
            entities.append(dict(key=n.key,entity_id=ident,mode='reuse' if n.entity_id is not None else 'create',kind=n.kind,
                canonical_label=n.canonical_label,state=n.state,metadata=metadata,
                mention=dict(mention_id=str(uuid5(UUID(ident),f'mention:{source_id}:1:{digest(locator)}')),
                    exact_source_spelling=n.exact_source_spelling,evidence=proof),aliases=_unique(aliases,'alias_id')))
        for edge in bundle.relations:
            source,target=bindings[edge.source_key],bindings[edge.target_key]
            if source==target or (edge.source_key in kinds and edge.target_key in kinds and not valid_relation_shape(
                    edge.kind,kinds[edge.source_key],kinds[edge.target_key])):raise ValueError('invalid directional source graph endpoints')
            proof=[evidence(e)[0] for e in edge.evidence]
            rid=str(uuid5(UUID(source),f'edge:{target}:{edge.kind}:{source_id}:1:{digest(edge.model_dump(mode="json"))}'))
            relations.append(dict(relation_id=rid,source_id=source,target_id=target,kind=edge.kind,evidence=proof,
                time_scope=edge.time_scope,state=edge.state,review_note=edge.review_note))
    facts=[]
    if type(poi_facts) is not list or len(poi_facts)>32:raise ValueError('bounded independent source POI facts required')
    from .stage_graph import StagedPoiFact
    from .contracts import StagePoiFactInput
    allowed_kinds=StagePoiFactInput.model_fields['kind'].annotation.__args__
    for raw in poi_facts:
        fact=StagedPoiFact.model_validate(raw);wire=fact.model_dump(mode='json')
        if str(fact.candidate_id)!=str(uuid5(UUID(source_id),f'poi-fact:1:{fact.candidate_key}')) or fact.kind not in allowed_kinds:
            raise ValueError('exact source POI fact identity/type required')
        proof=[]
        for rid in wire['region_ids']:
            region=regions.get(rid)
            if region is None or region['page_id'] not in wire['page_ids']:raise ValueError('source POI hypothesis evidence unavailable')
            proof.append(dict(region_id=rid,page_id=region['page_id'],source_region_sha256=hashlib.sha256(region['text'].encode()).hexdigest()))
        if not proof or {p['page_id'] for p in proof}!=set(wire['page_ids']):raise ValueError('complete source POI region/page provenance required')
        facts.append(dict(wire,evidence=proof,geometry_acceptance='source_hypothesis_not_owner_geometry'))
    links=[]
    for finding in findings:
        keys=finding['entity_keys']
        if len(set(keys))!=len(keys) or any(key not in bindings for key in keys):raise ValueError('explicit finding entity key unresolved')
        if keys:links.append(dict(assertion_id=finding['assertion_id'],entity_keys=keys,entity_ids=[bindings[key] for key in keys]))
    entities=_unique(entities,'key');refs=_unique(refs,'key');relations=_unique(relations,'relation_id');facts=_unique(facts,'candidate_key')
    if len(entities)>128 or len(refs)>128 or len(relations)>256 or sum(len(e['aliases']) for e in entities)>512:
        raise ValueError('aggregate source graph bound')
    # A key cannot simultaneously declare a fresh entity and an existing ref.
    declared={e['key'] for e in entities}
    refs=[r for r in refs if r['key'] not in declared]
    value=dict(codec=GRAPH_CODEC,source_id=source_id,source_revision=1,owner_person_id=person_id,bundles=raw_bundles,
        poi_fact_inputs=[f.model_dump(mode='json') for f in sorted((StagedPoiFact.model_validate(f) for f in poi_facts),key=lambda f:f.candidate_key)],
        entities=entities,references=refs,relations=relations,poi_facts=facts,finding_entities=sorted(links,key=lambda l:l['assertion_id']))
    if len(canonical(value))>MAX_GRAPH_BYTES:raise ValueError('source graph byte bound')
    return value


def resolve_source_graph(graph,*,source_id,person_id,semantic):
    regions={str(r.region_id):dict(page_id=str(r.page_id),text=r.source_text) for page in graph.pages for r in page.regions}
    passages={str(c.chunk_id):dict(body=c.text,region_ids=[str(r) for r in c.region_ids],page_ids=[str(p) for p in c.page_ids]) for c in graph.chunks}
    return _resolve(source_id=source_id,person_id=person_id,bundles=[b.model_dump(mode='json') for b in graph.entity_candidates],
        poi_facts=[f.model_dump(mode='json') for f in graph.poi_facts],regions=regions,passages=passages,findings=semantic['findings'])


def validate_graph_import_payload(payload,*,source_id,person_id):
    from .source_import_v4 import validate_import_v4_payload
    if type(payload) is not bytes or not 1<=len(payload)<=MAX_IMPORT_BYTES:raise ValueError('bounded exact graph import required')
    doc=json.loads(payload)
    if canonical(doc)!=payload:raise ValueError('canonical source graph import required')
    if 'source_graph' not in doc:
        # Exact sourcev4 without graph remains a valid current11 source import.
        if any(finding['entity_keys'] for finding in doc.get('semantic',{}).get('findings',[])):
            raise ValueError('selected source entities require exact source graph')
        return validate_import_v4_payload(payload,source_id=source_id)
    value=doc['source_graph']
    if type(value) is not dict or len(canonical(value))>MAX_GRAPH_BYTES:raise ValueError('bounded source graph required')
    base={k:v for k,v in doc.items() if k!='source_graph'}
    validate_import_v4_payload(canonical(base),source_id=source_id)
    regions={r['region_id']:dict(page_id=r['page_id'],text=r['source_text']) for r in doc['semantic']['regions']}
    material={p['passage_id']:p for p in doc['source_manifest']['passages']}
    passages={p['passage_id']:dict(body=p['body'],region_ids=material[p['passage_id']]['region_ids'],
        page_ids=material[p['passage_id']]['page_ids']) for p in doc['passages']}
    expected=_resolve(source_id=source_id,person_id=person_id,bundles=value.get('bundles'),poi_facts=value.get('poi_fact_inputs'),
        regions=regions,passages=passages,findings=doc['semantic']['findings'])
    if expected!=value:raise ValueError('source graph resolved bytes/actor/provenance differ')
    return doc
