"""Bounded projections from an authorized legacy inventory, never a live export.

Callers provide already selected rows from their existing read-only snapshot.
The builder has no database/file/provider credential, opens no connection and
does not resolve Street Story or Cartography identities by name.
"""
from __future__ import annotations

import json
from collections import defaultdict

from .geo_migration import (Alias, Blocked, Edition, Exclusion, GeoManifestItem,
    GeoMigrationManifest, GeoObservation, InventoryCounts, LocatorHint, ExternalId, LegacyResolution, OwnerReference, Place, Provenance,
    Relation, exact_uuid, sha, MAX_PLACE_PROVENANCE)


class GeoWhitelistError(ValueError):
    pass


def _index(rows):
    result={}
    for row in rows:
        identity=exact_uuid(row['id'])
        if identity in result:raise GeoWhitelistError('duplicate_legacy_identity')
        result[identity]=row
    return result


def _json_field(value):
    value=json.loads(value) if type(value) is str else value
    if type(value) is not dict:raise GeoWhitelistError('invalid_legacy_geographic_metadata')
    return value


def build_geo_whitelist(*,source_inventory_sha256,expected_counts,entities,mentions,aliases=(),relations=(),
                        source_metadata=(),page_metadata=(),region_metadata=(),chunk_metadata=(),geo_intents=()):
    """Return deterministic closure or an explicit non-installable gap report.

    Only poi_ref nodes and located_in edges between those nodes qualify. There
    is no argument for old chunks, vectors, originals, story/case/draft bodies,
    queue leases or external canonical databases. Source/page *metadata* is
    consumed solely to construct visibly unanchored minimal locators.
    """
    batches=(entities,mentions,aliases,relations,source_metadata,page_metadata,region_metadata,chunk_metadata,geo_intents)
    if any(not isinstance(rows,(list,tuple)) or len(rows)>4096 for rows in batches) or sum(map(len,batches))>16384:
        raise GeoWhitelistError('bounded_geographic_inventory_required')
    nodes=_index(entities);docs=_index(source_metadata);pages=_index(page_metadata)
    regions=_index(region_metadata);chunks=_index(chunk_metadata)
    mention_index=_index(mentions);_index(aliases);_index(relations)
    places={key:value for key,value in nodes.items() if value.get('kind')=='poi_ref'}
    counts=InventoryCounts(entities=len(entities),geo_places=len(places),mentions=len(mentions),
        aliases=len(aliases),relations=len(relations),geo_intents=len(geo_intents),
        accepted_geo_metadata=sum(bool((node.get('metadata') or {}).get('geo_queue')) for node in places.values()))
    if type(expected_counts) is not InventoryCounts or expected_counts!=counts:
        raise GeoWhitelistError('geographic_inventory_coverage_mismatch')
    by_entity=defaultdict(list)
    for mention in mention_index.values():
        if mention.get('entity_id') in places:by_entity[mention['entity_id']].append(mention)
    output={}

    def add(stable_id,owner,revision,payload,dependencies=(),status=None):
        kind=payload.kind
        item=GeoManifestItem(stable_id=stable_id,owner_person_id=exact_uuid(owner),source_revision=str(revision),
            status=status or ('left_with_owner' if kind=='owner_reference' else
                'excluded_non_geo' if kind=='excluded_non_geo' else
                'blocked_missing_dependency' if kind=='blocked_geo_identity' else 'legacy_unanchored'),
            payload=payload,dependencies=tuple(sorted(set(dependencies))))
        if item.key in output and output[item.key]!=item:raise GeoWhitelistError('conflicting_geographic_identity')
        output[item.key]=item
        return item.key

    def block(row,table,reason,dependency):
        return add(row['id'],row['owner_user_id'],row.get('revision','unknown'),
            Blocked(source_table=table,reason=reason),dependencies=(dependency,))

    def provenance(record,document,revision,evidence,owner,kind='mention',ordinal=0):
        missing='provenance:'+record+(f':{ordinal}' if ordinal else '')
        doc=docs.get(document)
        if doc is None:return missing,'missing_source_metadata'
        if doc.get('owner_user_id')!=owner:return missing,'source_dependency_mismatch'
        digest=doc.get('source_sha256')
        if type(digest) is not str or len(digest)!=64 or any(c not in '0123456789abcdef' for c in digest):
            return missing,'missing_source_sha'
        page=pages.get(evidence.get('page_id'))
        if (page is None or page.get('document_id') not in (document,doc.get('source_document_id'))
                or page.get('revision')!=revision or type(page.get('physical_page_index')) is not int):
            return missing,'missing_page_locator'
        region=regions.get(evidence.get('region_id'));chunk=chunks.get(evidence.get('chunk_id'))
        if (region is None or region.get('page_id')!=page['id'] or chunk is None
                or chunk.get('document_id')!=document or chunk.get('revision')!=revision
                or page['id'] not in chunk.get('page_ids',()) or region['id'] not in chunk.get('region_ids',())):
            return missing,'missing_page_locator'
        if not doc.get('title'):return missing,'missing_source_metadata'
        catalog=doc.get('catalog') or {}
        item=Provenance(edition=Edition(legacy_document_id=document,source_sha256=digest,title=doc['title'],
                authors=tuple(doc.get('authors') or ()),publication_year=doc.get('publication_year'),
                edition_statement=catalog.get('edition_statement')),
            legacy_source_revision=revision,source_record_kind=kind,source_record_id=record,locator_ordinal=ordinal,
            page_id=page['id'],physical_page_index=page['physical_page_index'],
            printed_page_number=page.get('printed_page_number'),region_id=evidence.get('region_id'),
            legacy_chunk_id=evidence.get('chunk_id'),legacy_evidence_sha256=sha(evidence))
        identity=record+(f':{ordinal}' if ordinal else '')
        return add(identity,owner,revision,item),None

    for identity,row in sorted(nodes.items()):
        if identity not in places:
            add(identity,row['owner_user_id'],row.get('revision','unknown'),
                Exclusion(source_table='rkb_entities',reason='non_geo_entity'))
            continue
        owner=row['owner_user_id'];found=sorted(by_entity[identity],key=lambda x:x['id'])
        if not found:
            block(row,'rkb_entities','missing_mention','mention-for-place:'+identity);continue
        if len(found)>MAX_PLACE_PROVENANCE:raise GeoWhitelistError('geographic_provenance_bound')
        keys=[];failures=[]
        for mention in found:
            key,error=provenance(mention['id'],mention['document_id'],mention['revision'],
                mention.get('evidence') or {},owner)
            keys.append(key)
            if error:failures.append(error)
        spelling=found[0].get('exact_source_spelling')
        if not spelling:
            block(row,'rkb_entities','missing_source_label','source-spelling:'+identity);continue
        metadata=row.get('metadata') or {}
        names=tuple((metadata.get('poi_locator') or {}).get('names') or ())
        accepted=metadata.get('geo_queue')
        legacy_resolution=(LegacyResolution(contract_version=accepted['contract_version'],
            attempt_ref=accepted['attempt_id'],resolution=accepted['resolution'],identity_state=accepted['identity_state'],
            recorded_metadata_sha256=sha(accepted)) if accepted is not None else None)
        item=Place(canonical_label=row['canonical_label'],exact_source_spelling=spelling,
            state=row['state'],provenance_keys=tuple(sorted(set(keys))),metadata_names=names,
            canonical_poi_ref=row.get('external_ref'),legacy_resolution=legacy_resolution)
        add(identity,owner,row['revision'],item,keys,
            'blocked_missing_dependency' if failures else 'legacy_unanchored')
        refs=[('street_story',row['external_ref'])] if row.get('external_ref') else []
        refs.extend(('cartography',ref) for ref in metadata.get('map_refs') or ())
        for authority,ref in refs:
            # A composite *link* locator does not remint the owner's POI ID.
            add(identity+'@'+ref,owner,row['revision'],
                OwnerReference(entity_id=identity,owner=authority,ref=ref),('place:'+identity,))

    for row in sorted(aliases,key=lambda x:x['id']):
        entity=nodes.get(row['entity_id'])
        if entity is None:raise GeoWhitelistError('missing_alias_owner_identity')
        owner=entity['owner_user_id']
        if entity['id'] not in places:
            add(row['id'],owner,row.get('revision','unknown'),
                Exclusion(source_table='rkb_entity_aliases',reason='alias_of_non_geo_entity'));continue
        key,error=provenance(row['id'],row['document_id'],row['revision'],row.get('evidence') or {},owner,'alias')
        add(row['id'],owner,row['revision'],Alias(entity_id=row['entity_id'],value=row['value'],
            language=row.get('language'),alias_type=row['alias_type'],time_scope=row.get('time_scope'),provenance_key=key),
            ('place:'+entity['id'],key),'blocked_missing_dependency' if error else None)

    for row in sorted(relations,key=lambda x:x['id']):
        source=nodes.get(row['source_id']);target=nodes.get(row['target_id'])
        owner=(source or target or {}).get('owner_user_id')
        if owner is None:raise GeoWhitelistError('missing_relation_owner_identity')
        if row.get('kind')!='located_in' or row['source_id'] not in places or row['target_id'] not in places:
            add(row['id'],owner,row.get('revision','unknown'),
                Exclusion(source_table='rkb_entity_relations',reason='non_geographic_relation'));continue
        if target['owner_user_id']!=owner:raise GeoWhitelistError('cross_owner_geographic_relation')
        evidence=row.get('evidence') or ()
        if not 1<=len(evidence)<=8:raise GeoWhitelistError('geographic_relation_evidence_required')
        keys=[];errors=[]
        for index,proof in enumerate(evidence):
            key,error=provenance(row['id'],row['document_id'],row['revision'],proof,owner,'relation',index)
            keys.append(key)
            if error:errors.append(error)
        add(row['id'],owner,row['revision'],Relation(source_id=row['source_id'],target_id=row['target_id'],
            time_scope=row.get('time_scope'),provenance_keys=tuple(keys),claim_state=row['state']),
            ('place:'+row['source_id'],'place:'+row['target_id'],*keys),'blocked_missing_dependency' if errors else None)

    seen=set()
    for row in sorted(geo_intents,key=lambda x:x['request_id']):
        if row['request_id'] in seen:raise GeoWhitelistError('duplicate_geographic_intent')
        seen.add(row['request_id'])
        node=places.get(row['entity_id']);mention=mention_index.get(row['mention_id'])
        if node is None or mention is None:
            owner=row['actor_id']
            add(row['request_id'],owner,row['source_revision'],Blocked(source_table='rkb_geo_intents',
                reason='missing_geographic_entity' if node is None else 'missing_mention'),
                ('place:'+row['entity_id'],'provenance:'+row['mention_id']));continue
        scope=_json_field(row['scope_json']);period=_json_field(row['period_json'])
        locator=_json_field(row['locator_json'])
        if locator.get('coordinate_method')!='source_locator_unverified' or locator.get('historical_geometry')!='not_verified':
            raise GeoWhitelistError('legacy_geographic_coordinate_acceptance_unproved')
        if (row['actor_id']!=node['owner_user_id'] or scope.get('owner_id')!=row['actor_id'] or
                row['document_id']!=mention['document_id'] or row['source_revision']!=mention['revision'] or
                row['source_sha256']!=docs.get(row['document_id'],{}).get('source_sha256') or
                row['original_spelling']!=mention['exact_source_spelling'] or row['mention_sha256']!=sha(dict(
                    doc=row['document_id'],revision=mention['revision'],mention_id=mention['id'],
                    evidence=mention['evidence'],name=mention['exact_source_spelling']))):
            raise GeoWhitelistError('geographic_intent_source_or_owner_mismatch')
        names=json.loads(row['names_json']) if type(row['names_json']) is str else row['names_json']
        if locator.get('names')!=names or row['original_spelling'] not in names:
            raise GeoWhitelistError('geographic_intent_locator_name_mismatch')
        add(row['request_id'],row['actor_id'],row['source_revision'],GeoObservation(entity_id=row['entity_id'],
            mention_id=row['mention_id'],original_spelling=row['original_spelling'],names=tuple(names),
            provenance_key='provenance:'+row['mention_id'],time_scope=period.get('time_scope'),
            period_precision=period['precision'],valid_year_from=period.get('valid_year_from'),valid_year_to=period.get('valid_year_to'),
            locator_hint=LocatorHint(latitude=locator.get('latitude'),longitude=locator.get('longitude'),
                external_ids=tuple(ExternalId(namespace=key,value=value) for key,value in sorted((locator.get('external_ids') or {}).items()))),
            calendar=period['calendar'],resolution='owner_reference_candidate' if node.get('external_ref') else 'unresolved'),
            ('place:'+row['entity_id'],'provenance:'+row['mention_id']))

    # The graph is small, bounded migration metadata, not a serving corpus scan.
    # Mark dependent rows closed as well; never quietly omit missing originals.
    for _ in range(5):
        changed=False
        for key,item in tuple(output.items()):
            if item.status in ('excluded_non_geo','blocked_missing_dependency'):continue
            if any(dep not in output or output[dep].status in ('excluded_non_geo','blocked_missing_dependency') for dep in item.dependencies):
                output[key]=item.model_copy(update={'status':'blocked_missing_dependency'});changed=True
        if not changed:break
    missing=tuple(sorted({dep for item in output.values() for dep in item.dependencies
        if dep not in output or output[dep].status in ('excluded_non_geo','blocked_missing_dependency')}))
    return GeoMigrationManifest(source_inventory_sha256=source_inventory_sha256,
        inventory_counts=counts,
        items=tuple(output[key] for key in sorted(output)),missing_dependencies=missing)
