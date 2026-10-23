"""Select one accepted model batch and its explicit evidence dependencies.

No model choices are repeated. Later accepted corrections under the same keys
supersede older selections. Unrelated batches are not semantic prerequisites.
The caller verifies the original chain before consuming these private views.
"""
from __future__ import annotations

import copy
from uuid import UUID, uuid5


def select_batch(documents, index, *, source_id,text_only=False):
    if type(index) is not int or not 0 <= index < len(documents):
        raise ValueError('accepted batch index required')
    latest_pages = {}; latest_chunks = {}; latest_stories = {}; latest_entities = {}
    latest_facts = {}; latest_media = {}; chunks_by_id = {}
    for n, doc in enumerate(documents):
        payload = doc['model_payload']
        for p in payload['pages']: latest_pages[p['page_id']] = (n, p)
        for c in payload['chunks']:
            latest_chunks[c['chunk_key']] = (n, c)
            chunks_by_id[str(uuid5(UUID(source_id), 'chunk:1:'+c['chunk_key']))] = c['chunk_key']
        if not text_only:
            for s in payload.get('story_candidates', []): latest_stories[s['candidate_key']] = (n, s)
            for e in (payload.get('entity_candidates') or {}).get('entities', []): latest_entities[e['key']] = (n, e)
            for f in payload.get('poi_facts', []): latest_facts[f['candidate_key']] = (n, f)
            for m in payload.get('poi_media_links', []): latest_media[m['link_key']] = (n, m)

    selected = documents[index]['model_payload']
    page_ids = {p['page_id'] for p in selected['pages']}
    chunk_keys = {c['chunk_key'] for c in selected['chunks']}
    story_keys = set() if text_only else {s['candidate_key'] for s in selected.get('story_candidates', [])}
    entity_keys = set() if text_only else {e['key'] for e in (selected.get('entity_candidates') or {}).get('entities', [])}
    relations = [] if text_only else (selected.get('entity_candidates') or {}).get('relations', [])
    references = {} if text_only else dict((selected.get('entity_candidates') or {}).get('entity_refs', {}))
    facts = [] if text_only else [latest_facts[f['candidate_key']][1] for f in selected.get('poi_facts', [])]
    media = [] if text_only else [latest_media[m['link_key']][1] for m in selected.get('poi_media_links', [])]

    def need_chunk(ident):
        key = chunks_by_id.get(ident)
        if key is None: raise ValueError('batch evidence chunk unavailable')
        chunk_keys.add(key)

    for key in story_keys:
        s = latest_stories[key][1]
        entity_keys.update(s.get('entity_keys', []))
        for ref in s['evidence_refs']:
            page_ids.add(ref['page_id'])
            # Keep the model's accepted passage windows. Compilation checks
            # exact offsets and quotation against them; no new window is made.
            for ck, (_, chunk) in latest_chunks.items():
                if any(r['page_id'] == ref['page_id'] and r['region_key'] == ref['region_key']
                       for field in ('region_refs', 'span_refs') for r in chunk.get(field, [])):
                    chunk_keys.add(ck)
    for edge in relations:
        entity_keys.update((edge['source_key'], edge['target_key']))
        for proof in edge['evidence']: need_chunk(proof['chunk_id'])
    for key in entity_keys:
        if key not in latest_entities:
            if key not in references:
                # An explicit older entity-ref is still an accepted choice.
                for doc in documents:
                    refs = (doc['model_payload'].get('entity_candidates') or {}).get('entity_refs', {})
                    if key in refs: references[key] = refs[key]
            if key not in references: raise ValueError('batch entity key unavailable')
            continue
        node = latest_entities[key][1]
        for proof in [node['evidence'], *[a['evidence'] for a in node.get('aliases', [])]]:
            need_chunk(proof['chunk_id'])
    for fact in facts:
        page_ids.update(r['page_id'] for r in fact.get('evidence_refs', []))
    for item in media: page_ids.add(item['page_id'])
    for key in chunk_keys:
        c = latest_chunks[key][1]
        for field in ('region_refs', 'span_refs', 'footnote_refs')+(() if text_only else ('illustration_refs',)):
            page_ids.update(r['page_id'] for r in c.get(field, []))
    if page_ids - latest_pages.keys(): raise ValueError('batch evidence page unavailable')

    result = []
    for n, doc in enumerate(documents):
        view = copy.deepcopy(doc); p = view['model_payload']
        chosen = []
        if n == 0:
            for at, accepted in enumerate(documents):
                chosen.extend((page, image) for page, image in zip(accepted['model_payload']['pages'], accepted['issued_images_b64'], strict=True)
                    if page['page_id'] in page_ids and latest_pages[page['page_id']][0] == at)
        p['pages'] = [copy.deepcopy(page) for page, _ in chosen]
        view['issued_images_b64'] = [image for _, image in chosen]
        p['chunks'] = [copy.deepcopy(c) for k, (_, c) in latest_chunks.items() if k in chunk_keys] if n == 0 else []
        p['story_candidates'] = [copy.deepcopy(s) for k, (at, s) in latest_stories.items() if at == n and k in story_keys]
        nodes = [copy.deepcopy(e) for k, (at, e) in latest_entities.items() if at == n and k in entity_keys]
        p['entity_candidates'] = dict(entities=nodes, entity_refs=references if n == len(documents)-1 else {},
                                      relations=relations if n == len(documents)-1 else [])
        p['poi_facts'] = facts if n == len(documents)-1 else []
        p['poi_media_links'] = media if n == len(documents)-1 else []
        result.append(view)
    return result, frozenset(latest_pages[p][1]['physical_page_index'] for p in page_ids)
