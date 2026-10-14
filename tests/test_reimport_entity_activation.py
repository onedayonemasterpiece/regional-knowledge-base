"""Synthetic reimport identity/cutover tests; vector quality is checked separately."""
from copy import deepcopy
from uuid import uuid4

import pytest

from regional_knowledge.contracts import Principal
from regional_knowledge.graph_service import GraphService, _merge_source_metadata
from regional_knowledge.object_store import UnavailableObjectStore
from regional_knowledge.sqlite_backend import SQLiteBackend
from regional_knowledge.sqlite_data import LocalContext, defaults
from regional_knowledge.supabase_backend import LexicalOnlyEmbedder
from test_model_led_locations import source, NoExternalCalls


@pytest.mark.asyncio
@pytest.mark.parametrize('explicit_uuid', [False, True])
async def test_reimport_preserves_identity_and_promotes_before_stories(tmp_path, monkeypatch, explicit_uuid):
    from regional_knowledge import sqlite_revision, story_ingestion
    backend = SQLiteBackend(corpus_path=tmp_path/'reimport.sqlite',
        embedder=LexicalOnlyEmbedder(), object_store=UnavailableObjectStore())
    actor = Principal(subject=str(uuid4()), client_id='test', issuer='test', access_token='synthetic')
    backend.corpus.put('rkb_users', [{**defaults('rkb_users'), 'id': actor.subject}])
    graph = GraphService(backend, NoExternalCalls())
    doc_id, evidence = source(backend, actor, 'На острове находилась читальня.')
    doc = backend.corpus.one('rkb_documents', doc_id)
    node = {'key': 'island', 'kind': 'poi_ref', 'canonical_label': 'острове',
            'exact_source_spelling': 'острове', 'evidence': evidence,
            'place_kind': 'island', 'place_context': 'Synthetic geographical island, not a municipality.',
            'map_refs': ['cartography://place/synthetic-island']}
    saved = await graph.stage(actor, doc_id, 1, {'entities': [node]})
    ident = saved['entities']['island']
    owner_ref = 'streetstory://poi/poi_ss_' + 'a'*24
    old = backend.corpus.one('rkb_entities', ident)
    backend.corpus.put('rkb_entities', [{**old, 'external_ref': owner_ref, 'state': 'candidate',
        'metadata': {**old['metadata'], 'resolution': 'model_selected'}}])
    # Stage the same verified source in a distinct revision with fresh page/chunk IDs.
    old_page = backend.corpus.one('rkb_pages', evidence['page_id'])
    old_region = backend.corpus.one('rkb_regions', evidence['region_id'])
    old_chunk = backend.corpus.one('rkb_chunks', evidence['chunk_id'])
    page_id, region_id, chunk_id, job_id = [str(uuid4()) for _ in range(4)]
    backend.corpus.put('rkb_pages', [{**old_page, 'id': page_id, 'revision': 2}])
    backend.corpus.put('rkb_regions', [{**old_region, 'id': region_id, 'page_id': page_id}])
    backend.corpus.put('rkb_chunks', [{**old_chunk, 'id': chunk_id, 'revision': 2,
        'page_ids': [page_id], 'region_ids': [region_id]}])
    next_evidence = {**evidence, 'chunk_id': chunk_id, 'page_id': page_id, 'region_id': region_id}
    current = {key: value for key, value in node.items()
               if key not in {'place_kind', 'place_context', 'map_refs'}}
    current['evidence'] = next_evidence
    if explicit_uuid:
        current['entity_id'] = ident
    await graph.stage(actor, doc_id, 2, {'entities': [current]},
                      staged_texts={chunk_id: old_chunk['source_text']})
    waiting = backend.corpus.one('rkb_entities', ident)
    assert waiting['revision'] == 1 and waiting['external_ref'] == owner_ref
    assert waiting['metadata']['_next_revision'] == 2
    assert waiting['metadata']['_next_metadata']['map_refs'] == node['map_refs']
    assert waiting['metadata']['_next_metadata']['place_kind'] == 'island'
    # Another accepted book may enrich the same entity while its seed reimport waits.
    other_id, other_evidence = source(backend, actor, 'Читальня стояла на острове.')
    await graph.stage(actor, other_id, 1, {'entities': [{**current, 'entity_id': ident,
        'evidence': other_evidence, 'map_refs': ['cartography://place/synthetic-second-source']} ]})
    waiting = backend.corpus.one('rkb_entities', ident)
    assert waiting['document_id'] == doc_id and waiting['revision'] == 1
    assert waiting['metadata']['_next_revision'] == 2
    backend.corpus.put('rkb_ingestion_jobs', [{**defaults('rkb_ingestion_jobs'),
        'id': job_id, 'document_id': doc_id, 'owner_user_id': actor.subject,
        'source_sha256': doc['source_sha256'], 'staged_revision': 2, 'state': 'ready'}])
    # Focus on atomic ordering; source/vector proof validation has independent tests.
    monkeypatch.setattr(sqlite_revision, 'validate', lambda *_: [])
    observed = []
    def stories(ctx, document, ingestion_id, revision):
        promoted = ctx.one('rkb_entities', ident)
        assert promoted['revision'] == 2
        assert promoted['external_ref'] == owner_ref
        assert promoted['metadata']['place_kind'] == 'island'
        assert promoted['metadata']['place_context'] == node['place_context']
        assert set(promoted['metadata']['map_refs']) == {
            'cartography://place/synthetic-island', 'cartography://place/synthetic-second-source'}
        assert not any(key.startswith('_next_') for key in promoted['metadata'])
        assert ctx.one('rkb_documents', doc_id)['active_revision'] == 2
        observed.append(ident)
    monkeypatch.setattr(story_ingestion, 'activate_stories', stories)
    with backend.corpus.connect() as db:
        db.execute('BEGIN IMMEDIATE')
        ctx = LocalContext(backend.corpus, db, actor.subject)
        read_rows = ctx.rows
        def bounded(table):
            if table in {'rkb_entities', 'rkb_entity_mentions'}:
                raise AssertionError('activation must not scan the global entity graph')
            return read_rows(table)
        ctx.rows = bounded
        params = {'p_document_id': doc_id, 'p_ingestion_id': job_id,
                  'p_revision': 2, 'p_poi_events': []}
        result = sqlite_revision.activate(ctx, params)
        assert result[0]['active_revision'] == 2
        assert sqlite_revision.activate(ctx, params)[0]['active_revision'] == 2
    assert observed == [ident]
    assert len(backend.corpus.rows('rkb_entities')) == 1
    await backend.aclose()


def test_source_metadata_merge_preserves_selected_refs_and_pending_revision():
    old = {'map_refs': ['cartography://place/first'], 'place_kind': 'island',
           'place_context': 'Synthetic context', 'review_note': 'Old proof',
           '_next_revision': 2, '_next_metadata': {'map_refs': ['cartography://place/first']}}
    before = deepcopy(old)
    merged = _merge_source_metadata(old, {'map_refs': ['cartography://place/second'],
                                         'place_context': None, 'review_note': None})
    assert old == before
    assert merged['place_context'] == old['place_context']
    assert merged['review_note'] == old['review_note']
    assert merged['_next_revision'] == 2
    assert merged['map_refs'] == ['cartography://place/first', 'cartography://place/second']
    assert _merge_source_metadata(merged, {'map_refs': merged['map_refs']}) == merged
