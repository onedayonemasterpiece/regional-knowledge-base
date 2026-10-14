"""Synthetic source-only acceptance for bounded, same-pass knowledge metadata."""
from __future__ import annotations

import json
from uuid import UUID, uuid4

import pytest

from regional_knowledge.contracts import StagePageInput
from regional_knowledge.entity_graph import GraphBundle, entity_id
from regional_knowledge.ingestion_enrichment import knowledge_status
from regional_knowledge.stage_graph import StagedGraph, compile_model_stage, merge_stage
from regional_knowledge.story_contracts import StageStoryCandidateInput
from regional_knowledge.story_ingestion import persist_stage, activate_stories, extraction_status
from regional_knowledge.sqlite_data import LocalContext
from regional_knowledge.story_calendar import StoryCalendar
from test_story_ingestion import fixture, accept_source  # reuse the existing isolated corpus


def enriched(fixture):
    corpus, registry, principal, doc, job, _graph, page, candidate = fixture
    corpus.put('rkb_users', [{'id': principal.subject, 'status': 'active'}])
    text = '3 мая 1901 года Анна открыла читальню на острове. В неё пришли 12 гостей.'
    raw = page.model_dump(mode='json')
    raw['regions'][0]['source_text'] = text
    raw['knowledge_reviewed'] = True
    page = StagePageInput.model_validate(raw)
    from regional_knowledge.contracts import StageChunkInput
    graph = merge_stage(StagedGraph(revision=1), compile_model_stage(
        StagedGraph(revision=1), document_id=doc['id'], revision=1, pages=[page],
        chunks=[StageChunkInput(chunk_key='opening', title='Synthetic opening',
                region_refs=[{'page_id': page.page_id, 'region_key': 'body'}])]))
    proof = {'chunk_id': str(graph.chunks[0].chunk_id), 'page_id': page.page_id,
             'region_id': str(graph.pages[0].regions[0].region_id), 'exact_quote': text}
    bundle = GraphBundle(entities=[
        {'key': 'anna', 'kind': 'person', 'canonical_label': 'Анна',
         'exact_source_spelling': 'Анна', 'evidence': proof},
        {'key': 'island', 'kind': 'poi_ref', 'canonical_label': 'Остров из синтетического источника',
         'exact_source_spelling': 'острове', 'place_kind': 'island', 'evidence': proof},
        {'key': 'reading-room', 'kind': 'organization', 'canonical_label': 'Читальня',
         'exact_source_spelling': 'читальню', 'evidence': proof},
        {'key': 'opening', 'kind': 'event', 'canonical_label': 'Открытие читальни',
         'exact_source_spelling': 'открыла читальню', 'evidence': proof},
    ])
    graph = graph.model_copy(update={'entity_candidates': [bundle]})
    for node in bundle.entities:
        corpus.put('rkb_entities', [{'id': str(entity_id(UUID(doc['id']), node.key)),
            'owner_user_id': principal.subject, 'document_id': doc['id'], 'revision': 1,
            'kind': node.kind, 'canonical_label': node.canonical_label, 'state': 'candidate',
            'metadata': {}}])
    candidate = StageStoryCandidateInput(
        candidate_key='opening', title='Открытие читальни',
        summary='Синтетический источник сообщает об открытии читальни.',
        proposition='По источнику Анна открыла читальню.', attributed_to='Аноним',
        evidence_refs=[{'page_id': page.page_id, 'region_key': 'body', 'original_excerpt': text}],
        entity_keys=['anna', 'island', 'reading-room', 'opening'],
        event_dates=[{'original_date_text': '3 мая 1901 года', 'precision': 'day',
                      'calendar': 'unspecified', 'year': 1901, 'month': 5, 'day': 3,
                      'rationale': 'В синтетическом источнике дата напечатана явно.'}],
        observations=[{'original_value_text': '12', 'value': '12', 'metric_key': 'visitors',
                       'unit_code': 'persons', 'subject_label': 'Читальня',
                       'period_text': '3 мая 1901 года', 'period_start_year': 1901,
                       'period_end_year': 1901, 'method_note': 'Число явно сообщено источником.',
                       'rationale': 'Сохранено заявленное количество, без конверсии единиц.'}])
    return corpus, registry, principal, doc, job, graph, page, candidate


def test_same_pass_date_quantity_and_all_graph_kinds_survive_activation(fixture):
    corpus, registry, principal, doc, job, graph, page, candidate = enriched(fixture)
    persist_stage(corpus, principal, job, graph, [page], [candidate])
    assert knowledge_status(corpus, job)['reviewed_pages'] == 1
    assert knowledge_status(corpus, job)['state'] != 'pass_completed'
    result = accept_source(corpus, principal, doc, job, graph)
    assert result == {'accepted': 1, 'reused': 0, 'needs_review': 0}
    status = knowledge_status(corpus, {**job, 'state': 'finalized'})
    assert status['state'] == 'pass_completed'
    assert status['complete_extraction_claimed'] is False
    assert status['retrieval_acceptance'] == 'not_evaluated'
    with corpus.connect() as db:
        record = db.execute('SELECT * FROM story_records').fetchone()
        snap = json.loads(record['snapshot'])
        assert {x['kind'] for x in snap['links']} == {'person', 'place', 'organization', 'event'}
        date = db.execute('SELECT * FROM story_event_dates').fetchone()
        assert (date['year'], date['month'], date['day'], date['calendar']) == (1901, 5, 3, 'unspecified')
        assert db.execute('SELECT value_decimal FROM story_observations').fetchone()[0] == '12'
    assert registry.get(principal, record['id'], view='event_dates_page')['items'][0]['proofs']
    # Explicit unspecified calendar is not silently converted to Gregorian.
    assert StoryCalendar(registry).query(principal, month=5, day=3, calendar='gregorian')['items'] == []
    assert StoryCalendar(registry).query(principal, month=5, day=3, calendar='unspecified')['items']
    with corpus.connect() as db:
        ctx = LocalContext(corpus, db, principal.subject)
        assert activate_stories(ctx, doc, job['id'], 1)['accepted'] == 0
        assert db.execute('SELECT count(*) FROM story_records').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM story_event_dates').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM story_observations').fetchone()[0] == 1


def test_bad_date_preserves_story_quantity_and_resumable_pending_item(fixture):
    corpus, registry, principal, doc, job, graph, page, candidate = enriched(fixture)
    data = candidate.model_dump(mode='json')
    data['event_dates'][0]['original_date_text'] = '4 мая 1901 года'
    candidate = StageStoryCandidateInput.model_validate(data)
    persist_stage(corpus, principal, job, graph, [page], [candidate])
    result = accept_source(corpus, principal, doc, job, graph)
    assert result['accepted'] == 1 and result['needs_review'] == 1
    status = knowledge_status(corpus, {**job, 'state': 'finalized'})
    assert status['state'] == 'awaiting_agent'
    assert status['pending_candidates'][0]['story_id']
    assert status['pending_candidates'][0]['issues'][0]['kind'] == 'event_dates'
    assert extraction_status(corpus, {**job, 'state': 'finalized'})['saved_candidates'] == 1
    with corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM story_records').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM story_event_dates').fetchone()[0] == 0
        assert db.execute('SELECT count(*) FROM story_observations').fetchone()[0] == 1
    # Restaging corrected model output uses the existing source/story identity.
    data['event_dates'][0]['original_date_text'] = '3 мая 1901 года'
    persist_stage(corpus, principal, job, graph, [], [StageStoryCandidateInput.model_validate(data)])
    replay = accept_source(corpus, principal, doc, job, graph)
    assert replay['accepted'] == 0 and replay['reused'] == 1 and replay['needs_review'] == 0
    with corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM story_records').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM story_event_dates').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM story_observations').fetchone()[0] == 1


def test_foreign_entity_and_unknown_key_do_not_turn_into_links(fixture):
    corpus, registry, principal, doc, job, graph, page, candidate = enriched(fixture)
    foreign_id = str(entity_id(UUID(doc['id']), 'anna'))
    old = corpus.one('rkb_entities', foreign_id)
    corpus.put('rkb_entities', [{**old, 'owner_user_id': str(uuid4())}])
    candidate = candidate.model_copy(update={'entity_keys': ['anna', 'missing', 'island']})
    persist_stage(corpus, principal, job, graph, [page], [candidate])
    result = accept_source(corpus, principal, doc, job, graph)
    assert result['accepted'] == 1 and result['needs_review'] == 1
    with corpus.connect() as db:
        snap = json.loads(db.execute('SELECT snapshot FROM story_records').fetchone()[0])
        assert len(snap['links']) == 1 and snap['links'][0]['kind'] == 'place'
        assert db.execute('SELECT count(*) FROM story_event_dates').fetchone()[0] == 1
    pending = knowledge_status(corpus, job)['pending_candidates'][0]['issues']
    assert {x['reason'] for x in pending} == {'unknown_graph_key', 'owned_active_entity_required'}


def test_legacy_review_is_not_backfilled_and_page_edit_clears_coverage(fixture):
    corpus, registry, principal, doc, job, graph, page, candidate = enriched(fixture)
    legacy = page.model_copy(update={'knowledge_reviewed': False})
    persist_stage(corpus, principal, job, graph, [legacy], [])
    assert knowledge_status(corpus, job)['next_page_index'] == 0
    persist_stage(corpus, principal, job, graph, [page], [])
    assert knowledge_status(corpus, job)['next_page_index'] is None
    persist_stage(corpus, principal, job, graph, [legacy], [])
    assert knowledge_status(corpus, job)['reviewed_pages'] == 0
    registry.migrate()
    assert knowledge_status(corpus, job)['reviewed_pages'] == 0


def test_metadata_schema_uses_same_date_rules_and_never_invents_precision():
    from regional_knowledge.story_contracts import StageEventDateInput
    data = {'original_date_text': '1901', 'precision': 'year', 'year': 1901,
            'rationale': 'Источник сообщает только год события.'}
    assert StageEventDateInput(**data).month is None
    with pytest.raises(ValueError):
        StageEventDateInput(**{**data, 'day': 3, 'month': 5})
    with pytest.raises(ValueError):
        StageEventDateInput(**{**data, 'evidence_indexes': [4]})
    with pytest.raises(ValueError):
        StagePageInput(page_id=str(uuid4()), physical_page_index=0, knowledge_reviewed=True)


@pytest.mark.asyncio
async def test_finalized_metadata_repair_uses_same_stage_and_clears_checkpoint(fixture, monkeypatch):
    from types import SimpleNamespace
    from regional_knowledge import stage_service
    from regional_knowledge.contracts import BookIngestOutput
    corpus, registry, principal, doc, job, graph, page, candidate = enriched(fixture)
    bad = candidate.model_dump(mode='json')
    bad['event_dates'][0]['original_date_text'] = '4 мая 1901 года'
    persist_stage(corpus, principal, job, graph, [page], [StageStoryCandidateInput.model_validate(bad)])
    accept_source(corpus, principal, doc, job, graph)
    finalized = {**job, 'state': 'finalized'}
    corpus.put('rkb_ingestion_jobs', [finalized])
    before_doc = corpus.one('rkb_documents', doc['id'])
    assert knowledge_status(corpus, finalized)['needs_review_candidates'] == 1
    async def load(_service, _row):
        return graph
    async def document(_service, _principal, _id):
        return corpus.authorize(_principal.subject, _id, owner=True)
    monkeypatch.setattr(stage_service, '_load_graph', load)
    monkeypatch.setattr(stage_service, '_document_row', document)
    service = SimpleNamespace(corpus=corpus, _ingestion_output=lambda row, message: BookIngestOutput(
        state='finalized', ingestion_id=row['id'], document_id=row['document_id'],
        next_action='done', message=message))
    payload = {'story_candidates': [candidate.model_dump(mode='json')]}
    out = await stage_service._stage_accepted_enrichment(service, principal, finalized, payload)
    assert out.next_action == 'done' and not out.warnings
    assert knowledge_status(corpus, finalized)['needs_review_candidates'] == 0
    assert knowledge_status(corpus, finalized)['state'] == 'pass_completed'
    assert before_doc == corpus.one('rkb_documents', doc['id'])
    # Replay through exactly the same route is idempotent.
    await stage_service._stage_accepted_enrichment(service, principal, finalized, payload)
    with corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM story_records').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM story_event_dates').fetchone()[0] == 1
    with pytest.raises(ValueError, match='metadata only'):
        await stage_service._stage_accepted_enrichment(service, principal, finalized,
                                                       {**payload, 'pages': [page.model_dump(mode='json')]})
    corpus.put('rkb_documents', [{**before_doc, 'active_revision': 2}])
    with pytest.raises(ValueError, match='exact active'):
        await stage_service._stage_accepted_enrichment(service, principal, finalized, payload)


def test_interrupted_finalized_metadata_never_looks_completed(fixture):
    corpus, registry, principal, doc, job, graph, page, candidate = enriched(fixture)
    persist_stage(corpus, principal, job, graph, [page], [candidate])
    accept_source(corpus, principal, doc, job, graph)
    finalized = {**job, 'state': 'finalized'}
    corpus.put('rkb_ingestion_jobs', [finalized])
    persist_stage(corpus, principal, finalized, graph, [], [candidate], allow_finalized=True)
    status = knowledge_status(corpus, finalized)
    assert status['state'] == 'awaiting_agent' and status['staged_candidates'] == 1
    with corpus.connect() as db:
        activate_stories(LocalContext(corpus, db, principal.subject), doc, job['id'], 1)
    assert knowledge_status(corpus, finalized)['state'] == 'pass_completed'


@pytest.mark.asyncio
async def test_status_by_document_resolves_latest_owned_checkpoint_without_full_scan(fixture, monkeypatch):
    from regional_knowledge.sqlite_backend import SQLiteBackend
    from regional_knowledge.supabase_backend import SupabaseRestBackend
    from regional_knowledge.contracts import BookIngestOutput
    corpus, registry, principal, doc, job, graph, page, candidate = enriched(fixture)
    newer = {**job, 'id': str(uuid4()), 'staged_revision': 2, 'state': 'staged'}
    corpus.put('rkb_ingestion_jobs', [newer])
    backend = object.__new__(SQLiteBackend)
    backend.corpus = corpus
    async def lookup(**kwargs):
        return corpus.one('rkb_ingestion_jobs', kwargs['ingestion_id'])
    backend._ingestion_row = lookup
    called = []
    async def base(_self, **kwargs):
        called.append(kwargs['ingestion_id'])
        return BookIngestOutput(state='staged', message='Synthetic status',
                                ingestion_id=kwargs['ingestion_id'], document_id=doc['id'])
    monkeypatch.setattr(SupabaseRestBackend, 'book_ingest', base)
    out = await backend.book_ingest(command='status', principal=principal,
        document_id=doc['id'], ingestion_id=None, payload=None, file=None, cursor=None)
    assert out.ingestion_id == newer['id'] and called == [newer['id']]
    with pytest.raises((PermissionError, LookupError)):
        await backend.book_ingest(command='status', principal=principal.model_copy(update={'subject': str(uuid4())}),
            document_id=doc['id'], ingestion_id=None, payload=None, file=None, cursor=None)
    with corpus.connect() as db:
        plan = db.execute("""EXPLAIN QUERY PLAN SELECT row_key FROM corpus_rows
            WHERE table_name='rkb_ingestion_jobs' AND json_extract(payload,'$.document_id')=?
              AND json_extract(payload,'$.owner_user_id')=?
            ORDER BY CAST(json_extract(payload,'$.staged_revision') AS INTEGER) DESC LIMIT 1""",
            (doc['id'], principal.subject)).fetchall()
        assert any('ingestion_status_by_source' in str(tuple(row)) for row in plan)


def test_concurrent_source_replays_keep_one_story_date_and_quantity(fixture):
    from concurrent.futures import ThreadPoolExecutor
    corpus, registry, principal, doc, job, graph, page, candidate = enriched(fixture)
    def replay(_):
        persist_stage(corpus, principal, job, graph, [page], [candidate])
        return accept_source(corpus, principal, doc, job, graph)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(replay, range(24)))
    assert sum(x['accepted'] for x in results) == 1
    with corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM story_records').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM story_event_dates').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM story_observations').fetchone()[0] == 1
        assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        assert not db.execute('PRAGMA foreign_key_check').fetchall()


@pytest.mark.asyncio
async def test_mcp_contract_has_inline_metadata_and_distinct_coverage(monkeypatch):
    from regional_knowledge.server import build_server
    monkeypatch.setenv('RKB_DEV_NOAUTH', '1')
    server = build_server()
    tools = {tool.name: tool for tool in await server.list_tools()}
    schema = tools['book_ingest'].input_schema
    candidate = schema['$defs']['StageStoryCandidateInput']['properties']
    assert {'event_dates', 'observations', 'entity_keys'} <= set(candidate)
    assert 'knowledge_reviewed' in schema['$defs']['StagePageInput']['properties']
    assert 'knowledge_extraction' in tools['book_ingest'].output_schema['properties']
    assert 'organization' in tools['book_ingest'].description
