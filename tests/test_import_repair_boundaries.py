"""Synthetic regressions for source coverage and accepted enrichment repair."""
import hashlib
import json
from types import SimpleNamespace
from uuid import UUID

import pytest

from regional_knowledge import stage_service, story_ingestion, sqlite_revision
from regional_knowledge.contracts import BookIngestOutput, StagePageInput, StageChunkInput
from regional_knowledge.stage_graph import StagedGraph, compile_model_stage, merge_stage, validate_graph
from regional_knowledge.sqlite_data import LocalContext, defaults
from regional_knowledge.story_contracts import StageStoryCandidateInput
from regional_knowledge.story_ingestion import persist_stage
from test_story_ingestion import fixture, accept_source
from test_ingestion_enrichment import enriched


@pytest.mark.parametrize('kind', ['header', 'footer', 'page_number', 'heading', 'body', 'caption', 'footnote', 'table', 'marginalia'])
def test_stage_and_activation_share_text_coverage_policy(fixture, kind):
    corpus, _, principal, doc, job, _, page, _ = fixture
    raw = page.model_dump(mode='json')
    raw['regions'].append({'region_key': 'extra', 'kind': kind,
        'bbox': {'left': 1, 'top': 810, 'right': 900, 'bottom': 890},
        'reading_order': 1, 'source_text': 'Synthetic extra text'})
    page = StagePageInput.model_validate(raw)
    graph = merge_stage(StagedGraph(revision=1), compile_model_stage(
        StagedGraph(revision=1), document_id=doc['id'], revision=1, pages=[page],
        chunks=[StageChunkInput(chunk_key='body', title='Synthetic',
            region_refs=[{'page_id': page.page_id, 'region_key': 'body'}])]))
    corpus.put('rkb_pages', [{'id': str(p.page_id), 'document_id': doc['id'],
        'revision': 1, 'physical_page_index': p.physical_page_index} for p in graph.pages])
    corpus.put('rkb_regions', [{'id': str(r.region_id), 'page_id': str(p.page_id),
        'kind': r.kind.value, 'source_text': r.source_text,
        'text_sha256': hashlib.sha256(r.source_text.encode()).hexdigest()}
        for p in graph.pages for r in p.regions])
    corpus.put('rkb_chunks', [{**defaults('rkb_chunks'), 'id': str(c.chunk_id),
        'document_id': doc['id'], 'revision': 1, 'source_text': c.text,
        'text_sha256': hashlib.sha256(c.text.encode()).hexdigest(),
        'search_material': c.text, 'search_material_sha256': hashlib.sha256(c.text.encode()).hexdigest(),
        'page_ids': [str(x) for x in c.page_ids], 'region_ids': [str(x) for x in c.region_ids],
        'footnote_region_ids': [], 'illustration_ids': []} for c in graph.chunks])
    preliminary = validate_graph(graph, expected_page_count=1)
    missing = any('uncovered textual regions' in e for e in preliminary.errors)
    assert missing == (kind not in {'header', 'footer', 'page_number'})
    with corpus.connect() as db:
        ctx = LocalContext(corpus, db, principal.subject)
        if missing:
            with pytest.raises(ValueError, match='textual region not covered'):
                sqlite_revision.validate(ctx, doc, 1, [])
        else:
            assert sqlite_revision.validate(ctx, doc, 1, [])
    # Exemption is only from retrieval; exact source material is preserved.
    stored = corpus.rows('rkb_regions')
    assert len(stored) == 2
    extra = next(r for r in stored if r['source_text'] == 'Synthetic extra text')
    assert extra['text_sha256'] == hashlib.sha256(b'Synthetic extra text').hexdigest()


async def service_for(monkeypatch, corpus, graph):
    async def load(service, row):
        return graph
    async def document(service, principal, ident):
        return corpus.authorize(principal.subject, str(ident), owner=True)
    monkeypatch.setattr(stage_service, '_load_graph', load)
    monkeypatch.setattr(stage_service, '_document_row', document)
    return SimpleNamespace(corpus=corpus, _ingestion_output=lambda row, message: BookIngestOutput(
        state='finalized', ingestion_id=str(row['id']), document_id=str(row['document_id']),
        next_action='done', message=message))


@pytest.mark.asyncio
async def test_accepted_repair_rolls_back_checkpoint_on_activation_failure(fixture, monkeypatch):
    corpus, _, principal, doc, job, graph, page, candidate = enriched(fixture)
    persist_stage(corpus, principal, job, graph, [page], [candidate])
    accept_source(corpus, principal, doc, job, graph)
    finalized = {**job, 'state': 'finalized'}
    corpus.put('rkb_ingestion_jobs', [finalized])
    with corpus.connect() as db:
        before = [tuple(r) for r in db.execute('SELECT * FROM story_ingest_candidates')]
    service = await service_for(monkeypatch, corpus, graph)
    def fail(*args):
        raise RuntimeError('synthetic activation failure')
    monkeypatch.setattr(story_ingestion, 'activate_stories', fail)
    with pytest.raises(RuntimeError, match='synthetic activation failure'):
        await stage_service._stage_accepted_enrichment(service, principal,
            {**finalized, 'id': UUID(job['id']), 'document_id': UUID(doc['id'])},
            {'story_candidates': [candidate.model_dump(mode='json')]})
    with corpus.connect() as db:
        assert before == [tuple(r) for r in db.execute('SELECT * FROM story_ingest_candidates')]
        assert db.execute('SELECT count(*) FROM story_records').fetchone()[0] == 1


@pytest.mark.asyncio
async def test_two_previously_persisted_repairs_resume_without_duplicate_stories(fixture, monkeypatch):
    corpus, _, principal, doc, job, graph, page, candidate = enriched(fixture)
    other = candidate.model_copy(update={'candidate_key': 'other', 'title': 'Second synthetic account'})
    candidates = [candidate, other]
    bad = []
    for c in candidates:
        data = c.model_dump(mode='json')
        data['observations'][0]['period_text'] = 'Not in the source'
        bad.append(StageStoryCandidateInput.model_validate(data))
    persist_stage(corpus, principal, job, graph, [page], bad)
    accept_source(corpus, principal, doc, job, graph)
    finalized = {**job, 'state': 'finalized'}
    corpus.put('rkb_ingestion_jobs', [finalized])
    # Simulate checkpoints left by the previous split-transaction implementation.
    persist_stage(corpus, principal, finalized, graph, [], candidates, allow_finalized=True)
    service = await service_for(monkeypatch, corpus, graph)
    row = {**finalized, 'id': UUID(job['id']), 'document_id': UUID(doc['id'])}
    payload = {'story_candidates': [c.model_dump(mode='json') for c in candidates]}
    for _ in range(2):
        await stage_service._stage_accepted_enrichment(service, principal, row, payload)
    with corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM story_records').fetchone()[0] == 2
        assert db.execute('SELECT count(*) FROM story_observations').fetchone()[0] == 2
        assert [r[0] for r in db.execute('SELECT state FROM story_ingest_candidates')] == ['accepted', 'accepted']
        assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
