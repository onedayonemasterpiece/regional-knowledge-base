"""Catalog corrections reuse source identities and existing article components."""
import hashlib
from uuid import uuid4

import pytest

from regional_knowledge.catalog_components import create, update_metadata
from regional_knowledge.contracts import CatalogUpdateInput
from regional_knowledge.sqlite_corpus import SQLiteCorpus
from regional_knowledge.sqlite_data import defaults


@pytest.fixture
def source(tmp_path):
    corpus = SQLiteCorpus(tmp_path / 'corpus.db')
    actor, ident, page = (str(uuid4()) for _ in range(3))
    doc = {**defaults('rkb_documents'), 'id': ident, 'owner_user_id': actor,
           'title': 'Wrong book label', 'authors': ['Wrong whole-file author'],
           'active_revision': 1, 'page_count': 1, 'source_sha256': 'a' * 64,
           'source_archive_status': 'pending'}
    corpus.put('rkb_users', [{'id': actor, 'status': 'active'}])
    corpus.put('rkb_documents', [doc])
    corpus.put('rkb_pages', [{'id': page, 'document_id': ident, 'revision': 1,
                            'physical_page_index': 0}])
    regions, chunks = [], []
    for i in range(3):
        rid, cid = str(uuid4()), str(uuid4())
        text = f'Synthetic article section {i}.'
        sha = hashlib.sha256(text.encode()).hexdigest()
        regions.append({'id': rid, 'page_id': page, 'kind': 'body',
                        'reading_order': i, 'source_text': text, 'text_sha256': sha})
        chunks.append({**defaults('rkb_chunks'), 'id': cid, 'document_id': ident,
                       'revision': 1, 'source_text': text, 'text_sha256': sha,
                       'search_material': text, 'search_material_sha256': sha,
                       'region_ids': [rid], 'page_ids': [page]})
    corpus.put('rkb_regions', regions)
    corpus.put('rkb_chunks', chunks)
    with corpus.connect() as db:
        for i, chunk in enumerate(chunks):
            db.execute('INSERT INTO chunk_positions VALUES(?,?,?,?,?,?)',
                       (chunk['id'], ident, 1, i*100, i*100+50, None))
    return corpus, actor, doc, regions, chunks


def correction():
    return {'expected_revision': 1, 'title': 'Unidentified journal fragment',
            'authors': [], 'language': 'ru', 'catalog': {'kind': 'journal_issue',
            'form': 'fragment', 'annotation': 'Issue details not verified.',
            'annotation_origin': 'user'}}


def test_correction_preserves_source_revision_archive_acl_and_vectors(source):
    corpus, actor, doc, regions, chunks = source
    before = corpus.rows('rkb_chunks')
    result = update_metadata(corpus, actor, doc['id'], correction())
    assert result['catalog']['kind'] == 'journal_issue'
    assert result['catalog']['form'] == 'fragment'
    for field in ('id', 'source_sha256', 'source_archive_status', 'active_revision',
                  'owner_user_id', 'content_visibility', 'source_visibility', 'rights_status'):
        assert result[field] == doc[field]
    assert corpus.rows('rkb_chunks') == before
    assert update_metadata(corpus, actor, doc['id'], correction()) == result
    with pytest.raises((PermissionError, LookupError)):
        update_metadata(corpus, str(uuid4()), doc['id'], correction())
    with pytest.raises(ValueError, match='revision changed'):
        update_metadata(corpus, actor, doc['id'], {**correction(), 'expected_revision': 2})


def test_partial_correction_validates_merged_provenance(source):
    corpus, actor, doc, _, _ = source
    update_metadata(corpus, actor, doc['id'], correction())
    result = update_metadata(corpus, actor, doc['id'], {
        'expected_revision': 1, 'catalog': {'annotation': 'Corrected description.'}})
    assert result['catalog']['annotation_origin'] == 'user'
    with pytest.raises(ValueError, match='annotation provenance required'):
        update_metadata(corpus, actor, doc['id'], {
            'expected_revision': 1, 'catalog': {'annotation_origin': None}})
    with pytest.raises(ValueError, match='cover kind required'):
        update_metadata(corpus, actor, doc['id'], {
            'expected_revision': 1, 'catalog': {'cover_page': 0}})
    update_metadata(corpus, actor, doc['id'], {
        'expected_revision': 1, 'catalog': {'cover_page': 0, 'cover_kind': 'title_page'}})
    result = update_metadata(corpus, actor, doc['id'], {
        'expected_revision': 1, 'catalog': {'cover_page': 0}})
    assert result['catalog']['cover_kind'] == 'title_page'


def test_article_registration_separates_neighbors_without_reimport(source):
    corpus, actor, doc, regions, chunks = source
    update_metadata(corpus, actor, doc['id'], correction())
    payload = {'title': 'Main article', 'authors': ['Synthetic author'],
        'catalog': {'kind': 'article', 'parent_id': doc['id'],
                    'region_ids': [regions[1]['id']]}}
    before = corpus.rows('rkb_chunks')
    first = create(corpus, actor, payload)
    assert create(corpus, actor, payload).document_id == first.document_id
    assert corpus.rows('rkb_chunks') == before
    with corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM catalog_components').fetchone()[0] == 1
        assert db.execute('SELECT chunk_id FROM component_chunks').fetchone()[0] == chunks[1]['id']
    middle = corpus.neighbors(actor, [chunks[1]['id']])[0]
    assert 'prev_id' not in middle and 'next_id' not in middle
    with pytest.raises(ValueError, match='remain a journal issue'):
        update_metadata(corpus, actor, doc['id'], {'expected_revision': 1,
                                                  'catalog': {'kind': 'book'}})
    with pytest.raises(ValueError, match='overlapping'):
        create(corpus, actor, {**payload, 'title': 'Conflicting article'})
    with corpus.connect() as db:
        assert db.execute('SELECT count(*) FROM catalog_components').fetchone()[0] == 1


@pytest.mark.parametrize('extra', [
    {'source_sha256': 'b'*64}, {'owner_user_id': str(uuid4())},
    {'active_revision': 2}, {'content_visibility': 'public'},
    {'catalog': {'region_ids': [str(uuid4())]}},
    {'catalog': {'parent_id': str(uuid4())}},
    {'catalog': {'unknown': 'discarded?'}},
])
def test_correction_rejects_identity_acl_and_boundary_changes(extra):
    with pytest.raises(ValueError):
        CatalogUpdateInput.model_validate({'expected_revision': 1, **extra})


@pytest.mark.asyncio
async def test_catalog_mcp_update_is_write_annotated_and_typed(monkeypatch):
    from regional_knowledge.server import build_server
    monkeypatch.setenv('RKB_DEV_NOAUTH', '1')
    server = build_server()
    tools = {tool.name: tool for tool in await server.list_tools()}
    catalog = tools['catalog']
    assert catalog.annotations.read_only_hint is False
    assert 'update' in catalog.input_schema['properties']['command']['enum']
    assert 'CatalogUpdateInput' in catalog.input_schema['$defs']
