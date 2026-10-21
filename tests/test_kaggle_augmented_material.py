"""No model/provider required; exact-input contracts for linked figure augmentation.

Synthetic model pages are used only to exercise the deterministic materializer.
No mocked success is asserted as a Kaggle or PostgreSQL product acceptance.
"""
import hashlib
from types import SimpleNamespace as S
from uuid import uuid4

import pytest

from regional_knowledge.search_material import graph_material, prepare_document_inputs


def fixture(*, description='Stone arch in the photo', caption='Printed caption from page', source='Source paragraph.'):
    page_id = str(uuid4())
    source_id, caption_id = str(uuid4()), str(uuid4())
    figure_id, chunk_id = str(uuid4()), str(uuid4())
    figure_region = S(region_id=source_id, page_id=page_id, kind='figure', source_text='', bbox='original')
    caption_region = S(region_id=caption_id, page_id=page_id, kind='caption', source_text=caption)
    figure = S(illustration_id=figure_id, page_id=page_id, source_region_id=source_id,
               caption_region_ids=[caption_id], visual_description=description,
               visual_description_provenance='model_observation')
    page = S(page_id=page_id, regions=[figure_region, caption_region])
    graph = S(pages=[page], illustrations=[figure])
    chunk = S(chunk_id=chunk_id, text=source, illustration_ids=[figure_id])
    return graph, chunk


def test_exactly_linked_caption_and_labelled_model_observation():
    graph, chunk = fixture()
    text, digest = graph_material(graph, chunk)
    assert text == ('Source paragraph.\n\n[Printed caption]\nPrinted caption from page\n\n'
                    '[Model observation; not printed source text]\nStone arch in the photo')
    assert digest == hashlib.sha256(text.encode('utf-8')).hexdigest()
    assert text.count('[Printed caption]') == 1
    assert text.count('[Model observation; not printed source text]') == 1
    chunk.illustration_ids = []
    assert graph_material(graph, chunk)[0] == 'Source paragraph.'


@pytest.mark.parametrize('change', ['unknown_figure','duplicate_figure','wrong_source_page',
                                    'wrong_source_kind','unknown_caption','wrong_caption_page',
                                    'wrong_caption_kind','unknown_provenance'])
def test_unverified_image_or_caption_association_fails_closed(change):
    graph, chunk = fixture()
    figure = graph.illustrations[0]
    by_key = {
        'unknown_figure': lambda: setattr(chunk, 'illustration_ids', [str(uuid4())]),
        'duplicate_figure': lambda: setattr(chunk, 'illustration_ids', chunk.illustration_ids * 2),
        'wrong_source_page': lambda: setattr(graph.pages[0].regions[0], 'page_id', str(uuid4())),
        'wrong_source_kind': lambda: setattr(graph.pages[0].regions[0], 'kind', 'body'),
        'unknown_caption': lambda: setattr(figure, 'caption_region_ids', [str(uuid4())]),
        'wrong_caption_page': lambda: setattr(graph.pages[0].regions[1], 'page_id', str(uuid4())),
        'wrong_caption_kind': lambda: setattr(graph.pages[0].regions[1], 'kind', 'body'),
        'unknown_provenance': lambda: setattr(figure, 'visual_description_provenance', 'unverified_caption'),
    }
    by_key[change]()
    with pytest.raises(ValueError):
        graph_material(graph, chunk)


@pytest.mark.asyncio
async def test_512_complete_tokens_accepted_513_rejected_without_truncation():
    graph, chunk = fixture(description='visual marker', caption='caption marker', source=' '.join(['word'] * 480))
    actual = graph_material(graph, chunk)[0]
    assert chunk.text != actual and len(actual) > len(chunk.text)
    calls = []
    async def count(texts):
        calls.extend(texts)
        # Pinned tokenizer is injected; counts already include special tokens.
        return [512 for _ in texts]
    values = await prepare_document_inputs(graph, [chunk], count)
    assert values[0]['input_tokens'] == 512
    assert values[0]['embedding_input'] == actual
    assert values[0]['body'] == chunk.text
    assert values[0]['illustration_ids'] == chunk.illustration_ids
    assert values[0]['embedding_input_sha256'] == hashlib.sha256(actual.encode()).hexdigest()
    assert calls == [actual]
    async def oversized(texts):
        assert texts == [actual]
        return [513]
    with pytest.raises(ValueError, match='complete_source_input_exceeds_512_tokens'):
        await prepare_document_inputs(graph, [chunk], oversized)


@pytest.mark.asyncio
async def test_long_input_batched_without_global_32_passage_product_cap():
    graph, chunk = fixture(description=None, caption='')
    chunk.illustration_ids = []
    chunks = [S(chunk_id=str(uuid4()), text='text ' + str(index), illustration_ids=[])
              for index in range(99)]
    batches=[]
    async def count(texts):
        batches.append(tuple(texts))
        return [6] * len(texts)
    output=await prepare_document_inputs(graph, chunks, count)
    assert len(output)==99
    assert [len(x) for x in batches]==[32,32,32,3]
    assert all(row['input_tokens']==6 for row in output)
    assert [row['body'] for row in output]==[c.text for c in chunks]


@pytest.mark.asyncio
@pytest.mark.parametrize('malformed', [[4,5], [], '4', [True], [0], [-1], [1000]])
async def test_incomplete_or_invalid_tokenizer_counts_cannot_accept(malformed):
    graph, chunk=fixture()
    async def count(texts): return malformed
    with pytest.raises(ValueError):
        await prepare_document_inputs(graph,[chunk],count)


@pytest.mark.asyncio
async def test_transport_oversize_rejected_without_invoking_tokenizer():
    graph, chunk=fixture(description=None,caption='',source='a'*40001)
    calls=[]
    async def count(texts): calls.append(texts);return [5]
    with pytest.raises(ValueError):
        await prepare_document_inputs(graph,[chunk],count)
    assert calls==[]


@pytest.mark.asyncio
async def test_invalid_token_ceiling_is_rejected():
    graph, chunk=fixture()
    async def count(texts): return [5]
    for ceiling in (0, 513, 512.0, True):
        with pytest.raises(ValueError):
            await prepare_document_inputs(graph,[chunk],count,max_tokens=ceiling)


@pytest.mark.asyncio
async def test_lazy_full_book_iteration_has_no_full_book_buffer_before_first_output():
    from regional_knowledge.search_material import iter_document_inputs
    graph, original = fixture()
    original.illustration_ids=[]
    produced=[]
    def chunks():
        for i in range(139):
            produced.append(i)
            yield S(chunk_id=str(uuid4()),text='page '+str(i),illustration_ids=[])
    calls=[]
    async def count(texts):
        calls.append(len(texts))
        return [4]*len(texts)
    stream=iter_document_inputs(graph,chunks(),count)
    first=await anext(stream)
    assert first['body']=='page 0'
    assert len(produced)==33 and calls==[32]
    await stream.aclose()
