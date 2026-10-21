"""Deterministic retrieval augmentation, separate from quoted source evidence."""
from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable


def material(source, illustrations):
    parts = [source] if source else []
    for item in illustrations:
        caption = item.get('caption_text') or ''
        description = item.get('visual_description') or ''
        if caption:
            parts.append('[Printed caption]\n' + caption)
        if description:
            parts.append('[Model observation; not printed source text]\n' + description)
    text = '\n\n'.join(parts)
    if not text.strip() or len(text) > 40000:
        raise ValueError('search material must contain 1..40000 characters')
    return text, hashlib.sha256(text.encode()).hexdigest()


def _kind(value):
    return getattr(value, 'value', value)


def _graph_material_indexed(chunk, regions, figures):
    """Accept only explicitly linked, source-bound figures and printed captions.

    Same-page proximity alone never establishes a chunk→figure relationship.
    Caller-visible model descriptions are indexed with a label, never presented
    as printed source text or quoted evidence.
    """
    descriptions = []
    seen = set()
    for iid in chunk.illustration_ids:
        iid = str(iid)
        if iid in seen:
            raise ValueError('duplicate chunk illustration reference')
        seen.add(iid)
        figure = figures.get(iid)
        if figure is None:
            raise ValueError('chunk references absent illustration')
        source = regions.get(str(figure.source_region_id))
        if (source is None or _kind(source.kind) != 'figure'
                or str(source.page_id) != str(figure.page_id)):
            raise ValueError('illustration source region/page mismatch')
        captions = []
        for ref in figure.caption_region_ids:
            caption = regions.get(str(ref))
            if (caption is None or _kind(caption.kind) != 'caption'
                    or str(caption.page_id) != str(figure.page_id)):
                raise ValueError('printed caption source association mismatch')
            captions.append(caption.source_text)
        if (figure.visual_description is not None
                and figure.visual_description_provenance != 'model_observation'):
            raise ValueError('unlabelled visual description is not source evidence')
        descriptions.append({
            'caption_text': '\n'.join(captions),
            'visual_description': figure.visual_description,
        })
    return material(chunk.text, descriptions)



def graph_material(graph, chunk):
    regions = {str(r.region_id): r for p in graph.pages for r in p.regions}
    figures = {str(i.illustration_id): i for i in graph.illustrations}
    return _graph_material_indexed(chunk, regions, figures)


async def iter_document_inputs(
    graph,
    chunks: Iterable,
    count_tokens: Callable[[list[str]], Awaitable[list[int]]],
    *,
    max_tokens: int = 512,
) -> AsyncIterator[dict]:
    """Tokenize complete material in bounded batches, yielding verified rows.

    The pinned tokenizer must run on Kaggle in the eventual source executor;
    no document vector, book corpus or media is routed through Dev here.
    """
    if type(max_tokens) is not int or not 1 <= max_tokens <= 512:
        raise ValueError('invalid complete input token ceiling')
    regions = {str(r.region_id): r for p in graph.pages for r in p.regions}
    figures = {str(i.illustration_id): i for i in graph.illustrations}
    pending = []
    byte_budget = 0
    seen = set()

    async def verified_rows(rows):
        counts = await count_tokens([row['embedding_input'] for row in rows])
        if type(counts) is not list or len(counts) != len(rows):
            raise ValueError('pinned tokenizer returned incomplete counts')
        for row, count in zip(rows, counts, strict=True):
            if type(count) is not int or not 1 <= count <= max_tokens:
                raise ValueError('complete_source_input_exceeds_512_tokens')
            row['input_tokens'] = count
            yield row

    for chunk in chunks:
        text, sha = _graph_material_indexed(chunk, regions, figures)
        passage_id = str(chunk.chunk_id)
        if passage_id in seen:
            raise ValueError('duplicate passage identity')
        seen.add(passage_id)
        row = dict(passage_id=passage_id,body=chunk.text,
                   embedding_input=text,embedding_input_sha256=sha,
                   illustration_ids=[str(x) for x in chunk.illustration_ids])
        size = len(text.encode('utf-8')) + 128
        if size > 96 * 1024:
            raise ValueError('single complete input exceeds tokenizer transport bound')
        if pending and (len(pending) == 32 or byte_budget + size > 96 * 1024):
            async for verified in verified_rows(pending):
                yield verified
            pending = []
            byte_budget = 0
        pending.append(row)
        byte_budget += size
    if not seen:
        raise ValueError('at least one source chunk required')
    if pending:
        async for verified in verified_rows(pending):
            yield verified


async def prepare_document_inputs(
    graph,
    chunks: Iterable,
    count_tokens: Callable[[list[str]], Awaitable[list[int]]],
    *,
    max_tokens: int = 512,
) -> list[dict]:
    """Convenient bounded-tokenization wrapper; prefer streaming for books."""
    return [row async for row in iter_document_inputs(
        graph, chunks, count_tokens, max_tokens=max_tokens)]
