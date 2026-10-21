"""Versioned source manifest for model-read PDF/DjVu pages (Kaggle staging).

Unlike source-manifest.v1 this envelope deliberately DOES NOT assert that
model-produced source text equals a native PDF text layer. The caller must
already hold owner authorization, a *verified* original receipt and a validated
visual-review StagedGraph. This module provides deterministic binding; it is
not an acceptance receipt, SQL migration or remote persistence mechanism.

v1 is immutable and remains eligible for ordered replay with its v1 validator.
Never retrofit this envelope into a v1 accepted command.
"""
from __future__ import annotations

import hashlib
import json

from .model_source_provenance import build_model_source_provenance
from .search_material import graph_material, iter_document_inputs

CODEC = 'rkb.source-model-manifest.v2'
MAX_INPUT_TOKENS = 512
MAX_PAGES = 2500  # the existing parser's upper bound, NOT a target book-size cap


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _metadata(value):
    """Preserve the exact supplied source metadata, without an invented license."""
    if hasattr(value, 'model_dump'):
        value = value.model_dump(mode='json')
    if type(value) is not dict or any(k not in value for k in ('title',)):
        raise ValueError('source metadata required')
    keys = ('title', 'authors', 'publication_year', 'language', 'catalog')
    meta = {key: value.get(key) for key in keys}
    if (type(meta['title']) is not str or not meta['title'].strip()
            or len(meta['title']) > 500 or len(_canonical(meta)) > 16384):
        raise ValueError('bounded source title and metadata required')
    if meta['authors'] is None:
        meta['authors'] = []
    if (type(meta['authors']) is not list or len(meta['authors']) > 50
            or any(type(a) is not str or len(a) > 4000 for a in meta['authors'])):
        raise ValueError('bounded exact source authors required')
    if meta['language'] is not None and (
            type(meta['language']) is not str or len(meta['language']) > 80):
        raise ValueError('source language metadata invalid')
    year = meta['publication_year']
    if year is not None and (type(year) is not int or not 1 <= year <= 3000):
        raise ValueError('source publication year invalid')
    if meta['catalog'] is not None and type(meta['catalog']) is not dict:
        raise ValueError('catalog must be a structured object')
    # This module must not silently drop unknown metadata keys. The full
    # existing StartMetadataInput contract remains enforced by the caller.
    if set(value) - set(keys) - {'duplicate_policy', 'catalog_update', 'source_import'}:
        raise ValueError('unsupported source metadata fields')
    if (value.get('duplicate_policy', 'reuse') != 'reuse'
            or value.get('catalog_update') is not None
            or value.get('source_import') is not None):
        raise ValueError('fresh source metadata cannot embed another intake or revision')
    return meta


def assemble_model_manifest(*, original, graph, metadata, passage_inputs):
    """Compare all derived references to a graph whose stage IDs are stable.

    `passage_inputs` must be produced by the exact pinned tokenizer on Kaggle;
    caller-side counting is never a production acceptance proof. No vector,
    private URL, original PDF, image or credential goes into this envelope.
    """
    provenance, digest = build_model_source_provenance(receipt=original, graph=graph)
    rows = list(passage_inputs)
    indexed = {row['passage_id']: row for row in rows}
    passages = provenance['chunks']
    source_chunks = {str(chunk.chunk_id): chunk for chunk in graph.chunks}
    if len(rows) != len(passages) or len(indexed) != len(rows):
        raise ValueError('complete and unique passage material required')
    passage_rows = []
    for chunk in passages:
        row = indexed.get(chunk['chunk_id'])
        source_chunk = source_chunks.get(chunk['chunk_id'])
        if source_chunk is None:
            raise ValueError('passage source chunk is absent')
        expected_input, expected_sha = graph_material(graph, source_chunk)
        if (row is None or type(row.get('input_tokens')) is not int
                or not 1 <= row['input_tokens'] <= MAX_INPUT_TOKENS
                or row.get('illustration_ids') != chunk['illustration_ids']
                or type(row.get('embedding_input_sha256')) is not str
                or len(row['embedding_input_sha256']) != 64
                or set(row['embedding_input_sha256']) - set('0123456789abcdef')
                or _sha(row['body'].encode()) != chunk['source_body_sha256']
                or row['embedding_input'] != expected_input
                or row['embedding_input_sha256'] != expected_sha
                or _sha(row['embedding_input'].encode()) != row['embedding_input_sha256']):
            raise ValueError('exact complete embedding input or passage mismatch')
        passage_rows.append(dict(
            passage_id=chunk['chunk_id'], ordinal=chunk['ordinal'],
            source_body_sha256=chunk['source_body_sha256'],
            embedding_input_sha256=row['embedding_input_sha256'],
            input_tokens=row['input_tokens'],
            page_ids=chunk['page_ids'], region_ids=chunk['region_ids'],
            source_spans=chunk['source_spans'],
            illustration_ids=chunk['illustration_ids'],
        ))
    if (len(provenance['pages']) != original.page_count
            or not 1 <= len(provenance['pages']) <= MAX_PAGES):
        raise ValueError('complete source page coverage required')
    content = dict(codec=CODEC, source_id=provenance['source_id'], source_revision=1,
        original=dict(ingestion_id=original.ingestion_id,
            receipt_sha256=original.receipt_sha256, sha256=original.sha256,
            format=original.format, page_count=original.page_count),
        metadata=_metadata(metadata),
        provenance_sha256=digest,
        # This information is domain content. Install it beside PostgreSQL in
        # Kaggle, never in Dev's bounded controller/spool.
        pages=provenance['pages'], regions=provenance['regions'],
        illustrations=provenance['illustrations'], passages=passage_rows)
    return content


async def build_model_manifest(*, original, graph, metadata, tokenizer_counts):
    """Construct the full Kaggle-side manifest from streamed exact inputs."""
    inputs = [row async for row in iter_document_inputs(
        graph, graph.chunks, tokenizer_counts, max_tokens=MAX_INPUT_TOKENS)]
    manifest = assemble_model_manifest(original=original, graph=graph,
                                       metadata=metadata, passage_inputs=inputs)
    return manifest, _sha(_canonical(manifest))


def verify_model_manifest(manifest, *, original, graph, metadata, passage_inputs, sha256):
    """Independent exact readback check; no model call or SQL side effects.

    This verifies a copy of the immutable manifest against the original
    normalized model stage and previously tokenized complete material.
    It cannot by itself attest that remote Dataset/PG persistence succeeded.
    """
    if type(manifest) is not dict or type(sha256) is not str or len(sha256) != 64:
        raise ValueError('model manifest readback missing')
    expected = assemble_model_manifest(original=original, graph=graph,
                                       metadata=metadata,
                                       passage_inputs=passage_inputs)
    if (manifest != expected or _sha(_canonical(manifest)) != sha256):
        raise ValueError('source-model manifest content drift')
    return True
