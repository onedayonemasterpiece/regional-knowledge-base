"""Synthetic tests for audit logic; no private corpus or network/model calls."""
from __future__ import annotations
import ast
from pathlib import Path
import sys

import pytest

BENCH = Path(__file__).resolve().parents[1] / 'scripts' / 'benchmarks'
sys.path.insert(0, str(BENCH))
from chunk_size_audit import norm, split_size
from chunk_audit_score import covers, retrieve_metric, summary


def test_normalization_is_not_preview_truncation():
    text = 'Geschichte der Grund-\neigenthümer.\n\nMehr Text!'
    assert norm(text) == 'Geschichte der Grundeigenthümer. Mehr Text!'
    assert len(norm('A' * 10000)) == 10000


@pytest.mark.parametrize('cap', [700, 1000, 1400, 1800])
def test_size_variants_preserve_every_non_whitespace_character(cap):
    text = ' '.join(['Ein vollständiger Satz mit Umlauten äöü.'] * 170)
    ranges = split_size(text, cap)
    assert ranges[0][0] == 0 and ranges[-1][1] == len(text)
    assert all(left[1] == right[0] for left, right in zip(ranges, ranges[1:]))
    assert all(0 < b - a <= cap for a, b in ranges)
    assert ''.join(text[a:b] for a, b in ranges) == text


@pytest.mark.parametrize('intervals,start,end,expected', [
    ([], 10, 20, False),
    ([(10, 15), (15, 20)], 10, 20, True),
    ([(10, 14), (15, 20)], 10, 20, False),
    ([(8, 30)], 10, 20, True),
    ([(15, 20), (10, 17)], 10, 20, True),
    ([(1, 9), (11, 30)], 10, 20, False),
])
def test_span_coverage(intervals, start, end, expected):
    assert covers(intervals, start, end) is expected


def test_split_answer_must_be_retrieved_on_both_sides():
    source = 'Intro. The castle was founded in 1255. End.'
    proof = 'The castle was founded in 1255.'
    cut = source.index('founded')
    case = {'id': 'Q', 'family': 'F', 'split': 'dev', 'language': 'en',
            'doc': 'A', 'page': 3, 'proof': proof}
    chunks = [
        {'id': 'a', 'doc': 'A', 'pages': [3], 'text': source[:cut], 'start': 0, 'end': cut},
        {'id': 'b', 'doc': 'A', 'pages': [3], 'text': source[cut:], 'start': cut, 'end': len(source)},
    ]
    row = retrieve_metric(chunks, [0, 1], case, source)
    assert row['single_proof_exists'] is False
    assert row['span_hit1'] is False
    assert row['span_hit5'] is True
    assert row['span_hit5000chars'] is True


def test_other_source_with_same_numbers_is_not_labelled_as_seed_evidence():
    proof = 'The castle was founded in 1255.'
    c = {'id': 'Q', 'family': 'F', 'split': 'dev', 'language': 'en',
         'doc': 'A', 'page': 3, 'proof': proof}
    items = [{'id': 'wrong', 'doc': 'B', 'pages': [3], 'text': proof}]
    row = retrieve_metric(items, [0], c, proof)
    assert not row['single_proof_exists']
    assert not row['span_hit5']
    # This is a known-seed metric, not an assertion that all other evidence is false.


def test_retrieval_context_budget_is_fixed_in_characters():
    proof = 'The castle was founded in 1255.'
    c = {'id': 'Q', 'family': 'F', 'split': 'dev', 'language': 'en',
         'doc': 'A', 'page': 3, 'proof': proof}
    items = [{'id': 'a', 'doc': 'B', 'pages': [1], 'text': 'x' * 4990},
             {'id': 'b', 'doc': 'A', 'pages': [3], 'text': proof}]
    row = retrieve_metric(items, [0, 1], c, proof)
    assert row['single_hit5'] is True
    assert row['span_hit5000chars'] is False
    assert row['budget_used'] == 4990


def test_dense_scorer_has_no_lexical_alias_or_reranker_calls():
    tree = ast.parse((BENCH / 'chunk_audit_score.py').read_text())
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    names = {node.func.id if isinstance(node.func, ast.Name) else node.func.attr
             for node in calls if isinstance(node.func, (ast.Name, ast.Attribute))}
    assert names.isdisjoint({'lexical', 'local_rankings', 'main_search', 'search', 'rerank'})


def test_empty_summary_does_not_claim_success():
    assert summary([]) == {'n': 0}
