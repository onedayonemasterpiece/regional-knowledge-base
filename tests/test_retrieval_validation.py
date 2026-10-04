import importlib.util
from pathlib import Path
import pytest

spec = importlib.util.spec_from_file_location('validation_metrics', Path(__file__).resolve().parents[1]/'scripts/benchmarks/retrieval_validation_metrics.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def test_unjudged_is_unknown_and_exact_metrics_are_withheld():
    r = m.graded_metrics(['a','b'], {'a':2}, k=3, pool=['a','b'])
    assert r['precision_direct'] is None
    assert r['precision_direct_bounds'] == [1/3, 2/3]
    assert r['judgment_coverage'] == .5
    assert r['ndcg_pooled'] is None


def test_complete_graded_pool_distinguishes_context_and_direct_evidence():
    r = m.graded_metrics(['b','a','c'], {'a':2,'b':1,'c':0}, k=3, pool=['a','b','c'])
    assert r['precision_direct'] == 1/3
    assert r['precision_useful'] == 2/3
    assert 0 < r['ndcg_pooled'] < 1
    assert m.graded_metrics(['a','b','c'], {'a':2,'b':1,'c':0}, pool=['a','b','c'])['ndcg_pooled'] == 1


def test_shared_chunk_can_prove_multiple_required_facts():
    assert m.evidence_recall({'evidence_groups':[['a','alternative'],['a']]},['a']) == 1
    assert m.evidence_recall({'evidence_groups':[]},['a']) is None


def test_invalid_grades_and_duplicate_rankings_rejected():
    with pytest.raises(ValueError):m.graded_metrics(['a','a'],{'a':2})
    with pytest.raises(ValueError):m.graded_metrics(['a'],{'a':3})


def test_versioned_fixture_contracts_and_separate_original_review():
    import json
    import hashlib
    root=Path(__file__).resolve().parents[1]/'scripts/benchmarks'
    new=json.loads((root/'retrieval_validation_questions.v1.json').read_text())
    qs=new['questions']
    assert len(qs)==32 and len({q['id'] for q in qs})==32
    assert sum(q['status']=='negative' for q in qs)==10
    assert sum(q['status']=='source_gap' for q in qs)==2
    assert all(bool(q['evidence_groups'])==(q['status']=='answerable') for q in qs)
    assert all(q['source_provenance'] for q in qs)
    review=json.loads((root/'retrieval_validation_original_qrels.v2.json').read_text())
    assert review['based_on_sha256']==hashlib.sha256((root/'small_embedding_questions.json').read_bytes()).hexdigest()
    labels=json.loads((root/'retrieval_validation_judgments.v1.json').read_text())
    assert len({(x['question_id'],x['chunk_id']) for x in labels['judgments']})==len(labels['judgments'])
    assert all(x['grade'] in (0,1,2) and len(x['text_sha256'])==64 and x['rationale'] for x in labels['judgments'])
