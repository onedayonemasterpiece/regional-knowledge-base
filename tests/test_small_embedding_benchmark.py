"""Focused tests of benchmark arithmetic and fixture contracts, without ML installs."""
import importlib.util
import json
from pathlib import Path
import pytest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('small_embedding_metrics',ROOT/'scripts/benchmarks/small_embedding_metrics.py')
bench=importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def test_preprocessing_spaces():
    assert bench.preprocess('e5','город',query=True)=='query: город'
    assert bench.preprocess('e5','город',query=False)=='passage: город'
    assert bench.preprocess('gemma','city',query=True)=='task: search result | query: city'
    assert bench.preprocess('gemma','city',query=False,title='Port')=='title: Port | text: city'
    assert bench.preprocess('potion',' city ',query=True)==' city '
    with pytest.raises(ValueError):bench.preprocess('unknown','city',query=True)


def test_normalization_invalid_and_nonunit():
    assert bench.normalize([3,4])==[.6,.8]
    with pytest.raises(ValueError):bench.normalize([0,0])
    with pytest.raises(ValueError):bench.normalize([float('nan'),1])


def test_evidence_group_recall_and_first_relevant_mrr():
    questions=[{'id':'a','evidence_groups':[['x','alternate'],['y']]},{'id':'b','evidence_groups':[['z']]},{'id':'n','evidence_groups':[],'unanswerable':True}]
    for q in questions:q['classes']=['fact']
    ranks={'a':['noise','x','y'],'b':[],'n':['noise']}
    result=bench.metrics(questions,ranks)
    assert result['recall_at_1']==0
    assert result['recall_at_5']==.5
    assert result['mrr']==.25
    assert result['all_evidence_at_10']==1
    assert result['answerable_count']==2
    assert result['unanswerable']['n']['returned_count']==1


def test_rrf_equal_weights_dedup_ties_and_depth():
    assert bench.rrf(['a','b'],['b','a'])==['a','b']
    assert bench.rrf(['a','b'],['b'])==['b','a']
    assert bench.rrf(['a','b'],[],limit=1)==['a']
    with pytest.raises(ValueError):bench.rrf(['a','a'])


def test_question_fixture_integrity():
    fixture=json.loads((ROOT/'scripts/benchmarks/small_embedding_questions.json').read_text())
    questions=fixture['questions']
    corpus=[{'id':id} for id in sorted({id for q in questions for group in q['evidence_groups'] for id in group})]
    bench.validate_fixture(questions,corpus)
    for q in questions:
        assert {p['chunk_id'] for p in q['source_provenance']}==set(sum(q['evidence_groups'],[]))
        assert all(len(p['text_sha256'])==64 and p['physical_page_index']>=0 for p in q['source_provenance'])
    assert len(fixture['corpus_sha256'])==64
    broken=[dict(q) for q in questions];broken[0]['evidence_groups']=[['nonexistent']]
    with pytest.raises(ValueError):bench.validate_fixture(broken,corpus)


def test_measured_compatibility_dimensions_normalization_and_hashes():
    import hashlib
    import math
    import struct
    fixture=json.loads((ROOT/'scripts/benchmarks/small_embedding_compatibility.json').read_text())
    dimension={'e5':384,'gemma':768,'potion':256}[fixture['candidate']]
    assert fixture['contract']['dimension']==dimension
    assert len(fixture['cases'])==10
    assert len({case['text'] for case in fixture['cases']})==10
    for role in ('query','document'):
        flat=[]
        for case in fixture['cases']:
            vector=case[f'{role}_vector']
            assert len(vector)==dimension
            assert all(math.isfinite(x) for x in vector)
            assert math.isclose(sum(x*x for x in vector),1,abs_tol=1e-6)
            flat.extend(vector)
        digest=hashlib.sha256(struct.pack('<'+'f'*len(flat),*flat)).hexdigest()
        assert digest==fixture[f'{role}_vector_sha256']
    assert 0<fixture['comparison']['absolute_component_tolerance']<.01
    assert .99<fixture['comparison']['cosine_min']<=1
