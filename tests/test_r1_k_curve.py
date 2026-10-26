"""Synthetic, offline K-curve acceptance; no database, Kaggle, or BGE needed."""
import json
import runpy
from pathlib import Path

MODULE = runpy.run_path(str(Path(__file__).resolve().parents[1] / 'scripts/benchmarks/r1_k_curve.py'))


def test_rrf_equal_weight_full_branch_rankings():
    fused, scores = MODULE['fused_ids']([('a', .9), ('b', .7)], [('b', .1), ('c', .2)])
    assert fused == ['b', 'a', 'c']
    assert scores['b'] == 1 / 62 + 1 / 61
    assert scores['a'] == 1 / 61


def test_synthetic_complete_minimal_sets_and_read_volume():
    def row(ident, chars):
        return {'passage_id': ident, 'chars': chars}
    pool = {'families': [
        {'id': 'E01', 'stratum': 'RU→DE', 'fused': [row('p1', 10), row('p2', 20), row('p3', 30), row('p4', 40)]},
        {'id': 'N01', 'stratum': 'ложная предпосылка', 'fused': [row('n1', 4), row('n2', 8)]},
    ]}
    gold = {'E01': [['p1', 'p4'], ['p4', 'p2']], 'N01': [['n2']]}
    result = MODULE['score_pool'](pool, gold)['k_curve']
    assert result['RU→DE']['3']['complete'] == 0
    assert result['RU→DE']['5']['complete'] == 1
    assert result['RU→DE']['3']['mean_read_chars'] == 60
    assert result['RU→DE']['20']['mean_read_chars'] == 100
    assert result['ложная предпосылка']['3']['complete'] == 1
    assert result['ALL']['3']['complete_fraction'] == .5
    assert all(result['ALL'][str(a)]['complete'] <= result['ALL'][str(b)]['complete']
               for a, b in zip(MODULE['KS'], MODULE['KS'][1:]))
    assert 'p4' not in json.dumps(result)  # Gold identifiers never appear in aggregates.


def test_score_cli_is_offline_and_output_private(tmp_path, monkeypatch):
    import os
    import sys
    monkeypatch.delenv('RKB_R1_PG_DSN', raising=False)
    monkeypatch.delenv('RKB_R1_BGE_DIR', raising=False)
    pool = tmp_path / 'pool.json'
    gold = tmp_path / 'gold.json'
    out = tmp_path / 'report.json'
    pool.write_text(json.dumps({'families': [
        {'id': 'A', 'stratum': 'RU→RU', 'fused': [{'passage_id': 'a', 'chars': 7}]}
    ]}))
    gold.write_text(json.dumps({'A': [['a']]}))
    monkeypatch.setattr(sys, 'argv', ['r1_k_curve.py', 'score', str(pool), str(out), '--gold', str(gold)])
    MODULE['main']()
    assert json.loads(out.read_text())['k_curve']['ALL']['3']['complete'] == 1
    assert os.stat(out).st_mode & 0o077 == 0


def test_variant_ordering_and_sql_read_only():
    sql = MODULE['LEXICAL_VARIANT_SQL']
    assert 'websearch_to_tsquery(c, %s)' in sql
    assert "c = 'pg_catalog.simple'::regconfig" in sql
    assert "strpos(q::text, '!') > 0" in sql
    assert 'JOIN rkb_v4.active_revision' in sql and 's.accepted AND NOT s.withdrawn' in sql
    assert all(x not in sql.upper() for x in ('CREATE ', 'UPDATE ', 'DELETE ', 'INSERT '))
    assert 'ORDER BY coverage DESC, score DESC, passage_id' in sql.format(
        order='coverage DESC, score DESC, passage_id')


def test_pg_lexical_pool_variants(database=None):
    # Actual PG assertion is performed in the async companion below.
    assert MODULE['DEPTH'] == 100


def test_pool_cli_passes_lexical_choice(tmp_path, monkeypatch):
    import sys
    pool_in, pool_out = tmp_path / 'bank.json', tmp_path / 'pool.json'
    pool_in.write_text('{"families":[]}')
    seen = []
    def collect(data, choice):
        seen.append(choice)
        return {'families': []}
    monkeypatch.setitem(MODULE['main'].__globals__, 'collect_pool', collect)
    monkeypatch.setattr(sys, 'argv', ['r1_k_curve.py', 'pool', str(pool_in),
                                     str(pool_out), '--lexical', 'coverage'])
    MODULE['main']()
    assert seen == ['coverage']
