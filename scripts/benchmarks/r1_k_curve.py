"""R1: read-only offline K curve (isolated PG, no production API calls).
Usage: RKB_R1_PG_DSN=... RKB_R1_BGE_DIR=... python scripts/benchmarks/r1_k_curve.py pool bank.json private-pool.json
       python scripts/benchmarks/r1_k_curve.py score private-pool.json metrics.json --gold private-gold.json
Gold maps family IDs to alternative minimal sets of passage IDs (OR of ANDs).
Calls the installed SQL functions from sql/kaggle/001_search.sql and 040_lexical_restore_and.sql.
"""
from __future__ import annotations
import argparse, json, os, statistics, time
from collections import defaultdict
from pathlib import Path
KS = (3, 5, 8, 10, 20)
DEPTH = 100  # Matches kaggle_search._search; fuse ALL candidates before top-20.
def fused_ids(lexical, dense):
    scores = defaultdict(float)
    for branch in (lexical, dense):
        for rank, (ident, _) in enumerate(branch, 1):
            scores[str(ident)] += 1 / (60 + rank)
    return sorted(scores, key=lambda ident: (-scores[ident], ident)), scores
def score_pool(pool, gold):
    cases = pool['families']
    if set(gold) != {c['id'] for c in cases} or len(gold) != len(cases):
        raise ValueError('gold must cover every pool family exactly')
    by_stratum = defaultdict(list)
    for case in cases:
        alternatives = gold[case['id']]
        if (not isinstance(alternatives, list) or not alternatives
                or any(not isinstance(s, list) or not s or len(s) != len(set(s))
                       or any(not isinstance(x, str) or not x for x in s)
                       for s in alternatives)):
            raise ValueError('each family needs nonempty minimal passage-ID sets')
        by_stratum[case['stratum']].append((case, [set(s) for s in alternatives]))
    result = {}
    for stratum, entries in sorted({**by_stratum, 'ALL': [x for v in by_stratum.values() for x in v]}.items()):
        result[stratum] = {}
        for k in KS:
            success = 0
            sizes = []
            for case, alternatives in entries:
                rows = case['fused'][:k]
                success += any(s <= {r['passage_id'] for r in rows} for s in alternatives)
                sizes.append(sum(r['chars'] for r in rows))
            result[stratum][str(k)] = {'families': len(entries), 'complete': success,
                'complete_fraction': round(success / len(entries), 4),
                'mean_read_chars': round(statistics.mean(sizes), 1),
                'median_read_chars': statistics.median(sizes)}
    return {'k_curve': result, 'read_chars_basis': 'full stored passage bodies, hypothetical top-K read'}
# Pool-only SQL: same eligible corpus/configs/budget as SQL 040; never installed.
# Match terms come from websearch_to_tsquery (not separately tokenized input).
LEXICAL_VARIANT_SQL = """
WITH configs AS (
    SELECT c, raw.q,
        ARRAY(SELECT DISTINCT replace(m.term[1], '''''', '''')
              FROM regexp_matches(raw.q::text, $'((?:''|[^'])+)'$, 'g') AS m(term)) terms
    FROM unnest(ARRAY['pg_catalog.russian'::regconfig,
                      'pg_catalog.german'::regconfig, 'pg_catalog.simple'::regconfig]) c
    CROSS JOIN LATERAL (SELECT websearch_to_tsquery(c, %s) q) raw
), queries AS (
    SELECT c, terms,
           CASE WHEN c = 'pg_catalog.simple'::regconfig
                      OR strpos(q::text, '!') > 0 OR cardinality(terms) = 0 THEN q
                ELSE (SELECT string_agg(quote_literal(t), ' | ' ORDER BY t)::tsquery
                      FROM unnest(terms) AS u(t)) END match_q
    FROM configs
), candidates AS (
    SELECT hit.* FROM queries CROSS JOIN LATERAL (
        SELECT p.passage_id, ts_rank_cd(p.lexemes, match_q)::double precision score,
               (SELECT count(*)::double precision FROM unnest(terms) AS t(term)
                WHERE t.term = ANY(tsvector_to_array(p.lexemes)))
               / greatest(1, cardinality(terms)) coverage
        FROM rkb_v4.passage_search p
        JOIN rkb_v4.active_revision a USING (source_id, revision)
        JOIN rkb_v4.sources s USING (source_id)
        WHERE s.accepted AND NOT s.withdrawn AND p.config = c AND p.lexemes @@ match_q
        ORDER BY {order}
        LIMIT %s
    ) hit
)
SELECT passage_id, score FROM candidates ORDER BY {order} LIMIT %s
"""
def make_encoder():
    # Executed only inside the operator's isolated Kaggle environment; NEVER on Dev.
    from transformers import AutoModel, AutoTokenizer
    import torch
    import hashlib
    from regional_knowledge.bge_contract import MAX_TOKENS
    from regional_knowledge.kaggle_model_manifest import bge_model_manifest
    folder = Path(os.environ['RKB_R1_BGE_DIR']).resolve(strict=True)
    entries, _ = bge_model_manifest()
    for entry in entries:
        source = folder / Path(entry['name']).name
        if source.stat().st_size != entry['size']:
            raise ValueError('BGE-M3 artifact size/pin mismatch')
        digest = hashlib.sha256()
        with source.open('rb') as stream:
            for part in iter(lambda: stream.read(4 * 1024 * 1024), b''):
                digest.update(part)
        if digest.hexdigest() != entry['sha256']:
            raise ValueError('BGE-M3 artifact hash/pin mismatch')
    tokenizer = AutoTokenizer.from_pretrained(str(folder), local_files_only=True, use_fast=True)
    model = AutoModel.from_pretrained(str(folder), local_files_only=True).eval()
    def encode(question):
        tokens = tokenizer(question, max_length=MAX_TOKENS, truncation=True, return_tensors='pt')
        with torch.inference_mode():
            cls = model(**tokens).last_hidden_state[:, 0]
            return torch.nn.functional.normalize(cls, p=2, dim=1)[0].tolist()
    return encode
def collect_pool(bank, lexical_mode='and'):
    import psycopg
    from regional_knowledge.kaggle_search import vector_literal
    from uuid import UUID
    families = bank['families']
    if len(families) != 36 or len({f['id'] for f in families}) != 36:
        raise ValueError('expected the frozen 36-family question-only bank from PR 527')
    t_model = time.perf_counter()
    encode = make_encoder()
    model_ms = round((time.perf_counter() - t_model) * 1000, 2)
    output = []
    with psycopg.connect(os.environ['RKB_R1_PG_DSN'], autocommit=True) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                cur.execute('SET LOCAL hnsw.iterative_scan = strict_order')
                cur.execute('SET LOCAL hnsw.max_scan_tuples = 1000')
                cur.execute('SET LOCAL hnsw.ef_search = 100')
                cur.execute('SELECT max(version) FROM rkb_v4.schema_migrations')
                if cur.fetchone()[0] != 42:
                    raise ValueError('R1 requires operator-verified schema 42 snapshot')
                for family in families:
                    question = family['question']
                    elapsed = {}
                    began = time.perf_counter()
                    def timed(name, sql, params):
                        t = time.perf_counter()
                        cur.execute(sql, params)
                        rows = cur.fetchall()
                        elapsed[name + '_ms'] = round((time.perf_counter() - t) * 1000, 2)
                        return rows
                    t = time.perf_counter()
                    vector = vector_literal(encode(question))
                    elapsed['encode_ms'] = round((time.perf_counter() - t) * 1000, 2)
                    ordering = ('coverage DESC, score DESC, passage_id'
                                if lexical_mode == 'coverage' else 'score DESC, passage_id')
                    lex_sql = ('SELECT * FROM rkb_v4.lexical_candidates(%s,%s)' if lexical_mode == 'and'
                               else LEXICAL_VARIANT_SQL.format(order=ordering))
                    lex_params = (question, DEPTH) if lexical_mode == 'and' else (question, DEPTH, DEPTH)
                    lexical = timed('lexical', lex_sql, lex_params)
                    dense = timed('dense', 'SELECT * FROM rkb_v4.dense_candidates(%s::public.vector,%s)', (vector, DEPTH))
                    t = time.perf_counter()
                    fused, scores = fused_ids(lexical, dense)
                    elapsed['rrf_ms'] = round((time.perf_counter() - t) * 1000, 2)
                    ids = list(dict.fromkeys([str(x[0]) for x in lexical[:20] + dense[:20]] + fused[:20]))
                    rows = timed('metadata', '''SELECT p.passage_id,p.source_id,length(p.body)
                        FROM rkb_v4.passage_search p JOIN rkb_v4.active_revision a USING(source_id,revision)
                        JOIN rkb_v4.sources s USING(source_id)
                        WHERE s.accepted AND NOT s.withdrawn AND p.passage_id=ANY(%s::uuid[])''', ([UUID(x) for x in ids],))
                    meta = {str(p): (str(d), n) for p, d, n in rows}
                    if len(meta) != len(ids):
                        raise ValueError('ranked candidates missing from eligible snapshot')
                    def project(pairs):
                        return [{'passage_id': str(p), 'document_id': meta[str(p)][0],
                                 'rank': i, 'score': float(s), 'chars': meta[str(p)][1]}
                                for i, (p, s) in enumerate(pairs[:20], 1)]
                    elapsed['total_ms'] = round((time.perf_counter() - began) * 1000, 2)
                    output.append({'id': family['id'], 'stratum': family['stratum'],
                        'lexical': project(lexical), 'dense': project(dense),
                        'fused': project([(ident, scores[ident]) for ident in fused]),
                        'timings_ms': elapsed})
    return {'method': f'global lexical {lexical_mode} / BGE-M3 CLS-L2-512 HNSW / equal RRF k=60',
            'scores': {'lexical': 'ts_rank_cd (coverage mode sorts first by distinct-query-term fraction)', 'dense': 'cosine distance (lower better)',
                       'fused': 'RRF (higher better)'},
            'candidate_depth_per_branch': DEPTH, 'model_load_ms': model_ms, 'families': output}
def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode', choices=('pool', 'score'))
    p.add_argument('input', type=Path)
    p.add_argument('output', type=Path)
    p.add_argument('--gold', type=Path)
    p.add_argument('--lexical', choices=('and', 'or', 'coverage'), default='and', help='pool mode only')
    a = p.parse_args()
    if a.mode == 'score' and a.gold is None:
        p.error('--gold required for score mode')
    data = json.loads(a.input.read_text(encoding='utf-8'))
    result = collect_pool(data, a.lexical) if a.mode == 'pool' else score_pool(
        data, json.loads(a.gold.read_text(encoding='utf-8')))
    # Exclusive new file: no overwrite, no world-readable intermediate artifacts.
    with os.fdopen(os.open(a.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600),
                   'w', encoding='utf-8') as file:
        json.dump(result, file, ensure_ascii=False, indent=2)
    print(f"{a.mode} results saved privately")
if __name__ == '__main__':
    main()
