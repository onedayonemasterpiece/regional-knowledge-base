"""Pure benchmark contracts; no ML or production runtime dependency."""
import math


def preprocess(candidate, text, *, query, title='none'):
    if candidate == 'e5': return ('query: ' if query else 'passage: ') + text
    if candidate == 'gemma': return ('task: search result | query: ' if query else f'title: {title or "none"} | text: ') + text
    if candidate == 'potion': return text
    raise ValueError(candidate)


def normalize(vector):
    norm = math.sqrt(sum(float(x)**2 for x in vector))
    if not norm or not math.isfinite(norm): raise ValueError('invalid vector norm')
    return [float(x)/norm for x in vector]


def rrf(*rankings, k=60, limit=100):
    scores = {}
    for ranking in rankings:
        if len(ranking) != len(set(ranking)): raise ValueError('duplicate ranking IDs')
        for rank, chunk in enumerate(ranking[:limit], 1):
            scores[chunk] = scores.get(chunk, 0) + 1/(k+rank)
    return sorted(scores, key=lambda chunk: (-scores[chunk], chunk))


def validate_fixture(questions, corpus):
    ids = {r['id'] for r in corpus}
    if len(ids) != len(corpus): raise ValueError('duplicate corpus IDs')
    if len(questions) < 20: raise ValueError('at least 20 questions')
    if len({q['id'] for q in questions}) != len(questions): raise ValueError('duplicate questions')
    for q in questions:
        if not q['query'] or not q['classes']: raise ValueError('missing question fields')
        if bool(q['evidence_groups']) == bool(q.get('unanswerable')): raise ValueError('answerability mismatch')
        if any(not group or not set(group) <= ids for group in q['evidence_groups']): raise ValueError('invalid evidence')
    if sum(bool(q.get('unanswerable')) for q in questions) < 2: raise ValueError('two unanswerable controls required')


def metrics(questions, rankings):
    answerable = [q for q in questions if not q.get('unanswerable')]
    result = {'answerable_count': len(answerable)}
    for k in (1,5,10):
        # Macro evidence-group recall: each required fact contributes equally within a question.
        result[f'recall_at_{k}'] = sum(sum(bool(set(g) & set(rankings[q['id']][:k])) for g in q['evidence_groups'])/len(q['evidence_groups']) for q in answerable)/len(answerable)
        result[f'hit_at_{k}'] = sum(bool(set(sum(q['evidence_groups'],[])) & set(rankings[q['id']][:k])) for q in answerable)/len(answerable)
    result['mrr'] = sum(next((1/(i+1) for i,c in enumerate(rankings[q['id']]) if c in sum(q['evidence_groups'],[])),0) for q in answerable)/len(answerable)
    multi = [q for q in answerable if len(q['evidence_groups']) > 1]
    result['multi_count'] = len(multi)
    result['all_evidence_at_10'] = sum(all(set(g) & set(rankings[q['id']][:10]) for g in q['evidence_groups']) for q in multi)/len(multi) if multi else None
    result['unanswerable'] = {q['id']: {'returned_count': len(rankings[q['id']][:10]), 'top_ids': rankings[q['id']][:3]} for q in questions if q.get('unanswerable')}
    result['by_class'] = {}
    for cls in sorted({cls for q in answerable for cls in q['classes']}):
        subset = [q for q in answerable if cls in q['classes']]
        result['by_class'][cls] = {'n':len(subset),'hit_at_10':sum(bool(set(sum(q['evidence_groups'],[])) & set(rankings[q['id']][:10])) for q in subset)/len(subset)}
    return result
