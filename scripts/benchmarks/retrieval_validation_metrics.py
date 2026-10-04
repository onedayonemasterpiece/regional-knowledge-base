"""Coverage-aware graded metrics. Unknown judgments never become irrelevant."""
import math


def graded_metrics(ranking, judgments, *, k=10, pool=None):
    if len(ranking) != len(set(ranking)):
        raise ValueError('duplicate ranking IDs')
    if any(g not in (0, 1, 2) for g in judgments.values()):
        raise ValueError('grade must be 0/1/2')
    top = ranking[:k]
    unknown = [c for c in top if c not in judgments]
    direct = sum(judgments.get(c) == 2 for c in top)
    useful = sum(c in judgments and judgments[c] >= 1 for c in top)
    out = {'k': k, 'returned': len(top), 'judged': len(top)-len(unknown),
           'unknown': len(unknown), 'judgment_coverage': (len(top)-len(unknown))/len(top) if top else 1,
           'precision_direct': direct/k if not unknown else None,
           'precision_useful': useful/k if not unknown else None,
           'precision_direct_bounds': [direct/k, (direct+len(unknown))/k]}
    # IDCG is pooled, not claimed exhaustive across the whole corpus.
    complete = pool is not None and all(c in judgments for c in pool)
    if complete:
        ideal = sorted((judgments[c] for c in pool), reverse=True)[:k]
        idcg = sum((2**g-1)/math.log2(i+2) for i,g in enumerate(ideal))
        dcg = sum((2**judgments[c]-1)/math.log2(i+2) for i,c in enumerate(top)) if not unknown else None
        out['ndcg_pooled'] = dcg/idcg if idcg and dcg is not None else (0 if not idcg and not unknown else None)
    else:
        out['ndcg_pooled'] = None
    out['pool_judgments_complete'] = complete
    return out


def evidence_recall(question, ranking, k=10):
    groups = question['evidence_groups']
    if not groups:
        return None
    return sum(bool(set(g) & set(ranking[:k])) for g in groups)/len(groups)
