"""Explain exact sourced quantities without asserting identity or doing arithmetic."""
from .kaggle_fact_contracts import observation_position
from .story_contracts import StageObservationInput


def render_comparison(result):
    """Render only the currently gated pair; refresh never retains old private text."""
    if result.status != 'ok' or len(result.records) != 2 or len(result.evidence) != 2:
        raise ValueError('complete current observation pair required')
    left, right = result.records
    if (left['id'] == right['id'] or left['story_id'] != right['story_id'] or
            left['story_revision'] != right['story_revision']):
        raise ValueError('same Story pair required')
    facts = []
    for record in result.records:
        observation_position(record['id'])
        fact = StageObservationInput.model_validate(record['fact']).model_dump(mode='json')
        if fact != record['fact']:
            raise ValueError('exact sourced observation required')
        facts.append(fact)
    first, second = facts
    if first['metric_key'] != second['metric_key']:
        verdict = 'different_metrics'
    elif first['unit_code'] != second['unit_code']:
        verdict = 'different_units'
    elif first['subject_label'] != second['subject_label']:
        verdict = 'unverified_subject_identity'
    elif first['method'] != second['method'] or first['method'] == 'unknown':
        verdict = 'incompatible_or_unknown_method'
    elif first['period_start_year'] is None or second['period_start_year'] is None:
        verdict = 'period_not_confirmed'
    else:
        verdict = 'manual_identity_and_semantic_review_required'

    def item(record, evidence, fact):
        return dict(**record, **{k:v for k,v in fact.items() if k!='value'},
            observation_id=record['id'], value_decimal=fact['value'],
            bibliography=evidence.metadata.citation,
            source_locator=evidence.metadata.locator,
            source_dependencies=[ref.model_dump(mode='json') for ref in evidence.dependencies.source_revisions])

    return dict(story_id=left['story_id'], story_revision=left['story_revision'],
        left=item(left,result.evidence[0],first), right=item(right,result.evidence[1],second),
        comparable=False, difference=None, ratio=None, verdict=verdict,
        reason='Exact same label is not canonical identity or matching methodology.')
