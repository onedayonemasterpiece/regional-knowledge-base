"""Optional model mistakes do not turn a valid reviewed page into a failed import."""
import copy

import pytest

from regional_knowledge.kaggle_source_materializer import NativeSourceCompiler, SourceMaterializationRejected
from regional_knowledge.source_batch_context import load_current
from regional_knowledge.source_stage import canonical
from test_native_source_materializer import exact_batch, SOURCE, PERSON
from test_source_batch_context import Connection


@pytest.mark.asyncio
@pytest.mark.parametrize('optional', ['story_schema', 'story_evidence', 'graph_schema','illustration_evidence'])
async def test_current_text_compiles_before_invalid_optional_model_data(optional):
    doc = exact_batch(count=1)
    page = doc['model_payload']['pages'][0]
    if optional == 'graph_schema':
        doc['model_payload']['entity_candidates'] = {'entities': [{'missing_identity': True}]}
    elif optional == 'illustration_evidence':
        doc['model_payload']['chunks'][0]['illustration_refs']=[dict(
            page_id=exact_batch(start=8,count=1)['model_payload']['pages'][0]['page_id'],
            illustration_key='missing-picture')]
    elif optional == 'story_schema':
        doc['model_payload']['story_candidates'] = [{'candidate_key': 'incomplete-story'}]
    else:
        doc['model_payload']['story_candidates'] = [dict(candidate_key='bad-proof',
            title='Synthetic finding', summary='A model finding with malformed evidence.',
            proposition='The source states a synthetic fact.', narrative_outcome='facts_only',
            evidence_refs=[dict(page_id=page['page_id'], region_key='body', chunk_key='absent-passage')])]
    raw = canonical(doc)
    db = Connection()
    view = await load_current(db, raw, SOURCE, text_only=True)
    frozen = copy.deepcopy(view)
    async def count(_):
        return 10
    compiler = NativeSourceCompiler(count=count, current=lambda: True)
    result, media = await compiler.compile_current(raw, view, source_id=SOURCE,
        person_id=PERSON, text_only=True)
    assert canonical(doc) == raw and view == frozen
    assert [p['body'] for p in result['passages']] == [page['regions'][0]['source_text']]
    assert result['semantic']['findings'] == [] and result['semantic']['stories'] == []
    assert media['illustrations'] == [] and db.queries == []
    with pytest.raises(SourceMaterializationRejected):
        await compiler.compile_current(raw, view, source_id=SOURCE, person_id=PERSON)


@pytest.mark.asyncio
async def test_current_text_still_rejects_a_broken_transcription():
    doc = exact_batch(count=1)
    doc['model_payload']['pages'][0]['regions'][0]['source_text'] = ''
    raw = canonical(doc)
    view = await load_current(Connection(), raw, SOURCE, text_only=True)
    async def count(_):
        raise AssertionError('invalid transcription reached tokenization')
    with pytest.raises(SourceMaterializationRejected):
        await NativeSourceCompiler(count=count, current=lambda: True).compile_current(
            raw, view, source_id=SOURCE, person_id=PERSON, text_only=True)
