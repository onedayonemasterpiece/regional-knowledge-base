"""Pure exact command checks; no native authorization is inferred from builders."""
from dataclasses import replace
import json
import pytest
from regional_knowledge.story_editorial import *
from regional_knowledge.story_contracts import UpsertVariant

PERSON='60000000-0000-0000-0000-000000000002'
SOURCE='61000000-0000-0000-0000-000000000001'
STORY='73000000-0000-0000-0000-000000000001'
VARIANT='74000000-0000-0000-0000-000000000001'
FINDING='72000000-0000-0000-0000-000000000001'


def operation(**changes):
    return UpsertVariant(**dict(dict(op='upsert_variant',variant_id=VARIANT,expected_variant_revision=1,
        audience='general',format='story',body='The exact revised narrative remains saved.',
        blocks=[dict(text='The exact revised narrative remains saved.',assertion_id=FINDING,assertion_revision=1)]),**changes))


def command(op=None):
    return resolved_story_edit_command(sequence=2,operation_key='story-edit-first',request_id='request-edit',
        person_id=PERSON,logical_time='2026-10-20T18:00:00Z',source_id=SOURCE,story_id=STORY,
        expected_revision=1,source_revision=1,eligibility_revision=2,operation=op or operation(),
        embedding=[1.]+[0.]*1023,input_tokens=12)


def test_exact_body_and_old_variant_reference_are_replayable_without_model():
    c=command();doc=validate_story_edit_command(c)
    assert doc['operation']['body']==operation().body and doc['resolved_variant_id']==VARIANT
    assert command().encode()==c.encode()
    assert c.ids['actor_id']==c.person_id and c.expected_revisions['variant_revision']=='1'
    assert 'publish_ready' not in c.payload.decode()


def test_new_variant_id_is_resolved_once_and_keeps_exact_zero_expected_revision():
    c=command(operation(variant_id=None,expected_variant_revision=None))
    assert c.expected_revisions['variant_revision']=='0'
    assert validate_story_edit_command(c)['resolved_variant_id']==validate_story_edit_command(command(
        operation(variant_id=None,expected_variant_revision=None)))['resolved_variant_id']


@pytest.mark.parametrize('case',['actor','variant','source_revision','schema','input_tokens','fp32','assertion'])
def test_forged_or_ambiguous_editorial_bytes_cannot_enter_the_native_route(case):
    c=command();doc=validate_story_edit_command(c)
    if case=='actor':c=replace(c,person_id='60000000-0000-0000-0000-000000000003')
    if case=='variant':doc['resolved_variant_id']=STORY
    if case=='source_revision':c=replace(c,expected_revisions={**c.expected_revisions,'source_revision':'01'})
    if case=='schema':c=replace(c,schema_revision='rkb-v4-sql001-008-v1')
    if case=='input_tokens':doc['input_tokens']=513
    if case=='fp32':doc['embedding'][1]=.00000000001
    if case=='assertion':doc['operation']['blocks'][0]['assertion_revision']=None
    if case in ('variant','input_tokens','fp32','assertion'):c=replace(c,payload=json.dumps(doc,sort_keys=True,separators=(',',':')).encode())
    with pytest.raises((ValueError,TypeError)):validate_story_edit_command(c)
