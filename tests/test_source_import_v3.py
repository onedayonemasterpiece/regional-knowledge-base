"""Exact fresh8 acceptance; prior source command revisions stay immutable."""
from dataclasses import replace
from types import SimpleNamespace

import pytest

from regional_knowledge.source_import_v2 import STORY_REVISIONS, validate_import_v2_command
from regional_knowledge.source_import_v3 import (FACT_REVISIONS, resolved_import_v3_command,
    validate_import_v3_command,source_import_v3_handler)
from regional_knowledge.kaggle_source_commands import decode_command
from regional_knowledge.kaggle_apply import HandlerRegistry
from test_source_import_v2 import command as v2_command, payload, FINDING
from test_source_import import SOURCE
from test_kaggle_fact_read import date, observation


def facts_payload(*, narrative=False):
    import hashlib
    value=payload()
    text='On 29 February 1900 (Julian), the village reported 12.50 persons in 1900.'
    value['passages'][0]['body']=text
    value['semantic']['regions'][0].update(source_text=text,source_text_sha256=hashlib.sha256(text.encode()).hexdigest())
    finding=value['semantic']['findings'][0]
    finding.update(proposition=text,event_dates=[date()],observations=[observation()])
    finding['evidence'][0].update(start=0,end=len(text),original_excerpt=text)
    if not narrative:value['semantic']['stories']=[]
    return value


def command(value=None,**changes):
    original=v2_command()
    kwargs=dict(sequence=original.sequence,operation_key=original.operation_key,request_id=original.request_id,
        person_id=original.person_id,logical_time=original.logical_time,revisions=FACT_REVISIONS,
        source_id=SOURCE,payload=facts_payload() if value is None else value)
    return resolved_import_v3_command(**dict(kwargs,**changes))


def test_explicit_fresh8_preserves_exact_payload_and_never_reinterprets_v2():
    fresh=command()
    assert fresh.route=='source.import.v3' and validate_import_v3_command(fresh)==facts_payload()
    assert decode_command(fresh.encode())==fresh
    old=v2_command()
    assert validate_import_v2_command(old)==payload() and decode_command(old.encode())==old
    for invalid in (old,replace(fresh,schema_revision=STORY_REVISIONS.schema),replace(old,route='source.import.v3')):
        with pytest.raises(ValueError):validate_import_v3_command(invalid)
    with pytest.raises(ValueError):validate_import_v2_command(fresh)
    with pytest.raises(ValueError):command(revisions=STORY_REVISIONS)


@pytest.mark.parametrize('bad',['date_wording','value_wording','period_wording','duplicate_index'])
def test_v3_precondition_rejects_facts_absent_from_selected_evidence_before_acceptance(bad):
    value=facts_payload();finding=value['semantic']['findings'][0]
    if bad=='date_wording':finding['event_dates'][0]['original_date_text']='29 February 1904'
    if bad=='value_wording':finding['observations'][0]['original_value_text']='999.00'
    if bad=='period_wording':finding['observations'][0]['period_text']='1904'
    if bad=='duplicate_index':finding['observations'][0]['evidence_indexes']=[0,0]
    with pytest.raises(ValueError):command(value)
    # Existing accepted schema7 semantics are deliberately unchanged.
    assert validate_import_v2_command(v2_command(value))==value


def test_schema8_codec_uses_one_explicit_fixture_handler():
    registry=HandlerRegistry([source_import_v3_handler(revisions=FACT_REVISIONS,check_sha256='a'*64,apply_sha256='b'*64)])
    assert registry.resolve(command()).route=='source.import.v3'


def test_exact_decimal_exponent_is_preserved_without_conversion():
    value=facts_payload();value['semantic']['findings'][0]['observations'][0]['value']='1E+8'
    accepted=command(value)
    assert validate_import_v3_command(accepted)['semantic']['findings'][0]['observations'][0]['value']=='1E+8'
