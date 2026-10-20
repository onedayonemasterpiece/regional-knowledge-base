"""Resolved Case contracts only; no SQLite corpus, model or live actions."""
from dataclasses import replace
import hashlib
from uuid import UUID

import pytest
from pydantic import ValidationError
from regional_knowledge.disagreement_contracts import (CaseCreate,CaseGrant,EvidenceAdd,ContributionAdd,
    ContributionCorrect,ConclusionAdd,PersonalDraft,DraftPublication)
from regional_knowledge.disagreement_native import (CASE_REVISIONS,CASE_EDIT,CASE_ACCESS,CASE_DRAFT,CASE_PUBLISH,
    canonical,resolved_case_command,validate_case_command,required_capabilities,case_handler)

ACTOR='00000000-0000-0000-0000-000000000001'
CASE='00000000-0000-0000-0000-000000000002'
OBJECT='00000000-0000-0000-0000-000000000003'
SOURCE='00000000-0000-0000-0000-000000000004'
PAGE='00000000-0000-0000-0000-000000000005'
REGION='00000000-0000-0000-0000-000000000006'


def attribution():return dict(person_id=ACTOR,recorded_by=ACTOR,route='personal',link_id=None,link_revision=None,human_reading_confirmed=False)
def source():return dict(source_id=SOURCE,content_revision=1,eligibility_revision=1)
def create():return CaseCreate(question='Which date describes this event?',context='Exact test research context.',
    classification='apparent discrepancy',rationale='Model-authored synthetic comparison.',
    positions=[dict(id='phase-one',text='Source position one.',speaker='Book author',modality='direct statement')])


def command(action='create',value=None,**changes):
    args=dict(sequence=1,operation_key='synthetic-case-create',request_id='synthetic-request',person_id=ACTOR,
        logical_time='2026-10-20T18:00:00Z',case_id=CASE,object_id=CASE if action=='create' else OBJECT,
        action=action,command=value or create(),source_dependencies=[],attribution=attribution())
    return resolved_case_command(**dict(args,**changes))


def test_existing_typed_case_create_is_resolved_before_acceptance_and_has_no_owner_bypass():
    exact=command()
    assert exact.ids==dict(case_id=CASE,object_id=CASE,actor_id=ACTOR)
    assert exact.route==CASE_EDIT and exact.expected_revisions=={'case_revision':'0','object_version':'0'}
    assert required_capabilities(exact)=={'case_read','case_contribute'}
    assert validate_case_command(exact)['command']==create().model_dump(mode='json')
    assert b'owner_id' not in exact.payload and b'human_reading_confirmed":false' in exact.payload
    with pytest.raises(ValidationError):CaseCreate(**dict(create().model_dump(),actor_id=ACTOR))
    with pytest.raises(ValueError):command(source_dependencies=[source()])
    with pytest.raises(ValueError):command(object_id=OBJECT)


@pytest.mark.parametrize('field,value',[('route','source.import.v4'),('person_id',OBJECT),
    ('ids',{'case_id':CASE,'object_id':CASE,'actor_id':OBJECT}),('schema_revision','old-sqlite'),
    ('expected_revisions',{'case_revision':'00','object_version':'0'})])
def test_native_case_identity_and_semantics_cannot_be_changed_after_resolution(field,value):
    with pytest.raises(ValueError):validate_case_command(replace(command(),**{field:value}))


def test_evidence_requires_exact_current_source_revision_and_literal_input_contract():
    value=EvidenceAdd(document_id=UUID(SOURCE),source_revision=1,source_sha256='a'*64,page_id=UUID(PAGE),
        region_id=UUID(REGION),text_start=3,quote='Original quoted text.',context='Observed context.',
        position_ids=['phase-one'],relation='challenges',speaker='Book author',attribution='direct_statement')
    exact=command('add_evidence',value,source_dependencies=[source()])
    assert required_capabilities(exact)=={'case_read','case_contribute'}
    assert validate_case_command(exact)['command']['quote']==value.quote
    with pytest.raises(ValueError):command('add_evidence',value)
    with pytest.raises(ValueError):command('add_evidence',value,source_dependencies=[dict(source(),content_revision=2)])
    with pytest.raises(ValueError):command('add_evidence',value,source_dependencies=[source(),source()])


def test_private_draft_capability_and_scope_are_distinct_from_shared_publication():
    draft=PersonalDraft(text='Private thought. Selected conclusion.',based_on_seq=1,evidence_ids=[],position_ids=['phase-one'])
    exact=command('draft_save',draft,object_version=0)
    assert exact.route==CASE_DRAFT and required_capabilities(exact)=={'case_read','personal_conclusion'}
    handler=case_handler(CASE_DRAFT,revisions=CASE_REVISIONS,check_sha256='a'*64,apply_sha256='b'*64)
    assert handler.dependency.scope_class=='private_draft' and handler.dependency.scope_owner=='actor'
    publication=command('draft_publish',DraftPublication(text_start=17,text_end=37),object_version=1)
    assert publication.route==CASE_PUBLISH
    assert required_capabilities(publication)=={'case_read','personal_conclusion','case_contribute'}
    with pytest.raises(ValueError):command('draft_publish',DraftPublication(text_start=0,text_end=1),object_version=0)


def test_case_manager_grant_is_not_private_draft_authority_and_shared_conclusion_is_separate():
    grant=command('grant',CaseGrant(grantee=UUID(OBJECT),capability='manager',reason='synthetic grant'),case_revision=1)
    assert grant.route==CASE_ACCESS and required_capabilities(grant)=={'case_read','maintain_shared_summary'}
    handler=case_handler(CASE_ACCESS,revisions=CASE_REVISIONS,check_sha256='a'*64,apply_sha256='b'*64)
    assert handler.dependency.restriction and handler.dependency.scope_owner=='shared'
    common=dict(text='Model-authored exact conclusion.',outcome='insufficient_material',allowed_wording='Limited wording.',
        limitations='Only this observed material.',open_questions=['Missing independent original.'],based_on_seq=1,weight_analysis='No authority inferred.')
    personal=command('conclude',ConclusionAdd(kind='individual_opinion',**common),case_revision=1)
    shared=command('conclude',ConclusionAdd(kind='model_synthesis',make_current=True,**common),case_revision=1)
    assert required_capabilities(personal)=={'case_read','personal_conclusion'}
    assert required_capabilities(shared)=={'case_read','maintain_shared_summary'}
    with pytest.raises(ValidationError):ConclusionAdd(kind='individual_opinion',make_current=True,**common)


def test_correct_own_contribution_has_explicit_version_and_attribution_cannot_claim_human_reading():
    value=ContributionCorrect(original_text='Exact corrected own wording.',reason='Synthetic correction.')
    exact=command('correct_contribution',value,object_version=1)
    assert required_capabilities(exact)=={'case_read','case_contribute'}
    with pytest.raises(ValueError):command('correct_contribution',value,object_version=0)
    with pytest.raises(ValueError):command(attribution=dict(attribution(),human_reading_confirmed=True))
    with pytest.raises(ValueError):command(attribution=dict(attribution(),person_id=OBJECT))
    with pytest.raises(ValueError):command(attribution=dict(attribution(),route='expert'))
