"""Exact resolved native case commands; no SQLite journal or public identity fields."""
from __future__ import annotations

import hashlib
import json
from uuid import UUID

from .disagreement_contracts import (CaseCreate,CaseRevise,CaseState,CaseGrant,EvidenceAdd,
    ContributionAdd,ContributionCorrect,ConclusionAdd,PersonalDraft,DraftPublication)
from .kaggle_apply import Revisions,HandlerSpec,PgFunction,DependencySpec
from .recovery_journal import ExactCommand

CASE_REVISIONS=Revisions('rkb-v4-sql001-010-v1','rkb-v4-source-story-cases-v5','rkb-python-apply-v1')
CASE_EDIT='disagreement.edit.v1'
CASE_ACCESS='disagreement.access.v1'
CASE_DRAFT='disagreement.draft.v1'
CASE_PUBLISH='disagreement.publish.v1'
_ACTIONS={
    'create':(CASE_EDIT,CaseCreate),'revise':(CASE_EDIT,CaseRevise),'state':(CASE_EDIT,CaseState),
    'add_evidence':(CASE_EDIT,EvidenceAdd),'contribute':(CASE_EDIT,ContributionAdd),
    'correct_contribution':(CASE_EDIT,ContributionCorrect),'conclude':(CASE_EDIT,ConclusionAdd),
    'grant':(CASE_ACCESS,CaseGrant),'draft_save':(CASE_DRAFT,PersonalDraft),'draft_publish':(CASE_PUBLISH,DraftPublication),
}
_PRECONDITIONS=frozenset({'case_revision','object_version'})
DEPENDENCIES={route:DependencySpec(route,'private_draft' if route==CASE_DRAFT else 'case',
    frozenset({'case_id','object_id'}) if route==CASE_DRAFT else frozenset({'case_id'}),
    'actor' if route==CASE_DRAFT else 'shared',route==CASE_ACCESS) for route in (CASE_EDIT,CASE_ACCESS,CASE_DRAFT,CASE_PUBLISH)}


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def _uuid(value):
    if type(value) is not str or str(UUID(value))!=value:raise ValueError('canonical case/object/person UUID required')
    return value


def _revision(value):
    if type(value) is not str or not value.isascii() or not value.isdecimal() or not 1<=len(value)<=19 or str(int(value))!=value or not 0<=int(value)<2**63:
        raise ValueError('exact nonnegative native CAS revision required')
    return int(value)


def validate_case_payload(payload):
    if type(payload) is not bytes or not 1<=len(payload)<=128*1024:raise ValueError('bounded exact case command required')
    doc=json.loads(payload)
    if (type(doc) is not dict or set(doc)!={'codec','action','command','source_dependencies','attribution'}
            or doc['codec']!='rkb.disagreement-command.v1' or doc['action'] not in _ACTIONS
            or canonical(doc)!=payload):raise ValueError('canonical typed case payload required')
    _,kind=_ACTIONS[doc['action']]
    command=kind.model_validate_json(canonical(doc['command']),strict=True)
    if command.model_dump(mode='json')!=doc['command']:raise ValueError('fully resolved case input required')
    for key in ('evidence_ids','position_ids'):
        values=doc['command'].get(key,[])
        if len(set(values))!=len(values):raise ValueError('unique exact case references required')
    dependencies=doc['source_dependencies']
    if type(dependencies) is not list or len(dependencies)>32:raise ValueError('bounded exhaustive case source dependencies required')
    seen=[]
    for ref in dependencies:
        if (type(ref) is not dict or set(ref)!={'source_id','content_revision','eligibility_revision'}
                or type(ref['content_revision']) is not int or not 1<=ref['content_revision']<2**63
                or type(ref['eligibility_revision']) is not int or not 1<=ref['eligibility_revision']<2**63):raise ValueError('exact case source revisions required')
        seen.append(_uuid(ref['source_id']))
    if seen!=sorted(set(seen)):raise ValueError('canonical unique case source inventory required')
    attr=doc['attribution']
    if (type(attr) is not dict or set(attr)!={'person_id','recorded_by','route','link_id','link_revision','human_reading_confirmed'}
            or attr['route'] not in ('owner','personal','expert') or attr['human_reading_confirmed'] is not False
            or type(attr['recorded_by']) is not str or not 1<=len(attr['recorded_by'])<=256):
        raise ValueError('trusted exact case attribution required')
    _uuid(attr['person_id'])
    if attr['route']=='expert':
        _uuid(attr['link_id'])
        if type(attr['link_revision']) is not int or attr['link_revision']<1:raise ValueError('verified external attribution revision required')
    elif attr['link_id'] is not None or attr['link_revision'] is not None:
        raise ValueError('personal attribution cannot claim external observation')
    if doc['action']=='add_evidence':
        ref=next((ref for ref in dependencies if ref['source_id']==str(command.document_id)),None)
        if ref is None or ref['content_revision']!=command.source_revision:
            raise ValueError('new evidence must bind its exact current source dependency')
    return doc


def validate_case_command(command):
    if (type(command) is not ExactCommand or command.kind!='domain_command'
            or (command.schema_revision,command.function_revision,command.executor_revision)!=CASE_REVISIONS.wire()
            or set(command.ids)!={'case_id','object_id','actor_id'} or set(command.expected_revisions)!=_PRECONDITIONS):
        raise ValueError('exact ordered native case command required')
    for value in command.ids.values():_uuid(value)
    if command.ids['actor_id']!=command.person_id:raise ValueError('case actor must equal the accepted command person')
    doc=validate_case_payload(command.payload)
    if command.route!=_ACTIONS[doc['action']][0] or doc['attribution']['person_id']!=command.person_id:
        raise ValueError('case route/action/attribution mismatch')
    case_revision=_revision(command.expected_revisions['case_revision'])
    version=_revision(command.expected_revisions['object_version'])
    action=doc['action']
    if action=='create':
        if case_revision or version or command.ids['object_id']!=command.ids['case_id'] or doc['source_dependencies']:
            raise ValueError('new private case requires absent exact identity')
    elif action in ('revise','state','grant','conclude') and case_revision<1:
        raise ValueError('case CAS revision required')
    elif action in ('correct_contribution','draft_publish') and version<1:
        raise ValueError('object CAS version required')
    if action not in ('draft_save','correct_contribution','draft_publish') and version:
        raise ValueError('unexpected object revision')
    return doc


def required_capabilities(command):
    doc=validate_case_command(command);action=doc['action']
    capabilities={'case_read'}
    if action in ('create','add_evidence','contribute','correct_contribution'):capabilities.add('case_contribute')
    elif action in ('revise','state','grant'):capabilities.add('maintain_shared_summary')
    elif action=='conclude':capabilities.add('personal_conclusion' if doc['command']['kind']=='individual_opinion' else 'maintain_shared_summary')
    elif action=='draft_save':capabilities.add('personal_conclusion')
    elif action=='draft_publish':capabilities.update(('personal_conclusion','case_contribute'))
    return frozenset(capabilities)


def resolved_case_command(*,sequence,operation_key,request_id,person_id,logical_time,case_id,object_id,
                          action,command,source_dependencies,attribution,case_revision=0,object_version=0,
                          revisions=CASE_REVISIONS):
    if revisions!=CASE_REVISIONS or action not in _ACTIONS:raise ValueError('ordered case handler required')
    _,kind=_ACTIONS[action]
    if type(command) is not kind:raise TypeError('exact case action input required')
    doc=dict(codec='rkb.disagreement-command.v1',action=action,command=command.model_dump(mode='json'),
        source_dependencies=source_dependencies,attribution=attribution)
    exact=ExactCommand(sequence=sequence,operation_key=operation_key,request_id=request_id,person_id=person_id,
        logical_time=logical_time,route=_ACTIONS[action][0],schema_revision=revisions.schema,
        function_revision=revisions.function,executor_revision=revisions.executor,
        ids=dict(case_id=case_id,object_id=object_id,actor_id=person_id),
        expected_revisions=dict(case_revision=str(case_revision),object_version=str(object_version)),payload=canonical(doc))
    validate_case_command(exact)
    return exact


def case_handler(route,*,check_sha256,apply_sha256,revisions=CASE_REVISIONS):
    if route not in DEPENDENCIES or revisions!=CASE_REVISIONS:raise ValueError('exact installed case handler required')
    return HandlerSpec('domain_command',route,revisions,PgFunction('check_disagreement_v1',check_sha256),
        PgFunction('apply_disagreement_v1',apply_sha256),_PRECONDITIONS,dependency=DEPENDENCIES[route])
