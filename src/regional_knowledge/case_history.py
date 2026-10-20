"""Compact policy projection of exactly verified successful Case commands.

No body, quote, private draft or source text is retained in this projection.
"""
from __future__ import annotations
from dataclasses import dataclass
import json
from .disagreement_native import validate_case_command
from .kaggle_case_contracts import canonical


@dataclass(frozen=True)
class CaseHistory:
    wire:bytes

    @property
    def cases(self):return json.loads(self.wire)


def project_cases(commands,statuses,previous=None):
    cases={} if previous is None else previous.cases
    for command,handler,_ in sorted(commands,key=lambda value:value[0].sequence):
        if not command.route.startswith('disagreement.') or statuses.get(command.sequence)!='applied':continue
        if command.schema_revision=='rkb-v4-sql001-011-v1':
            from .kaggle_domain_bindings import validate_current_command
            validate_current_command(command)
            doc=json.loads(command.payload)
        else:doc=validate_case_command(command)
        action=doc['action'];op=doc['command'];case_id=command.ids['case_id'];actor=command.person_id
        if action=='create':
            if case_id in cases:raise ValueError('duplicate verified Case creation')
            cases[case_id]=dict(owner=actor,revision=1,acl_revision=1,event_seq=0,state='open',
                current_conclusion_id=None,grants={},sources={},drafts={})
        state=cases.get(case_id)
        if state is None:raise ValueError('Case history has no exact creation')
        role=4 if state['owner']==actor else {'viewer':1,'contributor':2,'curator':3,'manager':4}.get(state['grants'].get(actor),0)
        needed=1 if action=='draft_save' else 4 if action=='grant' else 3 if action in ('revise','state') or action=='conclude' and op['make_current'] else 2
        if action!='create' and role<needed:raise ValueError('accepted Case role history differs')
        if action in ('revise','state','grant','conclude') and int(command.expected_revisions['case_revision'])!=state['revision']:
            raise ValueError('accepted Case revision history differs')
        if action=='grant':
            state['acl_revision']+=1
            if op['capability'] is None:state['grants'].pop(op['grantee'],None)
            else:state['grants'][op['grantee']]=op['capability']
        if action in ('revise','state','grant') or action=='conclude' and op['make_current']:state['revision']+=1
        if action=='state':state['state']=op['state']
        if action=='conclude' and op['make_current']:state['current_conclusion_id']=command.ids['object_id']
        if action=='add_evidence':
            source=op['document_id'];revision=op['source_revision']
            if source in state['sources'] and state['sources'][source]!=revision:
                raise ValueError('Case source content history differs')
            state['sources'][source]=revision
        if action=='draft_save':
            key=command.ids['object_id'];old=state['drafts'].get(key)
            expected=int(command.expected_revisions['object_version'])
            if (old is not None and old['actor']!=actor or (old['version'] if old else 0)!=expected):
                raise ValueError('Case draft author/version history differs')
            state['drafts'][key]=dict(actor=actor,version=expected+1)
        else:state['event_seq']+=1
    return CaseHistory(canonical(cases))
