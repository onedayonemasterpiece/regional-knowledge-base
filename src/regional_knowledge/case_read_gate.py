"""Case ACL and private-author output gate on the existing verified history."""
from __future__ import annotations
from dataclasses import dataclass

from .case_history import CaseHistory
from .control_spool import _database,_command
from .disagreement_native import DEPENDENCIES,CASE_REVISIONS,case_handler
from .kaggle_case_contracts import CaseReadRequest,CaseReadResult,validate_case_record
from .kaggle_protocol import TrustedActorContext
from .source_read_gate import SourceReadGate,SourceGateDenied,DispatchPermit,_Read,binding_key


@dataclass(frozen=True)
class CaseReadFacts:
    sources:object
    anchor:object
    version:object
    sequence:int
    pending:bool
    unknown:bool


class CaseReadGate(SourceReadGate):
    def cases_supported(self):
        from .kaggle_domain_bindings import DOMAIN_REVISIONS,current_handler
        from .source_activation import ACTIVATION_REVISIONS
        from .kaggle_native_source_bindings import native_source_handler
        for handler in self.spool.registry.handlers.values():
            if handler.route not in DEPENDENCIES:continue
            if handler.revisions==CASE_REVISIONS:
                expected=case_handler(handler.route,check_sha256=handler.check.definition_sha256,apply_sha256=handler.apply.definition_sha256)
            elif handler.revisions==DOMAIN_REVISIONS:
                expected=current_handler(handler.route,check_sha256=handler.check.definition_sha256,apply_sha256=handler.apply.definition_sha256)
            elif handler.revisions==ACTIVATION_REVISIONS:
                expected=native_source_handler(handler.route,check_sha256=handler.check.definition_sha256,apply_sha256=handler.apply.definition_sha256)
            else:continue
            if handler==expected:return True
        return False

    async def _authority(self,authority,query):
        await self.authority.require(authority,'case_read')
        if query.section in ('draft_get','draft_list'):
            current=await self.authority.recheck(authority)
            if 'personal_conclusion' not in current.capabilities:
                raise SourceGateDenied('private conclusion capability unavailable')

    def _policy(self,query,authority):
        anchor=self.spool._source_anchor
        state=getattr(anchor,'case_state',None)
        if anchor is None or not anchor.current() or type(state) is not CaseHistory:return None,None,0
        case=state.cases.get(query.case_id)
        if case is None:raise SourceGateDenied('Case access unavailable')
        role=4 if case['owner']==authority.person_id else {'viewer':1,'contributor':2,'curator':3,'manager':4}.get(case['grants'].get(authority.person_id),0)
        if role<1:raise SourceGateDenied('Case access unavailable')
        if query.section=='draft_get':
            draft=case['drafts'].get(query.draft_id)
            if draft is None or draft['actor']!=authority.person_id:
                raise SourceGateDenied('private draft access unavailable')
        return anchor,case,role

    async def dispatch(self,authority,query,binding):
        if type(query) is not CaseReadRequest:raise SourceGateDenied('fixed Case read required')
        CaseReadRequest.model_validate_json(query.model_dump_json())
        self._expire();await self._authority(authority,query)
        if not self.cases_supported() or len(self._permits)+len(self._tickets)>=self.max_retained:
            raise SourceGateDenied('Case semantics unavailable')
        key=binding_key(binding)
        if not self.binding_current(binding) or query.connection_generation!=key[2]:raise SourceGateDenied('runtime binding changed')
        # Native read_disagreement_v1 checks actor role and draft ownership in
        # the PG read transaction; a global history projection is not admission.
        actor=TrustedActorContext(person_id=authority.person_id,audience=authority.destination.key,
            auth_epoch=authority.auth_revision,visibility_epoch=0,context_ref=authority.context_ref)
        query=query.model_copy(update={'actor':actor,'min_data_seq':max(query.min_data_seq,key[4])})
        permit=DispatchPermit(query)
        self._permits[permit]=_Read(authority,query,binding,key,self.clock()+min(self.ttl_seconds,query.deadline_ms/1000))
        return permit

    def _local_scope(self,query,actor):
        keys=['case:'+query.case_id]
        prefix='draft:'+actor+':'+query.case_id+':' if query.section in ('draft_get','draft_list') else None
        with self.spool._db() as db:
            clauses=['source_id=?'];params=[keys[0]]
            if prefix is not None:
                if query.section=='draft_get':clauses.append('source_id=?');params.append(prefix+query.draft_id)
                else:clauses.append('(source_id>=? AND source_id<?)');params.extend([prefix,prefix+'\uffff'])
            row=db.execute("SELECT coalesce(max(sequence),0),coalesce(sum(outcome_state<>'DURABLE'),0),"
                "coalesce(sum(seal_state='SENT_UNKNOWN' OR apply_state='SENT_UNKNOWN' OR outcome_state='SENT_UNKNOWN'),0) "
                'FROM rkb_control_operations WHERE '+' OR '.join(clauses),params).fetchone()
        return row[0],bool(row[1]),bool(row[2])

    async def _facts(self,query,authority,anchor,case):
        version=self.spool._source_version
        ids=tuple(sorted(set(case['sources'])|({query.candidate_source_id} if query.candidate_source_id is not None else set())))
        sources=await _database(self.spool._read_facts,ids,ids,anchor,version,self.spool._source_covered_sequence)
        seq,pending,unknown=await _database(self._local_scope,query,authority.person_id)
        for raw,_,_ in anchor.pending:
            command=_command(raw)
            if command.ids.get('case_id')!=query.case_id:continue
            private=command.route=='disagreement.draft.v1'
            if private and (query.section not in ('draft_get','draft_list') or command.person_id!=authority.person_id
                    or query.section=='draft_get' and command.ids['object_id']!=query.draft_id):continue
            seq=max(seq,command.sequence);pending=True
        return CaseReadFacts(sources,anchor,version,seq,pending,unknown)

    def _current(self,facts):
        return (type(facts) is CaseReadFacts and facts.anchor is self.spool._source_anchor
            and facts.version is self.spool._source_version and self.spool.facts_current(facts.sources))

    @staticmethod
    def _closed(result,reason):
        return result.model_copy(update={'status':'denied' if reason=='access_denied' else 'unavailable',
            'record_b64':None,'reason':reason,'evidence':(),'prior_write_pending':reason=='prior_write_pending'})

    def _case_restriction_after(self,case_id,sequence):
        # A not-yet-observed local grant/revoke cannot make an older PG result
        # public. Unlike global history, this is just the affected Case ACL.
        with self.spool._db() as db:
            return db.execute("""SELECT 1 FROM rkb_control_operations WHERE source_id=?
                AND sequence>? AND json_extract(CAST(dependency_bytes AS TEXT),
                    '$.restriction')=1 LIMIT 1""",('case:'+case_id,sequence)).fetchone() is not None

    async def _gate(self,read,result):
        await self._authority(read.authority,read.query)
        if not self._binding(read):raise SourceGateDenied('runtime binding changed')
        q=read.query
        if type(result) is not CaseReadResult or any(getattr(q,k)!=getattr(result,k)
                for k in ('request_id','connection_generation','turn_generation')):
            raise SourceGateDenied('Case result request binding differs')
        try:CaseReadResult.model_validate_json(result.model_dump_json())
        except (ValueError,TypeError):result=self._closed(result,'internal_error')
        if result.status!='ok':
            read.facts,read.result=None,result
            return result
        try:
            record=validate_case_record(q,result.record)
            if (record.get('case_id')!=q.case_id or result.observed_database_generation!=read.key[3]
                    or result.observed_data_seq is None or result.observed_data_seq<q.min_data_seq
                    or result.observed_projection_seq is None or result.observed_projection_seq<q.min_data_seq
                    or result.observed_visibility_epoch is None):
                raise ValueError('native Case database binding differs')
            deps=record.get('source_dependencies')
            if type(deps) is not list or len(deps)>32:raise ValueError('Case dependencies invalid')
            seen=set()
            for dep in deps:
                if (type(dep) is not dict or set(dep)!={'source_id','content_revision','eligibility_revision'}
                        or dep['source_id'] in seen or type(dep['content_revision']) is not int
                        or type(dep['eligibility_revision']) is not int
                        or min(dep['content_revision'],dep['eligibility_revision'])<1):
                    raise ValueError('Case source references invalid')
                from .kaggle_case_contracts import exact_uuid
                exact_uuid(dep['source_id']);seen.add(dep['source_id'])
            if q.section=='operation':
                if record.get('operation_key')!=q.operation_key:
                    raise ValueError('Case operation is not this actor request')
            else:
                if (record.get('section')!=q.section or type(record.get('case_role')) is not int
                        or not 1<=record['case_role']<=4
                        or type(record.get('acl_revision')) is not int or record['acl_revision']<1
                        or type(record.get('event_seq')) is not int or record['event_seq']<0
                        or type(record.get('case_revision')) is not int or record['case_revision']<1
                        or q.revision is not None and record['case_revision']!=q.revision):
                    raise ValueError('native Case role/record differs')
                if (q.section=='draft_get' and (record.get('draft_id')!=q.draft_id
                        or record.get('version')!=(q.draft_version or record.get('current_version')))):
                    raise ValueError('private draft actor/selector differs')
                if (q.section=='edit_context' and (record.get('candidate_dependency') is not None
                        and record['candidate_dependency'].get('source_id')!=q.candidate_source_id)):
                    raise ValueError('Case candidate scope differs')
            if self._case_restriction_after(q.case_id,result.observed_data_seq):
                result=self._closed(result,'prior_write_pending')
        except (ValueError,TypeError,KeyError,AttributeError):
            result=self._closed(result,'internal_error')
        await self._authority(read.authority,q)
        if not self._binding(read):raise SourceGateDenied('runtime binding changed')
        if result.status=='ok' and self._case_restriction_after(q.case_id,result.observed_data_seq):
            result=self._closed(result,'prior_write_pending')
        read.facts=(q.case_id,result.observed_data_seq) if result.status=='ok' else None
        read.result=result
        return result

    def issuance_current(self,ticket):
        read=self._tickets.get(ticket)
        try:
            return bool(read is not None and self._binding(read) and read.result is not None
                and (read.facts is None or not self._case_restriction_after(*read.facts)))
        except Exception:return False
