"""Bounded request result/delivery control on one existing Dev authority.

A proof of remote intent is required before result-readiness or any Slack
send. This is an *unwired* F10 source slice: the provider adapter and native
PG evidence/issuance guard must be connected before any real delivery.
No new journal, service, source corpus or network access is created here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import hmac
import json
import re
import time
from uuid import UUID, NAMESPACE_URL, uuid5

from .contracts import Principal
from .expert_request_control import (
    TargetExpertRequestControl, ExecutorCredentials, RequestControlError, _canonical, _fail,
)
from .participants import ParticipantError
from .recovery_journal import AcceptedSeal, ExactCommand
from . import request_order


_RECEIPT = object()


@dataclass(frozen=True)
class NativeSlackReceipt:
    """Native transport confirmation; never a user-supplied tool argument."""
    app_id: str
    team_id: str
    bot_user_id: str
    channel_id: str
    thread_ts: str
    message_ts: str
    client_message_id: str
    text: str
    _trusted: object = field(default=None,repr=False,compare=False)


def verified_native_slack_receipt(raw, *, app_id, team_id, bot_user_id,
                                  channel_id, thread_ts):
    """Use only on a native verified Slack response/echo from the owner adapter."""
    if type(raw) is not dict or any(type(x) is not str or not x
                                   for x in (app_id,team_id,bot_user_id,channel_id,thread_ts)):
        _fail('native_receipt_required')
    if (raw.get('app_id')!=app_id or raw.get('user')!=bot_user_id
            or raw.get('channel')!=channel_id or raw.get('thread_ts')!=thread_ts
            or type(raw.get('client_msg_id')) is not str
            or type(raw.get('text')) is not str
            or not re.fullmatch(r'[0-9]+\.[0-9]+',str(raw.get('ts','')))):
        _fail('native_receipt_mismatch')
    return NativeSlackReceipt(app_id,team_id,bot_user_id,channel_id,thread_ts,
                              raw['ts'],raw['client_msg_id'],raw['text'],_RECEIPT)


class TargetExpertDeliveryControl:
    """Local control transitions with remote-verified result/send boundaries.

    The injected gate is the existing target source/Case output authority;
    no dummy allow-all gate is installed at runtime. Tests use a synthetic
    gate to prove deny/UNKNOWN order. This component never posts to Slack.
    """
    def __init__(self, control: TargetExpertRequestControl, *, source_gate):
        if type(control) is not TargetExpertRequestControl or not callable(source_gate):
            raise TypeError('existing request-control and trusted current source gate required')
        self.control=control
        self.source_gate=source_gate
        self._ensure_columns()
        if control.dataset_records is not None:
            from .expert_delivery_recovery import ExpertDatasetDeliveryRecords
            self.dataset_effects=ExpertDatasetDeliveryRecords(control.dataset_records)
        else:
            self.dataset_effects=None

    def _ensure_columns(self):
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            found={row['name'] for row in db.execute('PRAGMA table_info(rkb_expert_requests)')}
            for column,kind in (('result_remote_hash','TEXT'),('send_intent_hash','TEXT'),
                                ('result_generation','INTEGER')):
                if column not in found:
                    db.execute('ALTER TABLE rkb_expert_requests ADD COLUMN '+column+' '+kind)

    def _lease(self, db, credentials, request_id, generation, token, *, states):
        service=self.control._service(db,credentials)
        row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(str(request_id),)).fetchone()
        if (row is None or row['state'] not in states or row['generation']!=generation
                or row['executor_id']!=service['id'] or row['service_epoch']!=service['epoch']
                or row['lease_until'] is None or row['lease_until']<=self.control.clock()
                or type(token) is not str
                or not hmac.compare_digest(row['execution_token_hash'] or '',
                                           hashlib.sha256(token.encode()).hexdigest())):
            _fail('request_fence_denied')
        origin=json.loads(row['origin'])
        self.control._destination_allowed(origin)
        person=self.control.people.resolve_external(
            provider='slack',tenant=origin['team_id'],provider_subject=origin['user_id'],
            principal_factory=lambda who:Principal(subject=who,client_id='rkb-trusted-slack',
                issuer='slack',access_token=''),connection=db)
        if (person.principal.subject!=row['person_id'] or person.link_id!=row['link_id']
                or person.link_revision!=row['link_revision']):
            _fail('identity_changed')
        self.control.people.require(person,'public_search',connection=db)
        if not request_order.is_head(db,row):
            _fail('request_fence_denied')
        return row,origin

    def _source_current(self, row, origin):
        # In production the gate MUST check target PG/source access, private/
        # public audience, material revisions and issuance generation.
        # A failing provider/native check cannot be an allow-by-exception.
        if self.source_gate(dict(row),origin) is not True:
            _fail('source_access_unverified')

    def prepare_result(self, credentials: ExecutorCredentials, request_id, generation,
                       execution_token, text: str):
        if type(text) is not str or not 1<=len(text)<=12000 or len(text.encode())>24000:
            _fail('invalid_result_text')
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row,origin=self._lease(db,credentials,request_id,generation,execution_token,
                                   states=('leased','result_unsealed'))
            self._source_current(row,origin)
            # A reclaimed executor has a new execution generation. The
            # already authored result MUST retain its original generation,
            # byte identity and remote-intent dependency.
            original=row['result_generation']
            if row['result_hash'] and (type(original) is not int or original<1):
                _fail('result_generation_unproved')
            result_gen=original if row['result_hash'] else generation
            payload=_canonical(dict(codec='rkb.expert-result.v1',request_id=row['id'],
                                    generation=result_gen,person_id=row['person_id'],
                                    output_scope=row['output_scope'],text=text))
            digest=hashlib.sha256(payload.encode()).hexdigest()
            if row['result_hash']:
                if row['result_hash']!=digest:
                    _fail('result_conflict')
                return dict(request_id=row['id'],state='result_unsealed',saved=False)
            db.execute('UPDATE rkb_expert_requests SET state=?,result_json=?,result_hash=?,result_generation=?,updated_at=? WHERE id=?',
                       ('result_unsealed',payload,digest,result_gen,self.control.clock(),row['id']))
            return dict(request_id=row['id'],state='result_unsealed',saved=False)

    def exact_result_intent(self,request_id,sequence: int) -> ExactCommand:
        if self.dataset_effects is not None:_fail('dataset_effect_does_not_enter_pg_head')
        row=self.control._row(request_id)
        if (row['state'] not in ('result_unsealed','result_ready')
                or not row['result_hash'] or not row['result_json']
                or type(row['result_generation']) is not int or row['result_generation']<1
                or type(sequence) is not int or sequence<1):_fail('result_intent_unavailable')
        return ExactCommand(sequence=sequence,operation_key='expert-result:'+row['id'],
            request_id=row['id'],person_id=row['person_id'],route='expert.result.v1',
            logical_time=str(row['created_at']),schema_revision='rkb-expert-control-v1',
            function_revision='no-native-domain-effect',executor_revision='existing-control-owner',
            ids=dict(request_id=row['id']),expected_revisions=dict(generation=str(row['result_generation'])),
            payload=row['result_json'].encode(),kind='control')

    async def verify_remote_result(self,credentials,request_id,generation,token,command,seal):
        if self.dataset_effects is not None:_fail('dataset_effect_does_not_enter_pg_head')
        expected=self.exact_result_intent(request_id,command.sequence)
        if command.encode()!=expected.encode():_fail('result_intent_conflict')
        proof=await self.control.spool.journal.verify_accepted(seal,command)
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row,origin=self._lease(db,credentials,request_id,generation,token,
                                   states=('result_unsealed','result_ready'))
            self._source_current(row,origin)
            if row['result_remote_hash'] and row['result_remote_hash']!=proof:
                _fail('result_intent_conflict')
            if row['state']=='result_unsealed':
                db.execute('UPDATE rkb_expert_requests SET state=?,result_remote_hash=?,updated_at=? WHERE id=?',
                           ('result_ready',proof,self.control.clock(),row['id']))
            return dict(request_id=row['id'],state='result_ready',saved=False)

    def exact_send_intent(self,request_id,sequence: int) -> ExactCommand:
        if self.dataset_effects is not None:_fail('dataset_effect_does_not_enter_pg_head')
        row=self.control._row(request_id)
        if (row['state'] not in ('result_ready','send_intent_ready') or not row['result_remote_hash']
                or type(row['result_generation']) is not int or row['result_generation']<1
                or type(sequence) is not int or sequence<1):
            _fail('send_intent_unavailable')
        marker=str(uuid5(NAMESPACE_URL,'rkb:expert-send:'+row['id']+':'+str(row['result_generation'])))
        origin=json.loads(row['origin'])
        payload=dict(codec='rkb.expert-slack-send.v1',request_id=row['id'],
                     generation=row['result_generation'],marker=marker,origin=origin,
                     result_hash=row['result_hash'],output_scope=row['output_scope'])
        return ExactCommand(sequence=sequence,operation_key='expert-send:'+row['id'],
            request_id=row['id'],person_id=row['person_id'],route='expert.slack.send.v1',
            logical_time=str(row['created_at']),schema_revision='rkb-expert-control-v1',
            function_revision='no-native-domain-effect',executor_revision='existing-control-owner',
            ids=dict(request_id=row['id'],client_msg_id=marker),
            expected_revisions=dict(generation=str(row['result_generation'])),
            payload=_canonical(payload).encode(),kind='provider_intent')

    async def verify_remote_send(self,credentials,request_id,generation,token,command,seal):
        if self.dataset_effects is not None:_fail('dataset_effect_does_not_enter_pg_head')
        expected=self.exact_send_intent(request_id,command.sequence)
        if command.encode()!=expected.encode():_fail('send_intent_conflict')
        proof=await self.control.spool.journal.verify_accepted(seal,command)
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row,origin=self._lease(db,credentials,request_id,generation,token,
                                   states=('result_ready','send_intent_ready'))
            self._source_current(row,origin)
            if row['send_intent_hash'] and row['send_intent_hash']!=proof:_fail('send_intent_conflict')
            if row['state']=='result_ready':
                db.execute('UPDATE rkb_expert_requests SET state=?,delivery_id=?,send_intent_hash=?,updated_at=? WHERE id=?',
                           ('send_intent_ready',command.ids['client_msg_id'],proof,self.control.clock(),row['id']))
            return dict(request_id=row['id'],state='send_intent_ready',saved=False)

    def claim_send_once(self,credentials,request_id,generation,token):
        """One data-bearing permit only; after a crash, reconcile, never resend."""
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row,origin=self._lease(db,credentials,request_id,generation,token,
                                   states=('send_intent_ready',))
            self._source_current(row,origin)
            if self.dataset_effects is not None:
                # A stale local result/send hash is NOT provider permission.
                from .expert_delivery_recovery import (
                    result_bytes,parse_result,send_bytes,
                )
                rraw=result_bytes(row)
                rdoc,_=parse_result(row,rraw)
                sraw=send_bytes(row,rdoc,rraw)
                if (not self.control._dataset_covered(row)
                        or not self.dataset_effects.covered(request_id,'result',rraw)
                        or not self.dataset_effects.covered(request_id,'send',sraw)
                        or row['result_remote_hash']!=hashlib.sha256(rraw).hexdigest()
                        or row['send_intent_hash']!=hashlib.sha256(sraw).hexdigest()
                        or row['delivery_id']!=str(uuid5(
                            NAMESPACE_URL,'rkb:expert-send:'+row['id']+':'
                            +str(row['result_generation'])))):
                    _fail('dataset_original_intent_not_covered')
            if not row['result_remote_hash'] or not row['send_intent_hash'] or not row['delivery_id']:
                _fail('send_intent_unverified')
            message=json.loads(row['result_json'])
            if message['output_scope']=='public' and origin['channel_type']!='group':
                _fail('destination_scope_changed')
            db.execute('UPDATE rkb_expert_requests SET state=?,delivery_attempt=delivery_attempt+1,updated_at=? WHERE id=?',
                       ('sending',self.control.clock(),row['id']))
            return dict(request_id=row['id'],delivery_id=row['delivery_id'],
                        channel_id=origin['channel_id'],thread_ts=origin['thread_ts'],
                        text=message['text'])

    def uncertain_delivery(self,credentials,request_id,delivery_id):
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self.control._service(db,credentials)
            row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(str(request_id),)).fetchone()
            if row is None or row['delivery_id']!=delivery_id or row['state'] not in ('sending','unknown'):
                _fail('delivery_identity_conflict')
            db.execute('UPDATE rkb_expert_requests SET state=?,updated_at=? WHERE id=?',
                       ('unknown',self.control.clock(),row['id']))
        return dict(request_id=request_id,state='unknown',retry_send_allowed=False)

    def receipt(self,credentials,request_id,proof: NativeSlackReceipt):
        """Record provider-established effect even if access was later revoked."""
        if type(proof) is not NativeSlackReceipt or proof._trusted is not _RECEIPT:
            _fail('native_receipt_required')
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self.control._service(db,credentials)
            row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(str(request_id),)).fetchone()
            if row is None or row['state'] not in ('sending','unknown','delivered') or not row['send_intent_hash']:
                _fail('delivery_unavailable')
            origin=json.loads(row['origin']);message=json.loads(row['result_json'])
            if (proof.app_id!=self.control.app_id or proof.team_id!=self.control.team_id
                    or proof.bot_user_id!=self.control.bot_user_id
                    or proof.channel_id!=origin['channel_id'] or proof.thread_ts!=origin['thread_ts']
                    or proof.client_message_id!=row['delivery_id'] or proof.text!=message['text']):
                _fail('native_receipt_mismatch')
            if self.dataset_effects is not None:
                from .expert_delivery_recovery import (
                    result_bytes,parse_result,send_bytes,parse_send,receipt_bytes,
                )
                rraw=result_bytes(row);rdoc,_=parse_result(row,rraw)
                sraw=send_bytes(row,rdoc,rraw)
                sdoc,_=parse_send(row,sraw,rdoc,rraw)
                proof_raw=receipt_bytes(row,rdoc,sdoc,sraw,proof,
                                        bot_user_id=self.control.bot_user_id)
                if (not self.control._dataset_covered(row)
                        or not self.dataset_effects.covered(request_id,'result',rraw)
                        or not self.dataset_effects.covered(request_id,'send',sraw)
                        or not self.dataset_effects.covered(request_id,'receipt',proof_raw)
                        or row['result_remote_hash']!=hashlib.sha256(rraw).hexdigest()
                        or row['send_intent_hash']!=hashlib.sha256(sraw).hexdigest()):
                    _fail('dataset_delivery_receipt_not_covered')
            receipt=_canonical(dict(app_id=proof.app_id,team_id=proof.team_id,
                 bot_user_id=proof.bot_user_id,channel_id=proof.channel_id,
                 thread_ts=proof.thread_ts,message_ts=proof.message_ts,
                 client_message_id=proof.client_message_id,result_hash=row['result_hash']))
            if row['delivery_receipt']:
                if row['delivery_receipt']!=receipt:_fail('native_receipt_conflict')
                return dict(request_id=row['id'],state='delivered')
            if not request_order.is_head(db,row):_fail('delivery_order_conflict')
            db.execute('UPDATE rkb_expert_requests SET state=?,delivery_receipt=?,updated_at=? WHERE id=?',
                       ('delivered',receipt,self.control.clock(),row['id']))
            request_order.advance(db,row['dialog_id'])
            return dict(request_id=row['id'],state='delivered')


    def _require_dataset_effects(self):
        if self.dataset_effects is None:
            _fail('dataset_effect_store_required')
        return self.dataset_effects

    async def stage_dataset_result(self,credentials,request_id,generation,token):
        """Stage original prepared result with existing fenced Dataset CAS."""
        from .expert_delivery_recovery import result_bytes
        effect=self._require_dataset_effects()
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row,origin=self._lease(db,credentials,request_id,generation,token,
                                   states=('result_unsealed','result_ready'))
            self._source_current(row,origin)
            if not self.control._dataset_covered(row):
                _fail('request_not_remote_covered')
            raw=result_bytes(row)
        return await effect.stage(request_id,'result',raw)

    def confirm_dataset_result(self,credentials,request_id,generation,token):
        """Result-ready is a verified exact Dataset byte outcome, not local put."""
        from .expert_delivery_recovery import result_bytes
        effect=self._require_dataset_effects()
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row,origin=self._lease(db,credentials,request_id,generation,token,
                                   states=('result_unsealed','result_ready'))
            self._source_current(row,origin)
            if not self.control._dataset_covered(row):
                return dict(state='result_unsealed',saved=False,remote_accepted=False)
            raw=result_bytes(row)
            if not effect.covered(request_id,'result',raw):
                return dict(state='result_unsealed',saved=False,remote_accepted=False)
            receipt_hash=hashlib.sha256(raw).hexdigest()
            if row['result_remote_hash'] and row['result_remote_hash']!=receipt_hash:
                _fail('result_intent_conflict')
            db.execute("UPDATE rkb_expert_requests SET state='result_ready',"
                       "result_remote_hash=?,updated_at=? WHERE id=?",
                       (receipt_hash,self.control.clock(),row['id']))
            return dict(state='result_ready',saved=False,remote_accepted=True)

    async def stage_dataset_send(self,credentials,request_id,generation,token):
        """Original deterministic send intent; no provider action."""
        from .expert_delivery_recovery import result_bytes,parse_result,send_bytes
        effect=self._require_dataset_effects()
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row,origin=self._lease(db,credentials,request_id,generation,token,
                                   states=('result_ready','send_intent_ready'))
            self._source_current(row,origin)
            result_raw=result_bytes(row)
            result_doc,_=parse_result(row,result_raw)
            if (not effect.covered(request_id,'result',result_raw)
                    or row['result_remote_hash']!=hashlib.sha256(result_raw).hexdigest()):
                _fail('result_intent_unverified')
            raw=send_bytes(row,result_doc,result_raw)
        return await effect.stage(request_id,'send',raw)

    def confirm_dataset_send(self,credentials,request_id,generation,token):
        from .expert_delivery_recovery import result_bytes,parse_result,send_bytes,parse_send
        effect=self._require_dataset_effects()
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row,origin=self._lease(db,credentials,request_id,generation,token,
                                   states=('result_ready','send_intent_ready'))
            self._source_current(row,origin)
            result_raw=result_bytes(row)
            result_doc,_=parse_result(row,result_raw)
            if (not effect.covered(request_id,'result',result_raw)
                    or row['result_remote_hash']!=hashlib.sha256(result_raw).hexdigest()):
                _fail('result_intent_unverified')
            raw=send_bytes(row,result_doc,result_raw)
            if not effect.covered(request_id,'send',raw):
                return dict(state='result_ready',saved=False,remote_accepted=False)
            send_doc,_=parse_send(row,raw,result_doc,result_raw)
            proof_hash=hashlib.sha256(raw).hexdigest()
            if ((row['send_intent_hash'] and row['send_intent_hash']!=proof_hash)
                    or (row['delivery_id'] and row['delivery_id']!=send_doc['client_msg_id'])):
                _fail('send_intent_conflict')
            db.execute("UPDATE rkb_expert_requests SET state='send_intent_ready',"
                       "send_intent_hash=?,delivery_id=?,updated_at=? WHERE id=?",
                       (proof_hash,send_doc['client_msg_id'],self.control.clock(),row['id']))
            return dict(state='send_intent_ready',saved=False,remote_accepted=True)

    async def stage_dataset_native_receipt(self,credentials,request_id,proof):
        """Only the existing trusted NativeSlackReceipt, no caller success bool."""
        from .expert_delivery_recovery import (
            result_bytes,parse_result,send_bytes,parse_send,receipt_bytes,
        )
        effect=self._require_dataset_effects()
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self.control._service(db,credentials)
            row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',
                           (str(request_id),)).fetchone()
            if (row is None or row['state'] not in ('sending','unknown','delivered')
                    or not row['send_intent_hash'] or not row['delivery_id']):
                _fail('delivery_unavailable')
            result_raw=result_bytes(row)
            result_doc,_=parse_result(row,result_raw)
            send_raw=send_bytes(row,result_doc,result_raw)
            send_doc,_=parse_send(row,send_raw,result_doc,result_raw)
            if (not effect.covered(request_id,'result',result_raw)
                    or not effect.covered(request_id,'send',send_raw)):
                _fail('delivery_intent_not_remote_covered')
            raw=receipt_bytes(row,result_doc,send_doc,send_raw,proof,
                              bot_user_id=self.control.bot_user_id)
        return await effect.stage(request_id,'receipt',raw)

    def confirm_dataset_native_receipt(self,credentials,request_id,proof):
        """The existing receipt() verifies exact correlation AFTER coverage."""
        self._require_dataset_effects()
        return self.receipt(credentials,request_id,proof)

    def reconcile_restored_dataset_delivery(self,credentials,request_id):
        """Resolve restored intake ONLY from original covered records.

        Covered result without send: recovered_result_ready (NO permit).
        Covered send without provider receipt: unknown (NO retry permit).
        Covered result+send+verified native receipt: delivered, use existing
        receipt() for exact correlation and dialog advance.
        Local absence/noncovered records NEVER establish 'not sent'.
        """
        from .expert_delivery_recovery import parse_result,parse_send,parse_receipt
        effect=self._require_dataset_effects()
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self.control._service(db,credentials)
            row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',
                           (str(request_id),)).fetchone()
            if row is None:_fail('request_unavailable')
            if row['state'] not in ('recovery_review_required',
                    'recovered_result_ready','unknown','delivered'):
                return dict(state=row['state'],retry_send_allowed=False)
            if not self.control._dataset_covered(row):
                return dict(state='recovery_review_required',
                            missing_contract='intake_exact_coverage',
                            retry_send_allowed=False)
            try:
                self.control._dataset_current_identity(db,row)
            except (RequestControlError,ParticipantError):
                return dict(state='recovery_review_required',
                            missing_contract='current_verified_identity_or_capability',
                            retry_send_allowed=False)
            result_raw=effect.read_covered(request_id,'result')
            if result_raw is None:
                return dict(state='recovery_review_required',
                            missing_contract='accepted_original_result_record',
                            retry_send_allowed=False)
            result_doc,result_payload=parse_result(row,result_raw)
            if ((row['result_json'] and row['result_json']!=result_doc['result_json'])
                    or (row['result_hash'] and row['result_hash']!=result_doc['result_hash'])
                    or (row['result_generation'] and row['result_generation']!=result_doc['result_generation'])):
                _fail('result_intent_conflict')
            result_hash=hashlib.sha256(result_raw).hexdigest()
            send_raw=effect.read_covered(request_id,'send')
            if send_raw is None:
                # Do NOT imply that a provider message was never sent;
                # no original verified send intent is known to this owner.
                if row['state'] in ('unknown','delivered'):
                    return dict(state=row['state'],
                                missing_contract='original_send_disposition',
                                retry_send_allowed=False)
                db.execute("UPDATE rkb_expert_requests SET state='recovered_result_ready',"
                    "result_json=?,result_hash=?,result_generation=?,"
                    "result_remote_hash=?,updated_at=? WHERE id=?",
                    (result_doc['result_json'],result_doc['result_hash'],
                     result_doc['result_generation'],result_hash,
                     self.control.clock(),row['id']))
                return dict(state='recovered_result_ready',result_known=True,
                            missing_contract='original_send_disposition',
                            retry_send_allowed=False,claim_allowed=False)
            send_doc,send_payload=parse_send(row,send_raw,result_doc,result_raw)
            send_hash=hashlib.sha256(send_raw).hexdigest()
            if ((row['send_intent_hash'] and row['send_intent_hash']!=send_hash)
                    or (row['delivery_id'] and row['delivery_id']!=send_doc['client_msg_id'])):
                _fail('send_intent_conflict')
            if row['state']=='delivered':
                # A repeated restore must not turn a settled delivery back
                # into UNKNOWN. Require the exact already-covered native
                # receipt and same persisted receipt bytes, then NO-OP.
                settled=effect.read_covered(request_id,'receipt')
                if settled is None:
                    return dict(state='delivered',remote_covered=False,
                                retry_send_allowed=False,
                                missing_contract='original_receipt_remote_coverage')
                _,known=parse_receipt(row,settled,result_doc,send_doc,send_raw,
                    bot_user_id=self.control.bot_user_id)
                if row['delivery_receipt']!=_canonical(known):
                    _fail('native_receipt_conflict')
                return dict(state='delivered',remote_covered=True,
                            retry_send_allowed=False)
            db.execute("UPDATE rkb_expert_requests SET state='unknown',"
                "result_json=?,result_hash=?,result_generation=?,result_remote_hash=?,"
                "send_intent_hash=?,delivery_id=?,delivery_attempt=max(delivery_attempt,1),"
                "updated_at=? WHERE id=?",
                (result_doc['result_json'],result_doc['result_hash'],
                 result_doc['result_generation'],result_hash,send_hash,
                 send_doc['client_msg_id'],self.control.clock(),row['id']))
            receipt_raw=effect.read_covered(request_id,'receipt')
            if receipt_raw is None:
                return dict(state='unknown',retry_send_allowed=False,
                            missing_contract='provider_native_disposition_or_covered_receipt')
            _,native_record=parse_receipt(row,receipt_raw,result_doc,send_doc,send_raw,
                bot_user_id=self.control.bot_user_id)
            native_payload=dict(app_id=native_record['app_id'],
                user=native_record['bot_user_id'],
                channel=native_record['channel_id'],
                thread_ts=native_record['thread_ts'],
                ts=native_record['message_ts'],
                client_msg_id=native_record['client_message_id'],
                text=result_payload['text'])
        # Reconstruct the exact *previously verified and captured* receipt.
        # The existing factory binds app/team/bot/origin/marker; provider calls
        # and caller-controlled booleans are never involved.
        proof=verified_native_slack_receipt(native_payload,
            app_id=self.control.app_id,team_id=self.control.team_id,
            bot_user_id=self.control.bot_user_id,
            channel_id=native_record['channel_id'],
            thread_ts=native_record['thread_ts'])
        return self.receipt(credentials,request_id,proof)
