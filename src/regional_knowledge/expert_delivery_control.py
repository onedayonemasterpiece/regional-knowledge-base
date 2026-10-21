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

    def _ensure_columns(self):
        with self.control.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            found={row['name'] for row in db.execute('PRAGMA table_info(rkb_expert_requests)')}
            for column in ('result_remote_hash','send_intent_hash'):
                if column not in found:
                    db.execute('ALTER TABLE rkb_expert_requests ADD COLUMN '+column+' TEXT')

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
            payload=_canonical(dict(codec='rkb.expert-result.v1',request_id=row['id'],
                                    generation=generation,person_id=row['person_id'],
                                    output_scope=row['output_scope'],text=text))
            digest=hashlib.sha256(payload.encode()).hexdigest()
            if row['result_hash']:
                if row['result_hash']!=digest:
                    _fail('result_conflict')
                return dict(request_id=row['id'],state='result_unsealed',saved=False)
            db.execute('UPDATE rkb_expert_requests SET state=?,result_json=?,result_hash=?,updated_at=? WHERE id=?',
                       ('result_unsealed',payload,digest,self.control.clock(),row['id']))
            return dict(request_id=row['id'],state='result_unsealed',saved=False)

    def exact_result_intent(self,request_id,sequence: int) -> ExactCommand:
        row=self.control._row(request_id)
        if (row['state'] not in ('result_unsealed','result_ready')
                or not row['result_hash'] or not row['result_json'] or type(sequence) is not int
                or sequence<1):_fail('result_intent_unavailable')
        return ExactCommand(sequence=sequence,operation_key='expert-result:'+row['id'],
            request_id=row['id'],person_id=row['person_id'],route='expert.result.v1',
            logical_time=str(row['created_at']),schema_revision='rkb-expert-control-v1',
            function_revision='no-native-domain-effect',executor_revision='existing-control-owner',
            ids=dict(request_id=row['id']),expected_revisions=dict(generation=str(row['generation'])),
            payload=row['result_json'].encode(),kind='control')

    async def verify_remote_result(self,credentials,request_id,generation,token,command,seal):
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
        row=self.control._row(request_id)
        if (row['state'] not in ('result_ready','send_intent_ready') or not row['result_remote_hash']
                or type(sequence) is not int or sequence<1):
            _fail('send_intent_unavailable')
        marker=str(uuid5(NAMESPACE_URL,'rkb:expert-send:'+row['id']+':'+str(row['generation'])))
        origin=json.loads(row['origin'])
        payload=dict(codec='rkb.expert-slack-send.v1',request_id=row['id'],
                     generation=row['generation'],marker=marker,origin=origin,
                     result_hash=row['result_hash'],output_scope=row['output_scope'])
        return ExactCommand(sequence=sequence,operation_key='expert-send:'+row['id'],
            request_id=row['id'],person_id=row['person_id'],route='expert.slack.send.v1',
            logical_time=str(row['created_at']),schema_revision='rkb-expert-control-v1',
            function_revision='no-native-domain-effect',executor_revision='existing-control-owner',
            ids=dict(request_id=row['id'],client_msg_id=marker),
            expected_revisions=dict(generation=str(row['generation'])),
            payload=_canonical(payload).encode(),kind='provider_intent')

    async def verify_remote_send(self,credentials,request_id,generation,token,command,seal):
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
