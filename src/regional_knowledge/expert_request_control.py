"""Received expert requests using the existing identity/control and remote journal.

This is the admission-only source slice of CAS-F10. It does not register MCP
tools, invoke a provider, send Slack messages, execute domain writes or claim
that a local SQLite row is a remotely Saved request. The caller must use the
same existing ControlSpool writer for any exact remote seal.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import asyncio
import hashlib
import hmac
import json
import re
import secrets
import time
from uuid import UUID, NAMESPACE_URL, uuid5

from .contracts import Principal
from .participants import Participants, ParticipantError
from .recovery_journal import ExactCommand, AcceptedSeal, RecoveryJournal
from . import request_order


class RequestControlError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _fail(code):
    raise RequestControlError(code)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False)


@dataclass(frozen=True)
class VerifiedSlackMessage:
    """An observation from authenticated Slack bytes, not an MCP actor field."""
    app_id: str
    team_id: str
    user_id: str
    channel_id: str
    channel_type: str
    thread_ts: str
    message_ts: str
    event_id: str
    text: str
    _trusted: object = field(default=None, repr=False, compare=False)


_SIGNED = object()


def verified_slack_payload(raw: bytes, headers: dict, signing_secret: str, *,
                           now: float | None = None) -> dict:
    """Verify exact Slack bytes before parsing *any* event/challenge type."""
    now = time.time() if now is None else now
    if (type(raw) is not bytes or not 1 <= len(raw) <= 262144
            or type(signing_secret) is not str or not signing_secret
            or type(headers) is not dict):
        _fail('invalid_signed_body')
    values = {k.lower(): v for k, v in headers.items() if type(k) is str}
    timestamp, signature = values.get('x-slack-request-timestamp', ''), values.get('x-slack-signature', '')
    if type(timestamp) is not str or not timestamp.isascii() or not timestamp.isdecimal():
        _fail('slack_replay_window')
    if abs(now - int(timestamp)) > 300:
        _fail('slack_replay_window')
    expected = 'v0=' + hmac.new(signing_secret.encode(),
                               b'v0:' + timestamp.encode() + b':' + raw,
                               hashlib.sha256).hexdigest()
    if type(signature) is not str or not hmac.compare_digest(expected, signature):
        _fail('slack_signature_invalid')
    try:
        body = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        _fail('invalid_signed_body')
    if type(body) is not dict:
        _fail('invalid_signed_body')
    return body


def _verified_slack_observation(body: dict, *, app_id: str, team_id: str,
                                bot_user_id: str) -> VerifiedSlackMessage | None:
    """Only call on output of verified_slack_payload, never on caller JSON."""
    if (type(body) is not dict or body.get('api_app_id') != app_id
            or body.get('team_id') != team_id):
        _fail('slack_event_scope_mismatch')
    if body.get('type') != 'event_callback':
        return None
    event = body.get('event')
    if type(event) is not dict:
        _fail('slack_event_invalid')
    if event.get('bot_id') or event.get('user') == bot_user_id or event.get('subtype'):
        return None
    channel_type = ('group' if event.get('type') == 'app_mention'
                    else 'im' if event.get('type') == 'message'
                    and event.get('channel_type') == 'im' else None)
    if channel_type is None:
        return None
    required = (body.get('event_id'), event.get('user'), event.get('channel'),
                event.get('ts'), event.get('text'))
    if any(type(v) is not str or not v for v in required):
        _fail('slack_event_invalid')
    thread = event.get('thread_ts', event['ts'])
    if type(thread) is not str or not re.fullmatch(r'[0-9]+\.[0-9]+', thread):
        _fail('slack_event_invalid')
    if not re.fullmatch(r'[0-9]+\.[0-9]+', event['ts']):
        _fail('slack_event_invalid')
    if not 1 <= len(event['text']) <= 12000:
        _fail('invalid_request_text')
    return VerifiedSlackMessage(app_id, team_id, event['user'], event['channel'],
                                channel_type, thread, event['ts'],
                                body['event_id'], event['text'], _SIGNED)


def verified_slack_event(raw: bytes, headers: dict, signing_secret: str, *,
                         app_id: str, team_id: str, bot_user_id: str,
                         now: float | None = None) -> VerifiedSlackMessage | None:
    """Only signed/replay-checked bytes can create a trusted observation."""
    return _verified_slack_observation(
        verified_slack_payload(raw, headers, signing_secret, now=now),
        app_id=app_id,team_id=team_id,bot_user_id=bot_user_id)


@dataclass(frozen=True)
class ExecutorCredentials:
    service_id: str
    token: str = field(repr=False)


class TargetExpertRequestControl:
    """Bounded Dev control rows only; no SQLite book/Case authority or executor."""
    def __init__(self, participants: Participants, spool, *,
                 app_id: str, team_id: str, bot_user_id: str,
                 allowed_channels: tuple[str, ...] = (), clock=None):
        from .control_spool import ControlSpool
        if (type(participants) is not Participants
                or type(spool) is not ControlSpool or spool.participants is not participants
                or type(spool.journal) is not RecoveryJournal):
            raise TypeError('one existing identity/spool/remote writer required')
        if any(type(x) is not str or not 1 <= len(x) <= 160
               for x in (app_id, team_id, bot_user_id, *allowed_channels)):
            raise ValueError('fixed Slack scope required')
        self.people, self.corpus, self.spool = participants, participants.corpus, spool
        self.app_id, self.team_id, self.bot_user_id = app_id, team_id, bot_user_id
        self.allowed_channels = frozenset(allowed_channels)
        self.clock = clock or time.time
        with self.corpus.connect() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS rkb_expert_services(
              id TEXT PRIMARY KEY, kind TEXT NOT NULL, token_hash TEXT NOT NULL,
              enabled INTEGER NOT NULL, epoch INTEGER NOT NULL,
              app_id TEXT NOT NULL, team_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS rkb_expert_requests(
              id TEXT PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
              event_hash TEXT NOT NULL, message_key TEXT NOT NULL UNIQUE,
              person_id TEXT NOT NULL,link_id TEXT,link_revision INTEGER,
              origin TEXT NOT NULL,input_text TEXT NOT NULL,
              output_scope TEXT NOT NULL,state TEXT NOT NULL,
              generation INTEGER NOT NULL DEFAULT 0,executor_id TEXT,
              service_epoch INTEGER,execution_token_hash TEXT,lease_until REAL,
              created_at REAL NOT NULL,updated_at REAL NOT NULL,
              remote_command_hash TEXT,remote_intent TEXT,
              result_hash TEXT,result_json TEXT,delivery_id TEXT,
              delivery_service_id TEXT,delivery_service_epoch INTEGER,
              delivery_receipt TEXT,delivery_attempt INTEGER NOT NULL DEFAULT 0,
              next_delivery_at REAL NOT NULL DEFAULT 0,reconcile_cursor TEXT,
              reconcile_not_before REAL NOT NULL DEFAULT 0,
              trigger_event_id TEXT);
            CREATE INDEX IF NOT EXISTS rkb_expert_requests_queue
              ON rkb_expert_requests(state,created_at,id);
            """)
        request_order.schema(self.corpus)
        # Dataset-only control records do NOT enter the PG accepted head.
        # The original journal proof path remains for non-Dataset fixtures.
        from .dataset_recovery_store import DatasetRecoveryStore
        from .expert_request_recovery import ExpertDatasetRequestRecords
        store = spool.journal.store
        if (type(store) is DatasetRecoveryStore
                and (store.queue is not spool.queue or store.prefix != spool.journal.prefix + '/')):
            raise TypeError('expert Dataset records require exact existing queue and stream')
        self.dataset_records = (ExpertDatasetRequestRecords(store)
                                if type(store) is DatasetRecoveryStore else None)
        self._restore_cursor = None
        self._restore_complete = self.dataset_records is None
        self._restore_version = (store.latest_checkpoint.dataset_version
                                 if self.dataset_records is not None else None)
        self._received_cursor = None
        if self.dataset_records is not None:
            self.rebuild_covered_dataset_requests()  # One bounded page only.

    def _dataset_current_identity(self, db, row):
        origin = json.loads(row['origin'])
        self._destination_allowed(origin)
        context = self.people.resolve_external(
            provider='slack', tenant=origin['team_id'],
            provider_subject=origin['user_id'],
            principal_factory=lambda person: Principal(
                subject=person, client_id='rkb-trusted-slack',
                issuer='slack', access_token='verified-local-adapter'),
            connection=db)
        if (context.principal.subject != row['person_id']
                or context.link_id != row['link_id']
                or context.link_revision != row['link_revision']):
            _fail('identity_changed')
        self.people.require(context, 'public_search', connection=db)

    def _dataset_covered(self, row):
        if self.dataset_records is None:
            return False
        from .expert_request_recovery import ExpertRecordError
        try:
            return self.dataset_records.covered(row)
        except ExpertRecordError:
            _fail('request_record_conflict')

    def _refresh_dataset_restore_version(self):
        """Restart bounded traversal when a newer verified Dataset is promoted.

        Old cursor was tied to the former covered_version; retaining it could
        skip now-covered rows after promotion. Previously reconstructed rows
        are immutable, idempotent and STILL not executable without receipts.
        """
        if self.dataset_records is None:
            return
        version=self.dataset_records.store.latest_checkpoint.dataset_version
        if version!=self._restore_version:
            self._restore_version=version
            self._restore_cursor=None
            self._restore_complete=False

    def rebuild_covered_dataset_requests(self):
        """Restore ONE fixed-size keyset page with exact captured dialog sequence.

        Snapshot rows can arrive in random UUID order; do NOT regenerate a
        sequential dialog_seq from that order or treat an old intake-only
        record as safe to execute/send. Explicit later calls advance the cursor.
        """
        self._refresh_dataset_restore_version()
        if self.dataset_records is None or self._restore_complete:
            return 0
        from .expert_request_recovery import from_row, _canonical
        records, next_cursor = self.dataset_records.covered_page(
            after=self._restore_cursor, through_version=self._restore_version)
        rebuilt = 0
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            for record in records:
                origin = record['origin']
                if origin['app_id'] != self.app_id or origin['team_id'] != self.team_id:
                    _fail('request_restore_app_scope')
                current = db.execute(
                    'SELECT * FROM rkb_expert_requests WHERE id=? OR event_id=? OR message_key=?',
                    (record['request_id'], record['event_id'],
                     record['message_key'])).fetchone()
                if current is not None:
                    if (current['id'] != record['request_id']
                            or from_row(current) != _canonical(record)):
                        _fail('request_record_conflict')
                    continue
                historic = dict(
                    origin=_canonical(origin).decode(), person_id=record['person_id'],
                    link_id=record['link_id'], link_revision=record['link_revision'])
                try:
                    self._dataset_current_identity(db, historic)
                except (ParticipantError, RequestControlError):
                    continue  # Historical record remains saved, NOT new access.
                tenant=hashlib.sha256(request_order.canonical(
                    ['slack',origin['app_id'],origin['team_id']]).encode()).hexdigest()
                conversation=request_order.canonical(
                    [origin['channel_id'],origin['thread_ts']])
                seq=record['dialog_seq']; dialog=record['dialog_id']
                # Exact saved dialog position, not a new assignment dependent on
                # UUID pagination. The original order is an immutable input.
                db.execute("""INSERT OR IGNORE INTO rkb_request_dialogs
                    (id,client_kind,tenant_id,conversation_key,owner_person_id,
                     next_seq,head_seq,head_person_id)
                    VALUES(?,'slack',?,?,NULL,?,?,?)""",
                    (dialog,tenant,conversation,seq,seq,record['person_id']))
                prior=db.execute('SELECT * FROM rkb_request_dialogs WHERE id=?',
                                 (dialog,)).fetchone()
                if (prior['client_kind'],prior['tenant_id'],prior['conversation_key'],
                    prior['owner_person_id'])!=('slack',tenant,conversation,None):
                    _fail('request_restore_dialog_conflict')
                db.execute("""UPDATE rkb_request_dialogs
                    SET next_seq=max(next_seq,?),
                        head_person_id=CASE WHEN ?<=head_seq THEN ? ELSE head_person_id END,
                        head_seq=min(head_seq,?)
                    WHERE id=?""",
                    (seq,seq,record['person_id'],seq,dialog))
                db.execute('INSERT OR IGNORE INTO rkb_request_people(person_id) VALUES(?)',
                           (record['person_id'],))
                db.execute("""INSERT INTO rkb_expert_requests
                    (id,event_id,event_hash,message_key,person_id,link_id,link_revision,
                     origin,input_text,output_scope,state,created_at,updated_at,
                     dialog_id,dialog_seq,client_kind,tenant_id,client_request_key)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (record['request_id'],record['event_id'],record['event_hash'],
                     record['message_key'],record['person_id'],record['link_id'],
                     record['link_revision'],historic['origin'],record['input_text'],
                     record['output_scope'],'recovery_review_required',
                     record['created_at'],self.clock(),dialog,seq,'slack',tenant,
                     record['event_id']))
                rebuilt += 1
        self._restore_cursor=next_cursor
        self._restore_complete=next_cursor is None
        return rebuilt

    async def stage_received_dataset(self, request_id):
        """Fenced local CAS; absence of coverage remains HTTP 503."""
        if self.dataset_records is None:
            return False
        row = await asyncio.to_thread(self._row, request_id)
        if row['state'] in ('recovery_review_required','blocked'):
            return False
        if row['state'] not in ('received','pending'):
            return self._dataset_covered(row)
        return await self.dataset_records.stage(row)

    def _activate_dataset_covered(self, db, *, request_id=None):
        if self.dataset_records is None:
            return 0
        if request_id is not None:
            rows=db.execute(
                "SELECT * FROM rkb_expert_requests WHERE state='received' AND id=?",
                (request_id,)).fetchall()
        else:
            # Fixed, keyset-paged received scan; covered and not-yet-covered
            # histories must never make all claims fail at 513 entries.
            from .expert_request_recovery import RESTORE_PAGE_SIZE
            rows=db.execute(
                "SELECT * FROM rkb_expert_requests "
                "WHERE state='received' AND id>? "
                "ORDER BY id LIMIT ?",(self._received_cursor or '',
                                        RESTORE_PAGE_SIZE+1)).fetchall()
            more=len(rows)>RESTORE_PAGE_SIZE
            if more:
                self._received_cursor=rows[RESTORE_PAGE_SIZE-1]['id']
                rows=rows[:RESTORE_PAGE_SIZE]
            else:
                self._received_cursor=None
        updated=0
        for row in rows:
            if not self._dataset_covered(row):
                continue
            try:
                self._dataset_current_identity(db,row)
            except (ParticipantError,RequestControlError):
                db.execute("UPDATE rkb_expert_requests SET state='blocked',updated_at=? WHERE id=?",
                           (self.clock(),row['id']))
                continue
            db.execute("UPDATE rkb_expert_requests SET state='pending',updated_at=? WHERE id=? AND state='received'",
                       (self.clock(),row['id']))
            updated+=1
        return updated

    def confirm_dataset_coverage(self, request_id):
        """Claim/HTTP acknowledgment only after exact covered bytes and fresh IAM."""
        if self.dataset_records is None:
            return False
        self._refresh_dataset_restore_version()
        if not self._restore_complete:
            return False  # Never ACK while an older dialog head might be missing.
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self._activate_dataset_covered(db,request_id=str(request_id))
            row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',
                           (str(request_id),)).fetchone()
            if (row is None or row['state'] in ('received','blocked',
                                                'recovery_review_required')
                    or not self._dataset_covered(row)):
                return False
            self._dataset_current_identity(db,row)
            return True

    def reconcile_dataset_coverage(self):
        """One bounded continuation for covered intake AND historical pages."""
        if self.dataset_records is None:
            return 0
        self._refresh_dataset_restore_version()
        if not self._restore_complete:
            return self.rebuild_covered_dataset_requests()
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            return self._activate_dataset_covered(db)

    def reconcile_restored_request(self, request_id):
        """Inspect only; existing native receipt API cannot prove a lost send.

        A restored intake carries no accepted result/send-intent hashes or
        provider correlation. Do not reset to pending or mint a new permit.
        The required proof must bind original request + result/hash + send
        marker + provider readback to the existing delivery controls.
        """
        row=self._row(request_id)
        if row['state']!='recovery_review_required':
            return dict(request_id=row['id'],state=row['state'],
                        retry_send_allowed=False)
        return dict(request_id=row['id'],state='recovery_review_required',
            retry_send_allowed=False,ack_allowed=False,claim_allowed=False,
            missing_contract='accepted_original_result_and_send_intent_with_provider_disposition',
            required=('original_result_hash','accepted_result_intent',
                      'accepted_send_intent','original_client_msg_id',
                      'native_slack_receipt_or_proven_no_send'))

    def accept(self, event: VerifiedSlackMessage):
        if type(event) is not VerifiedSlackMessage or event._trusted is not _SIGNED:
            _fail('verified_slack_transport_required')
        # The historical dialog order must be reconstructed before admitting
        # another durable turn. One bounded page advances on each retry.
        self._refresh_dataset_restore_version()
        if self.dataset_records is not None and not self._restore_complete:
            self.rebuild_covered_dataset_requests()
            if not self._restore_complete:
                _fail('request_restore_pending')
        if (event.app_id != self.app_id or event.team_id != self.team_id
                or event.channel_type not in ('im','group')
                or event.channel_type == 'group' and event.channel_id not in self.allowed_channels):
            _fail('slack_event_scope_mismatch')
        origin = dict(app_id=event.app_id,team_id=event.team_id,user_id=event.user_id,
                      channel_id=event.channel_id,channel_type=event.channel_type,
                      thread_ts=event.thread_ts,message_ts=event.message_ts,event_id=event.event_id)
        digest = hashlib.sha256(_canonical(dict(origin=origin,text=event.text)).encode()).hexdigest()
        message_key = _canonical([event.team_id,event.channel_id,event.message_ts])
        ident = str(uuid5(NAMESPACE_URL,'rkb:expert-request:'+event.app_id+':'+event.team_id+':'+event.event_id))
        with self.corpus.connect() as db:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT * FROM rkb_expert_requests WHERE event_id=?',(event.event_id,)).fetchone()
            if old:
                if old['event_hash'] != digest or old['message_key'] != message_key:
                    _fail('event_id_conflict')
                # No new provider work; a duplicate is the same immutable request.
                # For Dataset persistence, local pending/legacy remote hash is
                # NEVER evidence of a verified captured version.
                if self.dataset_records is not None:
                    self._activate_dataset_covered(db,request_id=old['id'])
                    old=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(old['id'],)).fetchone()
                    covered=(old['state'] not in ('received','blocked','recovery_review_required')
                             and self._dataset_covered(old))
                    if covered:self._dataset_current_identity(db,old)
                    return dict(request_id=old['id'],state=old['state'],durable=covered)
                return dict(request_id=old['id'],state=old['state'],durable=old['remote_command_hash'] is not None)
            context = self.people.resolve_external(provider='slack',tenant=event.team_id,
                provider_subject=event.user_id,
                principal_factory=lambda person: Principal(subject=person,client_id='rkb-trusted-slack',
                    issuer='slack',access_token=''),connection=db)
            person = self.people.require(context,'public_search',connection=db)
            timestamp = self.clock()
            db.execute("""INSERT INTO rkb_expert_requests
                (id,event_id,event_hash,message_key,person_id,link_id,link_revision,origin,
                input_text,output_scope,state,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (ident,event.event_id,digest,message_key,person,context.link_id,context.link_revision,
                _canonical(origin),event.text,'public' if event.channel_type=='group' else 'personal',
                'received',timestamp,timestamp))
            request_order.assign_slack(db,ident,person,origin)
            return dict(request_id=ident,state='received',durable=False)

    def _destination_allowed(self,origin):
        """Recheck current Slack destination policy, not only intake policy."""
        if (type(origin) is not dict or origin.get('app_id')!=self.app_id
                or origin.get('team_id')!=self.team_id
                or origin.get('channel_type') not in ('group','im')
                or type(origin.get('channel_id')) is not str
                or not origin['channel_id']
                or origin['channel_type']=='group'
                and origin['channel_id'] not in self.allowed_channels):
            _fail('slack_destination_revoked')

    def _row(self,request_id):
        with self.corpus.connect() as db:
            row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(str(request_id),)).fetchone()
            if row is None:_fail('request_unavailable')
            return dict(row)

    def exact_remote_intent(self, request_id, sequence: int) -> ExactCommand:
        if self.dataset_records is not None:
            _fail('dataset_control_record_does_not_enter_pg_head')
        row=self._row(request_id)
        if type(sequence) is not int or sequence<1:_fail('invalid_remote_sequence')
        if row['state'] not in ('received','pending'):_fail('request_not_awaiting_remote_intent')
        payload=_canonical(dict(codec='rkb.expert-request-intent.v1',origin=json.loads(row['origin']),
                                text=row['input_text'],output_scope=row['output_scope'],
                                event_hash=row['event_hash']))
        return ExactCommand(sequence=sequence,operation_key='expert-request:'+row['id'],
            request_id=row['id'],person_id=row['person_id'],route='expert.request.v1',
            logical_time=str(row['created_at']),schema_revision='rkb-expert-control-v1',
            function_revision='no-native-domain-effect',executor_revision='existing-control-owner',
            ids={'request_id':row['id']},expected_revisions={'generation':'0'},
            payload=payload.encode(),kind='control')

    async def verify_remote_intent(self, request_id, command: ExactCommand, seal: AcceptedSeal):
        """Legacy external journal proof, never used by Dataset control records."""
        if self.dataset_records is not None:
            _fail('dataset_control_record_does_not_enter_pg_head')
        row=self._row(request_id)
        if (type(command) is not ExactCommand or
                command.encode()!=self.exact_remote_intent(request_id,command.sequence).encode()):
            _fail('remote_intent_conflict')
        confirmed=await self.spool.journal.verify_accepted(seal,command)
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(row['id'],)).fetchone()
            if old is None or old['event_hash']!=row['event_hash']:_fail('request_changed')
            if old['remote_command_hash'] and old['remote_command_hash']!=confirmed:
                _fail('remote_intent_conflict')
            if old['state']=='received':
                db.execute("UPDATE rkb_expert_requests SET state='pending',remote_command_hash=?,"
                           "remote_intent=?,updated_at=? WHERE id=?",(confirmed,seal.manifest.key,self.clock(),row['id']))
            elif old['state']=='pending' and old['remote_command_hash']!=confirmed:
                _fail('remote_intent_conflict')
            return dict(request_id=row['id'],state='pending' if old['state']=='received' else old['state'],
                        remote_accepted=True,saved=False)

    def provision_executor(self, owner_context, service_id, token, *, expected_epoch=0):
        from .participants import RequestAuthority
        if (type(owner_context) is not RequestAuthority
                or owner_context.route!='owner'
                or owner_context.principal.subject!=self.people.owner
                or any(type(x) is not str or not 8<=len(x)<=256 for x in (service_id,token))):
            _fail('owner_service_provision_required')
        digest=hashlib.sha256(token.encode()).hexdigest()
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self.people.require(owner_context,'participants_manage',connection=db)
            row=db.execute('SELECT * FROM rkb_expert_services WHERE id=?',(service_id,)).fetchone()
            if (row is not None and row['epoch']!=expected_epoch
                    or row is None and expected_epoch!=0):
                _fail('service_epoch_conflict')
            epoch=expected_epoch+1
            db.execute("""INSERT INTO rkb_expert_services VALUES(?,?,?,1,?,?,?)
                ON CONFLICT(id) DO UPDATE SET token_hash=excluded.token_hash,
                enabled=1,epoch=excluded.epoch""",
                (service_id,'executor',digest,epoch,self.app_id,self.team_id))
            return dict(service_id=service_id,epoch=epoch)

    def _service(self,db,credentials):
        if type(credentials) is not ExecutorCredentials:_fail('service_denied')
        row=db.execute('SELECT * FROM rkb_expert_services WHERE id=?',(credentials.service_id,)).fetchone()
        if (row is None or row['enabled']!=1 or row['kind']!='executor'
                or row['app_id']!=self.app_id or row['team_id']!=self.team_id
                or not hmac.compare_digest(row['token_hash'],
                                           hashlib.sha256(credentials.token.encode()).hexdigest())):
            _fail('service_denied')
        return row

    def claim(self, credentials, *, request_id=None, lease_seconds=120):
        if type(lease_seconds) is not int or not 1<=lease_seconds<=300:_fail('invalid_lease')
        self._refresh_dataset_restore_version()
        if self.dataset_records is not None and not self._restore_complete:
            self.rebuild_covered_dataset_requests()
            if not self._restore_complete:
                return {'state':'deferred','reason':'request_restore_pending'}
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            service=self._service(db,credentials)
            if self.dataset_records is not None:
                self._activate_dataset_covered(db)
            row=request_order.fair_head(db,self.clock(),
                "json_extract(r.origin,'$.app_id')=? AND json_extract(r.origin,'$.team_id')=?",
                (self.app_id,self.team_id))
            if row is None:return {'state':'empty'}
            if request_id is not None and row['id']!=str(request_id):return {'state':'deferred'}
            if self.dataset_records is not None:
                if not self._dataset_covered(row):
                    _fail('dataset_request_not_covered')
            elif not row['remote_command_hash']:_fail('unsealed_request_not_claimable')
            origin=json.loads(row['origin'])
            try:
                self._destination_allowed(origin)
                context=self.people.resolve_external(provider='slack',tenant=origin['team_id'],
                    provider_subject=origin['user_id'],principal_factory=lambda person:Principal(
                        subject=person,client_id='rkb-trusted-slack',issuer='slack',access_token=''),
                    connection=db)
                if (context.principal.subject!=row['person_id'] or context.link_id!=row['link_id']
                        or context.link_revision!=row['link_revision']):_fail('identity_changed')
                self.people.require(context,'public_search',connection=db)
            except (ParticipantError,RequestControlError):
                db.execute("UPDATE rkb_expert_requests SET state='blocked',updated_at=? WHERE id=?",
                           (self.clock(),row['id']))
                return dict(state='blocked',request_id=row['id'])
            generation=row['generation']+1
            token=secrets.token_urlsafe(32)
            # A lost pre-send worker may reclaim only after its lease expires.
            # Preserve the exact saved result/send intent and their ORIGINAL
            # generation; rotate only the execution generation and token.
            resume=row['state'] if row['state'] in (
                'result_unsealed','result_ready','send_intent_ready') else None
            db.execute("""UPDATE rkb_expert_requests SET state=?,generation=?,executor_id=?,
                service_epoch=?,execution_token_hash=?,lease_until=?,updated_at=?
                WHERE id=?""",(resume or 'leased',generation,service['id'],service['epoch'],
                hashlib.sha256(token.encode()).hexdigest(),self.clock()+lease_seconds,
                self.clock(),row['id']))
            request_order.claimed(db,row)
            return dict(state='leased',request_id=row['id'],generation=generation,
                        execution_token=token,**({'resume_state':resume} if resume else {}))

    def issuance_current(self,credentials,request_id,generation,execution_token,origin):
        """Nonblocking last-byte lease/destination fence for a trusted receiver.

        The final HTTP source gate separately refreshes real source/participant
        rights. This checks the existing control row and executor service with
        a bounded read-only SQLite connection; a busy database denies output.
        """
        import sqlite3
        if (type(credentials) is not ExecutorCredentials or type(generation) is not int
                or type(execution_token) is not str or type(origin) is not dict):
            return False
        db=None
        try:
            db=sqlite3.connect(self.corpus.path.resolve().as_uri()+'?mode=ro',
                uri=True,timeout=0.002,isolation_level=None)
            db.row_factory=sqlite3.Row
            service=self._service(db,credentials)
            row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',
                           (str(request_id),)).fetchone()
            if (row is None or self.dataset_records is not None and not self._dataset_covered(row)
                    or row['state']!='leased' or row['generation']!=generation
                    or row['executor_id']!=service['id'] or row['service_epoch']!=service['epoch']
                    or row['lease_until'] is None or row['lease_until']<=self.clock()
                    or not hmac.compare_digest(row['execution_token_hash'] or '',
                        hashlib.sha256(execution_token.encode()).hexdigest())
                    or json.loads(row['origin'])!=origin):
                return False
            self._destination_allowed(origin)
            # Recheck the verified Slack person/link and current capabilities
            # via the SAME bounded read-only SQLite connection immediately before
            # ASGI sends the buffered evidence. An async SourceReadGate refresh
            # can have finished *before* a concurrent unlink/grant revocation.
            current=self.people.resolve_external(
                provider='slack',tenant=origin['team_id'],provider_subject=origin['user_id'],
                principal_factory=lambda person:Principal(
                    subject=person,client_id='rkb-trusted-slack',issuer='slack',access_token=''),
                connection=db)
            if (current.principal.subject!=row['person_id']
                    or current.link_id!=row['link_id']
                    or current.link_revision!=row['link_revision']
                    or self.people.require(current,'public_search',connection=db)!=row['person_id']):
                return False
            return True
        except (sqlite3.Error,ValueError,TypeError,ParticipantError,RequestControlError,OverflowError):
            return False
        finally:
            if db is not None:db.close()

    def claim_context(self,credentials,request_id,generation,execution_token):
        with self.corpus.connect() as db:
            db.execute('BEGIN')
            service=self._service(db,credentials)
            row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(str(request_id),)).fetchone()
            if (row is None or self.dataset_records is not None and not self._dataset_covered(row)
                    or row['state'] not in ('leased','result_unsealed','result_ready','send_intent_ready')
                    or row['generation']!=generation
                    or row['executor_id']!=service['id'] or row['service_epoch']!=service['epoch']
                    or row['lease_until'] is None or row['lease_until']<=self.clock()
                    or type(execution_token) is not str or
                    not hmac.compare_digest(row['execution_token_hash'] or '',
                        hashlib.sha256(execution_token.encode()).hexdigest())):
                _fail('request_fence_denied')
            origin=json.loads(row['origin'])
            self._destination_allowed(origin)
            context=self.people.resolve_external(provider='slack',tenant=origin['team_id'],
                provider_subject=origin['user_id'],principal_factory=lambda person:Principal(
                    subject=person,client_id='rkb-trusted-slack',issuer='slack',access_token=''),
                connection=db)
            if (context.principal.subject!=row['person_id'] or context.link_id!=row['link_id']
                    or context.link_revision!=row['link_revision']):_fail('identity_changed')
            self.people.require(context,'public_search',connection=db)
            return dict(request_id=row['id'],generation=generation,person_id=row['person_id'],
                        origin=origin,input_text=row['input_text'],scope=row['output_scope'])
