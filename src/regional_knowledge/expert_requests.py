"""Durable expert inbox/execution/result/outbox in the corpus SQLite authority.

Provider verification and HTTP/Socket I/O belong to adapters. Trusted Slack
observations and native delivery receipts are never client/MCP payloads.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
import hashlib
import hmac
import json
import math
import re
import secrets
from uuid import UUID, uuid4, uuid5, NAMESPACE_URL

from pydantic import BaseModel, ConfigDict, Field

from .contracts import Principal
from .disagreement_journal import DisagreementJournal, JournalError
from .participants import ParticipantError, RequestAuthority
from .request_authority import token_hash,wake_event_id,track_resources
from . import request_order
from .sqlite_corpus import canonical


class ExpertRequestError(Exception):
    def __init__(self,code):
        self.code=code
        super().__init__(code)


def fail(code):
    raise ExpertRequestError(code)


@dataclass(frozen=True)
class SlackObservation:
    """Constructed only by authenticated provider transport, never from MCP."""
    app_id:str
    team_id:str
    user_id:str
    channel_id:str
    channel_type:str
    thread_ts:str
    message_ts:str
    event_id:str
    text:str


@dataclass(frozen=True)
class ServiceCredentials:
    service_id:str
    token:str=field(repr=False)


@dataclass(frozen=True)
class SlackDeliveryReceipt:
    app_id:str
    team_id:str
    bot_user_id:str
    channel_id:str
    thread_ts:str
    message_ts:str
    client_message_id:str
    text:str


class ExpertResult(BaseModel):
    model_config=ConfigDict(extra="forbid")
    text:str=Field(min_length=1,max_length=12000)
    document_ids:list[UUID]=Field(default_factory=list,max_length=50)
    case_ids:list[UUID]=Field(default_factory=list,max_length=50)


def principal(person_id):
    return Principal(subject=person_id,client_id="rkb-bound-expert-request",issuer="rkb-verified-slack-binding",access_token="")


class ExpertRequests:
    def __init__(self, participants, *, app_id, team_id, bot_user_id, allowed_channels=()):
        self.people=participants
        self.corpus=participants.corpus
        self.app_id=app_id
        self.team_id=team_id
        self.bot_user_id=bot_user_id
        self.allowed_channels=frozenset(allowed_channels)
        self.dependencies=None
        self.journal=DisagreementJournal(self.corpus,participants=participants)
        from .disagreement_drafts import PersonalDrafts
        self.drafts=PersonalDrafts(self.journal)
        with self.corpus.connect() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS rkb_expert_services(
              id TEXT PRIMARY KEY, kind TEXT NOT NULL, token_hash TEXT NOT NULL,
              enabled INTEGER NOT NULL, epoch INTEGER NOT NULL,
              app_id TEXT NOT NULL, team_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS rkb_expert_service_events(
              service_id TEXT NOT NULL, epoch INTEGER NOT NULL, actor_id TEXT NOT NULL,
              payload TEXT NOT NULL, created_at REAL NOT NULL, PRIMARY KEY(service_id,epoch));
            CREATE TABLE IF NOT EXISTS rkb_expert_requests(
              id TEXT PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
              event_hash TEXT NOT NULL, message_key TEXT NOT NULL UNIQUE, person_id TEXT NOT NULL,
              link_id TEXT NOT NULL, link_revision INTEGER NOT NULL,
              origin TEXT NOT NULL, input_text TEXT NOT NULL,
              output_scope TEXT NOT NULL, state TEXT NOT NULL,
              generation INTEGER NOT NULL DEFAULT 0, executor_id TEXT,
              service_epoch INTEGER, execution_token_hash TEXT, lease_until REAL,
              created_at REAL NOT NULL, updated_at REAL NOT NULL,
              result_hash TEXT, result_json TEXT,
              delivery_id TEXT, delivery_service_id TEXT, delivery_service_epoch INTEGER,
              delivery_receipt TEXT, delivery_attempt INTEGER NOT NULL DEFAULT 0,
              next_delivery_at REAL NOT NULL DEFAULT 0,
              reconcile_cursor TEXT, reconcile_not_before REAL NOT NULL DEFAULT 0,
              trigger_event_id TEXT);
            CREATE INDEX IF NOT EXISTS rkb_expert_requests_queue
              ON rkb_expert_requests(state,created_at,id);
            CREATE TABLE IF NOT EXISTS rkb_expert_request_events(
              request_id TEXT NOT NULL REFERENCES rkb_expert_requests(id),
              seq INTEGER NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL,
              created_at REAL NOT NULL, PRIMARY KEY(request_id,seq));
            CREATE TABLE IF NOT EXISTS rkb_expert_request_resources(
              request_id TEXT NOT NULL REFERENCES rkb_expert_requests(id),
              generation INTEGER NOT NULL, kind TEXT NOT NULL,
              resource_id TEXT NOT NULL, PRIMARY KEY(request_id,generation,kind,resource_id));
            """)

        # Upgrade source-only earlier queue snapshots without replacing rows.
        with self.corpus.connect() as db:
            if "trigger_event_id" not in {row[1] for row in db.execute("PRAGMA table_info(rkb_expert_requests)")}:
                db.execute("ALTER TABLE rkb_expert_requests ADD COLUMN trigger_event_id TEXT")
        request_order.schema(self.corpus)
        from .request_grants import schema
        schema(self.corpus)
        with self.corpus.connect() as db:
            if 'personal_resource' not in {r[1] for r in db.execute('PRAGMA table_info(rkb_expert_services)')}:
                db.execute('ALTER TABLE rkb_expert_services ADD COLUMN personal_resource TEXT')

    def _now(self):
        return self.people._time().timestamp()

    def _event(self,db,request_id,kind,payload):
        seq=db.execute("SELECT coalesce(max(seq),0)+1 FROM rkb_expert_request_events WHERE request_id=?",(request_id,)).fetchone()[0]
        db.execute("INSERT INTO rkb_expert_request_events VALUES(?,?,?,?,?)",(request_id,seq,kind,canonical(payload),self._now()))

    def provision_service(self,owner_context,service_id,kind,token,*,expected_epoch=0,enabled=True,personal_resource=None):
        """Owner bootstrap only; no public service-identity/secret creation tool."""
        if kind not in {"executor","delivery","notifier"} or not re.fullmatch(r"[a-z][a-z0-9_-]{4,79}",service_id) or len(token)<32:
            fail("invalid_service")
        if personal_resource is not None and (kind!='executor' or not isinstance(personal_resource,str) or not 1<=len(personal_resource)<=2048):fail('invalid_service')
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            actor=self.people._authority(db,owner_context,admin=True)
            old=db.execute("SELECT * FROM rkb_expert_services WHERE id=?",(service_id,)).fetchone()
            if (old["epoch"] if old else 0)!=expected_epoch:
                if old and old["epoch"]==expected_epoch+1 and old["kind"]==kind and old["enabled"]==enabled and old["token_hash"]==token_hash(token) and old["app_id"]==self.app_id and old["team_id"]==self.team_id and old['personal_resource']==personal_resource:
                    return {"service_id":service_id,"epoch":old["epoch"],"enabled":enabled,"kind":kind}
                fail("revision_conflict")
            db.execute("INSERT INTO rkb_expert_services(id,kind,token_hash,enabled,epoch,app_id,team_id,personal_resource) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,token_hash=excluded.token_hash,enabled=excluded.enabled,epoch=excluded.epoch,app_id=excluded.app_id,team_id=excluded.team_id,personal_resource=excluded.personal_resource",(service_id,kind,token_hash(token),enabled,expected_epoch+1,self.app_id,self.team_id,personal_resource))
            db.execute("INSERT INTO rkb_expert_service_events VALUES(?,?,?,?,?)",(service_id,expected_epoch+1,actor,canonical({"kind":kind,"enabled":enabled,"app_id":self.app_id,"team_id":self.team_id,"personal_resource":personal_resource}),self._now()))
            return {"service_id":service_id,"epoch":expected_epoch+1,"enabled":enabled,"kind":kind}

    def _service(self,db,credentials,kind):
        if not isinstance(credentials,ServiceCredentials): fail("service_denied")
        row=db.execute("SELECT * FROM rkb_expert_services WHERE id=?",(credentials.service_id,)).fetchone()
        if not row or not row["enabled"] or row["kind"]!=kind or row["app_id"]!=self.app_id or row["team_id"]!=self.team_id or not hmac.compare_digest(row["token_hash"],token_hash(credentials.token)):
            fail("service_denied")
        return row

    def _identity(self,db,row):
        origin=json.loads(row["origin"])
        if origin["app_id"]!=self.app_id or origin["team_id"]!=self.team_id: fail("request_unavailable")
        if row['client_kind']=='mcp':
            from .request_grants import allowed
            if not allowed(db,origin['grant_id'],row['person_id'],origin['resource'],self._now(),origin['client_id']):fail('identity_changed')
            context=RequestAuthority(Principal(subject=row['person_id'],client_id=origin['client_id'],issuer=origin['resource'],access_token=''),'personal')
            self.people.require(context,'public_search',connection=db)
            return context
        context=self.people.resolve_external(provider="slack",tenant=origin["team_id"],provider_subject=origin["user_id"],principal_factory=principal,connection=db)
        if context.principal.subject!=row["person_id"] or context.link_id!=row["link_id"] or context.link_revision!=row["link_revision"]:
            fail("identity_changed")
        self.people.require(context,"public_search",connection=db)
        return context

    def accept(self,observation:SlackObservation):
        if not isinstance(observation,SlackObservation): fail("unverified_transport")
        o=observation
        if (o.app_id!=self.app_id or o.team_id!=self.team_id or o.user_id==self.bot_user_id
            or not re.fullmatch(r"[UW][A-Z0-9]+",o.user_id)
            or o.channel_type not in {"im","channel","group","mpim"}
            or (o.channel_type=="im" and not o.channel_id.startswith("D"))
            or (o.channel_type!="im" and o.channel_id not in self.allowed_channels)
            or not 1<=len(o.text)<=12000 or not o.event_id
            or not re.fullmatch(r"[0-9]+\.[0-9]+",o.message_ts)
            or not re.fullmatch(r"[0-9]+\.[0-9]+",o.thread_ts)):
            fail("route_denied")
        origin={k:getattr(o,k) for k in ("app_id","team_id","user_id","channel_id","channel_type","thread_ts","message_ts","event_id")}
        digest=hashlib.sha256(canonical([origin,o.text]).encode()).hexdigest()
        message_key=hashlib.sha256(canonical([{k:v for k,v in origin.items() if k!="event_id"},o.text]).encode()).hexdigest()
        with self.corpus.connect() as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA busy_timeout=1500")
            db.execute("BEGIN IMMEDIATE")
            context=self.people.resolve_external(provider="slack",tenant=o.team_id,provider_subject=o.user_id,principal_factory=principal,connection=db)
            self.people.require(context,"public_search",connection=db)
            old=db.execute("SELECT * FROM rkb_expert_requests WHERE event_id=?",(o.event_id,)).fetchone()
            if old and old["event_hash"]!=digest: fail("event_identity_conflict")
            if old is None:
                old=db.execute("SELECT * FROM rkb_expert_requests WHERE message_key=?",(message_key,)).fetchone()
            if old:
                self._identity(db,old)
                return {"request_id":old["id"],"state":old["state"],"duplicate":True}
            ident=str(uuid4()); timestamp=self._now()
            db.execute("""INSERT INTO rkb_expert_requests(id,event_id,event_hash,message_key,person_id,link_id,link_revision,
                origin,input_text,output_scope,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,'pending',?,?)""",
                (ident,o.event_id,digest,message_key,context.principal.subject,context.link_id,context.link_revision,canonical(origin),o.text,"personal" if o.channel_type=="im" else "public",timestamp,timestamp))
            self._event(db,ident,"accepted",{"event_id":o.event_id,"output_scope":"personal" if o.channel_type=="im" else "public"})
            request_order.assign_slack(db,ident,context.principal.subject,origin)
            return {"request_id":ident,"state":"pending","duplicate":False}

    def personal_submit(self,context,grant_id,resource,*,dialog_key,client_request_key,text):
        """Trusted same-resource OAuth adapter supplies the verified grant proof."""
        if (context.route not in {'personal','owner'} or not isinstance(text,str) or not 1<=len(text)<=12000
            or any(not isinstance(v,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{7,127}',v) for v in (dialog_key,client_request_key))):fail('invalid_arguments')
        from .request_grants import allowed
        with self.corpus.connect() as db:
            db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            actor=self.people.require(context,'public_search',connection=db)
            client=context.principal.client_id
            if not allowed(db,grant_id,actor,resource,self._now(),client):fail('identity_changed')
            tenant=hashlib.sha256(canonical(['mcp',resource,client]).encode()).hexdigest()
            key=hashlib.sha256(canonical([tenant,actor,dialog_key,client_request_key]).encode()).hexdigest()
            digest=hashlib.sha256(text.encode()).hexdigest()
            old=db.execute('SELECT * FROM rkb_expert_requests WHERE message_key=?',(key,)).fetchone()
            if old:
                if old['event_hash']!=digest:fail('event_identity_conflict')
                self._identity(db,old)
                return {'request_id':old['id'],'state':old['state'],'duplicate':True,'dialog_id':old['dialog_id'],'dialog_seq':old['dialog_seq']}
            ident=str(uuid4());now=self._now()
            origin={'app_id':self.app_id,'team_id':self.team_id,'client_id':client,'resource':resource,'grant_id':grant_id}
            db.execute('''INSERT INTO rkb_expert_requests(id,event_id,event_hash,message_key,person_id,link_id,link_revision,
                origin,input_text,output_scope,state,created_at,updated_at) VALUES(?,?,?,?,?,'',0,?,?,'personal','pending',?,?)''',
                (ident,'mcp:'+key,digest,key,actor,canonical(origin),text,now,now))
            dialog,seq=request_order.assign(db,ident,actor,client_kind='mcp',tenant_id=tenant,conversation_key=dialog_key,
                client_request_key=client_request_key,owner_person_id=actor)
            self._event(db,ident,'accepted',{'client_kind':'mcp','dialog_seq':seq})
            return {'request_id':ident,'state':'pending','duplicate':False,'dialog_id':dialog,'dialog_seq':seq}

    def personal_result(self,context,grant_id,resource,request_id,*,ack_hash=None):
        """Authenticated pull/ACK is this client's delivery receipt, never Slack."""
        from .request_grants import allowed
        with self.corpus.connect() as db:
            db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            actor=self.people.require(context,'public_search',connection=db)
            if context.route not in {'personal','owner'} or not allowed(db,grant_id,actor,resource,self._now(),context.principal.client_id):fail('identity_changed')
            row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(str(request_id),)).fetchone()
            if not row or row['person_id']!=actor or row['client_kind']!='mcp':fail('request_unavailable')
            origin=json.loads(row['origin'])
            if origin['resource']!=resource or origin['client_id']!=context.principal.client_id:fail('request_unavailable')
            self._identity(db,row)
            result=ExpertResult.model_validate_json(row['result_json']) if row['result_json'] else None
            if result:self._resources(db,row,result)
            if ack_hash is not None:
                if not result or ack_hash!=row['result_hash'] or row['state'] not in {'result_ready','delivered'}:fail('provider_receipt_mismatch')
                if row['state']!='delivered':
                    if not request_order.is_head(db,row):fail('request_fence_denied')
                    receipt={'client_kind':'mcp','person_id':actor,'client_id':origin['client_id'],'resource':resource,'result_hash':ack_hash}
                    db.execute("UPDATE rkb_expert_requests SET state='delivered',delivery_receipt=?,updated_at=? WHERE id=?",(canonical(receipt),self._now(),row['id']))
                    request_order.advance(db,row['dialog_id']);self._event(db,row['id'],'delivery_verified',receipt)
                return {'request_id':row['id'],'state':'delivered','result_hash':row['result_hash']}
            return {'request_id':row['id'],'state':row['state'],'dialog_id':row['dialog_id'],'dialog_seq':row['dialog_seq'],
                'result_hash':row['result_hash'],'result':result.model_dump(mode='json') if result else None}

    def get(self,context,request_id):
        with self.corpus.connect() as db:
            db.execute("BEGIN")
            actor=self.people.require(context,"public_search",connection=db)
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(str(request_id),)).fetchone()
            if not row or row["person_id"]!=actor: fail("request_unavailable")
            self._identity(db,row)
            if context.request_id and context.request_id!=row["id"]: fail("request_unavailable")
            if row["result_json"]:
                self._resources(db,row,ExpertResult.model_validate_json(row["result_json"]))
            return {"request_id":row["id"],"event_id":row["event_id"],"state":row["state"],"generation":row["generation"],
                    "dialog_id":row["dialog_id"],"dialog_seq":row["dialog_seq"],"context_seq":row["context_seq"],
                    "wake_event_id":row["trigger_event_id"] or wake_event_id(row["id"],row["generation"]),
                    "output_scope":row["output_scope"],"input_text":row["input_text"],"origin":json.loads(row["origin"]),
                    "result":json.loads(row["result_json"]) if row["result_json"] else None,
                    **({'checkpoint':self.dependencies.saved(db,row)} if self.dependencies else {}),
                    "delivery_receipt":json.loads(row["delivery_receipt"]) if row["delivery_receipt"] else None}

    def claim(self,credentials,*,request_id=None,lease_seconds=120,event_id=None):
        if not 1<=lease_seconds<=300: fail("invalid_lease")
        if self.dependencies:
            with self.corpus.connect() as db:self._service(db,credentials,'executor')
            self.dependencies.refresh()
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            service=self._service(db,credentials,"executor")
            row=request_order.fair_head(db,self._now(),
                "json_extract(r.origin,'$.app_id')=? AND json_extract(r.origin,'$.team_id')=? AND (r.client_kind='slack' OR (r.client_kind='mcp' AND json_extract(r.origin,'$.resource')=?))",(self.app_id,self.team_id,service['personal_resource']))
            if not row: return {"state":"empty"}
            # A wake or explicit ID is only a hint. Never let it select a later
            # dialog turn or jump another person's turn, nor disclose their ID.
            if request_id and row['id']!=str(request_id):return {"state":"deferred"}
            expected_event=wake_event_id(row["id"],row["generation"])
            if event_id is not None and str(event_id)!=expected_event:fail("event_identity_conflict")
            try:
                self._identity(db,row)
                if self.dependencies:self.dependencies.saved(db,row)
            except (ParticipantError,ExpertRequestError):
                db.execute("UPDATE rkb_expert_requests SET state='blocked',updated_at=? WHERE id=?",(self._now(),row["id"]))
                self._event(db,row["id"],"access_revoked",{})
                return {"state":"blocked","request_id":row["id"]}
            generation=row["generation"]+1;token=secrets.token_urlsafe(32)
            db.execute("""UPDATE rkb_expert_requests SET state='leased',generation=?,executor_id=?,service_epoch=?,
                execution_token_hash=?,lease_until=?,updated_at=?,trigger_event_id=?,context_seq=dialog_seq-1 WHERE id=?""",(generation,service["id"],service["epoch"],token_hash(token),self._now()+lease_seconds,self._now(),expected_event,row["id"]))
            request_order.claimed(db,row)
            self._event(db,row["id"],"execution_claimed",{"generation":generation,"executor_id":service["id"]})
            return {"request_id":row["id"],"generation":generation,"execution_token":token,"state":"leased"}

    def context(self,credentials,request_id,generation,execution_token,*,connection=None,allow_result_replay=False):
        if connection is None:
            with self.corpus.connect() as db:
                db.execute("BEGIN")
                return self.context(credentials,request_id,generation,execution_token,connection=db,allow_result_replay=allow_result_replay)
        db=connection
        service=self._service(db,credentials,"executor")
        row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(str(request_id),)).fetchone()
        if (not row or row["generation"]!=generation or row["executor_id"]!=service["id"] or row["service_epoch"]!=service["epoch"]
            or not hmac.compare_digest(row["execution_token_hash"] or "",token_hash(execution_token))): fail("request_fence_denied")
        base=self._identity(db,row)
        if allow_result_replay and row["result_json"] and row["state"] in {"result_ready","sending","unknown","retry_wait","delivered"}:
            return base
        context=replace(base,route='delegated' if row['client_kind']=='mcp' else base.route,request_id=row["id"],request_generation=generation,executor_id=service["id"],execution_token=execution_token,public_only=row["output_scope"]=="public")
        self.people.require(context,"public_search",connection=db)
        return context

    def history(self,credentials,request_id,generation,execution_token,*,after_seq=0,limit=10):
        """Read retained prior turns at execution time, under the current ACL.

        Dependency reads are recorded in the same transaction. A caller cannot
        launder prior private evidence by omitting it from its final references.
        """
        if type(after_seq) is not int or after_seq<0 or type(limit) is not int or not 1<=limit<=20:fail('invalid_cursor')
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            context=self.context(credentials,request_id,generation,execution_token,connection=db)
            row=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(str(request_id),)).fetchone()
            rows=db.execute('''SELECT * FROM rkb_expert_requests WHERE dialog_id=? AND dialog_seq>? AND dialog_seq<=?
                ORDER BY dialog_seq LIMIT ?''',(row['dialog_id'],after_seq,row['context_seq'],limit+1)).fetchall()
            items=[]
            for prior in rows[:limit]:
                item={'dialog_seq':prior['dialog_seq'],'request_id':prior['id']}
                try:
                    if prior['state']!='delivered' or not prior['result_json']:fail('context_unavailable')
                    result=ExpertResult.model_validate_json(prior['result_json'])
                    self._resources(db,prior,result,context=context)
                except (ParticipantError,ExpertRequestError,JournalError):
                    item['state']='withheld'
                else:
                    item.update(state='available',input_text=prior['input_text'],result=result.model_dump(mode='json'))
                    known=db.execute('SELECT DISTINCT kind,resource_id FROM rkb_expert_request_resources WHERE request_id=?',(prior['id'],)).fetchall()
                    track_resources(db,context,
                        document_ids={str(v) for v in result.document_ids}|{r['resource_id'] for r in known if r['kind']=='document'},
                        case_ids={str(v) for v in result.case_ids}|{r['resource_id'] for r in known if r['kind']=='case'})
                items.append(item)
            return {'request_id':row['id'],'dialog_id':row['dialog_id'],'context_seq':row['context_seq'],'items':items,
                'next_after_seq':items[-1]['dialog_seq'] if len(rows)>limit else None}

    def _resources(self,db,row,result,*,context=None):
        base=context or self._identity(db,row)
        known=db.execute("SELECT DISTINCT kind,resource_id FROM rkb_expert_request_resources WHERE request_id=?",(row["id"],)).fetchall()
        documents={str(v) for v in result.document_ids}|{r["resource_id"] for r in known if r["kind"]=="document"}
        cases={str(v) for v in result.case_ids}|{r["resource_id"] for r in known if r["kind"]=="case"}
        for ident in documents:
            doc=self.people.sources._document_allowed(db,base.principal.subject,str(ident))
            if not doc: fail("resource_access_revoked")
            from .expert_source_scope import public_document
            if (row["output_scope"]=="public" or base.public_only) and not public_document(doc):
                fail("public_projection_required")
        for case_id in cases:
            if row["output_scope"]=="public" or base.public_only: fail("public_projection_required")
            try:self.journal._case(db,base.principal.subject,str(case_id))
            except JournalError: fail("resource_access_revoked")

    def record_result(self,credentials,request_id,generation,execution_token,result:ExpertResult):
        payload=result.model_dump(mode="json"); digest=hashlib.sha256(canonical(payload).encode()).hexdigest()
        with self.corpus.connect() as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            self.context(credentials,request_id,generation,execution_token,connection=db,allow_result_replay=True)
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(str(request_id),)).fetchone()
            self._resources(db,row,result)
            if row["result_json"]:
                if row["result_hash"]!=digest: fail("result_conflict")
                return {"request_id":row["id"],"generation":generation,"result_hash":digest,"state":row["state"]}
            db.execute("UPDATE rkb_expert_requests SET state='result_ready',result_hash=?,result_json=?,updated_at=? WHERE id=?",(digest,canonical(payload),self._now(),row["id"]))
            self._event(db,row["id"],"result_recorded",{"generation":generation,"result_hash":digest})
            return {"request_id":row["id"],"generation":generation,"result_hash":digest,"state":"result_ready"}

    def claim_delivery(self,credentials,request_id):
        with self.corpus.connect() as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            service=self._service(db,credentials,"delivery")
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(str(request_id),)).fetchone()
            if not row or not row["result_json"]: fail("request_unavailable")
            if row['client_kind']!='slack':fail('request_unavailable')
            result=ExpertResult.model_validate_json(row["result_json"])
            self._resources(db,row,result)
            if row["state"] in {"sending","unknown"}:
                return {"request_id":row["id"],"state":"reconciliation_required","delivery_id":row["delivery_id"],"send_allowed":False}
            if row["state"]=="delivered": return {"request_id":row["id"],"state":"delivered","send_allowed":False}
            if not request_order.is_head(db,row):return {"request_id":row["id"],"state":"deferred","send_allowed":False}
            if row["state"] not in {"result_ready","retry_wait"}: fail("request_unavailable")
            if row["state"]=="retry_wait" and row["next_delivery_at"]>self._now():
                return {"request_id":row["id"],"state":"retry_wait","send_allowed":False,"retry_at":row["next_delivery_at"]}
            delivery_id=str(uuid5(NAMESPACE_URL,"rkb:expert-result:"+row["id"]+":"+row["result_hash"]))
            db.execute("UPDATE rkb_expert_requests SET state='sending',delivery_id=?,delivery_service_id=?,delivery_service_epoch=?,updated_at=?,delivery_attempt=delivery_attempt+1 WHERE id=?",(delivery_id,service["id"],service["epoch"],self._now(),row["id"]))
            self._event(db,row["id"],"delivery_claimed",{"delivery_id":delivery_id,"result_hash":row["result_hash"]})
            return {"request_id":row["id"],"state":"sending","delivery_id":delivery_id,"send_allowed":True,"origin":json.loads(row["origin"]),"text":result.text}

    def next_delivery(self,credentials):
        with self.corpus.connect() as db:
            self._service(db,credentials,"delivery")
            row=db.execute("SELECT r.id FROM rkb_expert_requests r JOIN rkb_request_dialogs d ON d.id=r.dialog_id AND d.head_seq=r.dialog_seq WHERE r.client_kind='slack' AND (state='result_ready' OR (state='retry_wait' AND next_delivery_at<=?)) AND json_extract(origin,'$.app_id')=? AND json_extract(origin,'$.team_id')=? ORDER BY updated_at,r.id LIMIT 1",(self._now(),self.app_id,self.team_id)).fetchone()
            return row[0] if row else None

    def block_delivery(self,credentials,request_id,reason):
        """Advance past a verified pre-send access failure without losing history."""
        if reason not in {"capability_denied","identity_unverified","identity_changed","resource_access_revoked","not_found_or_not_accessible","public_projection_required","request_fence_denied"}: fail("invalid_block_reason")
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._service(db,credentials,"delivery")
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(str(request_id),)).fetchone()
            if not row: fail("request_unavailable")
            origin=json.loads(row["origin"])
            if origin["app_id"]!=self.app_id or origin["team_id"]!=self.team_id: fail("request_unavailable")
            if row["state"] in {"result_ready","retry_wait","sending"}:
                db.execute("UPDATE rkb_expert_requests SET state='blocked',updated_at=? WHERE id=?",(self._now(),row["id"]))
                self._event(db,row["id"],"delivery_access_blocked",{"reason_code":reason})
            return {"request_id":row["id"],"state":"blocked" if row["state"] in {"result_ready","retry_wait","sending"} else row["state"]}

    def rate_limited_delivery(self,credentials,request_id,delivery_id,retry_after):
        """Trusted adapter calls only for native confirmed 429/no-effect reply."""
        if type(retry_after) not in {int,float} or not math.isfinite(retry_after) or retry_after<1: fail("invalid_retry_after")
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            service=self._service(db,credentials,"delivery")
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(str(request_id),)).fetchone()
            if not row or row["delivery_service_id"]!=service["id"] or row["delivery_service_epoch"]!=service["epoch"] or row["delivery_id"]!=delivery_id or row["state"]!="sending": fail("request_fence_denied")
            if not request_order.is_head(db,row):fail("request_fence_denied")
            retry_at=self._now()+retry_after
            db.execute("UPDATE rkb_expert_requests SET state='retry_wait',next_delivery_at=?,updated_at=? WHERE id=?",(retry_at,self._now(),row["id"]))
            self._event(db,row["id"],"delivery_rate_limited",{"delivery_id":delivery_id,"retry_at":retry_at})
            return {"request_id":row["id"],"state":"retry_wait","retry_at":retry_at}

    def unknown_delivery(self,credentials,request_id,delivery_id):
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            service=self._service(db,credentials,"delivery")
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(str(request_id),)).fetchone()
            if not row or row["delivery_service_id"]!=service["id"] or row["delivery_service_epoch"]!=service["epoch"] or row["delivery_id"]!=delivery_id: fail("request_fence_denied")
            if row["state"]=="sending":
                db.execute("UPDATE rkb_expert_requests SET state='unknown',updated_at=? WHERE id=?",(self._now(),row["id"]))
                self._event(db,row["id"],"delivery_unknown",{"delivery_id":delivery_id})
            return {"request_id":row["id"],"state":"unknown" if row["state"]=="sending" else row["state"]}

    def check_delivery(self,credentials,request_id,delivery_id):
        """Worker repeats immediately before native I/O; no second send claim."""
        with self.corpus.connect() as db:
            db.execute("BEGIN")
            service=self._service(db,credentials,"delivery")
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(str(request_id),)).fetchone()
            if not row or row["delivery_service_id"]!=service["id"] or row["delivery_service_epoch"]!=service["epoch"] or row["delivery_id"]!=delivery_id or row["state"]!="sending": fail("request_fence_denied")
            if not request_order.is_head(db,row):fail('request_fence_denied')
            result=ExpertResult.model_validate_json(row["result_json"])
            self._resources(db,row,result)
            return {"request_id":row["id"],"delivery_id":row["delivery_id"],"origin":json.loads(row["origin"]),"text":result.text}

    def receipt(self,credentials,request_id,receipt:SlackDeliveryReceipt):
        if not isinstance(receipt,SlackDeliveryReceipt): fail("unverified_transport")
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            service=self._service(db,credentials,"delivery")
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(str(request_id),)).fetchone()
            if not row or row["delivery_service_id"]!=service["id"] or row["delivery_service_epoch"]!=service["epoch"]: fail("request_fence_denied")
            origin=json.loads(row["origin"])
            if (receipt.app_id!=self.app_id or receipt.team_id!=origin["team_id"] or receipt.bot_user_id!=self.bot_user_id
                or receipt.channel_id!=origin["channel_id"] or receipt.thread_ts!=origin["thread_ts"]
                or receipt.client_message_id!=row["delivery_id"] or not row["result_json"]
                or receipt.text!=json.loads(row["result_json"])["text"] or not re.fullmatch(r"[0-9]+\.[0-9]+",receipt.message_ts)):
                fail("provider_receipt_mismatch")
            value={"app_id":receipt.app_id,"bot_user_id":receipt.bot_user_id,"team_id":receipt.team_id,"channel_id":receipt.channel_id,
                   "thread_ts":receipt.thread_ts,"message_ts":receipt.message_ts,"client_message_id":receipt.client_message_id,"result_hash":row["result_hash"]}
            if row["delivery_receipt"]:
                if json.loads(row["delivery_receipt"])!=value: fail("provider_receipt_conflict")
                return value
            if row["state"] not in {"sending","unknown","retry_wait"}: fail("request_unavailable")
            # A real receipt after revocation still records what actually happened;
            # it never creates another delivery or makes revoked content readable.
            db.execute("UPDATE rkb_expert_requests SET state='delivered',delivery_receipt=?,updated_at=? WHERE id=?",(canonical(value),self._now(),row["id"]))
            request_order.advance(db,row['dialog_id'])
            self._event(db,row["id"],"delivery_verified",value)
            return value

    def receipt_from_echo(self,credentials,receipt:SlackDeliveryReceipt):
        """Only an authenticated provider adapter can submit an observed echo."""
        if not isinstance(receipt,SlackDeliveryReceipt): fail("unverified_transport")
        with self.corpus.connect() as db:
            self._service(db,credentials,"delivery")
            row=db.execute("SELECT id FROM rkb_expert_requests WHERE delivery_id=? AND json_extract(origin,'$.app_id')=? AND json_extract(origin,'$.team_id')=?",(receipt.client_message_id,self.app_id,self.team_id)).fetchone()
        return self.receipt(credentials,row[0],receipt) if row else None

    def next_reconciliation(self,credentials):
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._service(db,credentials,"delivery")
            row=db.execute("""SELECT * FROM rkb_expert_requests WHERE state IN ('sending','unknown') AND reconcile_not_before<=?
                AND json_extract(origin,'$.app_id')=? AND json_extract(origin,'$.team_id')=?
                ORDER BY reconcile_not_before,updated_at,id LIMIT 1""",(self._now(),self.app_id,self.team_id)).fetchone()
            if not row: return None
            # Reserve a bounded readback interval before network I/O; concurrent
            # workers and restarts cannot hot-poll the same uncertain send.
            db.execute("UPDATE rkb_expert_requests SET reconcile_not_before=? WHERE id=?",(self._now()+60,row["id"]))
            return {"request_id":row["id"],"origin":json.loads(row["origin"]),"delivery_id":row["delivery_id"],"cursor":row["reconcile_cursor"]}

    def reconciliation_progress(self,credentials,request_id,*,cursor=None,retry_after=60):
        if (cursor is not None and (not isinstance(cursor,str) or len(cursor)>2048)) or type(retry_after) not in {int,float} or not math.isfinite(retry_after) or retry_after<1:
            fail("invalid_reconciliation_cursor")
        with self.corpus.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            self._service(db,credentials,"delivery")
            row=db.execute("SELECT * FROM rkb_expert_requests WHERE id=?",(str(request_id),)).fetchone()
            if not row: fail("request_unavailable")
            origin=json.loads(row["origin"])
            if origin["app_id"]!=self.app_id or origin["team_id"]!=self.team_id: fail("request_unavailable")
            if row["state"] in {"sending","unknown"}:
                db.execute("UPDATE rkb_expert_requests SET reconcile_cursor=?,reconcile_not_before=? WHERE id=?",(cursor,self._now()+retry_after,row["id"]))
            return {"request_id":row["id"],"state":row["state"]}
