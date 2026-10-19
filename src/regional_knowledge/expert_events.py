"""MCP Events subscriptions and durable request wake-ups in corpus SQLite."""
import asyncio
from datetime import datetime,timezone
import hashlib
import json
import logging
import random
import secrets
from uuid import uuid4

from cryptography.fernet import Fernet

from .expert_requests import ExpertRequestError
from .expert_webhooks import WebhookError,WebhookSender,callback_parts,secret_bytes
from .participants import ParticipantError
from .request_authority import wake_event_id

EVENT="regional_knowledge.expert_request.created"
log=logging.getLogger(__name__)


def iso(value):return datetime.fromtimestamp(value,timezone.utc).isoformat().replace('+00:00','Z')


class ExpertEvents:
    def __init__(self,requests,encryption_key,*,sender=None,local_callbacks=()):
        self.requests=requests;self.corpus=requests.corpus
        self.cipher=Fernet(encryption_key)
        self.local_callbacks=frozenset(local_callbacks)
        self.sender=sender or WebhookSender(local_callbacks=local_callbacks)
        with self.corpus.connect() as db:
            db.executescript("""
            CREATE TABLE IF NOT EXISTS rkb_expert_subscriptions(
              id TEXT PRIMARY KEY, service_id TEXT NOT NULL, service_epoch INTEGER NOT NULL,
              url TEXT NOT NULL, secret_enc TEXT NOT NULL, old_secret_enc TEXT,
              rotate_until REAL, verified_at REAL NOT NULL, expires_at REAL NOT NULL,
              active INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS rkb_expert_event_outbox(
              request_id TEXT NOT NULL, subscription_id TEXT NOT NULL,
              generation INTEGER NOT NULL, event_id TEXT NOT NULL,
              state TEXT NOT NULL, next_at REAL NOT NULL, lease_until REAL,
              lease_token TEXT, attempts INTEGER NOT NULL DEFAULT 0,
              last_status INTEGER, reason_code TEXT,
              PRIMARY KEY(request_id,subscription_id,generation));
            CREATE INDEX IF NOT EXISTS rkb_expert_event_pending ON rkb_expert_event_outbox(state,next_at);
            """)

    def _identity(self,credentials,url):
        callback_parts(url,self.local_callbacks)
        return hashlib.sha256(json.dumps([self.requests.app_id,self.requests.team_id,credentials.service_id,EVENT,url],separators=(',',':')).encode()).hexdigest()

    def _decrypt(self,value):return self.cipher.decrypt(value.encode()).decode()

    async def subscribe(self,credentials,url,secret,*,ttl_ms=86400000,max_age_ms=None):
        secret_bytes(secret)
        if ttl_ms is not None and (type(ttl_ms) is not int or ttl_ms<1):raise ExpertRequestError('invalid_subscription_ttl')
        if max_age_ms is not None and (type(max_age_ms) is not int or max_age_ms<0):raise ExpertRequestError('invalid_verification_age')
        ident,cached=await asyncio.to_thread(self._prepare_subscription,credentials,url,secret,max_age_ms)
        if not cached:await self.sender.verify(url,ident,secret)
        expires=await asyncio.to_thread(self._save_subscription,credentials,ident,url,secret,ttl_ms,cached)
        log.info(json.dumps({'event':'expert_subscription_verified','subscription_id':ident,'service_id':credentials.service_id}))
        return {'id':ident,'refreshBefore':iso(expires),'cursor':None,'truncated':False}

    def _prepare_subscription(self,credentials,url,secret,max_age_ms):
        ident=self._identity(credentials,url);now=self.requests._now()
        with self.corpus.connect() as db:
            service=self.requests._service(db,credentials,'executor')
            old=db.execute('SELECT * FROM rkb_expert_subscriptions WHERE id=?',(ident,)).fetchone()
            cached=bool(old and old['active'] and old['service_epoch']==service['epoch'] and old['expires_at']>now
                and old['verified_at']>now-(300 if max_age_ms is None else min(300,max_age_ms/1000))
                and secrets.compare_digest(self._decrypt(old['secret_enc']),secret))
        return ident,cached

    def _save_subscription(self,credentials,ident,url,secret,ttl_ms,cached):
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            service=self.requests._service(db,credentials,'executor');now=self.requests._now()
            old=db.execute('SELECT * FROM rkb_expert_subscriptions WHERE id=?',(ident,)).fetchone()
            previous=None;until=None
            if old:
                if not secrets.compare_digest(self._decrypt(old['secret_enc']),secret):
                    previous=old['secret_enc'];until=now+300
                elif old['rotate_until'] and old['rotate_until']>now:
                    previous=old['old_secret_enc'];until=old['rotate_until']
            expires=now+min(ttl_ms if ttl_ms is not None else 86400000,86400000)/1000
            verified=old['verified_at'] if cached else now
            db.execute('''INSERT INTO rkb_expert_subscriptions VALUES(?,?,?,?,?,?,?,?,?,1)
                ON CONFLICT(id) DO UPDATE SET service_epoch=excluded.service_epoch,url=excluded.url,
                secret_enc=excluded.secret_enc,old_secret_enc=excluded.old_secret_enc,rotate_until=excluded.rotate_until,
                verified_at=excluded.verified_at,expires_at=excluded.expires_at,active=1''',
                (ident,credentials.service_id,service['epoch'],url,self.cipher.encrypt(secret.encode()).decode(),previous,until,verified,expires))
            if not cached:
                db.execute("UPDATE rkb_expert_event_outbox SET state='pending',next_at=?,lease_until=NULL,lease_token=NULL,reason_code=NULL WHERE subscription_id=? AND state IN ('blocked','cancelled') AND reason_code IN ('subscription_unavailable','http_4xx')",(now,ident))
        return expires

    def unsubscribe(self,credentials,url):
        ident=self._identity(credentials,url)
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE');self.requests._service(db,credentials,'executor')
            db.execute('UPDATE rkb_expert_subscriptions SET active=0 WHERE id=? AND service_id=?',(ident,credentials.service_id))
            db.execute("UPDATE rkb_expert_event_outbox SET state='cancelled',reason_code='subscription_unavailable' WHERE subscription_id=? AND state!='accepted'",(ident,))
        return {'id':ident,'unsubscribed':True}

    def _fill(self,db,now):
        rows=db.execute('''SELECT r.id,r.generation,s.id AS subscription_id FROM rkb_expert_requests r
            JOIN rkb_expert_subscriptions s ON s.active=1 AND s.expires_at>?
            JOIN rkb_expert_services g ON g.id=s.service_id AND g.enabled=1 AND g.epoch=s.service_epoch AND g.kind='executor'
              AND g.app_id=? AND g.team_id=?
            WHERE (r.state='pending' OR (r.state='leased' AND r.lease_until<=?))
              AND json_extract(r.origin,'$.app_id')=? AND json_extract(r.origin,'$.team_id')=?
              AND NOT EXISTS(SELECT 1 FROM rkb_expert_event_outbox o WHERE o.request_id=r.id
                AND o.subscription_id=s.id AND o.generation=r.generation)
            ORDER BY r.created_at,r.id,s.id LIMIT 100''',
            (now,self.requests.app_id,self.requests.team_id,now,self.requests.app_id,self.requests.team_id)).fetchall()
        for row in rows:
            db.execute("INSERT INTO rkb_expert_event_outbox(request_id,subscription_id,generation,event_id,state,next_at) VALUES(?,?,?,?,'pending',?)",(row['id'],row['subscription_id'],row['generation'],wake_event_id(row['id'],row['generation']),now))

    def _claim_delivery(self):
        now=self.requests._now()
        with self.corpus.connect() as db:
            db.execute('BEGIN IMMEDIATE');self._fill(db,now)
            row=db.execute('''SELECT o.*,s.service_id,s.service_epoch,s.url,s.secret_enc,s.old_secret_enc,
                s.rotate_until,s.active,s.expires_at,g.enabled,g.epoch,g.app_id,g.team_id,
                r.created_at,r.state AS request_state,r.generation AS request_generation,r.lease_until AS execution_until
                FROM rkb_expert_event_outbox o JOIN rkb_expert_subscriptions s ON s.id=o.subscription_id
                JOIN rkb_expert_services g ON g.id=s.service_id JOIN rkb_expert_requests r ON r.id=o.request_id
                WHERE ((o.state='pending' AND o.next_at<=?) OR (o.state='in_flight' AND o.lease_until<=?))
                  AND json_extract(r.origin,'$.app_id')=? AND json_extract(r.origin,'$.team_id')=?
                ORDER BY o.next_at,o.request_id LIMIT 1''',(now,now,self.requests.app_id,self.requests.team_id)).fetchone()
            if not row:return False
            valid=(row['active'] and row['expires_at']>now and row['enabled'] and row['epoch']==row['service_epoch']
                and row['app_id']==self.requests.app_id and row['team_id']==self.requests.team_id)
            reason='subscription_unavailable'
            if valid:
                request=db.execute('SELECT * FROM rkb_expert_requests WHERE id=?',(row['request_id'],)).fetchone()
                try:self.requests._identity(db,request)
                except (ExpertRequestError,ParticipantError):valid=False;reason='request_access_revoked'
            eligible=(row['request_generation']==row['generation'] and (row['request_state']=='pending' or
                row['request_state']=='leased' and row['execution_until']<=now))
            if not valid or not eligible:
                db.execute("UPDATE rkb_expert_event_outbox SET state='cancelled',reason_code=? WHERE request_id=? AND subscription_id=? AND generation=?",(reason if not valid else 'request_already_claimed',row['request_id'],row['subscription_id'],row['generation']))
                return True
            claim=str(uuid4())
            db.execute("UPDATE rkb_expert_event_outbox SET state='in_flight',lease_until=?,lease_token=?,attempts=attempts+1 WHERE request_id=? AND subscription_id=? AND generation=?",(now+30,claim,row['request_id'],row['subscription_id'],row['generation']))
        return dict(row),claim,now

    async def deliver_one(self):
        acquired=await asyncio.to_thread(self._claim_delivery)
        if isinstance(acquired,bool):return acquired
        row,claim,now=acquired
        event={'eventId':row['event_id'],'name':EVENT,'timestamp':iso(row['created_at']),'cursor':None,
               'data':{'request_id':row['request_id'],'expected_generation':row['generation']}}
        status=None;retry_after=None;reason=None
        try:
            old=self._decrypt(row['old_secret_enc']) if row['old_secret_enc'] and row['rotate_until']>now else None
            status,_,retry_after=await self.sender.send(row['url'],row['subscription_id'],row['event_id'],event,self._decrypt(row['secret_enc']),old)
            accepted=200<=status<300
            reason=None if accepted else 'http_4xx' if 400<=status<500 and status not in (408,425,429) else 'callback_deferred'
        except Exception:
            accepted=False;reason='callback_deferred'
        state='accepted' if accepted else 'blocked' if reason=='http_4xx' else 'pending'
        delay=min(300,2**min(row['attempts']+1,8))+random.uniform(0,1)
        if retry_after:
            try:delay=max(delay,int(retry_after))
            except (TypeError,ValueError):pass
        await asyncio.to_thread(self._finish_delivery,row,claim,state,delay,status,reason)
        log.info(json.dumps({'event':'expert_wake_delivery','request_id':row['request_id'],'subscription_id':row['subscription_id'],
            'event_id':row['event_id'],'state':state,'provider_status':status,'reason_code':reason,'attempt':row['attempts']+1}))
        return True

    def _finish_delivery(self,row,claim,state,delay,status,reason):
        with self.corpus.connect() as db:
            db.execute("UPDATE rkb_expert_event_outbox SET state=?,next_at=?,lease_until=NULL,last_status=?,reason_code=? WHERE request_id=? AND subscription_id=? AND generation=? AND lease_token=?",(state,self.requests._now()+delay,status,reason,row['request_id'],row['subscription_id'],row['generation'],claim))

    async def run(self):
        while True:
            try:
                if await self.deliver_one():continue
            except asyncio.CancelledError:raise
            except Exception as error:
                log.warning(json.dumps({'event':'expert_wake_worker_wait','error_type':type(error).__name__}))
            await asyncio.sleep(1)
